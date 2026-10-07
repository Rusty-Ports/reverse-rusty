//! `impl ClusterEngine` — the autoscaler driver: collect a [`LoadSnapshot`], run the pure
//! policy ([`evaluate`]), execute the executable subset (`rebalance`), and return the full
//! decision (incl. advisories). The policy itself lives in [`crate::cluster::autoscale`].

use crate::cluster::autoscale::{
    evaluate, AutoscaleConfig, AutoscaleDecision, LoadSnapshot, ResizeObservation, ScalingAction,
};
use crate::cluster::control::{NodeDescriptor, NodeId};
use crate::cluster::shard::ShardError;
#[cfg(feature = "distributed")]
use crate::events::{DurabilityOp, EngineEvent};

use super::ClusterEngine;
use crate::cluster::coordinator::layout::Layout;

impl ClusterEngine {
    /// Collect the deterministic policy input: membership + the shard→node map from the
    /// control plane ([`Self::control_state`]) and per-shard corpus from the shards
    /// ([`Self::shard_query_counts`] — the only load signal that crosses the
    /// [`Shard`](crate::cluster::ClusterEngine) seam, so this works in-process AND across
    /// nodes). Fail-closed: a control-plane or shard error propagates rather than yielding a
    /// partial/blind snapshot.
    pub fn collect_load(&self, config: &AutoscaleConfig) -> Result<LoadSnapshot, ShardError> {
        self.collect_load_in(&self.layout(), config)
    }

    pub(in crate::cluster::coordinator) fn collect_load_in(
        &self,
        layout: &Layout,
        config: &AutoscaleConfig,
    ) -> Result<LoadSnapshot, ShardError> {
        let state = self.control_state()?;
        let shard_corpus = layout.shard_query_counts()?;
        // A replicated row is on every shard, so that part of a shard's corpus is the same size
        // everywhere and does not shrink when shards are added: it must not drive split pressure.
        // That is the broad lane (classes C and D, ADR-080) and the replicated always-visible
        // rows: top-64 pairs, phrase proxies, and class-C plans a rebuild kept in default reads
        // (ADR-203). Each shard counts its own; the per-shard size is the total / num_shards.
        let mut replicated = 0u64;
        for shard in layout.shards.iter() {
            replicated += shard.replicated_rows()?;
        }
        let num_shards = u64::from(state.num_shards).max(1);
        let replicated_corpus = (replicated / num_shards) as usize;
        Ok(LoadSnapshot {
            nodes: state.nodes,
            assignments: state.assignments,
            shard_corpus,
            replicated_corpus,
            num_shards: state.num_shards,
            replication_factor: config.target_replication_factor,
        })
    }

    /// Collect one [`ResizeObservation`] for the [`ResizeGovernor`](crate::cluster::ResizeGovernor)
    /// (ADR-179): the serving shard count and placement generation, the advisory
    /// [`recommended_shard_count`](crate::cluster::recommended_shard_count), and the largest
    /// per-shard selective corpus (the load a split can relieve). Fail-closed like
    /// [`Self::collect_load`]. The layout fields come from the serving ring rather than control
    /// state, so the governor's precondition names exactly what a resize would replace.
    pub fn resize_observation(
        &self,
        config: &AutoscaleConfig,
    ) -> Result<ResizeObservation, ShardError> {
        let layout = &*self.layout();
        let snapshot = self.collect_load_in(layout, config)?;
        let max_selective_corpus = snapshot
            .shard_corpus
            .iter()
            .map(|&corpus| corpus.saturating_sub(snapshot.replicated_corpus))
            .max()
            .unwrap_or(0);
        Ok(ResizeObservation {
            num_shards: layout.num_shards(),
            placement_generation: layout.generation.0,
            recommended: crate::cluster::recommended_shard_count(&snapshot, config),
            max_selective_corpus,
        })
    }

    /// One autoscaler cycle: validate the config (fail-closed), collect the snapshot, run the
    /// policy, **execute the executable subset** (each [`Rebalance`](ScalingAction::Rebalance)
    /// reconciles placement, idempotently — a no-op when already balanced; on a remote cluster routed
    /// by the committed map it drives the DATA-MOVING [`rebalance_and_move`](Self::rebalance_and_move)
    /// so routing follows the new map without manufacturing the ADR-086 false negative, ADR-090/092),
    /// and return the full [`AutoscaleDecision`] including the advisories
    /// ([`Handoff`](ScalingAction::Handoff)/[`RecommendSplit`](ScalingAction::RecommendSplit)/…)
    /// for the caller to log or act on. A disabled config yields an empty decision ⇒ a no-op
    /// tick, so a default-config caller is byte-identical to no autoscaler at all.
    pub fn tick(&self, config: &AutoscaleConfig) -> Result<AutoscaleDecision, ShardError> {
        self.tick_in(&self.layout(), config)
    }

    pub(in crate::cluster::coordinator) fn tick_in(
        &self,
        layout: &Layout,
        config: &AutoscaleConfig,
    ) -> Result<AutoscaleDecision, ShardError> {
        let problems = config.validate();
        if !problems.is_empty() {
            return Err(ShardError::Config(format!(
                "invalid autoscale config: {}",
                problems.join("; ")
            )));
        }
        // Opportunistically converge any partial-apply divergence (ADR-047) each cycle — a cheap
        // no-op when nothing is queued (the default path). Repairing before snapshotting load
        // keeps the autoscaler's view consistent with the converged cluster.
        let _ = self.resync_in(layout);
        let snapshot = self.collect_load_in(layout, config)?;
        let decision = evaluate(&snapshot, config);
        // Execute the executable subset. A `Rebalance` reconciles placement (idempotent — a no-op
        // when already balanced).
        for action in &decision.actions {
            if let ScalingAction::Rebalance { rf } = action {
                #[cfg(feature = "distributed")]
                if let Some(handle) = self.handle.clone() {
                    // A REMOTE cluster routed by the committed map: a MAP-ONLY `rebalance` would
                    // re-point routing at nodes holding DIFFERENT data — the ADR-086 false negative
                    // (the boot guard refuses such a map). Drive the DATA-MOVING rebalance instead, so
                    // data follows the new map (ADR-090/092). Each move reserves its endpoint
                    // footprint in the busy-endpoint ledger (ADR-095 — so it never interleaves a
                    // CONFLICTING manual move or the reconcile loop) and the sweep is
                    // best-effort — a partial/failed sweep is surfaced as an event and retried by the
                    // next tick or the reconcile loop, never failing the enclosing `tick` (mirroring
                    // `drive_autoscaled_handoff`). The `handle.is_some()` gate keeps the in-process /
                    // lean path byte-identical: only a gRPC-built cluster carries a runtime handle.
                    match self.rebalance_and_move_in(layout, *rf, &handle) {
                        Ok(report) => {
                            if let Some((pos, reason)) = report.failed {
                                self.emit(EngineEvent::DurabilityFailure {
                                    op: DurabilityOp::ReplicaDesync,
                                    detail: format!(
                                        "autoscaler data-moving rebalance stopped at shard {pos}; \
                                         already-moved positions are consistent, the rest are retried \
                                         next tick / by the reconcile loop"
                                    ),
                                    error: reason,
                                });
                            }
                        }
                        Err(e) => self.emit(EngineEvent::DurabilityFailure {
                            op: DurabilityOp::ReplicaDesync,
                            detail: "autoscaler data-moving rebalance failed pre-flight; \
                                     retried next tick"
                                .into(),
                            error: e.to_string(),
                        }),
                    }
                    continue;
                }
                // In-process (or lean build): map-only rebalance is correct — the advisory map; the
                // local shards do not move. Unchanged, byte-identical.
                self.rebalance(*rf)?;
            }
        }
        // An advisory `Handoff` (the policy, ADR-045) is now DRIVEN through `execute_handoff`
        // (ADR-048) — but only when NO rebalance ran this tick, because a rebalance moves placement
        // and would make the handoff's `from`/`to` (from the pre-rebalance snapshot) stale; the
        // skipped handoff is re-evaluated next tick. `RecommendSplit`/`RecommendScaleOut` stay
        // advisory (returned in the decision only). Gated: a `Handoff` can't arise in-process (skew
        // needs ≥2 loaded nodes) and `execute_handoff` is `distributed`-only, so the lean build
        // returns the recommendation without acting — byte-identical to before.
        #[cfg(feature = "distributed")]
        {
            let rebalanced = decision
                .actions
                .iter()
                .any(|a| matches!(a, ScalingAction::Rebalance { .. }));
            if !rebalanced {
                for action in &decision.actions {
                    if let ScalingAction::Handoff { position, from, to } = action {
                        self.drive_autoscaled_handoff(layout, &snapshot, *position, *from, *to);
                    }
                }
            }
        }
        Ok(decision)
    }

    /// Drive an autoscaler-recommended [`Handoff`](ScalingAction::Handoff) through
    /// [`reassign_and_move`](Self::reassign_and_move) (ADR-090, evolving ADR-048's
    /// `execute_handoff`-only wiring): the move now ALSO commits the new owner into the cluster-state
    /// document, so an autoscaler-driven move keeps the committed map consistent with the live routing
    /// (and reserves its endpoint footprint in the busy-endpoint ledger, ADR-095). Best-effort and
    /// side-effecting only: it never
    /// fails the enclosing `tick`. Skips silently when the cluster has no runtime handle (an
    /// in-process cluster can't hand off to a remote node) or when the recommendation is stale (a
    /// concurrent change moved `position` off `from`). A move that can't be performed (e.g. a node
    /// without a registered endpoint) or whose durable transition fails surfaces as an event so the
    /// operator can see why. A proven clean failure auto-unfences; an ambiguous failure retains its
    /// intent for retry or cold-start resolution, so the next tick can retry without guessing authority.
    #[cfg(feature = "distributed")]
    pub(in crate::cluster::coordinator) fn drive_autoscaled_handoff(
        &self,
        layout: &Layout,
        snapshot: &LoadSnapshot,
        position: u32,
        from: NodeId,
        to: NodeId,
    ) {
        // A degenerate self-handoff (`from == to`) is nothing to move; skip it silently (the
        // endpoint-level no-op for two distinct ids on one endpoint lives in `reassign_and_move`).
        if from.0 == to.0 {
            return;
        }
        // Only a gRPC-built cluster carries a runtime handle (and remote endpoints to move between).
        // An in-process cluster has neither, so there is nothing to do.
        let Some(handle) = self.handle.clone() else {
            return;
        };
        // Re-validate against the snapshot: skip a stale recommendation whose source no longer owns
        // the position rather than driving a move off the wrong owner.
        let owns_position = snapshot
            .assignments
            .iter()
            .any(|a| a.position == position && a.primary.0 == from.0);
        if !owns_position {
            return;
        }
        // Drive the durable move through `reassign_and_move` (ADR-175): it resolves endpoints from
        // membership, records the transition, proves recovery under a source fence, conditionally
        // commits, and only then swaps live routing. A missing endpoint or failed proof surfaces as
        // an Err we report as a skip; a preserved intent makes retry/startup resolution deterministic.
        if let Err(e) = self.reassign_and_move_in(layout, position as usize, to, &handle) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "autoscaler-driven handoff of shard {position} from node {} to node {} could not be \
                     performed (e.g. a node has no registered endpoint); skipping (the decision still \
                     reports it, retried next tick)",
                    from.0, to.0
                ),
                error: e.to_string(),
            });
        }
    }

    /// Event-driven entry: a node joined — register it, then run a [`Self::tick`]. The tick's
    /// membership-drift rule turns the new node into a rebalance that folds it into the map.
    pub fn on_node_joined(
        &self,
        node: NodeDescriptor,
        config: &AutoscaleConfig,
    ) -> Result<AutoscaleDecision, ShardError> {
        let layout = &*self.layout();
        self.register_node(node)?;
        self.tick_in(layout, config)
    }

    /// Event-driven entry: a node left — deregister it, then run a [`Self::tick`] (which
    /// rebalances its positions onto the survivors).
    pub fn on_node_left(
        &self,
        id: NodeId,
        config: &AutoscaleConfig,
    ) -> Result<AutoscaleDecision, ShardError> {
        let layout = &*self.layout();
        self.deregister_node(id)?;
        self.tick_in(layout, config)
    }
}
