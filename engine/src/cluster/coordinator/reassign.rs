//! `impl ClusterEngine` — data-moving live reassignment (ADR-090, `distributed` feature): tie a
//! committed shard→node assignment change to a physical data move, so a reassignment moves the bytes
//! AND routing follows — live and across a coordinator restart.
//!
//! Design: docs/design/clustering-and-scaling.md §9. Builds on ADR-086 (route by the committed map +
//! the boot guard) and ADR-044/043/048 (`execute_handoff` + `HandoffShard` + auto-unfence).
//!
//! ## Durable cutover
//! Each move first commits a versioned intent containing the exact assignment generation,
//! placement identity, normalized endpoint identities, and future live generation. Recovery then
//! prepares the target, fences the old primary, and drains to convergence. While that fence still
//! makes the old routing write-quiescent, the mover records every desired member's exact live-set
//! fingerprint and conditionally commits the assignment. Only after that atomic decision does the
//! local [`HandoffShard`](super::super::handoff::HandoffShard) swap live routing. A crash therefore
//! leaves one durable phase that startup can resume; live routing never gets ahead of consensus.
//!
//! The local [`MoveLedger`](ledger::MoveLedger) still schedules conflict-free waves efficiently.
//! Cross-coordinator safety comes from the control state machine: active intents reserve their
//! complete normalized endpoint footprints, and MarkReady/Commit compare the full immutable
//! predicate rather than a read-then-blind-write check. Shard-process coordinator leases prevent a
//! second process from duplicating the same physical work. Disjoint positions may still move in
//! parallel.
//! The whole module is `distributed`-gated; the in-process/default path never compiles it and is
//! byte-identical.

use std::cell::Cell;
use std::time::Instant;

use tokio::runtime::Handle;

use crate::cluster::control::{
    ClusterState, MoveCommand, MoveInitialAuthority, NodeId, ShardAssignment,
};
use crate::cluster::remote::RemoteShard;
use crate::cluster::shard::{Shard, ShardError};

use super::distributed::handoff::{normalized_endpoint, HandoffRoute};
use super::ClusterEngine;

/// Group-aware (RF>1) data-moving reassignment — `rebalance_group_targets` +
/// `ClusterEngine::reassign_group_and_move` (ADR-094).
mod group;
pub(in crate::cluster::coordinator) use group::rebalance_group_targets;

/// Durable intent construction, deterministic operation identity, evidence, and bounded command
/// retries shared by the RF=1 and replica-group movers.
mod intent;

mod recovery;
pub use recovery::recover_durable_moves;

/// The busy-endpoint move ledger + RAII ticket (ADR-095) — the per-node concurrency guard every
/// data-moving op reserves its footprint in.
mod ledger;
pub(in crate::cluster::coordinator) use ledger::MoveLedger;

/// Conflict-free wave planning + scoped-thread wave execution for multi-position sweeps
/// (ADR-095). Scheduling-only — safety lives in the ledger.
mod parallel;
pub(in crate::cluster::coordinator) use parallel::plan_waves;

/// Bounded plan→reserve→revalidate attempts (ADR-095): a move plans its endpoint footprint from a
/// committed read, reserves it in the ledger (possibly waiting out a conflicting in-flight move),
/// then re-reads to confirm neither the position's committed entry NOR any member's endpoint
/// resolution changed while it waited — a change (e.g. the conflicting move just committed this
/// very position, or a `register_node` replaced a member's addr) re-plans from the fresh state.
/// More than a couple of iterations means the map is churning under a storm of concurrent
/// commits; fail typed rather than spin.
const PLAN_ATTEMPTS: usize = 4;

/// Outcome of a [`ClusterEngine::reassign_and_move`] (ADR-090).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReassignOutcome {
    /// The live and committed authorities already agree on `to`: nothing moved and no new control
    /// proposal was necessary. The requested assignment is already committed.
    NoChange { position: u32, generation: u64 },
    /// The data moved to `to` AND the committed map now names it — fully consistent. `generation` is
    /// the position's new handoff/fence generation (the value
    /// [`handoff_generations`](super::ClusterEngine::handoff_generations) reports).
    Moved {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
    /// Live routing already reached `to` because an earlier raw handoff or a
    /// prior move completed without committing. This invocation performed no
    /// second data copy; it reconciled the durable assignment to the attested
    /// live primary.
    Reconciled {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
    /// Legacy public outcome retained for source/API compatibility. Durable reassignment no longer
    /// produces it: failures return `Err` with a resumable intent, and startup resolves that intent
    /// before serving instead of acknowledging live/durable divergence.
    MovedButNotCommitted {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
        /// Whether this invocation performed the physical routing flip. Retained for legacy
        /// callers; the built-in durable mover does not construct this variant.
        moved: bool,
    },
}

/// Outcome of a [`ClusterEngine::rebalance_and_move`] (ADR-090): which positions converged to their
/// committed targets, the first failure (if any — the sweep stops there, fail-forward / resume), and
/// the changed positions not yet attempted. A converged position may have moved physically or may
/// have reconciled a target that was already live.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebalanceMoveReport {
    /// Positions whose desired primary committed this pass, including durable authority
    /// reconciliation when that target was already the attested live primary.
    pub moved: Vec<u32>,
    /// The lowest-position failure (with the error message); the sweep stopped at its wave (at the
    /// default `max_parallel_moves = 1` this is exactly "the first position that failed").
    pub failed: Option<(u32, String)>,
    /// Changed positions left for a re-run: everything after the failing wave, plus (at
    /// `max_parallel_moves ≥ 2`) any ADDITIONAL same-wave failure. A proven clean failure rolled
    /// back; an ambiguous one retained its durable intent. A re-run handles either deterministically.
    pub not_attempted: Vec<u32>,
}

struct PlannedReassign<'a> {
    state: ClusterState,
    expected: ShardAssignment,
    committed_from: NodeId,
    committed_from_endpoint: String,
    live_from_endpoint: String,
    target_endpoint: String,
    route: HandoffRoute,
    ticket: ledger::MoveTicket<'a>,
}

impl ClusterEngine {
    /// Move shard `position`'s data to node `to` AND commit the new owner — the data-moving analogue
    /// of [`reassign_shard`](Self::reassign_shard) (ADR-090). Resolves `from` (the current committed
    /// primary) and `to` to endpoints from membership, persists a durable intent, then runs
    /// peer-recover → fence → drain → fingerprint → conditional assignment commit → live swap →
    /// cleanup. The replica guard below rejects a replicated position; the group method owns that
    /// shape.
    ///
    /// Fail-closed before a live flip and exact on the running coordinator after it:
    /// - a pre-cutover physical failure auto-unfences the source, removes the Preparing intent when
    ///   that cleanup is proven, and leaves routing plus assignment untouched;
    /// - an outcome-ambiguous control/fence failure preserves the intent and returns `Err`; cold
    ///   startup resumes or fails closed from its recorded phase before serving;
    /// - the target cannot receive live writes until consensus has named it, so no successful call
    ///   can leave a stale bare assignment.
    ///
    /// **A position with committed replicas is rejected** (a single-target move would de-replicate
    /// it) — the group-aware [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094)
    /// moves a replicated position. Requires a
    /// handoff-capable cluster (built via [`connect_remote`](Self::connect_remote)); an in-process
    /// cluster has one node owning every position, so `from == to` short-circuits to a no-op.
    pub fn reassign_and_move(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
    ) -> Result<ReassignOutcome, ShardError> {
        match self.reassign_and_move_with_start(position, to, handle, None, || true)? {
            Some(outcome) => Ok(outcome),
            None => Err(ShardError::DeadlineExceeded),
        }
    }

    /// Deadline-aware start admission for one data-moving reassignment.
    ///
    /// The complete committed/live/target endpoint footprint is reserved and
    /// revalidated before `try_start` is called. `None` guarantees that neither
    /// data movement nor a control-state commit began. Once `try_start` returns
    /// true, the operation runs to its exact terminal move-and-commit outcome.
    pub fn reassign_and_move_until<F>(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
        deadline: Instant,
        try_start: F,
    ) -> Result<Option<ReassignOutcome>, ShardError>
    where
        F: FnOnce() -> bool,
    {
        self.reassign_and_move_with_start(position, to, handle, Some(deadline), try_start)
    }

    fn reassign_and_move_with_start<F>(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
        deadline: Option<Instant>,
        try_start: F,
    ) -> Result<Option<ReassignOutcome>, ShardError>
    where
        F: FnOnce() -> bool,
    {
        let pos = u32::try_from(position).map_err(|_| {
            ShardError::Config(format!(
                "reassign_and_move: shard position {position} exceeds the u32 wire/control limit"
            ))
        })?;
        // Plan → reserve → revalidate (ADR-095): resolve the move's endpoint footprint from a
        // committed read, reserve it in the busy-endpoint ledger — blocking until every
        // CONFLICTING in-flight move completes (the ADR-090 serialization, now per-node) — then
        // confirm the committed entry, endpoint resolution, AND current live primary did not change
        // while we waited. The live-primary check is essential after a raw handoff: recovery must
        // seed from the authoritative live owner, never the
        // potentially stale owner still named by the committed map.
        let mut planned: Option<PlannedReassign<'_>> = None;
        for _ in 0..PLAN_ATTEMPTS {
            let state = self.control_state()?;
            let assignment = state
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "reassign_and_move: no committed assignment for shard position {position}"
                    ))
                })?;
            let from = assignment.primary;
            // A single-target move of a REPLICATED position is ambiguous and unsafe (ADR-090/094):
            // the move (`execute_handoff`) swaps the position to a SINGLE `RemoteShard` for `to`,
            // dropping the replica group, while the committed map would still advertise the old
            // replicas — so a failover could read a replica that no longer receives writes
            // (stale). The guard is PER-POSITION (the committed entry, not the cluster's
            // replication factor): a bare position on a replicated cluster is a plain single-shard
            // move, and a replicated position has the group-aware
            // [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094).
            if !assignment.replicas.is_empty() {
                return Err(ShardError::Config(format!(
                    "reassign_and_move: shard position {position} has {} committed replica(s); a \
                     single-target move would de-replicate it — use reassign_group_and_move (or \
                     rebalance_and_move / reconcile, which dispatch group moves) instead (ADR-094)",
                    assignment.replicas.len()
                )));
            }

            // Resolve node ids → endpoints. Fail-closed (never silently skip an unroutable node —
            // that would route a title nowhere). Mirrors `resolve_topology`'s stance.
            let addr_of = |id: NodeId| -> Result<String, ShardError> {
                state
                    .nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.addr.clone())
                    .ok_or_else(|| {
                        ShardError::ControlPlane(format!(
                            "reassign_and_move: node {} has no registered endpoint (addr)",
                            id.0
                        ))
                    })
            };
            let from_ep = addr_of(from)?;
            let tgt_ep = addr_of(to)?;
            let handoff = self.handoffs.get(position).ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} is not handoff-capable (the \
                     cluster was not built via connect_remote/connect_replicated)"
                ))
            })?;
            let live_ep = handoff.live_primary_endpoint().ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} has no live remote primary"
                ))
            })?;

            // Include the committed endpoint even when it differs from the live primary. Raw
            // handoff reconciliation and retries then share a ledger key, and GC/reassign
            // operations cannot reason about two recorded authorities concurrently.
            let footprint = [from_ep.as_str(), live_ep.as_str(), tgt_ep.as_str()];
            let ticket = match deadline {
                Some(deadline) => self.move_ledger.reserve_until(&footprint, deadline),
                None => Some(self.move_ledger.reserve(&footprint)),
            };
            let Some(ticket) = ticket else {
                return Ok(None);
            };

            // Revalidate the committed entry, both membership resolutions, and live routing while
            // the normalized endpoint reservation is held. Moving over a stale endpoint and then
            // committing the NodeId would make the next assignment-routed restart unsafe.
            let now = self.control_state()?;
            let addr_now = |id: NodeId| {
                now.nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.addr.as_deref())
            };
            let entry_unchanged = now
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .is_some_and(|a| a.primary == from && a.replicas.is_empty());
            let endpoint_is = |actual: Option<&str>, planned: &str| {
                actual.is_some_and(|actual| {
                    normalized_endpoint(actual) == normalized_endpoint(planned)
                })
            };
            let eps_unchanged =
                endpoint_is(addr_now(from), &from_ep) && endpoint_is(addr_now(to), &tgt_ep);
            let live_now = handoff.live_primary_endpoint().ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} lost its live remote primary"
                ))
            })?;
            let live_unchanged = normalized_endpoint(&live_now) == normalized_endpoint(&live_ep);
            if entry_unchanged && eps_unchanged && live_unchanged {
                let route = self.validate_handoff_route(position, &live_ep, &tgt_ep)?;
                planned = Some(PlannedReassign {
                    state: now,
                    expected: assignment.clone(),
                    committed_from: from,
                    committed_from_endpoint: from_ep,
                    live_from_endpoint: live_ep,
                    target_endpoint: tgt_ep,
                    route,
                    ticket,
                });
                break;
            }
            // Durable or live routing changed while waiting: the ticket drops here and the next
            // iteration re-plans from the fresh state.
        }
        let Some(PlannedReassign {
            state,
            expected,
            committed_from: from,
            committed_from_endpoint: from_ep,
            live_from_endpoint: live_ep,
            target_endpoint: tgt_ep,
            route,
            ticket,
        }) = planned
        else {
            return Err(ShardError::ControlPlane(format!(
                "reassign_and_move: the committed assignment or live primary for shard position \
                 {position} kept changing while planning ({PLAN_ATTEMPTS} attempts); retry once \
                 routing stops churning"
            )));
        };
        if !try_start() {
            return Ok(None);
        }

        let live_identity = normalized_endpoint(&live_ep);
        let committed_identity = normalized_endpoint(&from_ep);
        let target_identity = normalized_endpoint(&tgt_ep);
        if live_identity != committed_identity && live_identity != target_identity {
            // A raw handoff chain can leave physical authority on B while the durable assignment
            // still names A and this request wants C. First conditionally reconcile A → B as an
            // already-live authority, then re-plan B → C. This keeps every persisted intent's
            // expected primary equal to the source whose fence startup can attest.
            let mut live_nodes = state.nodes.iter().filter(|node| {
                node.addr
                    .as_deref()
                    .is_some_and(|endpoint| normalized_endpoint(endpoint) == live_identity)
            });
            let live_node = live_nodes.next().map(|node| node.id).ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "reassign_and_move: live source {live_ep} is not a registered membership \
                     endpoint; register its authoritative node before moving onward"
                ))
            })?;
            if live_nodes.next().is_some() {
                return Err(ShardError::ControlPlane(format!(
                    "reassign_and_move: live source {live_ep} aliases multiple membership nodes; \
                     reconcile that logical identity explicitly before moving onward"
                )));
            }
            let live_generation = self
                .handoffs
                .get(position)
                .ok_or_else(|| {
                    ShardError::Config(format!(
                        "reassign_and_move: shard position {position} is not handoff-capable"
                    ))
                })?
                .generation();
            let source_fence_generation = intent::plan_live_authority_fence(
                &state,
                &expected,
                &live_ep,
                live_generation,
                "reassign_and_move: attest chained live source",
            )?;
            let live_assignment = ShardAssignment {
                position: pos,
                primary: live_node,
                replicas: Vec::new(),
            };
            let reconcile_intent = intent::build_intent(
                &state,
                expected.clone(),
                live_assignment,
                live_generation,
                source_fence_generation,
                MoveInitialAuthority::Desired,
            )?;
            intent::propose(
                self.control.as_ref(),
                &MoveCommand::Begin(reconcile_intent.clone()),
                "reassign_and_move: persist chained-source reconciliation",
            )?;
            intent::commit_live_authority(
                self,
                &reconcile_intent,
                &live_ep,
                handle,
                "reassign_and_move: commit chained live source",
            )?;
            intent::propose(
                self.control.as_ref(),
                &MoveCommand::Finish {
                    operation_id: reconcile_intent.operation_id,
                },
                "reassign_and_move: finish chained-source reconciliation",
            )?;
            drop(ticket);
            return self.reassign_and_move(position, to, handle).map(Some);
        }

        // When the requested assignment is already committed, live routing must agree here. A
        // different physical source was reconciled durably by the branch above before re-planning.
        if from == to && normalized_endpoint(&from_ep) == normalized_endpoint(&tgt_ep) {
            if state
                .moves
                .intents
                .iter()
                .any(|intent| intent.position == pos)
            {
                return Err(ShardError::ControlPlane(format!(
                    "reassign_and_move: shard position {position} has an unresolved committed \
                     move intent; restart the coordinator so startup can attest the recorded \
                     cutover before repairing live routing"
                )));
            }
            let HandoffRoute::AlreadyAtTarget { generation } = route else {
                return Err(ShardError::ControlPlane(format!(
                    "reassign_and_move: shard position {position} still routes to {live_ep} after \
                     durable source reconciliation; refusing an unrecorded repair"
                )));
            };
            return Ok(Some(ReassignOutcome::NoChange {
                position: pos,
                generation,
            }));
        }

        let (generation, source_fence_generation, initial_authority) = match route {
            HandoffRoute::AlreadyAtTarget { generation } => {
                let source_fence_generation = intent::plan_live_authority_fence(
                    &state,
                    &expected,
                    &tgt_ep,
                    generation,
                    "reassign_and_move: attest already-live target",
                )?;
                (
                    generation,
                    source_fence_generation,
                    MoveInitialAuthority::Desired,
                )
            }
            HandoffRoute::Move => {
                let handoff = self.handoffs.get(position).ok_or_else(|| {
                    ShardError::Config(format!(
                        "reassign_and_move: shard position {position} is not handoff-capable"
                    ))
                })?;
                let generation = handoff.generation() + 1;
                (generation, generation, MoveInitialAuthority::Expected)
            }
        };
        let desired = ShardAssignment {
            position: pos,
            primary: to,
            replicas: Vec::new(),
        };
        let move_intent = intent::build_intent(
            &state,
            expected,
            desired,
            generation,
            source_fence_generation,
            initial_authority,
        )?;
        intent::propose(
            self.control.as_ref(),
            &MoveCommand::Begin(move_intent.clone()),
            "reassign_and_move: persist intent",
        )?;

        let moved = matches!(route, HandoffRoute::Move);
        let cutover = Cell::new(false);
        let result = match route {
            HandoffRoute::AlreadyAtTarget { .. } => {
                cutover.set(true);
                intent::commit_live_authority(
                    self,
                    &move_intent,
                    &tgt_ep,
                    handle,
                    "reassign_and_move: commit already-live target",
                )?;
                Ok(generation)
            }
            HandoffRoute::Move => self.execute_handoff_inner_with_cutover(
                position,
                &live_ep,
                &tgt_ep,
                handle,
                |target, prepared_generation| {
                    cutover.set(true);
                    let evidence = intent::recovery_evidence(
                        prepared_generation,
                        vec![intent::member_evidence(to, target)?],
                    );
                    intent::propose(
                        self.control.as_ref(),
                        &MoveCommand::MarkReady {
                            operation_id: move_intent.operation_id,
                            evidence,
                        },
                        "reassign_and_move: persist target evidence",
                    )?;
                    intent::propose(
                        self.control.as_ref(),
                        &MoveCommand::Commit {
                            operation_id: move_intent.operation_id,
                        },
                        "reassign_and_move: conditional assignment commit",
                    )
                },
            ),
        };
        let generation = match result {
            Ok(generation) => generation,
            Err(error) => {
                // Physical errors auto-unfence in the handoff layer. Remove the still-preparing
                // intent only when a fence probe proves that cleanup completed; otherwise preserve
                // it so startup can inspect the ambiguity and fail closed.
                if !cutover.get() {
                    if let Ok(source) = RemoteShard::connect_for_coordinator_with_security(
                        &live_ep,
                        handle.clone(),
                        self.dict.fingerprint(),
                        self.tag_dict.fingerprint(),
                        pos,
                        self.coordinator_id,
                        &self.client_security,
                    ) {
                        if source.fence(0).is_ok_and(|fence| fence == 0) {
                            intent::abort(
                                self,
                                &move_intent,
                                "reassign_and_move: abort clean preparation",
                            );
                        }
                    }
                }
                return Err(error);
            }
        };

        intent::propose(
            self.control.as_ref(),
            &MoveCommand::Finish {
                operation_id: move_intent.operation_id,
            },
            "reassign_and_move: finish durable move",
        )?;
        let outcome = if moved {
            ReassignOutcome::Moved {
                position: pos,
                from,
                to,
                generation,
            }
        } else {
            ReassignOutcome::Reconciled {
                position: pos,
                from,
                to,
                generation,
            }
        };
        Ok(Some(outcome))
    }

    /// Data-moving analogue of [`rebalance`](Self::rebalance) (ADR-090/094): recompute the desired
    /// HRW shard→node map at replication factor `rf`, then move each position whose **group**
    /// (primary or replica set) changes — sequentially, in position order (=
    /// [`rebalance_and_move_with`](Self::rebalance_and_move_with) at `max_parallel_moves = 1`, the
    /// byte-identical default). Dispatch is by SHAPE: a bare→bare change runs the proven
    /// single-shard [`reassign_and_move`](Self::reassign_and_move) byte-identically (the RF=1
    /// path); any change touching replicas runs the group-aware
    /// [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094), so an `rf > 1` sweep
    /// creates the replica placements it plans — closing the ADR-090 RF>1 deferral.
    pub fn rebalance_and_move(
        &self,
        rf: usize,
        handle: &Handle,
    ) -> Result<RebalanceMoveReport, ShardError> {
        self.rebalance_and_move_with(rf, 1, handle)
    }

    /// [`rebalance_and_move`](Self::rebalance_and_move) with wave parallelism (ADR-095): the
    /// changed positions are partitioned into conflict-free waves
    /// ([`plan_waves`](super::reassign::plan_waves) — moves sharing any node serialize, per the
    /// chained-reshuffle constraint: position `p`: F→T while `q`: T→U would make T a handoff
    /// target and a fenced source at once) and up to `max_parallel_moves` disjoint moves run
    /// concurrently per wave. `max_parallel_moves <= 1` is the sequential path, byte-identical to
    /// the pre-ADR-095 sweep. Safety never rests on the planner: every move still reserves its own
    /// footprint in the busy-endpoint ledger.
    ///
    /// Stops at the first failing WAVE (fail-forward / resume — at the default this is exactly
    /// "stops on the first failure") and returns a [`RebalanceMoveReport`]; already-moved positions
    /// are each consistent, so a partial rebalance is a valid resumable state, never a false
    /// negative. A hard pre-flight error (no nodes, control-plane read failure) is an `Err`;
    /// per-position failures land in the report.
    pub fn rebalance_and_move_with(
        &self,
        rf: usize,
        max_parallel_moves: usize,
        handle: &Handle,
    ) -> Result<RebalanceMoveReport, ShardError> {
        let state = self.control_state()?;
        if state.nodes.is_empty() {
            return Err(ShardError::ControlPlane(
                "rebalance_and_move: the cluster has no nodes to place shards on".into(),
            ));
        }
        // Positions whose GROUP moves (a data move), in deterministic position order, partitioned
        // into conflict-free waves (singletons in target order at the default parallelism).
        let targets = rebalance_group_targets(&state, rf);
        let waves = plan_waves(&state, &targets, max_parallel_moves);

        let mut report = RebalanceMoveReport::default();
        for (wi, wave) in waves.iter().enumerate() {
            let mut wave_failed = false;
            for (pos, outcome) in self.execute_move_wave(&state, &targets, wave, handle) {
                match outcome {
                    Ok(ReassignOutcome::Moved { .. } | ReassignOutcome::Reconciled { .. }) => {
                        report.moved.push(pos);
                    }
                    // Resolved equal under us (a concurrent move already placed it): not a failure.
                    Ok(ReassignOutcome::NoChange { .. }) => {}
                    Ok(ReassignOutcome::MovedButNotCommitted { .. }) => {
                        // Legacy compatibility arm. Stop after this wave rather than piling more
                        // movement onto a result whose caller says its commit is incomplete.
                        wave_failed = true;
                        if report.failed.is_none() {
                            report.failed = Some((
                                pos,
                                "data moved but committing the new owner failed (see the emitted \
                                 event); stopped the rebalance so the durable map stays \
                                 reconcilable — re-run to resume"
                                    .into(),
                            ));
                        } else {
                            report.not_attempted.push(pos);
                        }
                    }
                    Err(e) => {
                        // A proven clean failure rolled this position back; an ambiguous failure
                        // preserved its durable intent. Already-committed positions stay consistent.
                        // Stop after this wave and report for deterministic resume/startup recovery.
                        wave_failed = true;
                        if report.failed.is_none() {
                            report.failed = Some((pos, e.to_string()));
                        } else {
                            report.not_attempted.push(pos);
                        }
                    }
                }
            }
            if wave_failed {
                report
                    .not_attempted
                    .extend(waves[wi + 1..].iter().flatten().map(|&i| targets[i].0));
                break;
            }
        }
        // Position-sorted regardless of wave completion order (a no-op at the sequential default,
        // where waves are singletons in target order).
        report.moved.sort_unstable();
        report.not_attempted.sort_unstable();
        Ok(report)
    }
}

#[cfg(test)]
mod tests;
