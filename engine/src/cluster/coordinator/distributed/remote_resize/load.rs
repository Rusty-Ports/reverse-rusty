//! Building, loading, proving, and committing a remote resize's staged layout (ADR-180).

use std::sync::Arc;
use std::time::Instant;

use crate::cluster::control::{
    MoveCommandOutcome, ResizeCommand, ResizeIntent, ResizeLayout, ResizePositionEvidence,
};
use crate::cluster::live_source_wire::MAX_EXPORT_DURATION;
use crate::cluster::remote::RemoteShard;
use crate::cluster::shard::Shard;
use crate::segment::PlacedQuery;

use super::{ClusterConfig, ClusterEngine, RemoteResizeRequest, ResizeProgress, ShardError};

/// Queries buffered before each staged-layout placement pass.
const EXPORT_BATCH: usize = 4096;

/// A staged layout that holds the complete exported corpus, proven and committed.
pub(super) struct StagedBuild {
    pub(super) staged: ClusterEngine,
    /// Distinct live logical ids exported, sorted: the new layout's complete membership.
    pub(super) logical_ids: Vec<u64>,
    /// Physical rows loaded (replicated rows count once per position).
    pub(super) loaded: u64,
    /// Old-layout slots retired before the commit.
    pub(super) retired_slots: usize,
}

/// The node endpoints of both layouts, in position order.
pub(super) struct Layouts<'a> {
    pub(super) expected: &'a [String],
    pub(super) targets: &'a [String],
}

impl ClusterEngine {
    /// Build the staged layout on the empty targets, stream the live corpus into it, prove each
    /// position, retire the old layout's nodes, record `Ready`, and commit.
    pub(super) fn build_and_commit(
        &self,
        handle: &tokio::runtime::Handle,
        request: &RemoteResizeRequest,
        intent: &ResizeIntent,
        layouts: &Layouts<'_>,
        source_durable: bool,
        progress: &ResizeProgress,
    ) -> Result<StagedBuild, ShardError> {
        let target_endpoints = layouts.targets;
        if self.pending_repairs() != 0 {
            return Err(ShardError::ControlPlane(
                "a partial write was queued before the resize fence; run resync and retry".into(),
            ));
        }
        let config = ClusterConfig {
            num_shards: request.num_shards,
            vnodes: self.vnodes,
            replication_factor: 1,
            per_shard: self.per_shard.clone(),
            include_broad: self.include_broad,
            remote_placement_generation: intent.desired.placement_generation,
            ..ClusterConfig::default()
        };
        let staged = Self::connect_remote_with_security_mode(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            Arc::clone(&self.tag_dict),
            &config,
            target_endpoints,
            handle,
            self.client_security.clone(),
            self.coordinator_id,
        )?;
        if staged.num_queries()? != 0 {
            return Err(ShardError::Config(
                "remote resize targets must be empty; wipe their data directories and retry".into(),
            ));
        }
        let targets: Vec<RemoteShard> = target_endpoints
            .iter()
            .enumerate()
            .map(|(position, endpoint)| self.slot_client(handle, endpoint, position))
            .collect::<Result<_, _>>()?;
        // Writes are fenced and drained, so these are the fingerprints the export must reproduce;
        // retirement re-checks them to prove nothing landed on a source in between.
        let before = self.source_fingerprints(handle, layouts.expected)?;
        let (logical_ids, loaded) = self.load_staged_layout(&staged, &targets)?;

        let mut evidence = Vec::with_capacity(targets.len());
        for (position, target) in targets.iter().enumerate() {
            // Prove the loaded rows survive a target restart before they can become the layout
            // of record: a durable `Seal` (ADR-181) commits segments, sources, and the sidecar,
            // and a volatile target refuses it. A volatile source layout has nothing to prove.
            if source_durable {
                target.seal_for_checkpoint()?;
            }
            let (fingerprint_lo, fingerprint_hi, live_count) = target.content_fingerprint()?;
            if live_count != loaded[position] {
                return Err(ShardError::Protocol(format!(
                    "staged position {position} holds {live_count} rows but {} were loaded",
                    loaded[position]
                )));
            }
            evidence.push(ResizePositionEvidence {
                position: position as u32,
                fingerprint_lo,
                fingerprint_hi,
                live_count,
            });
        }
        // From the moment `Commit` may apply, the old layout may no longer be the layout of record.
        // Retire its nodes first, at the storage layer, so no coordinator (this one after a crash,
        // cancellation, or lost reply, or any other) can answer from it afterwards. Reads fail loud
        // from here until installation.
        let retired_slots = self.retire_old_layout(
            handle,
            request.operation_id,
            intent.desired.placement_generation,
            layouts.expected,
            &before,
            progress,
        )?;
        self.expect_resize_outcome(ResizeCommand::MarkReady {
            operation_id: request.operation_id,
            evidence,
        })?;
        progress.commit_proposed.set(true);
        self.commit_resize(request.operation_id, &intent.desired)?;
        Ok(StagedBuild {
            staged,
            logical_ids,
            loaded: loaded.iter().sum(),
            retired_slots,
        })
    }

    /// Stream the deduplicated live corpus into the staged layout through one `StageIngest`
    /// stream per target position, so each target seals full-size segments and writes its source
    /// store once. Returns the sorted distinct logical ids exported and each position's loaded row
    /// count.
    fn load_staged_layout(
        &self,
        staged: &ClusterEngine,
        targets: &[RemoteShard],
    ) -> Result<(Vec<u64>, Vec<u64>), ShardError> {
        // Each source position's export may take up to its own export bound; the streams stay
        // open across all of them, plus one more bound to finish.
        let rounds = u32::try_from(self.shards.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let deadline = Instant::now()
            .checked_add(MAX_EXPORT_DURATION.saturating_mul(rounds))
            .ok_or_else(|| ShardError::Config("staged load deadline overflows".into()))?;
        let mut loads: Vec<_> = targets
            .iter()
            .map(|target| target.open_staged_load(deadline))
            .collect();
        let mut logical_ids = Vec::new();
        let exported = {
            let mut emit = |position: usize, chunk: &[PlacedQuery]| -> Result<(), ShardError> {
                loads[position].send(chunk)
            };
            let mut batch = Vec::with_capacity(EXPORT_BATCH);
            let mut load_error: Option<ShardError> = None;
            let exported = self.export_live_corpus(&mut |query| {
                logical_ids.push(query.logical_id);
                batch.push((query.logical_id, query.version, query.dsl, query.tags));
                if batch.len() >= EXPORT_BATCH {
                    let placed = staged.place_resize_batch(&batch, &mut emit);
                    batch.clear();
                    if let Err(error) = placed {
                        load_error = Some(error);
                        return Err(ShardError::Protocol("staged layout load failed".into()));
                    }
                }
                Ok(())
            });
            if let Some(error) = load_error {
                return Err(error);
            }
            let exported = exported?;
            staged.place_resize_batch(&batch, &mut emit)?;
            exported
        };
        let mut loaded = Vec::with_capacity(loads.len());
        for (position, load) in loads.into_iter().enumerate() {
            let report = load.finish()?;
            if report.rejected_parse != 0 || report.rejected_class_d != 0 {
                return Err(ShardError::Protocol(format!(
                    "staged position {position} rejected {} re-placed queries",
                    report.rejected_parse + report.rejected_class_d
                )));
            }
            loaded.push(report.ingested as u64);
        }
        if logical_ids.len() as u64 != exported {
            return Err(ShardError::Protocol(format!(
                "the export visited {} queries but reported {exported}",
                logical_ids.len()
            )));
        }
        logical_ids.sort_unstable();
        Ok((logical_ids, loaded))
    }

    /// Commit, resolving an ambiguous proposal result by reading the committed layout back.
    fn commit_resize(&self, operation_id: u64, desired: &ResizeLayout) -> Result<(), ShardError> {
        let proposed = self.propose_resize(ResizeCommand::Commit { operation_id });
        if matches!(
            proposed,
            Ok(MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied)
        ) {
            return Ok(());
        }
        let state = self.control_state()?;
        if state.num_shards == desired.num_shards
            && state.placement_generation == desired.placement_generation
            && state.assignments == desired.assignments
        {
            return Ok(());
        }
        Err(match proposed {
            Ok(outcome) => {
                ShardError::ControlPlane(format!("remote resize commit was refused ({outcome:?})"))
            }
            Err(error) => error,
        })
    }

    /// Whether any slot of the current layout persists to disk.
    pub(super) fn layout_is_durable(
        &self,
        handle: &tokio::runtime::Handle,
        endpoints: &[String],
    ) -> Result<bool, ShardError> {
        for (position, endpoint) in endpoints.iter().enumerate() {
            if self.slot_client(handle, endpoint, position)?.is_durable()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A direct client for one slot, stamped with this coordinator's identity.
    pub(super) fn slot_client(
        &self,
        handle: &tokio::runtime::Handle,
        endpoint: &str,
        position: usize,
    ) -> Result<RemoteShard, ShardError> {
        RemoteShard::connect_for_coordinator_with_security(
            endpoint,
            handle.clone(),
            self.dict.fingerprint(),
            self.tag_dict.fingerprint(),
            position as u32,
            self.coordinator_id,
            &self.client_security,
        )
    }
}
