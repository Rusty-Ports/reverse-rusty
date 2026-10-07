use super::{
    ClusterEngine, ClusterMutation, DurabilityOp, EngineEvent, PendingRepair, ResyncReport,
    ShardError,
};
use crate::cluster::coordinator::layout::Layout;

impl ClusterEngine {
    /// Record a partial multi-shard apply (ADR-047): queue the failed shards for repair (keyed by
    /// logical id, so the latest mutation for an id wins), emit a `ClusterPartialApply` durability
    /// event, and build the [`ShardError::PartiallyApplied`] the caller returns. The caller is
    /// told the write failed (ADR-194): the queue is this process's memory, and the coordinator
    /// that queues repairs, a remote one, has no log to rebuild it from. A retry of the write or
    /// [`Self::resync`] converges it.
    pub(super) fn note_partial(
        &self,
        mutation: ClusterMutation,
        logical: u64,
        applied: Vec<usize>,
        failed: Vec<usize>,
        first_err: Option<ShardError>,
    ) -> ShardError {
        self.note_partial_deferring(mutation, logical, applied, failed, &[], first_err)
    }

    /// [`note_partial`](Self::note_partial) for an upsert that also left `deferred` shards
    /// untouched on purpose: they hold (or may hold) a copy the upsert must tombstone, and
    /// that waits until the failed shards hold the new version (ADR-185). They are queued
    /// with the failed shards and reported separately, since nothing failed on them.
    pub(super) fn note_partial_deferring(
        &self,
        mutation: ClusterMutation,
        logical: u64,
        applied: Vec<usize>,
        failed: Vec<usize>,
        deferred: &[usize],
        first_err: Option<ShardError>,
    ) -> ShardError {
        let detail = first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
        let mut targets = failed.clone();
        targets.extend(deferred.iter().copied().filter(|s| !failed.contains(s)));
        targets.sort_unstable();
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                logical,
                PendingRepair {
                    mutation,
                    failed_shards: targets,
                },
            );
        let cleanup = if deferred.is_empty() {
            String::new()
        } else {
            format!(", cleanup deferred on {deferred:?}")
        };
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ClusterPartialApply,
            detail: format!(
                "logical {logical}: applied on {applied:?}, failed on {failed:?}{cleanup}"
            ),
            error: detail.clone(),
        });
        ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            detail,
        }
    }

    /// Drop any queued partial-apply entry for `logical` — a later full apply (or delete)
    /// supersedes it, so `resync` must not re-drive a stale mutation (e.g. resurrect a removed
    /// query). Cheap (an uncontended lock + a `BTreeMap` miss) on the default path, where the
    /// queue is always empty.
    pub(super) fn clear_pending(&self, logical: u64) {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&logical);
    }

    /// Re-drive every queued partial-apply mutation (ADR-047) against its still-failed shards,
    /// converging a cluster left divergent by a mid-fan-out remote write failure. Re-driving
    /// touches ONLY the failed shards, and each re-drive is safe to repeat (see
    /// [`repair_form`]), so already-converged shards are untouched and a still-unreachable shard
    /// stays queued. A no-op (empty report) on the in-process / RF=1 path, which never queues
    /// anything.
    pub fn resync(&self) -> ResyncReport {
        let admitted = self.admit_mutation();
        self.resync_admitted(&admitted.layout)
    }

    /// [`Self::resync`] for a caller that loaded `layout` before it could hold the mutation
    /// barrier. It takes the barrier and does nothing when the layout has been replaced
    /// meanwhile; the next pass repairs under the new one.
    pub(in crate::cluster::coordinator) fn resync_in(&self, layout: &Layout) -> ResyncReport {
        let _barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_published(layout) {
            return ResyncReport::default();
        }
        self.resync_admitted(layout)
    }

    /// The repair pass, for a caller that holds the mutation barrier. Exhaustive cross-shard
    /// reads take the exclusive side of that barrier: a repair re-drive changes what shards
    /// show, like a live write, and must not slip between sequential shard reads.
    fn resync_admitted(&self, layout: &Layout) -> ResyncReport {
        // A remote resize starts only with no queued repairs and refuses new writes, so a
        // re-drive during its copy could only diverge the layout it is exporting (ADR-180).
        if self.ensure_resize_write_fence_open().is_err() {
            return ResyncReport::default();
        }
        // Snapshot IDs only. The mutation must stay in the queue until we hold
        // its ID lock: a successful newer write can clear it while this pass
        // is busy with another ID. Draining payloads here would lose that
        // supersession evidence and later resurrect the older mutation.
        let pending = self.pending_repair_ids();
        let mut repaired = 0usize;
        let mut still_pending = 0usize;
        for logical in pending {
            // Select the CURRENT repair under the same full-operation ID lock
            // as live writers. A cleared entry needs no repair; a replacement
            // entry carries the newer failed mutation and its current targets.
            let _logical_guard = self.logical_write_guard(logical);
            match self.redrive_pending(layout, logical) {
                Redrive::NothingQueued => {}
                Redrive::Converged => repaired += 1,
                Redrive::StillPending { .. } => still_pending += 1,
            }
        }
        ResyncReport {
            repaired,
            still_pending,
        }
    }

    /// Re-drive the repair queued for `logical`, if there is one, against the shards it still
    /// has to reach. The caller holds the mutation barrier and `logical`'s ID lock, as every
    /// live write of that id does, so no newer write can supersede the entry mid-repair.
    pub(super) fn redrive_pending(&self, layout: &Layout, logical: u64) -> Redrive {
        let repair = self
            .pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&logical);
        let Some(pr) = repair else {
            return Redrive::NothingQueued;
        };
        let redrive = repair_form(&pr.mutation);
        let mut still_failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        // An upsert installs before it removes (ADR-185): the shards that store the
        // new version are re-driven first, and a shard that only tombstones the id
        // waits until all of them succeeded. Its old copy may be the only one a title
        // can still reach.
        let (stores, clears): (Vec<usize>, Vec<usize>) = match &redrive {
            ClusterMutation::Upsert { placement, .. } => pr
                .failed_shards
                .iter()
                .partition(|&&s| crate::cluster::shard::upsert_stores_at(placement, s as u32)),
            _ => (pr.failed_shards.clone(), Vec::new()),
        };
        for (targets, is_install) in [(&stores, true), (&clears, false)] {
            if !is_install && !still_failed.is_empty() {
                still_failed.extend(targets.iter().copied());
                break;
            }
            for &s in targets {
                if let Err(e) = crate::cluster::shard::apply_mutation(
                    layout.shards[s].as_ref(),
                    &layout.norm,
                    &layout.dict,
                    &redrive,
                    Some(s as u32),
                ) {
                    still_failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if still_failed.is_empty() {
            // A converged Remove has now deleted the row everywhere, so the
            // fail-closed reservation retained at the partial-apply point is
            // releasable — without this, the id would 409 every future
            // add_query until a coordinator reopen (review finding).
            if matches!(pr.mutation, ClusterMutation::Remove { .. }) {
                self.remove_logical_id(logical);
            }
            return Redrive::Converged;
        }
        let detail = first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ClusterPartialApply,
            detail: format!("resync: logical {logical} still failing on {still_failed:?}"),
            error: detail.clone(),
        });
        // Re-queue only the still-failed shards before the caller releases the ID
        // lock. No same-ID writer or repair can supersede this work yet.
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                logical,
                PendingRepair {
                    mutation: pr.mutation,
                    failed_shards: still_failed.clone(),
                },
            );
        Redrive::StillPending {
            pending: still_failed,
            detail,
        }
    }

    /// The logical ids whose last write is still queued for repair, ascending. A coordinator
    /// that is about to stop reports them, because the queue stops with it (ADR-194).
    #[must_use]
    pub fn pending_repair_ids(&self) -> Vec<u64> {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }

    /// Number of mutations currently queued for partial-apply repair (ADR-047): 0 on a healthy
    /// cluster, and always 0 on the in-process / RF=1 path (whose writes never fail). A nonzero
    /// value means at least one shard is lagging: retry those writes or call [`Self::resync`].
    /// Nothing in the server drains the queue on its own. Introspection for operators + tests.
    #[must_use]
    pub fn pending_repairs(&self) -> usize {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Replay one recovered mutation through the same `apply` funnel as live writes.
    pub(in crate::cluster::coordinator) fn replay_apply(
        &self,
        layout: &Layout,
        m: ClusterMutation,
    ) -> Result<(), ShardError> {
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                if !self.insert_logical_id(logical) {
                    return Err(ShardError::DuplicateLogicalId(logical));
                }
                self.apply_add(layout, logical, version, &dsl, &tags, &placement)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(layout, logical)?;
                self.remove_logical_id(logical);
            }
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                let fresh = self.insert_logical_id(logical);
                self.apply_upsert(layout, logical, version, &dsl, &tags, &placement, fresh)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment. Excludes mutations and
    /// checkpoints while it runs (ADR-197): a flush that wrote a segment between a
    /// checkpoint's registry snapshot and its orphan sweep would have that file deleted.
    pub fn flush(&self) -> Result<(), ShardError> {
        let stable = self.stable();
        let _quiesced = self.quiesce_mutations();
        let layout = &*stable.layout;
        for s in layout.shards.iter() {
            s.flush()?;
        }
        self.compact_logical_ids();
        Ok(())
    }
}

/// What re-driving one logical id's queued repair did.
pub(super) enum Redrive {
    /// No repair was queued for the id.
    NothingQueued,
    /// Every shard the repair still had to reach now holds it; the entry is gone.
    Converged,
    /// Some shard still refuses it. The entry is queued again for the `pending` shards;
    /// `detail` is the first shard error.
    StillPending { pending: Vec<usize>, detail: String },
}

/// The mutation a repair sends for `queued`. A failed shard write is ambiguous: the shard may
/// have applied it before the error came back. A queued `Add` is therefore re-driven as a
/// replace of that id on the shard, which stores the row when it never arrived and leaves one
/// row when it did. A plain second insert would leave two rows for one id, and the shard's
/// logical-id enumeration refuses that.
fn repair_form(queued: &ClusterMutation) -> ClusterMutation {
    match queued {
        ClusterMutation::Add {
            logical,
            version,
            dsl,
            tags,
            placement,
        } => ClusterMutation::Upsert {
            logical: *logical,
            version: *version,
            dsl: dsl.clone(),
            tags: tags.clone(),
            placement: placement.clone(),
        },
        other => other.clone(),
    }
}
