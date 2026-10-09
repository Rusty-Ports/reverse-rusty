//! `impl ClusterEngine` — runtime shard-count change (ADR-078, ADR-065 criterion 7).
//!
//! A resize swaps the consistent-hash [`HashRing`] for a fresh one over `K′` shards and
//! rebuilds the cluster from its live source set — every query re-extracted, **re-placed**
//! under the new ring (a different `num_shards` moves ~1/N of anchors to a different shard),
//! and re-ingested. This is the SAME blue/green rebuild [`ClusterEngine::set_vocab`] performs
//! (ADR-046), with the RING swapped instead of the NORMALIZER: the shared
//! [`rebuild_from_live`] core does the work, and re-placing every query under a fresh ring
//! makes correctness trivial — placement and routing both read the new ring, exactly the
//! invariant a fresh [`ClusterEngine::build`] relies on (the module-level cover proof in
//! [`crate::cluster::coordinator`]). The hard "online ring re-keying under live handoff"
//! problem is sidestepped entirely.
//!
//! **In-process only (v1).** Like [`set_vocab`](ClusterEngine::set_vocab), a resize refuses a
//! non-local / handoff-wrapped cluster: a remote shard would not be rebuilt under the new
//! ring, so its placement and the coordinator's routing would disagree — a silent
//! cross-process false negative. A cross-process resize (shipping the re-keyed data to remote
//! shards over the live-handoff machinery) is a documented follow-on.
//!
//! **Durable for free.** The only durable change is `num_shards` growing/shrinking + a
//! correspondingly longer/shorter per-shard segment registry — both already expressible in
//! the existing `ClusterManifest` (no format bump). [`checkpoint`](ClusterEngine::checkpoint)
//! writes `num_shards = layout.ring.num_shards()` and [`open`](ClusterEngine::open) re-derives
//! `HashRing::new(num_shards, vnodes)`, so a resized cluster reopens byte-identically.
//!
//! **Dict + vocab + tags preserved.** A resize does not touch the feature space: the normalizer
//! is reused as-is and — because the normalizer is unchanged — the frozen dict is REUSED verbatim
//! (same dense ids, same hot-mask, same fingerprint). A resize is a ring change, not a model
//! change, so the manifest's dict fingerprint and the control-plane's `dict_fingerprint` stay
//! valid; an installed alias's equivalences ride the reused dict (`extract_readonly` auto-expands
//! them), and per-query tags carry through as stored `TagId`s exactly as
//! [`set_vocab`](ClusterEngine::set_vocab) does (ADR-074).

use std::sync::Arc;

use crate::cluster::autoscale::{AutoscaleConfig, LoadSnapshot};
use crate::cluster::control::ClusterStateChange;
use crate::cluster::ring::HashRing;
use crate::cluster::shard::ShardError;
use crate::events::{DurabilityOp, EngineEvent};

use super::ClusterEngine;
use crate::cluster::coordinator::layout::LayoutChange;

mod control;
mod rebuild;
mod timings;
mod visibility;

pub(in crate::cluster::coordinator) use timings::Lap;
pub use timings::RebuildTimings;

impl ClusterEngine {
    /// Resize the cluster to `new_num_shards` positions (ADR-078) — a blue/green rebuild of
    /// the cluster under a fresh `HashRing::new(new_num_shards, vnodes)`: re-place every live
    /// query, build fresh shards beside the old ones, publish them as one layout, and
    /// (for a durable cluster) commit the result via [`checkpoint`](Self::checkpoint). The
    /// vocabulary and dict are UNCHANGED (the normalizer is reused; the dict is re-minted
    /// identically; declared aliases + per-query tags carry through). Returns the number of
    /// live queries rebuilt.
    ///
    /// Refuses (errors) if `new_num_shards == 0`, or any shard is non-local / handoff-wrapped
    /// (the in-process-only boundary [`set_vocab`](Self::set_vocab) enforces). A no-op
    /// (`Ok(0)`) when `new_num_shards` already equals the current count.
    pub fn resize(&self, new_num_shards: usize) -> Result<usize, ShardError> {
        let change = self.begin_layout_change()?;
        self.resize_in(&change, new_num_shards)
    }

    /// [`Self::resize`] inside a layout change the caller began, so that deciding on a shard
    /// count and resizing to it are one step.
    pub(in crate::cluster::coordinator) fn resize_in(
        &self,
        change: &LayoutChange<'_>,
        new_num_shards: usize,
    ) -> Result<usize, ShardError> {
        if new_num_shards == 0 {
            return Err(ShardError::Config(
                "resize: new_num_shards must be ≥ 1".into(),
            ));
        }
        let new_num_shards_control = u32::try_from(new_num_shards).map_err(|_| {
            ShardError::Config(
                "resize: new_num_shards exceeds the control-plane representation".into(),
            )
        })?;
        let before = change.current();
        // In-process only (same correctness boundary as set_vocab): a remote shard would keep
        // its old placement while the coordinator routes under the new ring — a silent
        // cross-process false negative. Checked BEFORE the no-op short-circuit so the boundary is
        // consistent even for a same-K call. Always compiled, so a future non-local shard can't
        // slip past it on a non-distributed build (where this never fires).
        if before.shards.iter().any(|s| !s.is_local()) {
            return Err(ShardError::Config(
                "resize is in-process only: a cross-process (remote) shard is not rebuilt under \
                 the new ring in v1 (it would be a silent false negative)"
                    .into(),
            ));
        }
        #[cfg(feature = "distributed")]
        if !before.handoffs.is_empty() {
            return Err(ShardError::Config(
                "resize is in-process only: a handoff-wrapped (movable) shard position is not \
                 supported by a resize in v1"
                    .into(),
            ));
        }
        // A failed control proposal happens after the serving swap, so the next
        // call must finish that exact transition before it is allowed to build
        // another generation. Otherwise two failed, different-target resizes
        // can advance the live generation twice while a later successful
        // SetShardCount advances the control document only once.
        self.finish_pending_resize_control_commit(&before)?;
        if new_num_shards == before.ring.num_shards() {
            // The LIVE ring already has this many shards — a full rebuild would change nothing.
            // But a PRIOR resize may have swapped the ring in RAM and then FAILED to update the
            // control plane or checkpoint, leaving one or both at the old count. A bare `Ok(0)`
            // here would falsely acknowledge that partial transition. The preflight above has
            // already repaired and attested the control count + placement generation. Re-ensure
            // the durable commit (checkpoint is idempotent — a clean one is cheap) + on-disk dir
            // set, so a retry HEALS rather than masks either failure seam.
            if self.data_dir.is_some() {
                self.checkpoint_quiesced(&before)?;
            }
            return Ok(0);
        }

        let new_ring = HashRing::new(new_num_shards, self.vnodes)?;

        // Queued partial-apply repairs (ADR-047) index the old shard space. They are dropped
        // when the new layout is published, not before: until then reads still run on the
        // old shards, and an exhaustive read must go on refusing while one is queued.

        // Rebuild under the new ring, reusing the current normalizer + vocab (None ⇒ preserve
        // before.vocab and re-resolve ITS equivalences onto the re-minted dict).
        let new_norm = Arc::clone(&before.norm);
        let next_generation = before
            .generation
            .next()
            .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
        // The old layout is released before the rebuild, which holds a second corpus.
        drop(before);
        let (rebuilt, after) =
            self.rebuild_from_live(change, new_norm, new_ring, None, next_generation)?;
        let mut lap = Lap::start();

        // Keep the cluster-state document consistent with the new shard count so `collect_load`
        // / `assignment_for` (introspection + the autoscaler) see K′ positions, not a stale K.
        // Durability rides the manifest (which `open` re-seeds the control plane from), so this
        // only needs to be live-correct.
        self.propose_layout_change(
            &after,
            ClusterStateChange::SetShardCount {
                num_shards: new_num_shards_control,
            },
        )?;
        let control = self.control.cluster_state()?;
        self.attest_resize_control_state(&after, &control)?;

        // Commit the rebuild durably, THEN drop now-orphaned shard dirs (shrink only). Order
        // matters: the orphan dirs are still referenced by the OLD manifest until `checkpoint`
        // commits the new one, so deleting them earlier would break crash-recovery to the old K.
        if self.data_dir.is_some() {
            // Operations that loaded the old layout finish on it. Its files go once they
            // have, here or at a later checkpoint.
            self.await_retired_layouts();
            self.checkpoint_quiesced(&after)?;
        }
        self.note_rebuild_commit(lap.lap());
        Ok(rebuilt)
    }

    /// Resize to the autoscaler's [`recommended_shard_count`] for the current load, if any
    /// shard crossed the split threshold. Operator-/test-facing convenience: collects the
    /// load snapshot, computes the recommendation, and applies it via [`resize`](Self::resize).
    /// Returns the new shard count if a resize happened, else `None` (no recommendation, or it
    /// already equals the current count). Refuses a non-local cluster (the gather boundary).
    pub fn resize_to_recommended(
        &self,
        config: &AutoscaleConfig,
    ) -> Result<Option<usize>, ShardError> {
        // Measuring the load and resizing to what it recommends are one layout change: the
        // recommendation is for the layout the resize then replaces, and for no other.
        let change = self.begin_layout_change()?;
        let (snapshot, current_shards) = {
            let current = change.current();
            (
                self.collect_load_in(&current, config)?,
                current.num_shards(),
            )
        };
        match recommended_shard_count(&snapshot, config) {
            Some(k) if k != current_shards => self.resize_in(&change, k).map(|_| Some(k)),
            _ => Ok(None),
        }
    }

    /// Best-effort removal of every top-level `shard_NNN` directory whose index is `≥
    /// num_shards`, called AFTER the manifest commit so the committed manifest no longer
    /// references them. This asserts the invariant "a committed cluster's on-disk dir set is
    /// exactly `shard_000..shard_{num_shards-1}`": a SHRINK's orphan dirs are removed (so a later
    /// GROW back through these positions cannot self-restart `new_durable` from a stale sidecar),
    /// and the heal path (a same-K retry after a failed checkpoint) re-asserts it too. Scans
    /// rather than taking an old count, so it is correct without knowing the prior shape. An
    /// orphan left behind is benign for correctness (`open` reads only `0..num_shards`).
    pub(in crate::cluster::coordinator) fn remove_shard_dirs_at_or_above(&self, num_shards: usize) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(idx) = name
                .to_str()
                .and_then(|n| n.strip_prefix("shard_"))
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue; // not a shard dir (the manifest, the log, a replica is nested, …)
            };
            if idx < num_shards {
                continue;
            }
            let sd = entry.path();
            let removed =
                crate::fault::step("remove", &sd).and_then(|()| std::fs::remove_dir_all(&sd));
            match removed {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => self.emit(EngineEvent::DurabilityFailure {
                    op: DurabilityOp::WalReset,
                    detail: format!(
                        "removing orphaned shard dir {} failed (benign: it is not in the committed \
                         manifest, so `open` ignores it)",
                        sd.display()
                    ),
                    error: e.to_string(),
                }),
            }
        }
    }
}

/// Recommend a new `num_shards` from the load snapshot (ADR-078): `None` when split detection
/// is disabled (`split_corpus_threshold == 0`) or no shard is over threshold; otherwise
/// `current_num_shards + count(over-threshold shards)` — one fresh bucket per hot shard. Pure and
/// deterministic (no clock, no randomness), and monotone within a single snapshot: it never
/// recommends shrinking, so an operator/driver applying it cannot thrash a cluster smaller.
pub fn recommended_shard_count(snapshot: &LoadSnapshot, config: &AutoscaleConfig) -> Option<usize> {
    if config.split_corpus_threshold == 0 {
        return None;
    }
    let over = snapshot
        .shard_corpus
        .iter()
        // Discount the replicated broad lane (on every shard regardless of K, ADR-080): only the
        // SELECTIVE load is reduced by splitting, so only it counts as split pressure. Else every
        // shard looks hot and a driver applying `resize_to_recommended` grows without bound
        // (codex review).
        .filter(|&&c| c.saturating_sub(snapshot.replicated_corpus) > config.split_corpus_threshold)
        .count();
    if over == 0 {
        None
    } else {
        Some(snapshot.num_shards as usize + over)
    }
}
