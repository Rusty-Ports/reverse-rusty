//! The cluster upsert apply funnel (ADR-185): replace-by-id that an unfenced reader can
//! never catch between versions.
//!
//! The previous funnel tombstoned the id on every shard and then inserted the new version,
//! each step publishing its own shard snapshot — so a title matched between the passes saw
//! neither version, on every re-put and every bulk index item. This one is built from
//! [`Shard::replace_placed`], an atomic per-shard replace, and takes one of two shapes:
//!
//! - **The placement is unchanged** (a re-put, a tag or version edit, a bulk re-index). The
//!   coordinator asks each placement shard to replace *only if it already holds this
//!   placement*. When every shard does, the upsert is complete: the row's owner for any
//!   title is the same shard before and after, and that shard switched versions in one
//!   step. No reader coordination is needed and none is taken.
//! - **The placement moves** (or a shard declines for any other reason). Nothing has been
//!   mutated yet, so the coordinator enters the [move fence](super::super::move_fence) and
//!   rewrites every shard inside it; readers that overlap retry. New-placement shards are
//!   written before stale copies are tombstoned, so even an unfenced point read finds a
//!   version.
//!
//! Either way the order is fixed: **install the new version on every placement shard, and
//! only then remove copies elsewhere.** When an install fails, nothing else is removed. The
//! old copies keep serving, and the shards that still need a tombstone are queued for
//! repair together with the failed ones, so `resync` finishes the upsert in the same order.
//!
//! Live writes and log replay run this same funnel, so live and replayed application agree.

use super::{
    extract_readonly, placement_of, AddOutcome, ClusterEngine, ClusterMutation, ShardError, Target,
};
use crate::cluster::shard::{PlacedWrite, ReplaceMode, ReplaceStatus, Shard};

/// Per-shard results of one upsert fan-out.
#[derive(Default)]
struct Fanout {
    /// Prior live copies tombstoned, across every shard.
    removed: usize,
    /// Shards that now hold the new version.
    applied: Vec<usize>,
    failed: Vec<usize>,
    first_err: Option<ShardError>,
}

impl Fanout {
    fn fail(&mut self, shard: usize, error: ShardError) {
        self.failed.push(shard);
        self.first_err.get_or_insert(error);
    }

    /// Replace on `shard`, unless an earlier step already failed there (its repair
    /// re-drives the whole upsert). Returns the status when the call succeeded.
    fn replace(
        &mut self,
        shards: &[Box<dyn Shard>],
        shard: usize,
        write: &PlacedWrite<'_>,
        mode: ReplaceMode,
    ) -> Option<ReplaceStatus> {
        if self.failed.contains(&shard) {
            return None;
        }
        match shards[shard].replace_placed(write, mode) {
            Ok(status) => {
                if let ReplaceStatus::Replaced { removed } = status {
                    self.removed += removed;
                }
                if status.applied() {
                    self.applied.push(shard);
                }
                Some(status)
            }
            Err(error) => {
                self.fail(shard, error);
                None
            }
        }
    }

    /// Tombstone any copy on every shard outside `placement_shards` (idempotent on a
    /// shard that holds none). Only called once every placement shard holds the new
    /// version: until then a copy elsewhere may be the only one a title can still reach.
    fn sweep(&mut self, shards: &[Box<dyn Shard>], placement_shards: &[usize], logical: u64) {
        debug_assert!(
            self.failed.is_empty(),
            "sweeping before the install finished"
        );
        for (s, shard) in shards.iter().enumerate() {
            if placement_shards.contains(&s) {
                continue;
            }
            match shard.delete_by_logical_id(logical) {
                Ok(removed) => self.removed += removed,
                Err(error) => self.fail(s, error),
            }
        }
    }
}

/// What must happen to copies outside the new placement once it is installed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cleanup {
    /// No copy can exist elsewhere.
    None,
    /// A copy may exist elsewhere; removing it only takes a duplicate away.
    Strays,
    /// The placement is moving: the removal is the second half of a fenced rewrite.
    Move,
}

impl ClusterEngine {
    /// Apply an UPSERT to the shards — the state-machine `apply` for replace-by-id, shared
    /// by the live write path (after logging) and log replay. Placement is decided FIRST: a
    /// class-D / parse rejection returns before any shard is touched (a failed replace never
    /// deletes, ADR-067 parity). `fresh` says this call introduced the id to the logical-id
    /// directory. Partial failures ride the ADR-047 machinery with the `Upsert` itself as
    /// the queued repair mutation; re-driving it on one shard is an idempotent atomic replace
    /// (or a tombstone, where the new placement does not store the row).
    pub(in crate::cluster::coordinator) fn apply_upsert(
        &self,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
        fresh: bool,
    ) -> Result<(usize, AddOutcome), ShardError> {
        self.note_tags(tags);
        // This mutation was accepted and appended already. Re-application (live
        // or recovery) must not re-litigate it against today's configurable or
        // compiled-in policy limits.
        let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
            ShardError::Log(format!(
                "parsing acknowledged cluster upsert during apply: {error}"
            ))
        })?;
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Force accept=true: apply is reached ONLY for already-accepted writes (live upsert
        // classified + accepted before logging; replay sees only logged=accepted frames), so this
        // placement is configuration-independent — a knob flip on reopen neither drops nor
        // resurrects (codex review). The empty-class-D guard in `placement_of` still rejects a
        // never-stored empty query defensively.
        let target = placement_of(
            &self.dict,
            &self.ring,
            &ex,
            true,
            self.per_shard.hot_anchor_threshold,
        );
        let expected = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        if &expected != placement {
            return Err(crate::ownership::OwnershipError::PlacementDecisionMismatch.into());
        }
        let (placement_shards, outcome): (Vec<usize>, AddOutcome) = match target {
            Target::Reject => return Ok((0, AddOutcome::RejectedClassD)),
            // The broad lane is replicated to every shard (ADR-080).
            Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                ((0..self.shards.len()).collect(), AddOutcome::Replicated)
            }
            Target::Selective(shards) => (
                shards.clone(),
                AddOutcome::Placed {
                    shards: shards.clone(),
                },
            ),
        };
        let write = PlacedWrite {
            ex: &ex,
            logical: id,
            version,
            text: dsl,
            tags,
            placement,
        };

        // What this coordinator knows about copies it is not about to replace. A queued
        // repair or an unconverged directory means a shard outside the new placement may
        // still hold a stale copy, which only a sweep removes.
        let repair_pending = self
            .pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&id);
        let strays_possible = repair_pending || !self.logical_ids_converged();
        let known_absent = fresh && !repair_pending && self.logical_ids_authoritative();

        // Install the new version on every placement shard.
        let mut fan = Fanout::default();
        let mut moving = None;
        let mut cleanup = Cleanup::None;
        if known_absent {
            // No copy exists, so no reader can lose a version: place the new one. A shard
            // that nevertheless replaced a copy contradicts the directory.
            for &s in &placement_shards {
                let status = fan.replace(&self.shards, s, &write, ReplaceMode::Unconditional);
                if matches!(status, Some(ReplaceStatus::Replaced { .. })) {
                    cleanup = Cleanup::Move;
                }
            }
        } else {
            let mut declined: Vec<usize> = Vec::new();
            for &s in &placement_shards {
                if let Some(ReplaceStatus::Absent | ReplaceStatus::PlacementMismatch) =
                    fan.replace(&self.shards, s, &write, ReplaceMode::IfSamePlacement)
                {
                    declined.push(s);
                }
            }
            if declined.is_empty() {
                // Every placement shard that answered switched versions atomically.
                if strays_possible {
                    cleanup = Cleanup::Strays;
                }
            } else {
                // The placement is moving (or the copies disagree). The declining shards
                // changed nothing, so the whole rewrite still fits inside the fence.
                moving = Some(self.move_fence.begin_move());
                for &s in &declined {
                    fan.replace(&self.shards, s, &write, ReplaceMode::Unconditional);
                }
                cleanup = Cleanup::Move;
            }
            if !fan.failed.is_empty() {
                // A placement shard that failed proved nothing about where the old copies
                // are: this may be a move whose first step never landed.
                cleanup = Cleanup::Move;
            }
        }

        // Then remove copies elsewhere — unless an install failed. A shard without the
        // new version means a copy elsewhere may be the only one some title still
        // reaches, so the removal waits for the repair that completes the install.
        let installed = fan.failed.is_empty();
        if installed {
            match cleanup {
                Cleanup::None => {}
                // Removing a stale extra copy can only take a duplicate away: no fence.
                Cleanup::Strays => fan.sweep(&self.shards, &placement_shards, id),
                Cleanup::Move => {
                    let _move = moving
                        .take()
                        .unwrap_or_else(|| self.move_fence.begin_move());
                    fan.sweep(&self.shards, &placement_shards, id);
                }
            }
        }
        drop(moving);

        if !fan.failed.is_empty() {
            let deferred: Vec<usize> = if installed || cleanup == Cleanup::None {
                Vec::new()
            } else {
                (0..self.shards.len())
                    .filter(|s| !placement_shards.contains(s))
                    .collect()
            };
            fan.failed.sort_unstable();
            fan.failed.dedup();
            // `applied` reports the shards that now HOLD the new version, not every shard
            // that merely completed a tombstone (review finding).
            return Err(self.note_partial_deferring(
                ClusterMutation::Upsert {
                    logical: id,
                    version,
                    dsl: dsl.to_string(),
                    tags: tags.to_vec(),
                    placement: placement.clone(),
                },
                id,
                fan.applied,
                fan.failed,
                &deferred,
                fan.first_err,
            ));
        }
        self.clear_pending(id);
        Ok((fan.removed, outcome))
    }
}
