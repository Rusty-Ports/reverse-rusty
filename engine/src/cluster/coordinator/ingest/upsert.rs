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
    extract_readonly, fanout, planned, AddOutcome, ClusterEngine, ClusterMutation, ShardError,
    Target,
};
use crate::cluster::coordinator::layout::Layout;
use crate::cluster::shard::{Applied, FannedWrite, PlacedWrite, ReplaceMode, ReplaceStatus, Shard};

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

    /// Replace on each of `targets`, all at once (ADR-224), leaving out any shard where an
    /// earlier step already failed (its repair re-drives the whole upsert). Returns the
    /// status of every shard whose call succeeded, in the order of `targets`.
    fn replace(
        &mut self,
        shards: &[Box<dyn Shard>],
        targets: &[usize],
        write: &PlacedWrite<'_>,
        mode: ReplaceMode,
    ) -> Vec<(usize, ReplaceStatus)> {
        let targets: Vec<usize> = targets
            .iter()
            .copied()
            .filter(|shard| !self.failed.contains(shard))
            .collect();
        let sent = FannedWrite::Replace { write, mode };
        let mut answered = Vec::with_capacity(targets.len());
        for (shard, answer) in fanout::on_each(shards, &targets, &sent) {
            match answer {
                Ok(Applied::Replaced(status)) => {
                    if let ReplaceStatus::Replaced { removed } = status {
                        self.removed += removed;
                    }
                    if status.applied() {
                        self.applied.push(shard);
                    }
                    answered.push((shard, status));
                }
                Ok(other) => self.fail(shard, fanout::answered(shard, other, &sent)),
                Err(error) => self.fail(shard, error),
            }
        }
        answered
    }

    /// Tombstone any copy on every shard outside `placement_shards` (idempotent on a
    /// shard that holds none), on all of them at once. Only called once every placement
    /// shard holds the new version: until then a copy elsewhere may be the only one a
    /// title can still reach.
    fn sweep(&mut self, shards: &[Box<dyn Shard>], placement_shards: &[usize], logical: u64) {
        debug_assert!(
            self.failed.is_empty(),
            "sweeping before the install finished"
        );
        let elsewhere: Vec<usize> = (0..shards.len())
            .filter(|shard| !placement_shards.contains(shard))
            .collect();
        let sent = FannedWrite::Delete { logical };
        for (shard, answer) in fanout::on_each(shards, &elsewhere, &sent) {
            match answer {
                Ok(Applied::Deleted(removed)) => self.removed += removed,
                Ok(other) => self.fail(shard, fanout::answered(shard, other, &sent)),
                Err(error) => self.fail(shard, error),
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
    #[allow(clippy::too_many_arguments)]
    pub(in crate::cluster::coordinator) fn apply_upsert(
        &self,
        layout: &Layout,
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
        let ex = extract_readonly(&ast, &layout.norm, &layout.dict, &mut lc);
        // Force accept=true: apply is reached ONLY for already-accepted writes (live upsert
        // classified + accepted before logging; replay sees only logged=accepted frames), so this
        // placement is configuration-independent — a knob flip on reopen neither drops nor
        // resurrects (codex review). The empty-class-D guard in `placement_of` still rejects a
        // never-stored empty query defensively.
        let (target, class) = planned(
            &layout.dict,
            &layout.ring,
            &ex,
            true,
            self.per_shard.hot_anchor_threshold,
        );
        let expected = target.placement(layout.generation, layout.shards.len() as u32)?;
        if &expected != placement {
            return Err(crate::ownership::OwnershipError::PlacementDecisionMismatch.into());
        }
        let (placement_shards, outcome): (Vec<usize>, AddOutcome) = match target {
            Target::Reject => return Ok((0, AddOutcome::RejectedClassD)),
            // The broad lane is replicated to every shard (ADR-080).
            Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => (
                (0..layout.shards.len()).collect(),
                AddOutcome::Replicated { class },
            ),
            Target::Selective(shards) => (
                shards.clone(),
                AddOutcome::Placed {
                    shards: shards.clone(),
                    class,
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
            let placed = fan.replace(
                &layout.shards,
                &placement_shards,
                &write,
                ReplaceMode::Unconditional,
            );
            if placed
                .iter()
                .any(|(_, status)| matches!(status, ReplaceStatus::Replaced { .. }))
            {
                cleanup = Cleanup::Move;
            }
        } else {
            let declined: Vec<usize> = fan
                .replace(
                    &layout.shards,
                    &placement_shards,
                    &write,
                    ReplaceMode::IfSamePlacement,
                )
                .into_iter()
                .filter(|(_, status)| !status.applied())
                .map(|(shard, _)| shard)
                .collect();
            if declined.is_empty() {
                // Every placement shard that answered switched versions atomically.
                if strays_possible {
                    cleanup = Cleanup::Strays;
                }
            } else {
                // The placement is moving (or the copies disagree). The declining shards
                // changed nothing, so the whole rewrite still fits inside the fence.
                moving = Some(self.move_fence.begin_move());
                fan.replace(
                    &layout.shards,
                    &declined,
                    &write,
                    ReplaceMode::Unconditional,
                );
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
                Cleanup::Strays => fan.sweep(&layout.shards, &placement_shards, id),
                Cleanup::Move => {
                    let _move = moving
                        .take()
                        .unwrap_or_else(|| self.move_fence.begin_move());
                    fan.sweep(&layout.shards, &placement_shards, id);
                }
            }
        }
        drop(moving);

        if !fan.failed.is_empty() {
            let deferred: Vec<usize> = if installed || cleanup == Cleanup::None {
                Vec::new()
            } else {
                (0..layout.shards.len())
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
