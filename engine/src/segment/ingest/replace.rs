//! The placed atomic replace (ADR-185): the cluster shard's per-engine half of a
//! reader-atomic upsert.
//!
//! A cluster upsert used to reach a shard as two seam calls — delete, then insert —
//! each publishing its own snapshot, so a title matched between them saw neither
//! version. This funnel inserts the new version and tombstones the prior copies under
//! one `&mut Engine`, the placement-carrying twin of the standalone
//! [`apply_upsert`](Engine::apply_upsert): capture first, insert, and tombstone only
//! after the insert was accepted, so a rejected replace never deletes.

use super::{Arc, Engine, Extracted};
use crate::ownership::QueryPlacement;
use crate::segment::{HeldPlacement, ReplaceOutcome};

impl Engine {
    /// Every live copy of `logical`, as `(segment index, local)` with `usize::MAX`
    /// standing for the memtable — the reverse-index walk the delete funnel uses.
    fn live_copies(&self, logical: u64) -> Vec<(usize, u32)> {
        let mut copies = Vec::new();
        for (seg_idx, seg) in self.segments.iter().enumerate() {
            for &local in seg.locals_for_logical(logical) {
                if seg.is_alive(local) {
                    copies.push((seg_idx, local));
                }
            }
        }
        for &local in self.memtable.locals_for_logical(logical) {
            if self.memtable.is_alive(local) {
                copies.push((usize::MAX, local));
            }
        }
        copies
    }

    /// How the live copies of `logical` relate to `placement`: the precondition of a
    /// conditional replace. A same-placement upsert is the common case (a re-put, a
    /// tag or version edit, a bulk re-index) and needs no cross-shard coordination.
    pub fn held_placement(&self, logical: u64, placement: &QueryPlacement) -> HeldPlacement {
        let copies = self.live_copies(logical);
        if copies.is_empty() {
            return HeldPlacement::Absent;
        }
        let same = copies.iter().all(|&(seg_idx, local)| {
            if seg_idx == usize::MAX {
                self.memtable.placement(local).matches(placement)
            } else {
                self.segments[seg_idx].placement(local).matches(placement)
            }
        });
        if same {
            HeldPlacement::Same
        } else {
            HeldPlacement::Different
        }
    }

    /// Insert the new version of `logical` and tombstone every prior live copy in one
    /// critical section. The caller publishes one snapshot afterwards, so a reader
    /// sees the old version or the new one and never a gap or both.
    pub fn replace_extracted_with_placement(
        &mut self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &QueryPlacement,
    ) -> ReplaceOutcome {
        // Capture BEFORE inserting: the new row joins the same reverse index.
        let prior = self.live_copies(logical);
        if self
            .insert_extracted_with_placement(ex, logical, version, text, tags, placement)
            .is_none()
        {
            return ReplaceOutcome::Rejected;
        }
        let removed = prior.len();
        for (seg_idx, local) in prior {
            if seg_idx == usize::MAX {
                Arc::make_mut(&mut self.memtable).tombstone(local);
            } else if let Some(seg) = self.segments.get_mut(seg_idx) {
                Arc::make_mut(seg).tombstone(local);
            }
        }
        if removed == 0 {
            ReplaceOutcome::Inserted
        } else {
            self.refresh_phrase_capability();
            ReplaceOutcome::Replaced { removed }
        }
    }
}
