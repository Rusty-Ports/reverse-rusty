//! The optimistic move fence (ADR-185): how an unfenced read stays reader-atomic across an
//! upsert that moves a query between shards.
//!
//! An upsert that keeps its placement is atomic per shard and needs no coordination: the
//! same shard owns the row before and after, so each reader sees exactly one version. An
//! upsert that *moves* the query rewrites several shards, and no order of those steps is
//! safe for every title — ownership is the lowest common position for a selective row but
//! the per-title broad evaluator for a broad one — so between its first and last step a
//! reader could see neither version, or both.
//!
//! A move therefore runs inside this fence, a sequence counter in the style of a seqlock.
//! The mover makes the counter odd before its first shard call and even after its last. A
//! reader samples an even counter, fans out, and re-checks: unchanged means no move began
//! or ended while it was reading, so its view is one of the two consistent states; changed
//! means it may have mixed them, and it reads again. Readers pay two atomic loads and never
//! block a writer. Moves are rare — a DSL edit that changes the query's anchor or lane —
//! so retries are too; after a few, a reader excludes movers for one pass instead of
//! spinning.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Optimistic passes before a reader excludes movers. A read that overlaps this many
/// distinct moves is being starved by a write storm; one exclusive pass ends it.
const OPTIMISTIC_ATTEMPTS: usize = 4;

#[derive(Default)]
pub(super) struct MoveFence {
    /// Even ⇔ no move in progress. Bumped once when a move starts and once when it ends.
    seq: AtomicU64,
    /// Held by a mover for its whole move (movers are serialized, which keeps `seq`'s
    /// parity meaningful) and briefly by a reader that waits one out or excludes them.
    movers: Mutex<()>,
}

/// A move in progress; dropping it ends the move, including on an error return.
pub(super) struct MoveGuard<'a> {
    fence: &'a MoveFence,
    _serial: MutexGuard<'a, ()>,
}

impl Drop for MoveGuard<'_> {
    fn drop(&mut self) {
        self.fence.seq.fetch_add(1, Ordering::SeqCst);
    }
}

/// One pass of a fenced read, handed to the read body so it can tell an ownership
/// violation from the transient overlap a concurrent move explains.
pub(super) struct ReadPass<'a> {
    fence: &'a MoveFence,
    /// The counter this pass started under; `None` once movers are excluded.
    stamp: Option<u64>,
}

impl ReadPass<'_> {
    /// Whether a move started or finished since this pass began — the pass will be
    /// discarded and retried, so a duplicate id seen during it is not a bug.
    pub(super) fn overlapped_a_move(&self) -> bool {
        self.stamp
            .is_some_and(|stamp| self.fence.seq.load(Ordering::SeqCst) != stamp)
    }
}

impl MoveFence {
    fn serial(&self) -> MutexGuard<'_, ()> {
        self.movers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begin a placement-moving rewrite. Every shard call of the move must happen while
    /// the returned guard is alive.
    pub(super) fn begin_move(&self) -> MoveGuard<'_> {
        let serial = self.serial();
        self.seq.fetch_add(1, Ordering::SeqCst);
        MoveGuard {
            fence: self,
            _serial: serial,
        }
    }

    /// Run an unfenced read so that the value returned was computed with no move
    /// starting or finishing during it. `read` may run more than once and must not have
    /// effects that outlive a discarded pass.
    pub(super) fn read<T>(&self, mut read: impl FnMut(&ReadPass<'_>) -> T) -> T {
        for _ in 0..OPTIMISTIC_ATTEMPTS {
            let mut stamp = self.seq.load(Ordering::SeqCst);
            while stamp % 2 == 1 {
                // A move is in progress: wait for it rather than read a view to discard.
                drop(self.serial());
                stamp = self.seq.load(Ordering::SeqCst);
            }
            let pass = ReadPass {
                fence: self,
                stamp: Some(stamp),
            };
            let out = read(&pass);
            if !pass.overlapped_a_move() {
                return out;
            }
        }
        let _serial = self.serial();
        read(&ReadPass {
            fence: self,
            stamp: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::MoveFence;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_quiet_read_runs_once() {
        let fence = MoveFence::default();
        let runs = AtomicUsize::new(0);
        let out = fence.read(|pass| {
            runs.fetch_add(1, Ordering::Relaxed);
            assert!(!pass.overlapped_a_move());
            7
        });
        assert_eq!((out, runs.load(Ordering::Relaxed)), (7, 1));
    }

    #[test]
    fn a_read_that_overlaps_a_move_is_retried() {
        let fence = MoveFence::default();
        let runs = AtomicUsize::new(0);
        let out = fence.read(|pass| {
            let run = runs.fetch_add(1, Ordering::Relaxed);
            if run == 0 {
                // A whole move happens inside the first pass.
                drop(fence.begin_move());
                assert!(pass.overlapped_a_move());
            }
            run
        });
        assert_eq!(out, 1, "the overlapped pass is discarded");
        assert_eq!(runs.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_starved_read_excludes_movers_for_one_pass() {
        let fence = MoveFence::default();
        let runs = AtomicUsize::new(0);
        let out = fence.read(|pass| {
            let run = runs.fetch_add(1, Ordering::Relaxed);
            if run < super::OPTIMISTIC_ATTEMPTS {
                drop(fence.begin_move());
            } else {
                // Movers are excluded: this pass is authoritative whatever happened before.
                assert!(!pass.overlapped_a_move());
                assert!(fence.movers.try_lock().is_err());
            }
            run
        });
        assert_eq!(out, super::OPTIMISTIC_ATTEMPTS);
    }
}
