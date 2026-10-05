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
//! spinning. A reader that finds a move in progress waits for it, up to its own deadline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Optimistic passes before a reader excludes movers. A read that overlaps this many
/// distinct moves is being starved by a write storm; one exclusive pass ends it.
const OPTIMISTIC_ATTEMPTS: usize = 4;

/// How often a reader with a deadline re-tries the mover lock for its exclusive pass.
const EXCLUSIVE_POLL: Duration = Duration::from_micros(200);

#[derive(Default)]
pub(super) struct MoveFence {
    /// Even ⇔ no move in progress. Bumped once when a move starts and once when it ends.
    seq: AtomicU64,
    /// Held by a mover for its whole move (movers are serialized, which keeps `seq`'s
    /// parity meaningful) and by a reader for its one exclusive pass.
    movers: Mutex<()>,
    /// Where readers wait for a move in progress to end; the mover signals it on exit.
    idle_lock: Mutex<()>,
    idle: Condvar,
}

/// A move in progress; dropping it ends the move, including on an error return.
pub(super) struct MoveGuard<'a> {
    fence: &'a MoveFence,
    _serial: MutexGuard<'a, ()>,
}

impl Drop for MoveGuard<'_> {
    fn drop(&mut self) {
        self.fence.seq.fetch_add(1, Ordering::SeqCst);
        // Taking the lock orders this wake-up after any reader that has already seen the
        // odd counter and is about to wait, so the wake-up cannot be lost.
        drop(lock(&self.fence.idle_lock));
        self.fence.idle.notify_all();
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

fn lock(mutex: &Mutex<()>) -> MutexGuard<'_, ()> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MoveFence {
    /// Begin a placement-moving rewrite. Every shard call of the move must happen while
    /// the returned guard is alive.
    pub(super) fn begin_move(&self) -> MoveGuard<'_> {
        let serial = lock(&self.movers);
        self.seq.fetch_add(1, Ordering::SeqCst);
        MoveGuard {
            fence: self,
            _serial: serial,
        }
    }

    /// The counter of a fence with no move in progress, waiting out a move until
    /// `deadline`. `None` when the deadline passed first.
    fn quiescent_stamp(&self, deadline: Option<Instant>) -> Option<u64> {
        let mut stamp = self.seq.load(Ordering::SeqCst);
        if stamp.is_multiple_of(2) {
            return Some(stamp);
        }
        let mut guard = lock(&self.idle_lock);
        loop {
            stamp = self.seq.load(Ordering::SeqCst);
            if stamp.is_multiple_of(2) {
                return Some(stamp);
            }
            guard = match deadline {
                None => self
                    .idle
                    .wait(guard)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(deadline) => {
                    let remaining = deadline.checked_duration_since(Instant::now())?;
                    self.idle
                        .wait_timeout(guard, remaining)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }

    /// The mover lock for a reader's exclusive pass, or `None` when `deadline` passed.
    fn exclude_movers(&self, deadline: Option<Instant>) -> Option<MutexGuard<'_, ()>> {
        let Some(deadline) = deadline else {
            return Some(lock(&self.movers));
        };
        loop {
            match self.movers.try_lock() {
                Ok(guard) => return Some(guard),
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    return Some(poisoned.into_inner())
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(EXCLUSIVE_POLL);
                }
            }
        }
    }

    /// Run an unfenced read so that the value returned was computed with no move
    /// starting or finishing during it. `read` may run more than once and must not have
    /// effects that outlive a discarded pass.
    pub(super) fn read<T>(&self, mut read: impl FnMut(&ReadPass<'_>) -> T) -> T {
        // Without a deadline every wait is unbounded, so the first attempt always completes.
        loop {
            if let Some(out) = self.read_until(None, &mut read) {
                return out;
            }
        }
    }

    /// [`read`](Self::read) for a caller with a deadline: `None` when the deadline passed
    /// while a move was holding the read back, so the caller can fail with its own
    /// deadline error instead of being held past it.
    pub(super) fn read_until<T>(
        &self,
        deadline: Option<Instant>,
        mut read: impl FnMut(&ReadPass<'_>) -> T,
    ) -> Option<T> {
        for _ in 0..OPTIMISTIC_ATTEMPTS {
            let pass = ReadPass {
                fence: self,
                stamp: Some(self.quiescent_stamp(deadline)?),
            };
            let out = read(&pass);
            if !pass.overlapped_a_move() {
                return Some(out);
            }
        }
        let _serial = self.exclude_movers(deadline)?;
        Some(read(&ReadPass {
            fence: self,
            stamp: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{MoveFence, OPTIMISTIC_ATTEMPTS};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

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
            if run < OPTIMISTIC_ATTEMPTS {
                drop(fence.begin_move());
            } else {
                // Movers are excluded: this pass is authoritative whatever happened before.
                assert!(!pass.overlapped_a_move());
                assert!(fence.movers.try_lock().is_err());
            }
            run
        });
        assert_eq!(out, OPTIMISTIC_ATTEMPTS);
    }

    #[test]
    fn a_read_waits_for_a_move_in_progress_and_then_runs() {
        let fence = MoveFence::default();
        let runs = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let moving = fence.begin_move();
            let reader = scope.spawn(|| fence.read(|_| runs.fetch_add(1, Ordering::Relaxed)));
            std::thread::sleep(Duration::from_millis(50));
            let ran_during_the_move = runs.load(Ordering::Relaxed);
            drop(moving); // end the move before asserting, so the reader can finish
            assert_eq!(reader.join().expect("reader"), 0);
            assert_eq!(ran_during_the_move, 0, "the reader waited for the move");
        });
    }

    #[test]
    fn a_read_with_a_deadline_is_not_held_past_it_by_a_move() {
        let fence = MoveFence::default();
        let moving = fence.begin_move();
        let started = Instant::now();
        let out = fence.read_until(Some(started + Duration::from_millis(30)), |_| ());
        let waited = started.elapsed();
        drop(moving);
        assert!(
            out.is_none(),
            "the deadline passed while the move held the read back"
        );
        assert!(
            waited < Duration::from_secs(5),
            "returned at the deadline: {waited:?}"
        );
        assert_eq!(fence.read_until(Some(Instant::now()), |_| 3), Some(3));
    }
}
