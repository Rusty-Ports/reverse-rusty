//! The coordinator's cluster lock and search pool, and the gate between them (ADR-207).
//!
//! A worker of the search pool must never wait for a writer of the cluster lock. Workers
//! depend on each other: one holds the lock for a title and waits for that title's shard
//! fan-out, and the workers running the fan-out pick up other jobs of the pool while they
//! wait in turn, another title or another request. A reader of the lock waits when a writer
//! is queued. So if one of those jobs asked for the lock behind a queued writer, the writer
//! would be waiting for the first worker, and the first worker for this one.
//!
//! The types here keep a handler from getting that wrong:
//!
//! - Work reaches the pool only through [`SearchPool::enter`], which holds the pool's gate
//!   shared until the work has returned.
//! - The cluster's write lock is reachable only through
//!   [`ClusterAppState::write_cluster`] and its timed form, which hold the gate alone from
//!   before they ask for the lock until they have released it.
//!
//! So a writer is queued for the cluster lock, or holds it, only while the pool is empty,
//! and requests that arrive meanwhile wait on their own threads.
//!
//! This is the operation-permit pattern: every operation holds a shared permit, and a
//! transition takes all of them and waits for the operations in flight to drain. The gate
//! is itself a lock that prefers a waiting writer, so it is never taken twice for one
//! request: work that is already in the pool enters again without it.
//!
//! Lock order: write admission, then the gate, then the cluster lock.

use std::time::{Duration, Instant};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reverse_rusty::cluster::{ClusterEngine, ClusterReadView};

use super::ClusterAppState;

/// The coordinator's cluster, behind a lock whose write side only this module takes.
pub(crate) struct ClusterLock(RwLock<ClusterEngine>);

impl ClusterLock {
    pub(crate) fn new(cluster: ClusterEngine) -> Self {
        Self(RwLock::new(cluster))
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, ClusterEngine> {
        self.0.read()
    }

    pub(crate) fn try_read(&self) -> Option<RwLockReadGuard<'_, ClusterEngine>> {
        self.0.try_read()
    }

    pub(crate) fn try_read_for(
        &self,
        timeout: Duration,
    ) -> Option<RwLockReadGuard<'_, ClusterEngine>> {
        self.0.try_read_for(timeout)
    }

    /// The write lock with the gate left open, for a test that holds it to make a request
    /// wait. Nothing may be in the search pool meanwhile.
    #[cfg(test)]
    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, ClusterEngine> {
        self.0.write()
    }
}

/// The search pool and its gate.
pub(crate) struct SearchPool {
    workers: rayon::ThreadPool,
    /// Shared by every request with work in the pool. Alone for a writer of the cluster lock.
    gate: RwLock<()>,
}

impl SearchPool {
    pub(crate) fn new(workers: rayon::ThreadPool) -> Self {
        Self {
            workers,
            gate: RwLock::new(()),
        }
    }

    /// Open the gate for one request, before taking the cluster lock.
    ///
    /// Work that is already in the pool is under the hold of the request that brought it
    /// in, and that hold lasts until the work has returned. So a worker of the pool enters
    /// without taking the gate again. A second shared hold would wait behind a writer that
    /// is waiting for the first, which is the cycle the gate exists to prevent.
    pub(crate) fn enter(&self) -> Entered<'_> {
        let already_inside = self.workers.current_thread_index().is_some();
        Entered {
            workers: &self.workers,
            _open: (!already_inside).then(|| self.gate.read()),
        }
    }

    /// Enter the pool, run `work` in it, and leave.
    pub(crate) fn run<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        self.enter().run(work)
    }

    /// [`enter`](Self::enter) without waiting: `None` while a writer has closed the gate
    /// or is waiting to.
    #[cfg(test)]
    pub(crate) fn try_enter(&self) -> Option<Entered<'_>> {
        Some(Entered {
            workers: &self.workers,
            _open: Some(self.gate.try_read()?),
        })
    }

    /// The pool itself, for a test that occupies its workers.
    #[cfg(test)]
    pub(crate) fn workers(&self) -> &rayon::ThreadPool {
        &self.workers
    }
}

/// A request's place in the search pool: the gate stays open while this lives. Work that
/// entered from inside the pool holds nothing of its own (see [`SearchPool::enter`]).
pub(crate) struct Entered<'a> {
    workers: &'a rayon::ThreadPool,
    _open: Option<RwLockReadGuard<'a, ()>>,
}

impl Entered<'_> {
    pub(crate) fn run<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        self.workers.install(work)
    }
}

/// The cluster's write lock, with the gate of the search pool closed beneath it and write
/// admission held alone for at least as long.
pub(crate) struct ClusterWrite<'a> {
    // Declared first, so released first: the gate opens once the cluster can be read.
    cluster: RwLockWriteGuard<'a, ClusterEngine>,
    _closed: RwLockWriteGuard<'a, ()>,
    _admission: &'a RwLockWriteGuard<'a, ()>,
}

impl std::ops::Deref for ClusterWrite<'_> {
    type Target = ClusterEngine;

    fn deref(&self) -> &ClusterEngine {
        &self.cluster
    }
}

impl std::ops::DerefMut for ClusterWrite<'_> {
    fn deref_mut(&mut self) -> &mut ClusterEngine {
        &mut self.cluster
    }
}

impl ClusterAppState {
    /// Take the cluster's write lock: for a vocabulary rebuild, a resize, a cutover. The
    /// caller shows that it holds `write_admission` alone (ADR-206), and keeps it until the
    /// lock is released.
    ///
    /// It closes the gate of the search pool first. That waits for every request whose work
    /// is in the pool and keeps new ones on their own threads. Only then does it ask for the
    /// cluster lock, so no worker of the pool finds this writer queued.
    pub(crate) fn write_cluster<'a>(
        &'a self,
        admission: &'a RwLockWriteGuard<'a, ()>,
    ) -> ClusterWrite<'a> {
        self.check_admission(admission);
        let closed = self.pool.gate.write();
        ClusterWrite {
            cluster: self.cluster.0.write(),
            _closed: closed,
            _admission: admission,
        }
    }

    /// [`write_cluster`](Self::write_cluster) that gives up at `deadline`, or at once when
    /// there is none. It waits at the gate without holding the cluster lock.
    pub(crate) fn try_write_cluster_until<'a>(
        &'a self,
        admission: &'a RwLockWriteGuard<'a, ()>,
        deadline: Option<Instant>,
    ) -> Option<ClusterWrite<'a>> {
        self.check_admission(admission);
        let remaining = |deadline: Instant| deadline.checked_duration_since(Instant::now());
        let closed = match deadline {
            None => self.pool.gate.try_write()?,
            Some(deadline) => self.pool.gate.try_write_for(remaining(deadline)?)?,
        };
        let cluster = match deadline {
            None => self.cluster.0.try_write()?,
            Some(deadline) => self.cluster.0.try_write_for(remaining(deadline)?)?,
        };
        Some(ClusterWrite {
            cluster,
            _closed: closed,
            _admission: admission,
        })
    }

    fn check_admission(&self, admission: &RwLockWriteGuard<'_, ()>) {
        assert!(
            std::ptr::eq(
                RwLockWriteGuard::rwlock(admission),
                &raw const self.write_admission
            ),
            "the cluster's write lock is taken under this coordinator's write admission"
        );
    }

    /// Run `work` in the search pool under the cluster's mutation-frozen view: the path of a
    /// search that returns sources or an explanation (ADR-206). Call it from a blocking
    /// thread that holds no lock.
    ///
    /// The request shares write admission, as a write does. It does not need it to be kept
    /// apart from writes; the view does that. Sharing admission means it waits for a
    /// vocabulary change, a resize, a checkpoint or an exhaustive job, which it would wait
    /// for at the cluster lock or the view anyway, and not for other writes or for a whole
    /// bulk batch. The waits happen on the caller's thread, and the work runs inside the
    /// pool, within the configured thread budget.
    pub(crate) fn run_with_stable_view<T: Send>(
        &self,
        work: impl FnOnce(&ClusterReadView<'_>) -> T + Send,
    ) -> T {
        let _admission = self.write_admission.read();
        let entered = self.pool.enter();
        let cluster = self.cluster.read();
        let stable_view = cluster.consistent_read_view();
        entered.run(|| work(&stable_view))
    }
}
