//! How a request reaches the coordinator's cluster (ADR-210).
//!
//! The engine keeps a rebuild apart from everything but a search itself (its layout lock,
//! ADR-209), so the server holds it with no lock of its own and a search goes straight to
//! it. What the server adds is admission, for requests that must not sit inside the engine
//! for as long as a rebuild takes:
//!
//! - A **rebuild** (a vocabulary change, a resize) holds the topology guard and write
//!   admission alone for its whole run ([`ClusterAppState::admit_rebuild`], or the resize
//!   handler's timed form of the same two locks, in that order).
//! - A **write** shares write admission, and an operation that moves shards or changes
//!   membership takes the topology guard, with its own time budget. Both wait for a rebuild
//!   there, where they can give up, and not inside the engine.
//! - A **search** takes neither. One that returns sources or an explanation takes the
//!   engine's mutation-frozen view, which waits for the writes in flight and for the moment
//!   a rebuild swaps its layout in, and for nothing else.
//!
//! The search pool is a thread budget and nothing more. Its workers take no lock that a
//! rebuild holds or waits for, and nothing that runs in the pool may: a worker that waited
//! behind a rebuild would hold up the workers that are waiting for it (ADR-207 describes the
//! deadlock this used to need a gate against).

use std::time::Duration;

use parking_lot::RwLockWriteGuard;

use reverse_rusty::cluster::{ClusterReadView, ShardError};

use super::ClusterAppState;

/// What a rebuild holds at the server for its whole run: no movement, no membership change
/// and no write beside it.
pub(crate) struct RebuildAdmission<'a> {
    // Declared in the order they are released: writes first, then topology.
    _writes: RwLockWriteGuard<'a, ()>,
    _topology: RwLockWriteGuard<'a, ()>,
}

impl ClusterAppState {
    /// Admit a rebuild: the topology guard, then write admission, both alone. Call it on a
    /// blocking thread.
    ///
    /// A remote resize (ADR-180) holds the topology guard for its whole copy and releases
    /// write admission once its write fence is up. Waiting for it here would park this
    /// request for as long as the copy takes, so a raised fence refuses the rebuild instead,
    /// whether it is found on arrival or while waiting. The engine checks it once more when
    /// the change begins.
    pub(crate) fn admit_rebuild(&self) -> Result<RebuildAdmission<'_>, ShardError> {
        const RECHECK: Duration = Duration::from_millis(25);
        let topology = loop {
            self.cluster.ensure_resize_write_fence_open()?;
            if let Some(topology) = self.topology_guard.try_write_for(RECHECK) {
                break topology;
            }
        };
        Ok(RebuildAdmission {
            _writes: self.write_admission.write(),
            _topology: topology,
        })
    }

    /// Run `work` in the search pool under the cluster's mutation-frozen view: the path of a
    /// search that returns sources or an explanation. Call it from a blocking thread that
    /// holds no lock.
    ///
    /// The view is taken on the caller's thread, before the work enters the pool. It keeps
    /// every mutation out, and a rebuild publishes its layout on the same barrier, so the
    /// search sees one layout and one corpus throughout. It does not share write admission:
    /// a rebuild holds that for its whole run, and this search must only wait for the swap.
    pub(crate) fn run_with_stable_view<T: Send>(
        &self,
        work: impl FnOnce(&ClusterReadView<'_>) -> T + Send,
    ) -> T {
        let stable_view = self.cluster.consistent_read_view();
        self.pool.install(|| work(&stable_view))
    }
}
