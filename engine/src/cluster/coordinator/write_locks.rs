//! Same-ID mutation exclusion with storage proportional to active callers.
//!
//! Registration includes waiters, so an entry cannot be retired and replaced
//! while anyone can still acquire its lock. Registration and retirement use the
//! same short-lived registry mutex; neither holds it across an ID wait or shard
//! operation. The bulk barrier is acquired before registering an ID.

use std::collections::{btree_map, BTreeMap};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Default)]
pub(super) struct LogicalWriteLocks {
    bulk: RwLock<()>,
    // A tree releases nodes as entries retire; a hash table would retain its
    // historical peak capacity unless we added a separate shrinking policy.
    entries: Mutex<BTreeMap<u64, Registration>>,
}

struct Registration {
    lock: Arc<IdLock>,
    // Counts registered holders AND waiters, independently of transient Arcs.
    users: usize,
}

#[derive(Default)]
struct IdLock {
    held: Mutex<bool>,
    ready: Condvar,
}

#[must_use]
pub(super) struct LogicalWriteGuard<'a> {
    table: &'a LogicalWriteLocks,
    logical: u64,
    lock: Arc<IdLock>,
    // Released after Drop retires the registration, so bulk access excludes
    // the complete lifetime of both holders and already-registered waiters.
    _bulk: RwLockReadGuard<'a, ()>,
}

impl LogicalWriteLocks {
    pub(super) fn lock(&self, logical: u64) -> LogicalWriteGuard<'_> {
        let bulk = self
            .bulk
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let lock = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let registration = entries.entry(logical).or_insert_with(|| Registration {
                lock: Arc::new(IdLock::default()),
                users: 0,
            });
            registration.users += 1;
            Arc::clone(&registration.lock)
        };
        let held = lock
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut held = lock
            .ready
            .wait_while(held, |held| *held)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *held = true;
        drop(held);
        LogicalWriteGuard {
            table: self,
            logical,
            lock,
            _bulk: bulk,
        }
    }

    pub(super) fn lock_bulk(&self) -> RwLockWriteGuard<'_, ()> {
        self.bulk
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for LogicalWriteGuard<'_> {
    fn drop(&mut self) {
        *self
            .lock
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        self.lock.ready.notify_one();

        let mut entries = self
            .table
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let btree_map::Entry::Occupied(mut entry) = entries.entry(self.logical) {
            entry.get_mut().users -= 1;
            if entry.get().users == 0 {
                entry.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests;
