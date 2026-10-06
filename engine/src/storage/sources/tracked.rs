//! The lock around a source store's mutable contents.
//!
//! Every write access changes the version, so a reader that sees the same version twice
//! knows nothing was written in between (ADR-200). Writes cannot skip it: the contents are
//! reachable only through [`Tracked::read`] and [`Tracked::write`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Versions are unique across every store in the process, so two stores never share one
/// and a store that replaces another is always seen as different.
static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);

fn next_version() -> u64 {
    NEXT_VERSION.fetch_add(1, Ordering::Relaxed)
}

pub struct Tracked<T> {
    contents: RwLock<T>,
    version: AtomicU64,
}

impl<T> Tracked<T> {
    pub(super) fn new(contents: T) -> Self {
        Self {
            contents: RwLock::new(contents),
            version: AtomicU64::new(next_version()),
        }
    }

    pub(super) fn read(&self) -> RwLockReadGuard<'_, T> {
        self.contents.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Write access. The version changes whether or not the caller then changes anything,
    /// and it changes while the lock is held, before the caller can write.
    pub(super) fn write(&self) -> RwLockWriteGuard<'_, T> {
        let contents = self
            .contents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        self.version.store(next_version(), Ordering::Release);
        contents
    }

    /// The version of the contents as last written.
    pub(super) fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }
}
