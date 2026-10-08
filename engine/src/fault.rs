//! Named steps of durable operations, and a way for a test to stop at one (ADR-221).
//!
//! A durable operation is a sequence of filesystem steps: create a temporary file, sync it,
//! rename it over the old one, sync the directory, append to a log, remove what was
//! replaced. Whether the operation is safe depends on what is true between the steps, and
//! a test can only look there if it can stop there.
//!
//! The functions that perform those steps call [`step`] first, with the step's name and the
//! path it is about to act on. In a build that ships that call is nothing. In a test build
//! of this crate a [`Scope`] opened on a data directory records every step taken under it
//! and can be told to fail one, so a test can first learn which steps an operation takes and
//! then fail each of them in turn.

use std::io;
use std::path::Path;

#[cfg(test)]
mod plan;
#[cfg(test)]
pub(crate) use plan::{Scope, Step};

/// The step `name` is about to act on `path`. Returns the fault a test planned for it, if
/// there is one; the caller then does not perform the step.
#[cfg(test)]
#[inline]
pub(crate) fn step(name: &'static str, path: &Path) -> io::Result<()> {
    plan::on_step(name, path).map(|_| ())
}

/// The step `name` is about to act on `path`. Nothing in this build.
#[cfg(not(test))]
#[inline]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn step(_name: &'static str, _path: &Path) -> io::Result<()> {
    Ok(())
}

/// Sync `file`, which is at `path`: the step `sync`.
///
/// Under a [`Scope`] the step is recorded and can fail, and the file is not synced: a test
/// that never loses power cannot see the difference, and an operation's hundred real syncs
/// are most of what its test would spend.
#[inline]
pub(crate) fn sync(file: &std::fs::File, path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if plan::on_step("sync", path)? {
        return Ok(());
    }
    #[cfg(not(test))]
    let _ = path;
    file.sync_all()
}

/// Sync the directory that holds the entry `entry`, so that a create, a rename or a remove
/// of that entry is on disk: the step `sync_dir`. Under a [`Scope`], as [`sync`].
#[inline]
pub(crate) fn sync_dir_of(entry: &Path) -> io::Result<()> {
    #[cfg(test)]
    if plan::on_step("sync_dir", entry)? {
        return Ok(());
    }
    match crate::storage::directory_of(entry) {
        Some(directory) => std::fs::File::open(directory)?.sync_all(),
        None => Ok(()),
    }
}
