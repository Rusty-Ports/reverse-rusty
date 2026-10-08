//! What a build may find in its data directory, and the mark it leaves while it runs
//! (ADR-215).
//!
//! A durable build creates the shard directories, loads the corpus into them, and commits by
//! writing the cluster manifest. Until that write the directory holds shard state and no
//! manifest, and a start chooses between building and opening by whether a manifest is
//! there. A build that stopped part-way was therefore built over: each shard restored what
//! the unfinished build had left, and the corpus was loaded a second time on top.
//!
//! So a build says what it is doing. It writes a mark before it creates the first shard
//! directory, and the mark goes once a manifest exists. It writes the mark only into a
//! directory that holds nothing of a cluster, so shard state beside a mark was made by a
//! build that did not finish, and nothing was ever served from it: the next build removes
//! it and starts again. Shard state with no manifest and no mark is something else (a
//! cluster that has lost its manifest, or the leftovers of a release that left no mark) and
//! is refused.
//!
//! "Shard state" is a file under an entry named like a shard directory. A shard directory
//! that holds only directories holds no data: there is nothing in it to restore and nothing
//! to lose, and a build takes it as it takes an empty data directory.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::cluster::coordinator::{CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE};
use crate::cluster::shard::ShardError;

/// The mark's file in the cluster's data directory. Its existence is the state.
pub(in crate::cluster::coordinator) const BUILD_INCOMPLETE_FILE: &str = "build.incomplete";

/// What every shard directory's name starts with (`shard_000`, …).
const SHARD_DIR_PREFIX: &str = "shard_";

/// Take `dir` for a build, before the build creates anything in it.
///
/// - It holds a manifest: refused. A build does not replace a cluster.
/// - It holds the mark of a build that did not finish: that build's shard directories are
///   removed and the mark stays for this one. If a cluster log is there as well, the
///   unfinished build got as far as its manifest and the manifest has since been lost:
///   refused.
/// - It holds a file under a shard directory, or a cluster log, and no mark: refused.
/// - Otherwise the mark is written, and is on disk before this returns.
///
/// A refusal changes nothing in the directory.
pub(super) fn begin(dir: &Path) -> Result<(), ShardError> {
    let io = |what: &str, e: std::io::Error| {
        ShardError::Log(format!("{what} {} for a build: {e}", dir.display()))
    };
    std::fs::create_dir_all(dir).map_err(|e| io("creating", e))?;
    let has = |name: &str| dir.join(name).try_exists().map_err(|e| io("reading", e));
    if has(CLUSTER_MANIFEST_FILE)? {
        return Err(ShardError::Config(format!(
            "{} already holds a cluster ({CLUSTER_MANIFEST_FILE}); open it. A build does not \
             replace a cluster.",
            dir.display()
        )));
    }
    let marked = has(BUILD_INCOMPLETE_FILE)?;
    let log = has(CLUSTER_LOG_FILE)?;
    let shards = shard_entries(dir).map_err(|e| io("reading", e))?;
    let mut with_data = Vec::new();
    for entry in &shards {
        if holds_a_file(entry).map_err(|e| io("reading", e))? {
            with_data.push(entry);
        }
    }
    if marked {
        if log {
            return Err(ShardError::Config(format!(
                "{} holds the mark of a build that did not finish ({BUILD_INCOMPLETE_FILE}) \
                 and a cluster log ({CLUSTER_LOG_FILE}), but no manifest \
                 ({CLUSTER_MANIFEST_FILE}). That build had written its manifest, and the \
                 manifest is gone. Restore the directory from a backup. Nothing was changed.",
                dir.display()
            )));
        }
        for entry in &shards {
            let removed = if entry.is_dir() {
                std::fs::remove_dir_all(entry)
            } else {
                std::fs::remove_file(entry)
            };
            removed.map_err(|e| io("removing what an unfinished build left in", e))?;
        }
        return sync(dir).map_err(|e| io("syncing", e));
    }
    if log || !with_data.is_empty() {
        let found = with_data
            .iter()
            .filter_map(|entry| entry.file_name())
            .map(|name| name.to_string_lossy().into_owned())
            .chain(log.then(|| CLUSTER_LOG_FILE.to_string()))
            .collect::<Vec<_>>()
            .join(", ");
        let what_it_is = if log {
            "It was a working cluster and its manifest is gone: restore the directory from a \
             backup."
        } else {
            "Either a first start by an earlier release stopped before it finished (nothing \
             was ever served from this directory: empty it and start again), or a cluster \
             has lost its manifest and its log (restore the directory from a backup)."
        };
        return Err(ShardError::Config(format!(
            "{} holds cluster data ({found}) and no manifest ({CLUSTER_MANIFEST_FILE}), and \
             no build was in progress in it. {what_it_is} Nothing was changed.",
            dir.display()
        )));
    }
    let write = || -> std::io::Result<()> {
        let mut mark = std::fs::File::create(dir.join(BUILD_INCOMPLETE_FILE))?;
        mark.write_all(
            b"A cluster build is in progress in this directory, or stopped before it finished.\n\
              The next start removes the shard directories beside this file and builds again.\n",
        )?;
        mark.sync_all()?;
        sync(dir)
    };
    write().map_err(|e| io("marking", e))
}

/// Clear the mark: a manifest exists, so the directory holds a cluster and a start opens
/// it. Called at the end of a build, and by an open that finds the mark of a build that
/// stopped between its manifest and here. The removal is on disk before this returns.
pub(super) fn finish(dir: &Path) -> Result<(), ShardError> {
    let cleared = match std::fs::remove_file(dir.join(BUILD_INCOMPLETE_FILE)) {
        Ok(()) => sync(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    };
    cleared
        .map_err(|e| ShardError::Log(format!("clearing the build mark in {}: {e}", dir.display())))
}

/// The entries of `dir` that are named like a shard directory.
fn shard_entries(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(SHARD_DIR_PREFIX)
        {
            found.push(entry.path());
        }
    }
    found.sort();
    Ok(found)
}

/// Whether `entry` is a file, or a directory with a file anywhere under it.
fn holds_a_file(entry: &Path) -> std::io::Result<bool> {
    if !entry.is_dir() {
        return Ok(true);
    }
    for child in std::fs::read_dir(entry)? {
        if holds_a_file(&child?.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn sync(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
