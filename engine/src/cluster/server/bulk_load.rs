//! A bulk load this node has been told is in progress (ADR-196).
//!
//! A coordinator loads a corpus one shard after another. If it stops part-way, some shards
//! hold their part and others hold nothing, and the only thing the next coordinator used to
//! ask was whether the cluster holds any query at all: it found some, skipped the load, and
//! served the part as if it were the whole. So the coordinator marks every slot before the
//! first bucket and clears the marks after the last, and a slot remembers its mark across a
//! restart of this node. A coordinator that finds a mark knows the load never finished.

use std::path::Path;
use std::sync::atomic::Ordering;

use tonic::Status;

use super::{shard_dir, ShardServer};

/// The mark's file in a durable slot's directory. Its existence is the state.
const MARKER_FILE: &str = "bulk_load.incomplete";

impl ShardServer {
    /// Record (`true`) or clear (`false`) the unfinished-bulk-load mark of a hosted slot. On a
    /// durable node the change is on disk before this returns.
    pub(in crate::cluster::server) fn set_bulk_load_incomplete(
        &self,
        shard_id: u32,
        incomplete: bool,
    ) -> Result<(), Status> {
        let slot = self.slot(shard_id)?;
        if let Some(root) = &self.data_dir {
            let dir = shard_dir(root, shard_id as usize);
            let recorded = if incomplete { mark(&dir) } else { unmark(&dir) };
            recorded.map_err(|error| {
                Status::internal(format!(
                    "recording the bulk-load state of shard {shard_id}: {error}"
                ))
            })?;
        }
        slot.bulk_load_incomplete
            .store(incomplete, Ordering::Release);
        Ok(())
    }

    /// Whether a hosted slot carries the mark. A durable node answers from its disk, so the
    /// answer is the same before and after a restart.
    pub(in crate::cluster::server) fn bulk_load_incomplete(
        &self,
        shard_id: u32,
    ) -> Result<bool, Status> {
        let slot = self.slot(shard_id)?;
        let Some(root) = &self.data_dir else {
            return Ok(slot.bulk_load_incomplete.load(Ordering::Acquire));
        };
        shard_dir(root, shard_id as usize)
            .join(MARKER_FILE)
            .try_exists()
            .map_err(|error| {
                Status::internal(format!(
                    "reading the bulk-load state of shard {shard_id}: {error}"
                ))
            })
    }
}

/// Create the marker and make its directory entry durable.
fn mark(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::File::create(dir.join(MARKER_FILE))?.sync_all()?;
    std::fs::File::open(dir)?.sync_all()
}

/// Remove the marker and make the removal durable. Clearing an absent mark is a success.
fn unmark(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(dir.join(MARKER_FILE)) {
        Ok(()) => std::fs::File::open(dir)?.sync_all(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
