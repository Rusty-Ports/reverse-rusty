//! Paged live-corpus export for remote resize (ADR-180): a fixed id snapshot, then bounded
//! document pages, each fetched under one short engine lock.

use std::time::Instant;

use super::logical_ids::DuplicateRows;
use super::{LocalShard, ShardError};
use crate::cluster::shard::LiveTaggedQuery;

impl LocalShard {
    /// Whether this shard persists to a data directory (a checkpoint survives a restart).
    pub(crate) fn is_durable(&self) -> bool {
        self.data_dir.is_some()
    }

    /// The sorted live logical ids to export, refusing more than `max_documents` live rows. The
    /// lock wait, scan, and sort all observe `deadline`. The ids come from index rows, so a live
    /// row whose source is missing is still listed and fails its document fetch instead of
    /// vanishing. An id held by several live rows is exported once, as the one document its
    /// source store keeps.
    pub(crate) fn live_source_ids(
        &self,
        max_documents: usize,
        deadline: Instant,
    ) -> Result<Vec<u64>, ShardError> {
        self.snapshot_live_ids(max_documents, deadline, DuplicateRows::Collapse)
    }

    /// Fetch one page of documents for ids from [`Self::live_source_ids`], waiting for the engine
    /// lock no later than `deadline`. A document that is no longer live, or whose source
    /// disagrees with its exact row, fails the export: the caller holds writes paused, so any
    /// change means the snapshot is no longer complete.
    pub(crate) fn live_source_page(
        &self,
        ids: &[u64],
        deadline: Instant,
    ) -> Result<Vec<LiveTaggedQuery>, ShardError> {
        let engine = self.lock_until(deadline)?;
        let mut page = Vec::with_capacity(ids.len());
        for &logical in ids {
            match engine.live_source_document(logical) {
                Ok(Some(document)) => page.push(document),
                Ok(None) => {
                    return Err(ShardError::Protocol(format!(
                        "live corpus changed during export: logical id {logical} is no longer live"
                    )));
                }
                Err(logical) => return Err(ShardError::SourceUnavailable(logical)),
            }
        }
        Ok(page)
    }
}
