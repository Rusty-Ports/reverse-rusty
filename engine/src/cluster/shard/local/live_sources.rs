//! Paged live-corpus export for remote resize (ADR-180): a fixed id snapshot, then bounded
//! document pages, each fetched under one short engine lock.

use std::time::Instant;

use super::{LocalShard, ShardError};
use crate::cluster::shard::LiveTaggedQuery;

impl LocalShard {
    /// Whether this shard persists to a data directory (a checkpoint survives a restart).
    pub(crate) fn is_durable(&self) -> bool {
        self.data_dir.is_some()
    }

    /// The sorted live logical ids to export, refusing a corpus above `max_documents`.
    pub(crate) fn live_source_ids(
        &self,
        max_documents: usize,
        deadline: Instant,
    ) -> Result<Vec<u64>, ShardError> {
        if Instant::now() >= deadline {
            return Err(ShardError::DeadlineExceeded);
        }
        let ids = self.lock().live_exact_logical_ids_sorted();
        if ids.len() > max_documents {
            return Err(ShardError::Config(format!(
                "live corpus of {} documents exceeds the export limit {max_documents}",
                ids.len()
            )));
        }
        Ok(ids)
    }

    /// Fetch one page of documents for ids from [`Self::live_source_ids`]. A document that is
    /// no longer live, or whose source disagrees with its exact row, fails the export: the
    /// caller holds writes paused, so any change means the snapshot is no longer complete.
    pub(crate) fn live_source_page(&self, ids: &[u64]) -> Result<Vec<LiveTaggedQuery>, ShardError> {
        let engine = self.lock();
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
