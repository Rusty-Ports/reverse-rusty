//! Bounded, complete live-corpus export protocol for remote resize (ADR-180).
//!
//! The server snapshots one slot's sorted live ids and streams their documents; the client
//! validates frame identity, a stable total, strictly increasing ids, and an explicit
//! completion frame whose count equals everything delivered. Anything else fails the export
//! rather than yielding a silently shortened corpus.

use super::logical_id_wire::invalid;
use super::proto;

/// Upper bound on documents one slot export may carry.
pub(crate) const MAX_LIVE_SOURCE_DOCUMENTS: usize = 100_000_000;
/// Documents fetched per engine-lock hold on the server.
pub(crate) const LIVE_SOURCES_PAGE: usize = 256;
/// Server-side cap on one export's duration, whatever the caller requests.
pub(crate) const MAX_EXPORT_DURATION: std::time::Duration = std::time::Duration::from_hours(1);

/// One exported document: logical id, source DSL, stored version, and raw tags.
pub(crate) type LiveSourceRow = (u64, String, u32, Vec<(String, String)>);

/// Validates a `LiveSources` frame sequence and hands each document to the caller.
pub(crate) struct LiveSourceCollector {
    shard_id: u32,
    generation: u64,
    num_shards: u32,
    max_documents: u64,
    total: Option<u64>,
    delivered: u64,
    last_id: Option<u64>,
    complete: bool,
}

impl LiveSourceCollector {
    pub(crate) fn new(request: &proto::LiveSourcesRequest) -> Self {
        Self {
            shard_id: request.shard_id,
            generation: request.placement_generation,
            num_shards: request.num_shards,
            max_documents: request.max_documents.min(MAX_LIVE_SOURCE_DOCUMENTS as u64),
            total: None,
            delivered: 0,
            last_id: None,
            complete: false,
        }
    }

    /// Validate one frame and pass its documents, in order, to `visit`.
    pub(crate) fn push<E>(
        &mut self,
        frame: proto::LiveSourcesFrame,
        mut visit: impl FnMut(LiveSourceRow) -> Result<(), E>,
    ) -> Result<(), CollectError<E>> {
        if self.complete {
            return Err(CollectError::Wire(invalid(
                "live-source frame after completion",
            )));
        }
        if frame.shard_id != self.shard_id
            || frame.placement_generation != self.generation
            || frame.num_shards != self.num_shards
        {
            return Err(CollectError::Wire(invalid(
                "live-source export identity mismatch",
            )));
        }
        if frame.total_documents > self.max_documents {
            return Err(CollectError::Wire(invalid(
                "live-source export exceeds its document limit",
            )));
        }
        if self
            .total
            .is_some_and(|previous| previous != frame.total_documents)
        {
            return Err(CollectError::Wire(invalid(
                "live-source export count changed",
            )));
        }
        self.total = Some(frame.total_documents);
        if frame.complete {
            if !frame.documents.is_empty() || self.delivered != frame.total_documents {
                return Err(CollectError::Wire(invalid(
                    "live-source completion count mismatch",
                )));
            }
            self.complete = true;
            return Ok(());
        }
        let remaining = frame.total_documents.saturating_sub(self.delivered);
        if frame.documents.is_empty() || frame.documents.len() as u64 > remaining {
            return Err(CollectError::Wire(invalid(
                "live-source frame is empty or over count",
            )));
        }
        for document in frame.documents {
            if self.last_id.is_some_and(|last| last >= document.logical_id) {
                return Err(CollectError::Wire(invalid(
                    "live-source ids are unordered or duplicated",
                )));
            }
            self.last_id = Some(document.logical_id);
            self.delivered += 1;
            let tags = document
                .tags
                .into_iter()
                .map(|tag| (tag.key, tag.value))
                .collect();
            visit((document.logical_id, document.dsl, document.version, tags))
                .map_err(CollectError::Visit)?;
        }
        Ok(())
    }

    /// The delivered count, only after a valid completion frame.
    pub(crate) fn finish(self) -> Result<u64, tonic::Status> {
        if self.complete {
            Ok(self.delivered)
        } else {
            Err(invalid("live-source export ended without completion"))
        }
    }
}

/// A frame failure: either the wire violated the protocol or the caller's visitor refused a
/// document.
pub(crate) enum CollectError<E> {
    Wire(tonic::Status),
    Visit(E),
}

#[cfg(test)]
mod tests;
