//! Base segments a durable engine serves from memory because their file could not be
//! written (ADR-051), and the rule that no commit leaves one behind (ADR-190).

use super::{BaseSegment, Engine};
use crate::storage::MmapSegment;
use std::sync::Arc;

impl Engine {
    /// Whether a base segment of this durable engine exists only in memory: a flush or
    /// vocabulary rebuild whose segment write failed fell back to it (ADR-051). No manifest
    /// lists such a segment, so its rows are durable only as WAL frames.
    pub(in crate::segment) fn has_unpersisted_base_segment(&self) -> bool {
        self.config.data_dir.is_some()
            && self
                .segments
                .iter()
                .any(|segment| matches!(segment.as_ref(), BaseSegment::Memory(_)))
    }

    /// Whether the base segments are exactly the ones the last committed manifest lists.
    /// False while a flush or its commit has failed: a segment sealed since then is not in
    /// that manifest, so the WAL still holds its rows.
    pub(in crate::segment) fn base_segments_are_committed(&self) -> bool {
        self.segment_generations.len() == self.committed_segment_generations.len()
            && self
                .segment_generations
                .iter()
                .zip(&self.committed_segment_generations)
                .all(|(live, committed)| Arc::ptr_eq(live, committed))
    }

    /// Write every base segment that exists only in memory to disk and put the mapped file
    /// in its place. Returns whether none is left in memory.
    ///
    /// Every manifest commit calls this first. A manifest lists on-disk segments only and
    /// records a WAL watermark meaning "every mutation up to here is in these segments";
    /// the WAL is checkpointed and reset on the strength of it. Committing around an
    /// in-memory segment would make that statement false for its rows and then discard
    /// their only durable copy. So the segment reaches disk first, or the commit does not
    /// happen and the WAL stays as it is.
    ///
    /// The file holds the same rows at the same local ids, with the liveness they have
    /// now, and takes the segment's position and generation. Addresses handed out earlier
    /// stay valid, and a delete already applied to the segment is part of the file.
    pub(in crate::segment) fn persist_fallback_segments(&mut self) -> bool {
        let Some(dir) = self.config.data_dir.clone() else {
            return true;
        };
        for index in 0..self.segments.len() {
            let held = Arc::clone(&self.segments[index]);
            let BaseSegment::Memory(segment) = held.as_ref() else {
                continue;
            };
            let path = dir.join("segments").join(self.next_segment_filename());
            let mapped = crate::storage::write_segment(segment, &path)
                .and_then(|()| MmapSegment::open(&path));
            match mapped {
                Ok(mut mapped) => {
                    // Process-local, not in the file: carry it over (see `make_base_segment`).
                    mapped.vocab_epoch = segment.vocab_epoch;
                    self.segments[index] = Arc::new(BaseSegment::Mmap(mapped));
                }
                Err(error) => {
                    self.best_effort_remove_segment(&path);
                    self.persistence_healthy = false;
                    self.emit(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SegmentWrite,
                        detail: format!(
                            "a segment served from memory still cannot be written to {}; \
                             nothing is committed and its rows stay in the WAL",
                            path.display()
                        ),
                        error: error.to_string(),
                    });
                    return false;
                }
            }
        }
        true
    }
}
