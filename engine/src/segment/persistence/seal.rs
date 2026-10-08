//! Sealing the memtable before the top-64 mask is assigned for the first time (ADR-188).

use super::{fresh_segment_generation, Engine};
use std::sync::Arc;

impl Engine {
    /// Seal the memtable before the top-64 mask is assigned for the first time.
    ///
    /// A memtable row was compiled with no mask, and on a durable engine it also sits in
    /// the WAL as text. Once the mask exists, a restart would compile that text again and
    /// could plan it into the opt-in lane, hiding a query that default reads returned.
    /// Sealing first stores the row's class in a segment and retires its WAL frame, so no
    /// row is ever compiled on both sides of the first assignment. An engine without a WAL
    /// never replays, and a finalized dictionary never reassigns (which covers every
    /// cluster shard), so both need nothing.
    ///
    /// All-or-nothing, unlike [`flush`](Engine::flush): that hands the memtable over
    /// before it knows whether the segment can be written, and on failure leaves the rows
    /// in an in-memory segment that no manifest lists. Here the segment is built from a
    /// copy and the memtable is replaced only once the manifest names it. On any failure
    /// the engine is exactly as it was, the batch fails with the mask still unassigned,
    /// and a retry seals again.
    ///
    /// Rows a failed `flush` already left in a segment the manifest does not list are in the
    /// same position as memtable rows: compiled without a mask, durable only as WAL text.
    /// The commit below covers that segment too, writing it to disk first if it is still in
    /// memory (ADR-190), or fails, so the mask is never assigned while one exists.
    pub(in crate::segment) fn seal_before_first_mask(&mut self) -> std::io::Result<()> {
        if self.dict.is_finalized() || self.wal.is_none() || !self.owns_manifest {
            return Ok(());
        }
        if self.memtable.is_empty() {
            if !self.base_segments_are_committed() {
                if !self.commit_sources_and_manifest() {
                    return Err(std::io::Error::other(
                        "an earlier flush left queries that are not on disk yet and they \
                         still cannot be written; the batch was not ingested",
                    ));
                }
                self.checkpoint_wal();
                self.reset_wal_if_safe();
            }
            return Ok(());
        }
        let started = std::time::Instant::now();
        let mut sealed = (*self.memtable).clone();
        sealed.build_filter();
        let entries = sealed.len();
        let (base, path) = self.build_durable_base(sealed)?;

        let taken = self.take_memtable();
        self.segments.push(Arc::new(base));
        self.segment_generations.push(fresh_segment_generation());
        self.refresh_phrase_capability();

        // The manifest write is the commit point. If it fails, put everything back.
        if !self.commit_sources_and_manifest() {
            self.segments.pop();
            self.segment_generations.pop();
            self.put_memtable_back(taken);
            self.refresh_phrase_capability();
            if let Some(path) = path {
                self.best_effort_remove_segment(&path);
            }
            return Err(std::io::Error::other(
                "could not seal the memtable before the first mask assignment; \
                 the batch was not ingested",
            ));
        }
        self.emit(crate::events::EngineEvent::Flush {
            entries,
            base_segments_after: self.segments.len(),
            duration_secs: started.elapsed().as_secs_f64(),
        });
        self.checkpoint_wal();
        self.reset_wal_if_safe();
        Ok(())
    }
}
