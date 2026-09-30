//! Staged bulk load of a fresh remote-resize target slot (ADR-180): segments are sealed as rows
//! arrive, and the source store and checkpoint sidecar are written once when the load finishes.

use super::{LocalShard, ShardError};
use crate::segment::{IngestReport, PlacedQuery};

impl LocalShard {
    /// Rows per staged segment: the engine's memtable flush threshold, so a staged slot gets the
    /// segment shape ordinary writes would give it.
    pub(crate) fn staged_segment_rows(&self) -> usize {
        self.lock().config().memtable_flush_threshold.max(1)
    }

    /// Seal `items` into one segment without rewriting the source store or the sidecar.
    pub(crate) fn ingest_staged(&self, items: &[PlacedQuery]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted_staged(items);
        Self::publish(&eng, &self.snapshot);
        report
    }

    /// Finish a staged load: compact to the configured policy, write the source store once, then
    /// the checkpoint sidecar's segment registry. Fails when a staged segment, the store, or the sidecar could not be persisted: a
    /// restart reopens the slot from its sidecar, so a stale one would silently empty the load.
    pub(crate) fn finish_staged_load(&self) -> Result<(), ShardError> {
        let mut eng = self.lock();
        // Staged segments are sealed without a memtable flush, the only place the compaction
        // policy normally runs, so apply it here before the load can be proven: otherwise a large
        // load would leave every search probing far more segments than `max_segments` allows.
        // Each merge replaces at least two segments with one, so this ends.
        let mut merges_left = eng.num_segments();
        while merges_left > 0 && eng.maybe_compact().is_some() {
            merges_left -= 1;
        }
        Self::publish(&eng, &self.snapshot);
        if !eng.persist_staged_sources() {
            return Err(ShardError::Log(
                "staged load could not persist its segments and sources".into(),
            ));
        }
        self.write_sidecar_segments(&eng)
            .map_err(|(detail, error)| ShardError::Log(format!("staged load: {detail}: {error}")))
    }
}
