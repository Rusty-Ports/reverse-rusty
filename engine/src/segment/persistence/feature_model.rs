//! ADR-184 feature-model commits: what a standalone manifest may record as its vocabulary,
//! and the vocabulary-only commit that records a change needing no segment rewrite.

use super::{BaseSegment, Engine};
use std::sync::Arc;

impl Engine {
    /// Durably record a vocabulary change that needs no segment rewrite (ADR-184): an empty
    /// corpus, or a registry change outside the matching-relevant projections. The manifest is
    /// rewritten with the same segment registry and source selection but **the previously
    /// committed WAL watermark** — this commit captures no memtable state, so advancing the
    /// watermark would let recovery skip a delete whose insert still replays from the WAL.
    /// Refused (returns `false`) while persistence is degraded: the in-memory registry may
    /// then be a strict subset of the committed one, and rewriting the manifest from it would
    /// silently drop the unreadable segment. A no-op `true` for in-memory engines and cluster
    /// shards, whose coordinator manifest records the vocabulary.
    pub(in crate::segment) fn commit_feature_model(&mut self) -> bool {
        if !self.owns_manifest || self.config.data_dir.is_none() {
            return true;
        }
        if !self.persistence_healthy {
            return false;
        }
        // Keeping the old watermark is sound only for the registry that watermark was
        // committed with: rows sealed into a segment since then would also replay from the
        // WAL tail. Every registry change commits (or rolls back) on its own, so this holds
        // whenever persistence is healthy; refuse rather than assume it.
        let current: Vec<_> = self
            .segments
            .iter()
            .zip(&self.segment_generations)
            .filter(|(segment, _)| matches!(segment.as_ref(), BaseSegment::Mmap(_)))
            .map(|(_, generation)| generation)
            .collect();
        let registry_committed = current.len() == self.committed_segment_generations.len()
            && current
                .iter()
                .zip(&self.committed_segment_generations)
                .all(|(live, committed)| Arc::ptr_eq(live, committed));
        if !registry_committed {
            return false;
        }
        let selected_source = self.source_file_name.clone();
        self.write_manifest_capturing(&selected_source, self.committed_wal_watermark)
    }

    /// The vocabulary blob this commit records: empty for a bare-normalizer engine, otherwise
    /// the installed vocabulary's verified [`recordable_json`](crate::vocab::Vocab::recordable_json).
    pub(super) fn recordable_vocab(&self) -> Result<Vec<u8>, String> {
        match self.vocab.as_deref() {
            None => Ok(Vec::new()),
            Some(vocab) => vocab
                .recordable_json(&self.norm, &self.dict)
                .map(String::into_bytes),
        }
    }
}
