//! Cluster-wide live-corpus export for remote resize (ADR-180).

use std::collections::HashSet;

use super::{ClusterEngine, ShardError};

/// One exported logical query: the canonical source plus the metadata a rebuild re-applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportedQuery {
    pub logical_id: u64,
    pub dsl: String,
    pub version: u32,
    pub tags: Vec<(String, String)>,
}

impl ClusterEngine {
    /// Stream the cluster's deduplicated live corpus to `visit`, one position at a time. A
    /// query stored on several positions (replicated broad rows, multi-anchor placements) is
    /// visited once. Returns the number of distinct queries visited.
    ///
    /// The caller must hold writes paused for the duration: every position exports a fixed
    /// snapshot, and a corpus that changes mid-export fails loud rather than being skipped.
    pub fn export_live_corpus(
        &self,
        visit: &mut (dyn FnMut(ExportedQuery) -> Result<(), ShardError> + Send),
    ) -> Result<u64, ShardError> {
        let mut seen: HashSet<u64> = HashSet::new();
        for shard in &self.shards {
            shard.visit_live_sources(&mut |(logical_id, dsl, version, tags)| {
                if seen.insert(logical_id) {
                    visit(ExportedQuery {
                        logical_id,
                        dsl,
                        version,
                        tags,
                    })?;
                }
                Ok(())
            })?;
        }
        Ok(seen.len() as u64)
    }
}
