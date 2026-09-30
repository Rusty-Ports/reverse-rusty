//! Loading a staged remote-resize layout (ADR-180): place exported queries under this engine's
//! ring and send them to its shards in byte-bounded batches.

use super::{
    extract_readonly, placement_of, ClusterEngine, PlacedQuery, ShardError, TaggedEntry, Target,
};

/// Approximate request-size budget per ingest call, well under the default gRPC decode limit.
const LOAD_BATCH_BYTES: usize = 1024 * 1024;

impl ClusterEngine {
    /// Place `entries` (logical id, stored version, source, raw tags) under this engine's ring
    /// and ingest them into its shards, adding each position's accepted row count to `loaded`.
    /// Used only on a freshly built staged layout: it neither checks emptiness nor installs the
    /// logical-id directory, because the staged engine contributes only its shards and ring to the
    /// serving coordinator. Placement force-accepts, like log replay, so a stored class-D query is
    /// never dropped by the current admission knob; only an effectively empty query (stored
    /// nowhere) is skipped. A shard write error propagates and fails the resize.
    pub(in crate::cluster::coordinator) fn load_resize_batch(
        &self,
        entries: &[TaggedEntry],
        loaded: &mut [u64],
    ) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<PlacedQuery>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, version, text, tags) in entries {
            let Ok(ast) = crate::dsl::parse_for_recovery(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            // Re-place an already-admitted corpus the way apply/replay does: force-accept, so a
            // stored class-D query survives even when the current front-door knob is off.
            let target = placement_of(
                &self.dict,
                &self.ring,
                &ex,
                true,
                self.per_shard.hot_anchor_threshold,
            );
            let placement =
                target.placement(self.placement_generation(), self.shards.len() as u32)?;
            let positions: Vec<usize> = match target {
                Target::Reject => continue,
                Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                    (0..self.shards.len()).collect()
                }
                Target::Selective(positions) => positions,
            };
            for position in positions {
                buckets[position].push(PlacedQuery {
                    logical: *logical,
                    ex: ex.clone(),
                    dsl: text.clone(),
                    version: *version,
                    source_generation: None,
                    tags: tags.clone(),
                    tag_ids: Vec::new(),
                    rank: crate::rank::RankValues::default(),
                    placement: placement.clone(),
                });
            }
        }
        for (position, bucket) in buckets.into_iter().enumerate() {
            let mut start = 0;
            while start < bucket.len() {
                let mut end = start;
                let mut bytes = 0usize;
                while end < bucket.len() && (end == start || bytes < LOAD_BATCH_BYTES) {
                    let query = &bucket[end];
                    bytes = bytes.saturating_add(
                        64 + query.dsl.len()
                            + query
                                .tags
                                .iter()
                                .map(|(k, v)| k.len() + v.len() + 8)
                                .sum::<usize>(),
                    );
                    end += 1;
                }
                let report = self.shards[position].ingest_extracted(&bucket[start..end])?;
                if report.rejected_parse != 0 || report.rejected_class_d != 0 {
                    return Err(ShardError::Protocol(format!(
                        "staged position {position} rejected {} of {} re-placed queries",
                        report.rejected_parse + report.rejected_class_d,
                        end - start
                    )));
                }
                loaded[position] = loaded[position].saturating_add(report.ingested as u64);
                start = end;
            }
        }
        Ok(())
    }
}
