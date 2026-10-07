//! Placing a staged remote-resize layout's rows (ADR-180): place exported queries under this
//! engine's ring and hand them out per position in byte-bounded chunks.

use crate::cluster::coordinator::layout::Layout;

use super::{
    extract_readonly, placement_of, ClusterEngine, PlacedQuery, ShardError, TaggedEntry, Target,
};

/// Approximate size budget per staged-load request, well under the default gRPC decode limit.
const LOAD_BATCH_BYTES: usize = 1024 * 1024;

/// Conservative encoded size of one placed query in an ingest request.
fn placed_bytes(query: &PlacedQuery) -> usize {
    64 + query.dsl.len()
        + query
            .tags
            .iter()
            .map(|(key, value)| key.len() + value.len() + 8)
            .sum::<usize>()
        + 8 * query.placement.positions().len()
}

/// Receives each position's placed rows, one byte-bounded chunk at a time.
pub(in crate::cluster::coordinator) type PositionSink<'a> =
    dyn FnMut(usize, &[PlacedQuery]) -> Result<(), ShardError> + 'a;

fn unplaceable(logical: u64) -> ShardError {
    ShardError::Protocol(format!(
        "stored query {logical} cannot be re-placed under the new layout; the resize would drop it"
    ))
}

impl ClusterEngine {
    /// Place `entries` (logical id, stored version, source, raw tags) under this engine's ring
    /// and hand each position's rows to `emit` in byte-bounded chunks. Used only on a freshly built
    /// staged layout, whose shards the caller loads through a staged stream: it neither checks
    /// emptiness nor installs the logical-id directory, because the staged engine contributes only
    /// its shards and ring to the serving coordinator. Placement force-accepts, like log replay, so
    /// a stored class-D query is never dropped by the current admission knob. A stored query that
    /// no longer parses or places fails the resize rather than vanishing from the new layout. An
    /// `emit` error stops placement and is returned.
    pub(in crate::cluster::coordinator) fn place_resize_batch(
        &self,
        layout: &Layout,
        entries: &[TaggedEntry],
        emit: &mut PositionSink<'_>,
    ) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<PlacedQuery>> =
            (0..layout.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, version, text, tags) in entries {
            let Ok(ast) = crate::dsl::parse_for_recovery(text) else {
                return Err(unplaceable(*logical));
            };
            let ex = extract_readonly(&ast, &layout.norm, &layout.dict, &mut lc);
            // Re-place an already-admitted corpus the way apply/replay does: force-accept, so a
            // stored class-D query survives even when the current front-door knob is off.
            let target = placement_of(
                &layout.dict,
                &layout.ring,
                &ex,
                true,
                self.per_shard.hot_anchor_threshold,
            );
            let placement = target.placement(layout.generation, layout.shards.len() as u32)?;
            let positions: Vec<usize> = match target {
                Target::Reject => return Err(unplaceable(*logical)),
                Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                    (0..layout.shards.len()).collect()
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
                // Add documents while the batch stays within budget; a document that would
                // overflow it starts the next batch, and one oversized document travels alone.
                let mut end = start;
                let mut bytes = 0usize;
                while end < bucket.len() {
                    let cost = placed_bytes(&bucket[end]);
                    if end > start && bytes.saturating_add(cost) > LOAD_BATCH_BYTES {
                        break;
                    }
                    bytes = bytes.saturating_add(cost);
                    end += 1;
                }
                emit(position, &bucket[start..end])?;
                start = end;
            }
        }
        Ok(())
    }
}
