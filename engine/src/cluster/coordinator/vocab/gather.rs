//! Gathering the cluster's live corpus from its shards: the source set the index is a
//! materialized view of, read by a vocabulary change, a resize and the learning paths.

use std::collections::BTreeMap;

use crate::cluster::coordinator::layout::Layout;
use crate::cluster::coordinator::ClusterEngine;
use crate::cluster::shard::ShardError;

type LiveTaggedMetadata = (
    String,
    u32,
    u64,
    Vec<(String, String)>,
    Vec<crate::tagdict::TagId>,
    crate::rank::RankValues,
    crate::ownership::QueryPlacement,
);

impl ClusterEngine {
    /// The cluster's deduped live `(logical, dsl)` corpus, gathered across shards — the
    /// source set the index is a materialized view of. Errors on a non-local shard
    /// (the same boundary [`Self::set_vocab`] enforces).
    pub(super) fn live_corpus(layout: &Layout) -> Result<Vec<(u64, String)>, ShardError> {
        let mut live: BTreeMap<u64, String> = BTreeMap::new();
        for s in layout.shards.iter() {
            for (logical, dsl) in s.live_sources()? {
                live.entry(logical).or_insert(dsl);
            }
        }
        Ok(live.into_iter().collect())
    }

    /// [`live_corpus`](Self::live_corpus) plus each query's stored `version` and `TagId`s —
    /// the gather behind the tagged + version-preserving rebuild (ADR-074). A query fanned out
    /// to several shards carries the same version + tags on every copy (one `PlacedQuery` per
    /// copy, identical op streams), so dedup-by-logical keeps the first copy seen. Same
    /// non-local error boundary.
    /// `pub(super)` so the shared rebuild core in `coordinator::resize` can gather the corpus
    /// for both a vocabulary change ([`set_vocab`](Self::set_vocab)) and a resize.
    pub(in crate::cluster::coordinator) fn live_corpus_tagged(
        layout: &Layout,
    ) -> Result<Vec<crate::cluster::shard::LiveTaggedQuery>, ShardError> {
        let mut live: BTreeMap<u64, LiveTaggedMetadata> = BTreeMap::new();
        for s in layout.shards.iter() {
            for (logical, dsl, version, source_generation, raw_tags, tag_ids, rank, placement) in
                s.live_sources_tagged()?
            {
                live.entry(logical).or_insert((
                    dsl,
                    version,
                    source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    placement,
                ));
            }
        }
        Ok(live
            .into_iter()
            .map(
                |(
                    logical,
                    (dsl, version, source_generation, raw_tags, tag_ids, rank, placement),
                )| {
                    (
                        logical,
                        dsl,
                        version,
                        source_generation,
                        raw_tags,
                        tag_ids,
                        rank,
                        placement,
                    )
                },
            )
            .collect())
    }
}
