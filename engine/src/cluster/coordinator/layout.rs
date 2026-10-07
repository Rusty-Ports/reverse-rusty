//! The serving layout: everything a read or a write is routed and matched by.
//!
//! A layout is the feature space (normalizer, dictionary, vocabulary), the ring, the shards
//! and the placement generation they were built under. These always change together: a
//! vocabulary change or a resize builds a whole new layout and replaces the old one
//! (ADR-046, ADR-078, ADR-180). Nothing else on the coordinator replaces any of them.

use std::sync::Arc;

use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::ownership::PlacementGeneration;
use crate::vocab::Vocab;

#[cfg(feature = "distributed")]
use super::super::handoff::HandoffShard;
use super::super::ring::HashRing;
use super::super::shard::{Shard, ShardError};

#[derive(Clone)]
pub(in crate::cluster::coordinator) struct Layout {
    /// The one shared feature space, frozen when the layout is built.
    pub(in crate::cluster::coordinator) norm: Arc<Normalizer>,
    pub(in crate::cluster::coordinator) dict: Arc<Dict>,
    /// The vocabulary behind the normalizer, if one was installed (ADR-046). `None` when
    /// the cluster was built directly from a `Normalizer`. Kept so a durable cluster can
    /// persist it and a re-learn can merge into it.
    pub(in crate::cluster::coordinator) vocab: Option<Arc<Vocab>>,
    pub(in crate::cluster::coordinator) ring: HashRing,
    /// Shared, so that a layout can be copied with one field changed.
    pub(in crate::cluster::coordinator) shards: Arc<Vec<Box<dyn Shard>>>,
    /// Per-shard source-sidecar basenames selected by the current coordinator manifest.
    /// Index-aligned with `shards`.
    pub(in crate::cluster::coordinator) source_files: Vec<String>,
    /// Per-position handoff handles (ADR-043), index-aligned with `shards`. Empty on the
    /// in-process path; the gRPC builders wrap each position's backing in a [`HandoffShard`]
    /// so a position can be re-pointed at a new owner at runtime. `handoffs[i]` and
    /// `shards[i]` share one `HandoffShard`.
    #[cfg(feature = "distributed")]
    pub(in crate::cluster::coordinator) handoffs: Vec<Arc<HandoffShard>>,
    /// Monotonic placement identity (ADR-109). Changes only with the layout, never on a
    /// checkpoint or a physical data movement.
    pub(in crate::cluster::coordinator) generation: PlacementGeneration,
}

impl Layout {
    pub(in crate::cluster::coordinator) fn num_shards(&self) -> usize {
        self.ring.num_shards()
    }

    /// Total physical query count across shards (a replicated or any-of query is counted
    /// once per shard holding it).
    pub(in crate::cluster::coordinator) fn num_queries(&self) -> Result<usize, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).sum()
    }

    pub(in crate::cluster::coordinator) fn shard_query_counts(
        &self,
    ) -> Result<Vec<usize>, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).collect()
    }
}

impl super::ClusterEngine {
    /// The layout to run one operation under. Load it once, at the operation's entry, and
    /// hand it down: a rebuild may publish another at any moment, and an operation that
    /// routed by one layout and matched in another would be wrong.
    pub(in crate::cluster::coordinator) fn layout(&self) -> Arc<Layout> {
        self.layout.load_full()
    }

    /// Publish a copy of the current layout with `edit` applied. For assembly and for the
    /// operations that hold the engine exclusively.
    pub(in crate::cluster::coordinator) fn edit_layout(&mut self, edit: impl FnOnce(&mut Layout)) {
        let mut next = Layout::clone(&self.layout());
        edit(&mut next);
        self.layout.store(Arc::new(next));
    }

    /// Take the shards out, pass them through `wrap`, and publish the result. For a test that
    /// instruments the shards of an engine nothing else is using.
    #[cfg(test)]
    pub(in crate::cluster::coordinator) fn replace_shards(
        &mut self,
        wrap: impl FnOnce(Vec<Box<dyn Shard>>) -> Vec<Box<dyn Shard>>,
    ) {
        let mut emptied = Layout::clone(&self.layout());
        emptied.shards = Arc::new(Vec::new());
        let previous = self.layout.swap(Arc::new(emptied));
        let previous = Arc::try_unwrap(previous)
            .ok()
            .expect("no operation holds the layout");
        let shards = Arc::try_unwrap(previous.shards)
            .ok()
            .expect("no other layout shares the shards");
        self.edit_layout(|layout| layout.shards = Arc::new(wrap(shards)));
    }
}
