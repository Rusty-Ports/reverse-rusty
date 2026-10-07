//! One published layout, pinned for a reader (ADR-210).
//!
//! Each accessor of the engine reads the layout that is published when it is called. Two of
//! them called one after the other can therefore straddle a swap: eight shard counts from
//! the layout a resize replaced beside a shard total of nine from the one it published. A
//! reader that reports several things together pins the layout once and reads them all from
//! the pin, which is the rule for anything behind an atomic pointer: load it once for an
//! operation and hand it down.

use std::sync::Arc;

use super::layout::Layout;
use super::ClusterEngine;
use crate::cluster::shard::ShardError;
use crate::ownership::PlacementGeneration;

/// The layout that was published when [`ClusterEngine::published`] was called.
///
/// It stays what it was. A vocabulary change or a resize that swaps a new layout in does
/// not change this one, and its shards go on answering for as long as it is held. So
/// everything read through it belongs together. Hold it for one request and not across
/// requests: while it is held, the files of a layout that has been replaced are kept.
#[derive(Clone)]
pub struct PublishedLayout {
    layout: Arc<Layout>,
}

impl PublishedLayout {
    /// Number of shards.
    #[must_use]
    pub fn num_shards(&self) -> usize {
        self.layout.num_shards()
    }

    /// Total physical query count across shards (a replicated or any-of query is counted
    /// once per shard holding it).
    pub fn num_queries(&self) -> Result<usize, ShardError> {
        self.layout.num_queries()
    }

    /// Per-shard physical query counts.
    pub fn shard_query_counts(&self) -> Result<Vec<usize>, ShardError> {
        self.layout.shard_query_counts()
    }

    /// Per-class entry tally `[A, B, C, D, H]`, summed across shards.
    pub fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.layout.class_counts()
    }

    /// How many replicas, across all positions, reads may not fail over to (ADR-195).
    #[must_use]
    pub fn out_of_sync_replicas(&self) -> usize {
        self.layout.out_of_sync_replicas()
    }

    /// The logical placement generation of this layout (ADR-109).
    #[must_use]
    pub fn placement_generation(&self) -> PlacementGeneration {
        self.layout.generation
    }

    /// The normalizer of this layout.
    #[must_use]
    pub fn normalizer(&self) -> Arc<crate::normalize::Normalizer> {
        Arc::clone(&self.layout.norm)
    }

    /// The dictionary of this layout: the feature-id space its normalizer resolves into.
    #[must_use]
    pub fn dict(&self) -> Arc<crate::dict::Dict> {
        Arc::clone(&self.layout.dict)
    }

    /// The vocabulary behind this layout's normalizer, if one was installed.
    #[must_use]
    pub fn vocab(&self) -> Option<Arc<crate::vocab::Vocab>> {
        self.layout.vocab.clone()
    }
}

impl Layout {
    pub(in crate::cluster::coordinator) fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        let mut total = [0u64; 5];
        for shard in self.shards.iter() {
            let counts = shard.class_counts()?;
            for (sum, count) in total.iter_mut().zip(counts) {
                *sum += count;
            }
        }
        Ok(total)
    }

    pub(in crate::cluster::coordinator) fn out_of_sync_replicas(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.out_of_sync_replicas())
            .sum()
    }
}

impl ClusterEngine {
    /// Pin the published layout, for a reader that reports several things about it
    /// together. See [`PublishedLayout`].
    #[must_use]
    pub fn published(&self) -> PublishedLayout {
        PublishedLayout {
            layout: self.layout(),
        }
    }

    /// Whether a vocabulary change or a resize is rebuilding the cluster, or is waiting for
    /// the operations in flight before it starts. Searches answer meanwhile, from the layout
    /// it replaces; everything else waits for it.
    ///
    /// A rebuild publishes its layout and then commits the control state. A reader that
    /// compares the two can use this to tell a difference that a rebuild is in the middle of
    /// making from one that has been left behind.
    #[must_use]
    pub fn layout_change_in_progress(&self) -> bool {
        self.layout_change_is_running()
    }
}
