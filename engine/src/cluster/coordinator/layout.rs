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
use super::super::shard::Shard;

pub(in crate::cluster::coordinator) struct Layout {
    /// The one shared feature space, frozen when the layout is built.
    pub(in crate::cluster::coordinator) norm: Arc<Normalizer>,
    pub(in crate::cluster::coordinator) dict: Arc<Dict>,
    /// The vocabulary behind the normalizer, if one was installed (ADR-046). `None` when
    /// the cluster was built directly from a `Normalizer`. Kept so a durable cluster can
    /// persist it and a re-learn can merge into it.
    pub(in crate::cluster::coordinator) vocab: Option<Arc<Vocab>>,
    pub(in crate::cluster::coordinator) ring: HashRing,
    pub(in crate::cluster::coordinator) shards: Vec<Box<dyn Shard>>,
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
