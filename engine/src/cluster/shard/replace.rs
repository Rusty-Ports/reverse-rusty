//! The per-shard atomic replace seam (ADR-185).
//!
//! A cluster upsert must never let an unfenced reader see neither version of a query.
//! On one shard that means "tombstone the prior copy and insert the new one" has to be a
//! single visibility step — one engine critical section, one translog frame, one
//! published snapshot — instead of the two seam calls (delete, then insert) it used to
//! be. These are the request and reply shapes of that step.

use super::Extracted;
use crate::ownership::QueryPlacement;

/// One already-accepted query version, as the coordinator hands it to a shard.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PlacedWrite<'a> {
    pub(crate) ex: &'a Extracted,
    pub(crate) logical: u64,
    pub(crate) version: u32,
    pub(crate) text: &'a str,
    pub(crate) tags: &'a [(String, String)],
    pub(crate) placement: &'a QueryPlacement,
}

/// How a replace treats the copy the shard currently holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplaceMode {
    /// Replace only when every live copy on this shard already carries the new
    /// version's placement; otherwise change nothing and report why. A uniform
    /// [`ReplaceStatus::Replaced`] across the placement shards proves the upsert did not
    /// move the query, which is the case that needs no cross-shard coordination: the
    /// same shard owns the row before and after, so each reader sees exactly one
    /// version. Any other reply means the placement is moving, and because nothing was
    /// mutated the coordinator can still fence the move.
    IfSamePlacement,
    /// Replace whatever is held, or insert when nothing is.
    Unconditional,
}

/// What a replace did on one shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplaceStatus {
    /// `removed` prior live copies were tombstoned and the new version inserted, in one
    /// visibility step.
    Replaced { removed: usize },
    /// The shard held no live copy; the new version was inserted.
    Inserted,
    /// [`ReplaceMode::IfSamePlacement`] only: the shard holds no live copy. Unchanged.
    Absent,
    /// [`ReplaceMode::IfSamePlacement`] only: the live copy carries another placement.
    /// Unchanged.
    PlacementMismatch,
    /// The shard rejected the new version (class D); prior copies are untouched.
    Rejected,
}

impl ReplaceStatus {
    /// Whether the shard now holds the new version (or deliberately kept the old one
    /// after rejecting it) — i.e. the replace ran rather than declining its condition.
    pub(crate) fn applied(self) -> bool {
        !matches!(self, Self::Absent | Self::PlacementMismatch)
    }
}
