//! Which rows a rebuild keeps in default reads (ADR-203).
//!
//! A rebuild re-plans every stored row against the new dictionary. A row that default reads
//! return can come out as class C: an alias moved its rare term into an any-of group and left
//! only very common anchors, or the rebuilt dictionary ranks its anchor among the most common
//! terms. Class C is opt-in, so sending the row to the broad lane would remove it from every
//! read that does not ask for broad.

use crate::cluster::ring::HashRing;
use crate::compile::{anchor_plan, CostClass, Extracted};
use crate::dict::Dict;
use crate::ownership::{PlacementMode, QueryPlacement};

use super::super::{placement_of, Target};

/// Whether a default read (broad lane off) returns a row stored under `placement`.
///
/// The mode is the record of the lane: a selective or always-visible row is stored in a lane
/// every probe reads, and a row replicated into the broad lane is opt-in. A row with no
/// cluster placement is owned by no position, so no cluster read returns it.
pub(super) fn was_default_visible(placement: &QueryPlacement) -> bool {
    matches!(
        placement.mode(),
        PlacementMode::Selective | PlacementMode::ReplicatedAlwaysVisible
    )
}

/// [`placement_of`] for a stored row that a rebuild is re-placing.
///
/// A row that default reads returned, and whose plan is now class C, is replicated
/// always-visible instead of into the broad lane. Each shard stores a class-C plan that
/// arrives with that placement in its main lane (`Segment::add_compiled_*`). The main lane
/// is probed with every title feature and a title always probes at least one shard, so the
/// row is found whenever it was before.
///
/// A row that was already opt-in takes the lane its plan gives it. Class D has no positive
/// anchor to move, and a stored class-D row is never dropped by a rebuild, so class D is
/// accepted here whatever the current knob says.
pub(super) fn rebuild_placement_of(
    dict: &Dict,
    ring: &HashRing,
    ex: &Extracted,
    theta: u32,
    was_default_visible: bool,
) -> Target {
    if was_default_visible && anchor_plan(ex, dict, theta).class == CostClass::C {
        return Target::ReplicatedAlwaysVisible;
    }
    placement_of(dict, ring, ex, true, theta)
}
