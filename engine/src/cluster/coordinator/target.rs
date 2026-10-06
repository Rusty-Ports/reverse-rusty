//! Where the coordinator puts one compiled query, and what it tells the writer.

use crate::compile::{anchor_plan, uses_required_phrase_proxy, CostClass, Extracted};
use crate::dict::Dict;
use crate::error::ParseError;

use super::super::ring::HashRing;
use super::super::shard::ShardError;

/// Where a freshly added query landed.
///
/// An accepted write carries the cost class the coordinator planned it under. A shard
/// stores a row under the class of the same plan, made against the same dictionary, so
/// this is the class of the stored row; a remote shard started with another hot-anchor
/// threshold can store A where the coordinator says H, or the reverse, and no other
/// difference. Whether default reads return the row follows from the class alone
/// ([`CostClass::is_opt_in`]): a write is never placed in the broad lane under another
/// class, or outside it under class C or D.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    /// Selective query (class A / H / B any-of): placed on these shard(s).
    Placed {
        shards: Vec<usize>,
        class: CostClass,
    },
    /// Replicated to every shard (ADR-080): a class-B pair or phrase proxy, which stays
    /// always-visible, or a broad-lane query (class C / accepted class D).
    Replicated { class: CostClass },
    /// Compiled but rejected as cost-class D with `accept_class_d` off — no
    /// anchorable feature, stored nowhere.
    RejectedClassD,
    /// The DSL failed to parse.
    RejectedParse(ParseError),
}

impl AddOutcome {
    /// The cost class an accepted write was stored under; `None` for a rejected one.
    pub fn class(&self) -> Option<CostClass> {
        match self {
            Self::Placed { class, .. } | Self::Replicated { class } => Some(*class),
            Self::RejectedClassD | Self::RejectedParse(_) => None,
        }
    }
}

/// Internal placement decision for one compiled query.
pub(super) enum Target {
    /// Class D with `accept_class_d` off — no anchorable feature, stored nowhere.
    Reject,
    /// A class-B pair is replicated to every shard but remains always-visible.
    ReplicatedAlwaysVisible,
    /// Class C / accepted class D is replicated to every shard and evaluated on
    /// one broad-evaluation position per request.
    ReplicatedBroad,
    /// Selective shards (class A / B any-of), sorted + deduped, non-empty.
    Selective(Vec<usize>),
}

impl Target {
    pub(super) fn placement(
        &self,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<crate::ownership::QueryPlacement, ShardError> {
        use crate::ownership::QueryPlacement;
        match self {
            Self::Reject => Ok(QueryPlacement::standalone()),
            Self::ReplicatedAlwaysVisible => Ok(QueryPlacement::replicated_always_visible(
                generation, num_shards,
            )?),
            Self::ReplicatedBroad => Ok(QueryPlacement::replicated_broad(generation, num_shards)?),
            Self::Selective(positions) => Ok(QueryPlacement::selective(
                generation,
                num_shards,
                positions.iter().map(|&position| position as u32).collect(),
            )?),
        }
    }
}

/// The placement decision for one compiled query — see the module-level table. A free
/// fn over (`dict`, `ring`) so [`ClusterEngine::build`] can bucket the corpus before
/// the cluster value exists, and [`ClusterEngine::placement`] can delegate. Forbidden
/// features can't leak in: `anchor_plan` reads only positive `required` /
/// `anyof` / `required_phrases`, never either forbidden representation
/// (ADR-006 holds structurally).
///
/// `accept_class_d` (the per-shard [`EngineConfig`](crate::config::EngineConfig) knob)
/// gates the cluster always-candidate lane (ADR-068/080): a negation-only class-D query
/// is placed on the broad lane (every shard, under the universal signature) when the knob
/// is on, and rejected otherwise. The decision is re-derived identically on log replay
/// (same frozen dict + same config), so live ≡ replay.
///
/// `theta` is the hot-anchor threshold (ADR-105). A class-H query places
/// **selectively, exactly like class A**: its anchors are non-top-64 required
/// features, which `route()` ring-routes on the title side, so every matching
/// title probes the shard(s) holding it — no replication, no broad-eval-shard
/// gating (the tier is always-visible on the shards that own it). Because A and
/// H produce the IDENTICAL `Target`, placement is θ-invariant: a θ change (or a
/// coordinator/shard θ mismatch) can never move a query to a different shard,
/// only between the two always-probed indexes on the same shard — the ADR-105
/// benign-divergence property.
pub(super) fn placement_of(
    dict: &Dict,
    ring: &HashRing,
    ex: &Extracted,
    accept_class_d: bool,
    theta: u32,
) -> Target {
    planned(dict, ring, ex, accept_class_d, theta).0
}

/// [`placement_of`] with the cost class of the plan the placement was read from: the class
/// a shard stores the row under, since a shard plans against the same dictionary.
pub(super) fn planned(
    dict: &Dict,
    ring: &HashRing,
    ex: &Extracted,
    accept_class_d: bool,
    theta: u32,
) -> (Target, CostClass) {
    let ap = anchor_plan(ex, dict, theta);
    (target_of(&ap, dict, ring, ex, accept_class_d), ap.class)
}

fn target_of(
    ap: &crate::compile::AnchorPlan,
    dict: &Dict,
    ring: &HashRing,
    ex: &Extracted,
    accept_class_d: bool,
) -> Target {
    // A phrase-proxy positive cover can contain analyzer labels that ordinary
    // flat routing intentionally omits (structural/context gap labels). Keep
    // those candidate-only proxies off the ring: replicate the always-visible
    // class-B row, then whichever shard the title already probes can retrieve
    // it from its positioned probe labels. This includes mixed queries whose
    // sole flat required feature is top-64-hot and would otherwise be class C.
    if uses_required_phrase_proxy(ex, dict)
        && matches!(ap.class, CostClass::A | CostClass::B | CostClass::H)
    {
        return Target::ReplicatedAlwaysVisible;
    }
    match ap.class {
        CostClass::D => {
            // Stored only when the lane is on AND there is something to forbid: an
            // effectively-empty query (no positives, no negatives) would match every title,
            // so the shard engines reject it regardless (`rejects_class_d`). Rejecting HERE —
            // before fan-out — is load-bearing for `upsert`: a plan every shard would reject
            // must not tombstone the prior version first (a silent delete-with-no-replace).
            if accept_class_d && ex.has_negative_predicate() {
                Target::ReplicatedBroad
            } else {
                Target::Reject
            }
        }
        CostClass::C => Target::ReplicatedBroad,
        CostClass::A | CostClass::B | CostClass::H => {
            // A class-B-arity-2 query's only main anchor is an all-hot PAIR (a len-2
            // group): no rare feature to hash on, so it joins the replicated lane.
            // Class A and class-B any-of have only arity-1 non-hot anchors, which the
            // ring distributes selectively — and class H's arity-1 anchors are
            // non-top-64 by definition, so they ring-place the same way (chained
            // below; the defensive len!=1 guard would fail a future arity>1 hot
            // anchor safe into the replicated lane rather than mis-hashing it).
            if ap
                .main_anchors
                .iter()
                .chain(ap.hot_anchors.iter())
                .any(|g| g.len() != 1)
            {
                return Target::ReplicatedAlwaysVisible;
            }
            let mut shards: Vec<usize> = ap
                .main_anchors
                .iter()
                .chain(ap.hot_anchors.iter())
                .filter_map(|g| g.first().copied())
                .map(|f| ring.lookup(f))
                .collect();
            shards.sort_unstable();
            shards.dedup();
            if shards.is_empty() {
                Target::Reject
            } else {
                Target::Selective(shards)
            }
        }
    }
}
