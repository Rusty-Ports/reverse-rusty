//! `ControlPlane` — the coordinator's quorum-replicated CLUSTER-STATE seam (clustering
//! build-path step 5a / ADR-037).
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane), §10 step 5.
//!
//! [`ControlPlane`] is to cluster *state* what [`ClusterLog`](super::clog::ClusterLog) is
//! to the mutation *log*: a sync, fallible, `Send + Sync` seam that abstracts the
//! OPERATION, so the dependency-free [`InMemoryControlPlane`] shipped here can later be
//! swapped for an openraft-backed one *without touching the coordinator*. The crucial
//! difference from `ClusterLog`: a consensus library (openraft) owns its OWN log
//! internally, so this is NOT a log-append seam — it is a **document-mutation +
//! linearizable-read** seam. The intended mapping onto openraft (step 5b, `distributed`):
//!   - [`ControlPlane::cluster_state`]     → `ensure_linearizable` then read the state machine,
//!   - [`ControlPlane::propose`]           → `Raft::client_write` (commit on a quorum),
//!   - [`ControlPlane::change_membership`] → `Raft::change_membership` (joint consensus —
//!     deliberately NOT folded into `propose`, because changing the voter set is special
//!     in Raft and must stay a distinct operation),
//!   - [`ClusterState`]                    → the replicated state-machine document (snapshot),
//!   - [`ClusterStateChange`]              → the Raft log-entry payload.
//!
//! That mapping is why a few shape choices are load-bearing NOW even though the in-memory
//! backend doesn't need them: reads are a snapshot *pull* (openraft has no watch of *your*
//! document), membership is split from `propose`, and [`ControlError`] carries a
//! [`ForwardToLeader`](ControlError::ForwardToLeader) variant from day one (a follower's
//! `client_write` returns it) so adding the real backend later changes no call site.
//!
//! ## What it holds — and what it must NOT (the boundary invariant, ADR-037)
//! Consensus holds the SMALL, LOW-RATE cluster-state document: membership + the
//! shard→node map + ring params + the feature-model version + an epoch. It must NEVER
//! carry the high-rate query mutations (those stay on [`ClusterLog`](super::clog) + the
//! per-shard primary→replica path) nor the per-shard segment registry (that stays in the
//! LOCAL [`ClusterManifest`](crate::storage::ClusterManifest)). Routing ~750k/sec query
//! adds through one consensus group would cap throughput at commit latency and defeat the
//! content-routed design.
//!
//! ## Two distinct epochs
//! [`ClusterState::epoch`] is an APP-level counter bumped on each committed transition. It
//! is deliberately distinct from (a) openraft's term / `LogId` later and (b) the LOCAL
//! checkpoint generation in [`ClusterManifest`](crate::storage::ClusterManifest). Do not
//! conflate the three.
//!
//! ## Lean core
//! Dependency-free (std + `serde`, both already core): the seam + the in-memory backend
//! compile under `--no-default-features`, exactly like
//! [`NullClusterLog`](super::clog::NullClusterLog). The openraft backend (step 5b) is a
//! separate `distributed`-gated module; openraft never enters the lean core.

use std::sync::{Arc, Mutex, PoisonError};

use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::shard::ShardError;

mod move_intent;

pub use move_intent::{
    MoveCommand, MoveCommandOutcome, MoveControlState, MoveInitialAuthority, MoveIntent,
    MoveIntentPhase, MoveMemberEvidence, MoveMemberIdentity, MoveProposalResult,
    MoveRecoveryEvidence, MOVE_CONTROL_FORMAT_CURRENT, MOVE_CONTROL_FORMAT_LEGACY,
    MOVE_INTENT_VERSION,
};

/// Logical node identity — the concept the in-process clustering core never had (placement
/// was purely `FeatureId → ring → shard INDEX`). New-typed so it can't be confused with a
/// shard index/position (both bare integers); cf. [`LogPos`](super::clog) /
/// [`FeatureId`](crate::dict::FeatureId). The address + role live in [`NodeDescriptor`].
/// `0` is the conventional id of the single logical node in an in-process cluster.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// What a node is *eligible* to be — orthogonal to what data it holds (that is encoded by
/// its appearance as a primary/replica in [`ShardAssignment`]s). `Manager` = cluster-manager
/// (Raft-voter) eligible; the currently-voting subset is [`ClusterState::voters`]. Inert in
/// step 5a (single node); meaningful once the openraft backend lands.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeRole {
    /// Holds shard data (primary/replica). The only role exercised in step 5a.
    Data,
    /// Cluster-manager-eligible (can be elected / vote).
    Manager,
}

/// One cluster member: identity + transport address + role. `addr` is the gRPC endpoint
/// string the remote transport already passes around (e.g. `"http://127.0.0.1:50051"`);
/// `None` for an in-process logical node, which has no socket.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub addr: Option<String>,
    pub role: NodeRole,
}

/// One shard POSITION's placement across logical nodes. `position` is the shard INDEX the
/// ring produces (`0..num_shards`); `primary`/`replicas` are the nodes that host it.
/// Replication factor for the position is `1 + replicas.len()`. Composed with the ring
/// (`FeatureId → position`), this gives the coordinator `FeatureId → position → node → addr`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ShardAssignment {
    pub position: u32,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

/// Monotonic committed version of the cluster-state document — the value a successful
/// [`ControlPlane::propose`] returns. It is an application document version, not openraft's
/// commit index or term. New-typed (callers can't do arithmetic); `StateVersion(0)` = "genesis, nothing
/// committed beyond the initial document".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct StateVersion(pub u64);

/// The committed cluster-state document the control plane holds consensus over — the
/// node-level analogue of what [`ClusterManifest`](crate::storage::ClusterManifest) is for
/// *local* durability. Small and low-rate by construction (see the boundary invariant in
/// the module docs). Self-contained + `serde`-serializable: it is also the openraft snapshot
/// payload, so it must hold no engine handles / `Arc<Dict>`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClusterState {
    /// APP-level term, bumped on every committed transition. Distinct from openraft's
    /// term/`LogId` AND from [`ClusterManifest::epoch`](crate::storage::ClusterManifest)
    /// (the local checkpoint generation).
    pub epoch: u64,
    /// Cluster membership (incl. data nodes + their addresses), kept sorted by id.
    pub nodes: Vec<NodeDescriptor>,
    /// The current Raft VOTER set (manager nodes), kept sorted + deduped. Managed by
    /// [`ControlPlane::change_membership`] (→ openraft joint consensus), NOT by `propose`.
    pub voters: Vec<NodeId>,
    /// The shard→node map, one entry per position, kept sorted by position.
    pub assignments: Vec<ShardAssignment>,
    /// Ring parameters, so any node re-derives a byte-identical [`HashRing`](super::ring::HashRing).
    /// Mirrors the same two fields in [`ClusterManifest`](crate::storage::ClusterManifest).
    pub num_shards: u32,
    pub vnodes: u32,
    /// Feature-model version. `dict_fingerprint` is the frozen-dict identity (matches the
    /// manifest); `model_version` is a dense counter the deferred new-vocabulary epoch
    /// handshake will coordinate on.
    pub dict_fingerprint: u64,
    pub model_version: u64,
    /// ADR-109 logical placement identity. Bumped only by model/ring blue-green
    /// rebuild transitions, never by physical assignment or checkpoint changes.
    pub placement_generation: u64,
    /// Durable, versioned physical-move intents and per-position assignment generations.
    /// Kept nested so legacy states deserialize with one defaulted field.
    pub moves: MoveControlState,
}

#[derive(Serialize)]
struct CurrentEpoch {
    value: u64,
    control_format_version: u32,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EpochWire {
    Legacy(u64),
    Current {
        value: u64,
        control_format_version: u32,
    },
}

#[derive(Deserialize)]
struct ClusterStateWire {
    epoch: EpochWire,
    nodes: Vec<NodeDescriptor>,
    voters: Vec<NodeId>,
    assignments: Vec<ShardAssignment>,
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    model_version: u64,
    placement_generation: u64,
    #[serde(default)]
    moves: MoveControlState,
}

impl Serialize for ClusterState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterState", 10)?;
        if self.moves.format_version >= MOVE_CONTROL_FORMAT_CURRENT {
            state.serialize_field(
                "epoch",
                &CurrentEpoch {
                    value: self.epoch,
                    control_format_version: self.moves.format_version,
                },
            )?;
        } else {
            state.serialize_field("epoch", &self.epoch)?;
        }
        state.serialize_field("nodes", &self.nodes)?;
        state.serialize_field("voters", &self.voters)?;
        state.serialize_field("assignments", &self.assignments)?;
        state.serialize_field("num_shards", &self.num_shards)?;
        state.serialize_field("vnodes", &self.vnodes)?;
        state.serialize_field("dict_fingerprint", &self.dict_fingerprint)?;
        state.serialize_field("model_version", &self.model_version)?;
        state.serialize_field("placement_generation", &self.placement_generation)?;
        state.serialize_field("moves", &self.moves)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ClusterState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ClusterStateWire::deserialize(deserializer)?;
        let epoch = match wire.epoch {
            EpochWire::Legacy(value) => {
                if wire.moves.format_version != MOVE_CONTROL_FORMAT_LEGACY {
                    return Err(D::Error::custom(
                        "legacy cluster-state epoch carries non-legacy move state",
                    ));
                }
                value
            }
            EpochWire::Current {
                value,
                control_format_version,
            } => {
                if control_format_version != MOVE_CONTROL_FORMAT_CURRENT
                    || wire.moves.format_version != MOVE_CONTROL_FORMAT_CURRENT
                {
                    return Err(D::Error::custom(format!(
                        "unsupported move control format {control_format_version}"
                    )));
                }
                value
            }
        };
        Ok(Self {
            epoch,
            nodes: wire.nodes,
            voters: wire.voters,
            assignments: wire.assignments,
            num_shards: wire.num_shards,
            vnodes: wire.vnodes,
            dict_fingerprint: wire.dict_fingerprint,
            model_version: wire.model_version,
            placement_generation: wire.placement_generation,
            moves: wire.moves,
        })
    }
}

/// One atomic transition the control plane commits — the [`ClusterMutation`](super::clog)
/// analogue and the openraft log-entry payload. Coarse-grained + low-rate by
/// construction (membership/placement/model changes, never query writes). Applying the
/// ordered change stream reproduces the document deterministically (live ≡ replay).
///
/// Note: `AddNode`/`RemoveNode` are APP-level node *registration* (a node enters/leaves the
/// cluster document with an address + role). Changing the Raft *voter set* is the separate
/// [`ControlPlane::change_membership`] operation — see the module docs.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ClusterStateChange {
    /// Register (or replace, by [`NodeId`]) a cluster member.
    AddNode(NodeDescriptor),
    /// Deregister a member by id (idempotent). Pruning it from `voters`/`assignments` is
    /// the caller's separate responsibility (`change_membership` / a reassignment), exactly
    /// as removing a voter is distinct from removing a node in Raft.
    RemoveNode(NodeId),
    /// Place a shard position on nodes (replaces the entry for that position).
    AssignShard(ShardAssignment),
    /// Advance the feature-model version (sets the fingerprint, bumps `model_version`).
    BumpModelVersion { dict_fingerprint: u64 },
    /// Resize the cluster to `num_shards` positions (ADR-078): set the count, add a default
    /// single-node assignment (`primary NodeId(0)`, no replicas) for each new position on grow,
    /// and prune assignments for positions ≥ `num_shards` on shrink. The ring itself is
    /// re-derived by the coordinator from the new count; this keeps the cluster-state document
    /// (and thus `collect_load` / `assignment_for`) consistent. On a multi-node cluster a
    /// follow-up `rebalance` spreads the new positions across nodes.
    SetShardCount { num_shards: u32 },
    /// Versioned, idempotent physical-move transition. Callers use
    /// [`ControlPlane::propose_move`] to retain the application outcome.
    Move(MoveCommand),
}

/// Why a control-plane operation could not commit. Typed (not stringly) so callers can act
/// on it — chiefly [`ForwardToLeader`](Self::ForwardToLeader), which a follower's
/// `client_write` returns under openraft. The in-memory single-node backend never returns
/// any of these (it is always its own leader with a trivial quorum); they exist so the
/// openraft backend drops in behind the seam without changing the error shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    /// This node is not the leader; retry against `leader` (at `addr`, if known).
    ForwardToLeader {
        leader: Option<NodeId>,
        addr: Option<String>,
    },
    /// This node is not the leader and the leader is presently unknown.
    NotLeader,
    /// The proposal could not be committed on a quorum (e.g. lost majority).
    NoQuorum,
    /// A backend/transport error (I/O, storage, RPC) with detail.
    Backend(String),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlError::ForwardToLeader { leader, addr } => write!(
                f,
                "not the control-plane leader; forward to {leader:?} at {addr:?}"
            ),
            ControlError::NotLeader => {
                write!(
                    f,
                    "this node is not the control-plane leader (leader unknown)"
                )
            }
            ControlError::NoQuorum => {
                write!(f, "control-plane proposal could not reach a quorum")
            }
            ControlError::Backend(m) => write!(f, "control-plane backend error: {m}"),
        }
    }
}

impl std::error::Error for ControlError {}

/// Fold a control-plane error into the cluster's shared [`ShardError`] at the coordinator
/// boundary, so coordinator methods that already return `Result<_, ShardError>` can `?` it.
/// The structured detail is preserved in the message (the typed variant stays available to
/// callers that hold a [`ControlError`] directly).
impl From<ControlError> for ShardError {
    fn from(e: ControlError) -> Self {
        ShardError::ControlPlane(e.to_string())
    }
}

/// The cluster-state consensus seam — sync, fallible (`Result<_, ControlError>`),
/// `Send + Sync`, exactly like [`Shard`](super::shard::Shard) / [`ClusterLog`](super::clog).
/// `Send + Sync` is mandatory: a `Box<dyn ControlPlane>` lives in `ClusterEngine`, which is
/// asserted `Send + Sync` in `lib.rs`. Surfacing errors (never swallowing a stale/blind read
/// of the assignment map) is load-bearing — a silently-wrong map routes a title to the wrong
/// node, a shard-sized false negative.
pub trait ControlPlane: Send + Sync {
    /// A linearizable read of the committed cluster-state document. Cheap (an `Arc` clone),
    /// so a caller can hold a stable snapshot while it resolves a route. (Snapshot *pull*,
    /// not a watch: openraft offers no watch of an application document.)
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError>;

    /// The committed version (`ClusterState::epoch`) without cloning the whole document —
    /// lets a caller cheaply detect "did the map move?". `StateVersion(0)` before any commit.
    fn version(&self) -> Result<StateVersion, ControlError>;

    /// Propose ONE non-membership transition; returns the committed version once it is
    /// durable on a quorum (immediately, for the in-memory backend). Fail-closed: a rejected
    /// proposal returns `Err` and does NOT mutate the document — the control-plane analogue
    /// of the log-first write path.
    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError>;

    /// Propose a resumable physical-move transition and return its atomic compare-and-set
    /// outcome. Unlike general proposals, operation ids make this safe to retry after an
    /// ambiguous transport failure.
    fn propose_move(&self, _command: MoveCommand) -> Result<MoveProposalResult, ControlError> {
        Err(ControlError::Backend(
            "durable move proposals are not supported by this control-plane backend".into(),
        ))
    }

    /// Change the Raft VOTER set — DISTINCT from [`propose`](Self::propose) because joint
    /// consensus is special in Raft (maps to `Raft::change_membership`, not `client_write`).
    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError>;

    /// The current leader, if known — drives forward-to-leader. The single-node in-memory
    /// backend is always its own leader.
    fn leader(&self) -> Result<Option<NodeId>, ControlError>;

    /// Test-only fault injection: make subsequent `propose`/`change_membership` calls fail,
    /// so a coordinator test can prove the fail-closed contract through a `Box<dyn
    /// ControlPlane>`. Default no-op (mirrors [`ClusterLog::break_writes_for_test`](super::clog)).
    #[cfg(test)]
    fn break_proposals_for_test(&self) {}
}

/// Apply one transition to the document in place (NOT the epoch — the caller bumps that).
/// Canonicalizing (sorted `nodes`/`assignments`) so the committed document is order-
/// independent: replaying the same change set in log order yields a byte-identical
/// `ClusterState`, which is what makes the two-backend differential meaningful.
///
/// `pub(super)` so the openraft state machine (`control_raft.rs`, ADR-038) applies a
/// committed `Normal` log entry through the SAME funnel as [`InMemoryControlPlane`] — live
/// ≡ replay across both backends, the property the differential oracle relies on.
pub(super) fn apply(
    state: &mut ClusterState,
    change: ClusterStateChange,
) -> Option<MoveCommandOutcome> {
    match change {
        ClusterStateChange::AddNode(node) => {
            state.nodes.retain(|n| n.id != node.id);
            state.nodes.push(node);
            state.nodes.sort_by_key(|n| n.id.0);
        }
        ClusterStateChange::RemoveNode(id) => state.nodes.retain(|n| n.id != id),
        ClusterStateChange::AssignShard(a) => {
            let position = a.position;
            state.assignments.retain(|x| x.position != a.position);
            state.assignments.push(a);
            state.assignments.sort_by_key(|x| x.position);
            state.moves.bump_assignment_generation(position);
        }
        ClusterStateChange::BumpModelVersion { dict_fingerprint } => {
            state.dict_fingerprint = dict_fingerprint;
            state.model_version += 1;
            state.placement_generation = state.placement_generation.saturating_add(1);
        }
        ClusterStateChange::SetShardCount { num_shards } => {
            state.num_shards = num_shards;
            state.placement_generation = state.placement_generation.saturating_add(1);
            // Shrink: drop assignments for positions that no longer exist.
            state.assignments.retain(|a| a.position < num_shards);
            // Grow: add a default single-node assignment for each new position. A multi-node
            // caller follows with `rebalance` to spread the new positions across nodes.
            for position in 0..num_shards {
                if !state.assignments.iter().any(|a| a.position == position) {
                    state.assignments.push(ShardAssignment {
                        position,
                        primary: NodeId(0),
                        replicas: Vec::new(),
                    });
                }
            }
            state.assignments.sort_by_key(|x| x.position);
            state.moves.retain_positions_below(num_shards);
        }
        ClusterStateChange::Move(command) => return Some(move_intent::apply_move(state, command)),
    }
    None
}

/// The canonical single-logical-node cluster-state document: one `NodeId(0)`
/// (`Manager`-eligible, the sole voter, no socket), identity assignments
/// (`position i → primary NodeId(0)`, no replicas), and the build's ring params + dict
/// fingerprint. Shared by [`InMemoryControlPlane::single_node`] AND the openraft state
/// machine's genesis seed (`control_raft.rs`, ADR-038), so the two backends start from a
/// byte-identical document — the precondition that makes their differential meaningful.
pub(super) fn single_node_state(
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
) -> ClusterState {
    ClusterState {
        epoch: 0,
        nodes: vec![NodeDescriptor {
            id: NodeId(0),
            addr: None,
            role: NodeRole::Manager,
        }],
        voters: vec![NodeId(0)],
        assignments: (0..num_shards)
            .map(|position| ShardAssignment {
                position,
                primary: NodeId(0),
                replicas: Vec::new(),
            })
            .collect(),
        num_shards,
        vnodes,
        dict_fingerprint,
        model_version: 0,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.0,
        moves: MoveControlState::default(),
    }
}

/// The dependency-free, single-node control plane: applies every proposal immediately to an
/// in-RAM document and is always `Ok` (a single node trivially has a quorum) — the
/// [`NullClusterLog`](super::clog::NullClusterLog) analogue. It is BOTH the behavior of an
/// in-process cluster (one logical node owns every shard, so the default path is
/// byte-identical to the pre-ADR-037 cluster) AND the fast backend the differential oracle
/// runs the openraft backend against later.
pub struct InMemoryControlPlane {
    /// `Arc` inside the `Mutex` so a read clones an `Arc` handle (O(1)) and a write swaps in
    /// a fresh document — the in-memory mirror of openraft's `ArcSwap`-over-the-state-machine.
    state: Mutex<Arc<ClusterState>>,
    /// Test-only fault flag (see [`ControlPlane::break_proposals_for_test`]). Gated so a
    /// non-test build carries no unused field.
    #[cfg(test)]
    broken: std::sync::atomic::AtomicBool,
}

impl InMemoryControlPlane {
    /// Genesis from an explicit document (the openraft bootstrap analogue).
    pub fn new(initial: ClusterState) -> Self {
        InMemoryControlPlane {
            state: Mutex::new(Arc::new(initial)),
            #[cfg(test)]
            broken: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The DEFAULT single-logical-node control plane the coordinator uses when none is
    /// supplied: the [`single_node_state`] document wrapped in an in-memory backend. Every
    /// shard is "assigned" to the one node, so the RF=1 in-process path is byte-identical to
    /// before ADR-037.
    pub fn single_node(num_shards: u32, vnodes: u32, dict_fingerprint: u64) -> Self {
        InMemoryControlPlane::new(single_node_state(num_shards, vnodes, dict_fingerprint))
    }

    pub(crate) fn single_node_with_generation(
        num_shards: u32,
        vnodes: u32,
        dict_fingerprint: u64,
        generation: crate::ownership::PlacementGeneration,
    ) -> Self {
        let mut state = single_node_state(num_shards, vnodes, dict_fingerprint);
        state.placement_generation = generation.0;
        InMemoryControlPlane::new(state)
    }

    /// Lock the document, recovering a poisoned guard rather than panicking (a prior writer
    /// panic must not take down the cluster; the document is always whole). Matches the
    /// `PoisonError::into_inner` convention used across the cluster module.
    fn lock(&self) -> std::sync::MutexGuard<'_, Arc<ClusterState>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn proposals_broken(&self) -> bool {
        #[cfg(test)]
        if self.broken.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        false
    }
}

impl ControlPlane for InMemoryControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Ok(Arc::clone(&self.lock()))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.lock().epoch))
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        if self.proposals_broken() {
            return Err(ControlError::Backend(
                "proposals broken (test fault injection)".into(),
            ));
        }
        let mut current = self.lock();
        let mut next = (**current).clone();
        let _ = apply(&mut next, change);
        next.epoch += 1;
        let version = StateVersion(next.epoch);
        *current = Arc::new(next);
        Ok(version)
    }

    fn propose_move(&self, command: MoveCommand) -> Result<MoveProposalResult, ControlError> {
        if self.proposals_broken() {
            return Err(ControlError::Backend(
                "proposals broken (test fault injection)".into(),
            ));
        }
        let mut current = self.lock();
        let mut next = (**current).clone();
        let outcome = move_intent::apply_move(&mut next, command);
        next.epoch += 1;
        let version = StateVersion(next.epoch);
        *current = Arc::new(next);
        Ok(MoveProposalResult { version, outcome })
    }

    fn change_membership(&self, mut voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        if self.proposals_broken() {
            return Err(ControlError::Backend(
                "proposals broken (test fault injection)".into(),
            ));
        }
        voters.sort_unstable();
        voters.dedup();
        let mut current = self.lock();
        let mut next = (**current).clone();
        next.voters = voters;
        next.epoch += 1;
        let version = StateVersion(next.epoch);
        *current = Arc::new(next);
        Ok(version)
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(self.lock().voters.first().copied())
    }

    #[cfg(test)]
    fn break_proposals_for_test(&self) {
        self.broken
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;
