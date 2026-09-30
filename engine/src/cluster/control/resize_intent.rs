//! Durable remote-resize intent (ADR-180).
//!
//! A remote resize builds a complete new layout on separate target nodes, proves it, and only
//! then replaces the committed shard count, placement generation, and assignments together. The
//! physical work happens outside Raft, so the transition is recorded here first and every command
//! is idempotent by `operation_id`. Preconditions are evaluated inside the replicated state
//! machine, mirroring the ADR-175 move intent, so a racing coordinator or topology change can never
//! commit a layout the evidence did not cover.

use serde::{Deserialize, Serialize};

use super::move_intent::{normalized_move_endpoint, MoveCommandOutcome, MoveMemberIdentity};
use super::{ClusterState, NodeId, NodeRole, ShardAssignment};

pub const RESIZE_INTENT_VERSION: u32 = 1;
/// The control format that may carry a resize intent. Older binaries accept at most format 4,
/// so they reject a snapshot that could hold one instead of silently dropping it.
pub const RESIZE_CONTROL_FORMAT: u32 = 5;

/// One complete placement layout: ring size, logical placement generation, and the full
/// position → node map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeLayout {
    pub num_shards: u32,
    pub placement_generation: u64,
    pub assignments: Vec<ShardAssignment>,
}

impl ResizeLayout {
    /// Whether `state` currently holds exactly this layout.
    fn is_committed_in(&self, state: &ClusterState) -> bool {
        state.num_shards == self.num_shards
            && state.placement_generation == self.placement_generation
            && state.assignments == self.assignments
    }

    /// Sorted, gap-free positions `0..num_shards`, each with distinct member nodes.
    fn well_formed(&self) -> bool {
        self.num_shards >= 1
            && self.assignments.len() == self.num_shards as usize
            && self
                .assignments
                .iter()
                .enumerate()
                .all(|(index, assignment)| {
                    assignment.position as usize == index && assignment_nodes(assignment).is_some()
                })
    }
}

/// Exact content evidence for one target position, captured while writes are paused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizePositionEvidence {
    pub position: u32,
    pub fingerprint_lo: u64,
    pub fingerprint_hi: u64,
    pub live_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResizeIntentPhase {
    Preparing,
    Ready(Vec<ResizePositionEvidence>),
    Committed(Vec<ResizePositionEvidence>),
}

/// One resumable remote resize. At most one may exist, and none while a move intent is active.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeIntent {
    pub intent_version: u32,
    pub operation_id: u64,
    pub expected: ResizeLayout,
    pub desired: ResizeLayout,
    /// Every node in the desired layout with its normalized endpoint at planning time.
    pub members: Vec<MoveMemberIdentity>,
    pub phase: ResizeIntentPhase,
}

impl ResizeIntent {
    pub fn evidence(&self) -> Option<&[ResizePositionEvidence]> {
        match &self.phase {
            ResizeIntentPhase::Preparing => None,
            ResizeIntentPhase::Ready(evidence) | ResizeIntentPhase::Committed(evidence) => {
                Some(evidence)
            }
        }
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.intent_version == other.intent_version
            && self.operation_id == other.operation_id
            && self.expected == other.expected
            && self.desired == other.desired
            && self.members == other.members
    }
}

/// Idempotent resize commands, applied atomically by the control state machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResizeCommand {
    Begin(ResizeIntent),
    MarkReady {
        operation_id: u64,
        evidence: Vec<ResizePositionEvidence>,
    },
    Commit {
        operation_id: u64,
    },
    Abort {
        operation_id: u64,
    },
    Finish {
        operation_id: u64,
    },
}

fn assignment_nodes(assignment: &ShardAssignment) -> Option<Vec<NodeId>> {
    let mut nodes = Vec::with_capacity(1 + assignment.replicas.len());
    nodes.push(assignment.primary);
    nodes.extend(assignment.replicas.iter().copied());
    nodes.sort_unstable();
    let before = nodes.len();
    nodes.dedup();
    (nodes.len() == before).then_some(nodes)
}

fn layout_nodes(layout: &ResizeLayout) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = layout
        .assignments
        .iter()
        .flat_map(|a| std::iter::once(a.primary).chain(a.replicas.iter().copied()))
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}

fn registered_endpoint(state: &ClusterState, node: NodeId) -> Option<String> {
    state
        .nodes
        .iter()
        .find(|descriptor| descriptor.id == node)
        .and_then(|descriptor| descriptor.addr.as_deref())
        .map(normalized_move_endpoint)
}

/// The desired members are exactly the desired layout's nodes, sorted, each registered at its
/// recorded normalized endpoint, with no endpoint shared by two members or by any node of the
/// expected layout: the new layout is built on separate nodes, never beside the old one.
fn members_match(state: &ClusterState, intent: &ResizeIntent) -> bool {
    let desired_nodes = layout_nodes(&intent.desired);
    if intent
        .members
        .iter()
        .map(|m| m.node)
        .ne(desired_nodes.iter().copied())
    {
        return false;
    }
    let mut endpoints = Vec::with_capacity(intent.members.len());
    for member in &intent.members {
        if member.endpoint.is_empty()
            || member.endpoint != normalized_move_endpoint(&member.endpoint)
            || registered_endpoint(state, member.node).as_deref() != Some(&member.endpoint)
        {
            return false;
        }
        endpoints.push(member.endpoint.as_str());
    }
    endpoints.sort_unstable();
    if endpoints.windows(2).any(|pair| pair[0] == pair[1]) {
        return false;
    }
    layout_nodes(&intent.expected).into_iter().all(|node| {
        registered_endpoint(state, node)
            .is_some_and(|endpoint| endpoints.binary_search(&endpoint.as_str()).is_err())
    })
}

fn valid_begin(state: &ClusterState, intent: &ResizeIntent) -> bool {
    intent.intent_version == RESIZE_INTENT_VERSION
        && intent.operation_id != 0
        && matches!(intent.phase, ResizeIntentPhase::Preparing)
        && intent.expected.well_formed()
        && intent.desired.well_formed()
        && intent.expected.placement_generation.checked_add(1)
            == Some(intent.desired.placement_generation)
        && members_match(state, intent)
}

fn evidence_matches(intent: &ResizeIntent, evidence: &[ResizePositionEvidence]) -> bool {
    evidence.len() == intent.desired.num_shards as usize
        && evidence
            .iter()
            .enumerate()
            .all(|(index, item)| item.position as usize == index)
}

/// Apply one resize command. The caller still owns the document epoch bump.
pub(super) fn apply_resize(state: &mut ClusterState, command: ResizeCommand) -> MoveCommandOutcome {
    // Once any resize command is observed, every snapshot carries the format-5 fence, even for a
    // rejected command, so a compacted snapshot cannot hide resize state from an older binary.
    state.moves.format_version = state.moves.format_version.max(RESIZE_CONTROL_FORMAT);
    match command {
        ResizeCommand::Begin(intent) => {
            if !valid_begin(state, &intent) {
                return MoveCommandOutcome::Invalid;
            }
            if let Some(current) = &state.moves.resize {
                return if current.same_identity(&intent) {
                    MoveCommandOutcome::AlreadyApplied
                } else {
                    MoveCommandOutcome::Conflict
                };
            }
            if !state.moves.intents.is_empty() || !intent.expected.is_committed_in(state) {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.resize = Some(intent);
            MoveCommandOutcome::Applied
        }
        ResizeCommand::MarkReady {
            operation_id,
            evidence,
        } => {
            let Some(intent) = state.moves.resize.as_ref() else {
                return MoveCommandOutcome::Conflict;
            };
            if intent.operation_id != operation_id {
                return MoveCommandOutcome::Conflict;
            }
            if !evidence_matches(intent, &evidence) {
                return MoveCommandOutcome::Invalid;
            }
            match &intent.phase {
                ResizeIntentPhase::Preparing => {
                    if !intent.expected.is_committed_in(state) || !members_match(state, intent) {
                        return MoveCommandOutcome::Conflict;
                    }
                    if let Some(intent) = state.moves.resize.as_mut() {
                        intent.phase = ResizeIntentPhase::Ready(evidence);
                    }
                    MoveCommandOutcome::Applied
                }
                ResizeIntentPhase::Ready(current) | ResizeIntentPhase::Committed(current)
                    if current == &evidence =>
                {
                    MoveCommandOutcome::AlreadyApplied
                }
                ResizeIntentPhase::Ready(_) | ResizeIntentPhase::Committed(_) => {
                    MoveCommandOutcome::Conflict
                }
            }
        }
        ResizeCommand::Commit { operation_id } => {
            let Some(intent) = state.moves.resize.clone() else {
                return MoveCommandOutcome::Conflict;
            };
            if intent.operation_id != operation_id {
                return MoveCommandOutcome::Conflict;
            }
            match &intent.phase {
                ResizeIntentPhase::Preparing => MoveCommandOutcome::Conflict,
                ResizeIntentPhase::Ready(evidence) => {
                    if !intent.expected.is_committed_in(state) || !members_match(state, &intent) {
                        return MoveCommandOutcome::Conflict;
                    }
                    state.num_shards = intent.desired.num_shards;
                    state.placement_generation = intent.desired.placement_generation;
                    state.assignments.clone_from(&intent.desired.assignments);
                    // The old layout's nodes are retired at the storage layer before this commit
                    // and refuse every request, so they leave membership in the same transition:
                    // no rebalance or reconcile may pick them as a destination again.
                    let retired: Vec<NodeId> = layout_nodes(&intent.expected)
                        .into_iter()
                        .filter(|node| !layout_nodes(&intent.desired).contains(node))
                        .collect();
                    state
                        .nodes
                        .retain(|node| node.role != NodeRole::Data || !retired.contains(&node.id));
                    state.moves.retain_positions_below(state.num_shards);
                    for position in 0..state.num_shards {
                        state.moves.bump_assignment_generation(position);
                    }
                    if let Some(intent) = state.moves.resize.as_mut() {
                        intent.phase = ResizeIntentPhase::Committed(evidence.clone());
                    }
                    MoveCommandOutcome::Applied
                }
                ResizeIntentPhase::Committed(_) => {
                    if intent.desired.is_committed_in(state) {
                        MoveCommandOutcome::AlreadyApplied
                    } else {
                        MoveCommandOutcome::Conflict
                    }
                }
            }
        }
        ResizeCommand::Abort { operation_id } => {
            let Some(intent) = &state.moves.resize else {
                return MoveCommandOutcome::AlreadyApplied;
            };
            if intent.operation_id != operation_id
                || matches!(intent.phase, ResizeIntentPhase::Committed(_))
            {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.resize = None;
            MoveCommandOutcome::Applied
        }
        ResizeCommand::Finish { operation_id } => {
            let Some(intent) = &state.moves.resize else {
                return MoveCommandOutcome::AlreadyApplied;
            };
            if intent.operation_id != operation_id
                || !matches!(intent.phase, ResizeIntentPhase::Committed(_))
                || !intent.desired.is_committed_in(state)
            {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.resize = None;
            MoveCommandOutcome::Applied
        }
    }
}

#[cfg(test)]
mod tests;
