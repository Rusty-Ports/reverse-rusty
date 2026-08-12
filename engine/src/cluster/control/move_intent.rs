//! Durable data-movement intent and conditional assignment transitions.
//!
//! A live shard move has physical side effects outside Raft.  This document state is the
//! coordination point that makes those effects restartable and prevents two coordinators from
//! committing contradictory owners.  Every command is idempotent by `operation_id`; preconditions
//! are evaluated inside the replicated state machine, never by a read followed by a blind write.

use serde::{Deserialize, Serialize};

use super::{ClusterState, NodeId, ShardAssignment};

pub const MOVE_INTENT_VERSION: u32 = 1;
pub const MOVE_CONTROL_FORMAT_LEGACY: u32 = 1;
pub const MOVE_CONTROL_FORMAT_CURRENT: u32 = 2;

/// Per-position assignment generation used by the move compare-and-set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentGeneration {
    pub position: u32,
    pub generation: u64,
}

/// The durable control state added by the resumable-move protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveControlState {
    pub format_version: u32,
    #[serde(default)]
    pub assignment_generations: Vec<AssignmentGeneration>,
    #[serde(default)]
    pub intents: Vec<MoveIntent>,
}

impl Default for MoveControlState {
    fn default() -> Self {
        Self {
            format_version: MOVE_CONTROL_FORMAT_LEGACY,
            assignment_generations: Vec::new(),
            intents: Vec::new(),
        }
    }
}

impl MoveControlState {
    pub fn assignment_generation(&self, position: u32) -> u64 {
        self.assignment_generations
            .iter()
            .find(|entry| entry.position == position)
            .map_or(0, |entry| entry.generation)
    }

    pub(super) fn bump_assignment_generation(&mut self, position: u32) {
        if let Some(entry) = self
            .assignment_generations
            .iter_mut()
            .find(|entry| entry.position == position)
        {
            entry.generation = entry.generation.saturating_add(1);
            return;
        }
        self.assignment_generations.push(AssignmentGeneration {
            position,
            generation: 1,
        });
        self.assignment_generations
            .sort_unstable_by_key(|entry| entry.position);
    }

    pub(super) fn retain_positions_below(&mut self, num_shards: u32) {
        self.assignment_generations
            .retain(|entry| entry.position < num_shards);
        // Keep an unresolved intent even if a conflicting resize removed its position. Startup
        // must see and reject that ambiguity rather than silently forgetting external move work.
    }
}

/// A node identity is both its logical id and the normalized endpoint observed at planning time.
/// Re-registering the same id at another address therefore invalidates the transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveMemberIdentity {
    pub node: NodeId,
    pub endpoint: String,
}

/// Which side was serving writes when the intent was created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveInitialAuthority {
    /// The committed assignment was still live. A pre-ready crash can safely abort to it.
    Expected,
    /// A preceding raw handoff already made the desired assignment live. Startup must resume
    /// toward the desired side; returning to the committed source could be stale.
    Desired,
}

/// Exact live-set evidence captured while a normal move is fenced, or an attestation snapshot of
/// an already-live desired member. Fingerprints are order-independent 128-bit values plus count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveMemberEvidence {
    pub node: NodeId,
    pub fingerprint_lo: u64,
    pub fingerprint_hi: u64,
    pub live_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveRecoveryEvidence {
    pub live_generation: u64,
    pub members: Vec<MoveMemberEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveIntentPhase {
    Preparing,
    Ready(MoveRecoveryEvidence),
    Committed(MoveRecoveryEvidence),
}

/// One resumable move. Only one intent may exist for a position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveIntent {
    pub intent_version: u32,
    pub operation_id: u64,
    pub position: u32,
    pub expected_assignment_generation: u64,
    pub expected: ShardAssignment,
    pub desired: ShardAssignment,
    pub members: Vec<MoveMemberIdentity>,
    pub live_generation: u64,
    pub initial_authority: MoveInitialAuthority,
    pub phase: MoveIntentPhase,
}

impl MoveIntent {
    pub fn recovery_evidence(&self) -> Option<&MoveRecoveryEvidence> {
        match &self.phase {
            MoveIntentPhase::Preparing => None,
            MoveIntentPhase::Ready(evidence) | MoveIntentPhase::Committed(evidence) => {
                Some(evidence)
            }
        }
    }
}

/// Idempotent commands whose preconditions are evaluated atomically by the control state machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveCommand {
    Begin(MoveIntent),
    MarkReady {
        operation_id: u64,
        evidence: MoveRecoveryEvidence,
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

impl MoveCommand {
    pub fn operation_id(&self) -> u64 {
        match self {
            Self::Begin(intent) => intent.operation_id,
            Self::MarkReady { operation_id, .. }
            | Self::Commit { operation_id }
            | Self::Abort { operation_id }
            | Self::Finish { operation_id } => *operation_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveCommandOutcome {
    Applied,
    AlreadyApplied,
    Conflict,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MoveProposalResult {
    pub version: super::StateVersion,
    pub outcome: MoveCommandOutcome,
}

pub(super) fn normalized_move_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_ascii_lowercase()
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

fn expected_member_nodes(intent: &MoveIntent) -> Option<Vec<NodeId>> {
    let mut nodes = assignment_nodes(&intent.expected)?;
    nodes.extend(assignment_nodes(&intent.desired)?);
    nodes.sort_unstable();
    nodes.dedup();
    Some(nodes)
}

fn identities_match(state: &ClusterState, intent: &MoveIntent) -> bool {
    let Some(nodes) = expected_member_nodes(intent) else {
        return false;
    };
    if intent.members.len() != nodes.len()
        || !intent
            .members
            .windows(2)
            .all(|pair| pair[0].node < pair[1].node)
        || intent
            .members
            .iter()
            .map(|member| member.node)
            .ne(nodes.into_iter())
    {
        return false;
    }
    intent.members.iter().all(|member| {
        !member.endpoint.is_empty()
            && member.endpoint == normalized_move_endpoint(&member.endpoint)
            && state
                .nodes
                .iter()
                .find(|node| node.id == member.node)
                .and_then(|node| node.addr.as_deref())
                .is_some_and(|endpoint| normalized_move_endpoint(endpoint) == member.endpoint)
    })
}

fn valid_begin(state: &ClusterState, intent: &MoveIntent) -> bool {
    intent.intent_version == MOVE_INTENT_VERSION
        && intent.operation_id != 0
        && intent.position < state.num_shards
        && intent.expected.position == intent.position
        && intent.desired.position == intent.position
        && intent.expected != intent.desired
        && intent.live_generation != 0
        && matches!(intent.phase, MoveIntentPhase::Preparing)
        && identities_match(state, intent)
}

fn evidence_matches(intent: &MoveIntent, evidence: &MoveRecoveryEvidence) -> bool {
    let Some(mut desired_nodes) = assignment_nodes(&intent.desired) else {
        return false;
    };
    let evidence_nodes: Vec<NodeId> = evidence.members.iter().map(|member| member.node).collect();
    if !evidence_nodes.windows(2).all(|pair| pair[0] < pair[1]) {
        return false;
    }
    desired_nodes.sort_unstable();
    evidence_nodes == desired_nodes && evidence.live_generation == intent.live_generation
}

fn assignment_matches_expected(state: &ClusterState, intent: &MoveIntent) -> bool {
    state
        .assignments
        .iter()
        .find(|assignment| assignment.position == intent.position)
        == Some(&intent.expected)
        && state.moves.assignment_generation(intent.position)
            == intent.expected_assignment_generation
        && identities_match(state, intent)
}

fn replace_assignment(state: &mut ClusterState, assignment: ShardAssignment) {
    let position = assignment.position;
    state
        .assignments
        .retain(|current| current.position != position);
    state.assignments.push(assignment);
    state
        .assignments
        .sort_unstable_by_key(|current| current.position);
    state.moves.bump_assignment_generation(position);
}

/// Apply one move command. The caller still owns the document epoch bump.
pub(super) fn apply_move(state: &mut ClusterState, command: MoveCommand) -> MoveCommandOutcome {
    // Once any new command is observed, every snapshot uses the v2 compatibility fence even when
    // the command is malformed or loses a race. Otherwise a compacted rejected command could be
    // hidden from an old binary joining from the resulting snapshot.
    state.moves.format_version = MOVE_CONTROL_FORMAT_CURRENT;
    match command {
        MoveCommand::Begin(intent) => {
            if !valid_begin(state, &intent) {
                return MoveCommandOutcome::Invalid;
            }
            if let Some(current) = state
                .moves
                .intents
                .iter()
                .find(|current| current.operation_id == intent.operation_id)
            {
                return if current == &intent {
                    MoveCommandOutcome::AlreadyApplied
                } else {
                    MoveCommandOutcome::Conflict
                };
            }
            if let Some(current) = state
                .moves
                .intents
                .iter()
                .find(|current| current.position == intent.position)
            {
                return if current == &intent {
                    MoveCommandOutcome::AlreadyApplied
                } else {
                    MoveCommandOutcome::Conflict
                };
            }
            if !assignment_matches_expected(state, &intent) {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.intents.push(intent);
            state
                .moves
                .intents
                .sort_unstable_by_key(|intent| intent.position);
            MoveCommandOutcome::Applied
        }
        MoveCommand::MarkReady {
            operation_id,
            evidence,
        } => {
            let Some(index) = state
                .moves
                .intents
                .iter()
                .position(|intent| intent.operation_id == operation_id)
            else {
                return MoveCommandOutcome::Conflict;
            };
            let intent = &state.moves.intents[index];
            if !evidence_matches(intent, &evidence) {
                return MoveCommandOutcome::Invalid;
            }
            match &intent.phase {
                MoveIntentPhase::Preparing => {
                    if !assignment_matches_expected(state, intent) {
                        return MoveCommandOutcome::Conflict;
                    }
                    state.moves.intents[index].phase = MoveIntentPhase::Ready(evidence);
                    MoveCommandOutcome::Applied
                }
                MoveIntentPhase::Ready(current) | MoveIntentPhase::Committed(current)
                    if current == &evidence =>
                {
                    MoveCommandOutcome::AlreadyApplied
                }
                MoveIntentPhase::Ready(_) | MoveIntentPhase::Committed(_) => {
                    MoveCommandOutcome::Conflict
                }
            }
        }
        MoveCommand::Commit { operation_id } => {
            let Some(index) = state
                .moves
                .intents
                .iter()
                .position(|intent| intent.operation_id == operation_id)
            else {
                return MoveCommandOutcome::Conflict;
            };
            let intent = state.moves.intents[index].clone();
            match &intent.phase {
                MoveIntentPhase::Preparing => MoveCommandOutcome::Conflict,
                MoveIntentPhase::Ready(evidence) => {
                    if !assignment_matches_expected(state, &intent) {
                        return MoveCommandOutcome::Conflict;
                    }
                    replace_assignment(state, intent.desired.clone());
                    state.moves.intents[index].phase = MoveIntentPhase::Committed(evidence.clone());
                    MoveCommandOutcome::Applied
                }
                MoveIntentPhase::Committed(_) => {
                    let committed = state
                        .assignments
                        .iter()
                        .find(|assignment| assignment.position == intent.position);
                    if committed == Some(&intent.desired) {
                        MoveCommandOutcome::AlreadyApplied
                    } else {
                        MoveCommandOutcome::Conflict
                    }
                }
            }
        }
        MoveCommand::Abort { operation_id } => {
            let Some(index) = state
                .moves
                .intents
                .iter()
                .position(|intent| intent.operation_id == operation_id)
            else {
                return MoveCommandOutcome::AlreadyApplied;
            };
            if matches!(
                state.moves.intents[index].phase,
                MoveIntentPhase::Committed(_)
            ) {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.intents.remove(index);
            MoveCommandOutcome::Applied
        }
        MoveCommand::Finish { operation_id } => {
            let Some(index) = state
                .moves
                .intents
                .iter()
                .position(|intent| intent.operation_id == operation_id)
            else {
                return MoveCommandOutcome::AlreadyApplied;
            };
            let intent = &state.moves.intents[index];
            if !matches!(intent.phase, MoveIntentPhase::Committed(_))
                || state
                    .assignments
                    .iter()
                    .find(|assignment| assignment.position == intent.position)
                    != Some(&intent.desired)
            {
                return MoveCommandOutcome::Conflict;
            }
            state.moves.intents.remove(index);
            MoveCommandOutcome::Applied
        }
    }
}
