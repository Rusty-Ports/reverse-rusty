//! Shared durable-move protocol helpers.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::cluster::control::{
    ClusterState, ControlPlane, MoveCommand, MoveCommandOutcome, MoveInitialAuthority, MoveIntent,
    MoveIntentPhase, MoveMemberEvidence, MoveMemberIdentity, MoveRecoveryEvidence, NodeId,
    ShardAssignment, MOVE_INTENT_VERSION,
};
use crate::cluster::remote::RemoteShard;
use crate::cluster::shard::ShardError;

use super::super::distributed::handoff::normalized_endpoint;
use super::ClusterEngine;

const COMMAND_ATTEMPTS: usize = 3;

fn mix_u64(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x100_0000_01b3);
    }
}

fn mix_assignment(hash: &mut u64, assignment: &ShardAssignment) {
    mix_u64(hash, u64::from(assignment.position));
    mix_u64(hash, assignment.primary.0);
    mix_u64(hash, assignment.replicas.len() as u64);
    for replica in &assignment.replicas {
        mix_u64(hash, replica.0);
    }
}

fn operation_id(intent: &MoveIntent) -> u64 {
    // Fixed FNV-1a over the immutable predicate. A collision is harmless: Begin returns Conflict
    // because it also compares the full identity. Avoid zero because it is the invalid sentinel.
    let mut hash = 0xcbf2_9ce4_8422_2325;
    mix_u64(&mut hash, u64::from(intent.intent_version));
    mix_u64(&mut hash, u64::from(intent.position));
    mix_u64(&mut hash, intent.expected_assignment_generation);
    mix_u64(&mut hash, intent.placement_generation);
    mix_u64(&mut hash, intent.live_generation);
    mix_u64(&mut hash, intent.source_fence_generation);
    mix_u64(
        &mut hash,
        match intent.initial_authority {
            MoveInitialAuthority::Expected => 1,
            MoveInitialAuthority::Desired => 2,
        },
    );
    mix_assignment(&mut hash, &intent.expected);
    mix_assignment(&mut hash, &intent.desired);
    for member in &intent.members {
        mix_u64(&mut hash, member.node.0);
        for byte in member.endpoint.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

fn assignment_nodes(assignment: &ShardAssignment, nodes: &mut Vec<NodeId>) {
    nodes.push(assignment.primary);
    nodes.extend(assignment.replicas.iter().copied());
}

pub(super) fn build_intent(
    state: &ClusterState,
    expected: ShardAssignment,
    desired: ShardAssignment,
    live_generation: u64,
    source_fence_generation: u64,
    initial_authority: MoveInitialAuthority,
) -> Result<MoveIntent, ShardError> {
    let mut nodes = Vec::new();
    assignment_nodes(&expected, &mut nodes);
    assignment_nodes(&desired, &mut nodes);
    nodes.sort_unstable();
    nodes.dedup();
    let mut members = Vec::with_capacity(nodes.len());
    for node in nodes {
        let endpoint = state
            .nodes
            .iter()
            .find(|descriptor| descriptor.id == node)
            .and_then(|descriptor| descriptor.addr.as_deref())
            .ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "durable move: node {} has no registered endpoint",
                    node.0
                ))
            })?;
        members.push(MoveMemberIdentity {
            node,
            endpoint: normalized_endpoint(endpoint),
        });
    }
    let mut intent = MoveIntent {
        intent_version: MOVE_INTENT_VERSION,
        operation_id: 0,
        position: expected.position,
        expected_assignment_generation: state.moves.assignment_generation(expected.position),
        placement_generation: state.placement_generation,
        expected,
        desired,
        members,
        live_generation,
        source_fence_generation,
        initial_authority,
        phase: MoveIntentPhase::Preparing,
    };
    intent.operation_id = operation_id(&intent);
    Ok(intent)
}

fn endpoint_for(state: &ClusterState, node: NodeId, context: &str) -> Result<String, ShardError> {
    state
        .nodes
        .iter()
        .find(|descriptor| descriptor.id == node)
        .and_then(|descriptor| descriptor.addr.as_deref())
        .map(normalized_endpoint)
        .ok_or_else(|| {
            ShardError::ControlPlane(format!(
                "{context}: node {} has no registered endpoint",
                node.0
            ))
        })
}

fn connect(
    engine: &ClusterEngine,
    endpoint: &str,
    position: u32,
    handle: &tokio::runtime::Handle,
) -> Result<RemoteShard, ShardError> {
    RemoteShard::connect_for_coordinator_with_security(
        endpoint,
        handle.clone(),
        engine.dict.fingerprint(),
        engine.tag_dict.fingerprint(),
        position,
        engine.coordinator_id,
        &engine.client_security,
    )
    .map(|member| member.with_metrics(Arc::clone(&engine.transport_metrics)))
}

/// Choose the exact source fence that will make an already-live desired endpoint authoritative.
/// The caller persists it before applying the fence, so a crash cannot strand an unrecorded
/// physical side effect. Logical NodeId aliases share one physical endpoint and remain unfenced.
pub(super) fn plan_live_authority_fence(
    state: &ClusterState,
    expected: &ShardAssignment,
    desired_endpoint: &str,
    live_generation: u64,
    context: &str,
) -> Result<u64, ShardError> {
    let source_endpoint = endpoint_for(state, expected.primary, context)?;
    let aliases_source = source_endpoint == normalized_endpoint(desired_endpoint);
    if aliases_source {
        return Ok(0);
    }
    live_generation.checked_add(1).ok_or_else(|| {
        ShardError::ControlPlane(format!(
            "{context}: no source-fence generation remains after live generation {live_generation}"
        ))
    })
}

fn connect_and_adopt_source(
    engine: &ClusterEngine,
    move_intent: &MoveIntent,
    endpoint: &str,
    handle: &tokio::runtime::Handle,
) -> Result<RemoteShard, ShardError> {
    RemoteShard::connect_and_adopt_for_coordinator_with_security(
        endpoint,
        handle.clone(),
        crate::storage::serialize_dict(&engine.dict),
        engine.dict.fingerprint(),
        crate::storage::serialize_tagdict(&engine.tag_dict),
        engine.tag_dict.fingerprint(),
        move_intent.position,
        crate::ownership::PlacementGeneration(move_intent.placement_generation),
        engine.num_shards() as u32,
        engine.coordinator_id,
        &engine.client_security,
    )
    .map(|member| member.with_metrics(Arc::clone(&engine.transport_metrics)))
}

/// Persist evidence and conditionally commit an RF=1 target that is already the live authority.
/// `Begin` must have succeeded first. Re-probing the recorded source fence closes the observation →
/// evidence race; a changed fence preserves the intent for startup instead of guessing authority.
pub(super) fn commit_live_authority(
    engine: &ClusterEngine,
    move_intent: &MoveIntent,
    target_endpoint: &str,
    handle: &tokio::runtime::Handle,
    context: &str,
) -> Result<(), ShardError> {
    if !move_intent.desired.replicas.is_empty() {
        return Err(ShardError::Config(format!(
            "{context}: already-live authority reconciliation is RF=1 only"
        )));
    }
    let source_endpoint = move_intent
        .members
        .iter()
        .find(|member| member.node == move_intent.expected.primary)
        .map(|member| member.endpoint.as_str())
        .ok_or_else(|| {
            ShardError::ControlPlane(format!(
                "{context}: durable move lacks its expected source identity"
            ))
        })?;
    let source = connect_and_adopt_source(engine, move_intent, source_endpoint, handle)?;
    let actual = if move_intent.source_fence_generation == 0 {
        source.fence(0)?
    } else {
        source.fence(move_intent.source_fence_generation)?
    };
    if actual != move_intent.source_fence_generation {
        return Err(ShardError::ControlPlane(format!(
            "{context}: recorded source fence {} changed to {actual}",
            move_intent.source_fence_generation
        )));
    }
    let target = connect(engine, target_endpoint, move_intent.position, handle)?;
    let evidence = recovery_evidence(
        move_intent.live_generation,
        vec![member_evidence(move_intent.desired.primary, &target)?],
    );
    propose(
        engine.control.as_ref(),
        &MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence,
        },
        context,
    )?;
    propose(
        engine.control.as_ref(),
        &MoveCommand::Commit {
            operation_id: move_intent.operation_id,
        },
        context,
    )
}

pub(super) fn member_evidence(
    node: NodeId,
    member: &RemoteShard,
) -> Result<MoveMemberEvidence, ShardError> {
    let (fingerprint_lo, fingerprint_hi, live_count) = member.content_fingerprint()?;
    Ok(MoveMemberEvidence {
        node,
        fingerprint_lo,
        fingerprint_hi,
        live_count,
    })
}

pub(super) fn recovery_evidence(
    live_generation: u64,
    mut members: Vec<MoveMemberEvidence>,
) -> MoveRecoveryEvidence {
    members.sort_unstable_by_key(|member| member.node);
    MoveRecoveryEvidence {
        live_generation,
        members,
    }
}

pub(super) fn propose(
    control: &dyn ControlPlane,
    command: &MoveCommand,
    context: &str,
) -> Result<(), ShardError> {
    let operation_id = command.operation_id();
    let mut last_error = None;
    for attempt in 0..COMMAND_ATTEMPTS {
        match control.propose_move(command.clone()) {
            Ok(result) => {
                return match result.outcome {
                    MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(()),
                    MoveCommandOutcome::Conflict => Err(ShardError::ControlPlane(format!(
                        "{context}: durable move {operation_id} lost its conditional predicate"
                    ))),
                    MoveCommandOutcome::Invalid => Err(ShardError::ControlPlane(format!(
                        "{context}: durable move {operation_id} was rejected as invalid"
                    ))),
                };
            }
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < COMMAND_ATTEMPTS {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
    Err(last_error.map_or_else(
        || ShardError::ControlPlane(format!("{context}: proposal failed")),
        ShardError::from,
    ))
}

pub(super) fn abort(engine: &ClusterEngine, intent: &MoveIntent, context: &str) {
    if let Err(error) = propose(
        engine.control.as_ref(),
        &MoveCommand::Abort {
            operation_id: intent.operation_id,
        },
        context,
    ) {
        engine.emit(crate::events::EngineEvent::DurabilityFailure {
            op: crate::events::DurabilityOp::ReplicaDesync,
            detail: format!(
                "{context}: cleanup could not remove preparing durable move {}; startup will \
                 inspect and abort it before serving",
                intent.operation_id
            ),
            error: error.to_string(),
        });
    }
}
