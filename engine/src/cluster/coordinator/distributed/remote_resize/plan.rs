//! Planning a remote resize: the committed layout's endpoints and the complete resize intent.

use crate::cluster::control::{
    normalized_move_endpoint, ClusterState, MoveMemberIdentity, NodeId, ResizeIntent,
    ResizeIntentPhase, ResizeLayout, ShardAssignment, RESIZE_INTENT_VERSION,
};

use super::{RemoteResizeRequest, ShardError};

/// The primary endpoint of every current position, in position order.
pub(super) fn expected_endpoints(state: &ClusterState) -> Result<Vec<String>, ShardError> {
    state
        .assignments
        .iter()
        .map(|assignment| {
            if !assignment.replicas.is_empty() {
                return Err(ShardError::Config(
                    "remote resize supports replication factor 1".into(),
                ));
            }
            state
                .nodes
                .iter()
                .find(|node| node.id == assignment.primary)
                .and_then(|node| node.addr.clone())
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "position {} is assigned to node {} with no registered endpoint",
                        assignment.position, assignment.primary.0
                    ))
                })
        })
        .collect()
}

pub(super) fn member_endpoint(intent: &ResizeIntent, node: NodeId) -> Result<String, ShardError> {
    intent
        .members
        .iter()
        .find(|member| member.node == node)
        .map(|member| member.endpoint.clone())
        .ok_or_else(|| ShardError::Config(format!("target node {} has no endpoint", node.0)))
}

/// The complete intent: the committed layout, and the new layout with position `p` on
/// `targets[p % targets.len()]` at the next placement generation.
pub(super) fn resize_intent(
    state: &ClusterState,
    request: &RemoteResizeRequest,
) -> Result<ResizeIntent, ShardError> {
    let num_shards = u32::try_from(request.num_shards)
        .map_err(|_| ShardError::Config("remote resize shard count is out of range".into()))?;
    let placement_generation = state
        .placement_generation
        .checked_add(1)
        .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
    let assignments = (0..num_shards)
        .map(|position| ShardAssignment {
            position,
            primary: request.targets[position as usize % request.targets.len()].id,
            replicas: Vec::new(),
        })
        .collect();
    let mut members: Vec<MoveMemberIdentity> = request
        .targets
        .iter()
        .take(request.num_shards)
        .map(|target| MoveMemberIdentity {
            node: target.id,
            endpoint: target
                .addr
                .as_deref()
                .map(normalized_move_endpoint)
                .unwrap_or_default(),
        })
        .collect();
    members.sort_by_key(|member| member.node);
    Ok(ResizeIntent {
        intent_version: RESIZE_INTENT_VERSION,
        operation_id: request.operation_id,
        expected: ResizeLayout {
            num_shards: state.num_shards,
            placement_generation: state.placement_generation,
            assignments: state.assignments.clone(),
        },
        desired: ResizeLayout {
            num_shards,
            placement_generation,
            assignments,
        },
        members,
        phase: ResizeIntentPhase::Preparing,
    })
}
