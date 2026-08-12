//! Cold-start resolution of durable physical-move intents.

use std::collections::BTreeSet;

use crate::cluster::control::{
    ClusterState, ControlPlane, MoveCommand, MoveInitialAuthority, MoveIntent, MoveIntentPhase,
    MoveMemberIdentity, MoveRecoveryEvidence, NodeId, ShardAssignment, MOVE_INTENT_VERSION,
};
use crate::cluster::remote::RemoteShard;
use crate::cluster::security::ClientSecurity;
use crate::cluster::shard::ShardError;
use crate::dict::Dict;
use crate::ownership::PlacementGeneration;
use crate::tagdict::TagDict;

use super::super::distributed::handoff::normalized_endpoint;
use super::intent;

struct RecoveryContext<'a> {
    control: &'a dyn ControlPlane,
    dict: &'a Dict,
    tag_dict: &'a TagDict,
    dict_bytes: Vec<u8>,
    tag_dict_bytes: Vec<u8>,
    num_shards: u32,
    handle: &'a tokio::runtime::Handle,
    coordinator_id: u64,
    security: &'a ClientSecurity,
}

fn assignment_nodes(assignment: &ShardAssignment) -> Vec<NodeId> {
    let mut nodes = Vec::with_capacity(1 + assignment.replicas.len());
    nodes.push(assignment.primary);
    nodes.extend(assignment.replicas.iter().copied());
    nodes
}

fn member_identity<'a>(
    intent: &'a MoveIntent,
    node: NodeId,
) -> Result<&'a MoveMemberIdentity, ShardError> {
    intent
        .members
        .iter()
        .find(|member| member.node == node)
        .ok_or_else(|| {
            ShardError::ControlPlane(format!(
                "durable move {} lacks identity for node {}",
                intent.operation_id, node.0
            ))
        })
}

fn validate_identity(state: &ClusterState, intent: &MoveIntent) -> Result<(), ShardError> {
    if intent.intent_version != MOVE_INTENT_VERSION
        || intent.position >= state.num_shards
        || intent.placement_generation != state.placement_generation
    {
        return Err(ShardError::ControlPlane(format!(
            "durable move {} has an incompatible version, position, or placement generation",
            intent.operation_id
        )));
    }
    let mut expected_nodes = assignment_nodes(&intent.expected);
    expected_nodes.extend(assignment_nodes(&intent.desired));
    expected_nodes.sort_unstable();
    expected_nodes.dedup();
    let member_nodes: Vec<NodeId> = intent.members.iter().map(|member| member.node).collect();
    let unique_endpoints: BTreeSet<&str> = intent
        .members
        .iter()
        .map(|member| member.endpoint.as_str())
        .collect();
    if member_nodes != expected_nodes
        || !intent
            .members
            .windows(2)
            .all(|pair| pair[0].node < pair[1].node)
        || unique_endpoints.len() != intent.members.len()
    {
        return Err(ShardError::ControlPlane(format!(
            "durable move {} has an incomplete or non-canonical member identity set",
            intent.operation_id
        )));
    }
    for member in &intent.members {
        let current = state
            .nodes
            .iter()
            .find(|descriptor| descriptor.id == member.node)
            .and_then(|descriptor| descriptor.addr.as_deref())
            .ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "durable move {} node {} no longer has an endpoint",
                    intent.operation_id, member.node.0
                ))
            })?;
        if normalized_endpoint(current) != member.endpoint {
            return Err(ShardError::ControlPlane(format!(
                "durable move {} endpoint identity changed for node {}",
                intent.operation_id, member.node.0
            )));
        }
    }
    let assignment = state
        .assignments
        .iter()
        .find(|assignment| assignment.position == intent.position);
    match intent.phase {
        MoveIntentPhase::Preparing | MoveIntentPhase::Ready(_) => {
            if assignment != Some(&intent.expected)
                || state.moves.assignment_generation(intent.position)
                    != intent.expected_assignment_generation
            {
                return Err(ShardError::ControlPlane(format!(
                    "durable move {} expected assignment predicate no longer holds",
                    intent.operation_id
                )));
            }
        }
        MoveIntentPhase::Committed(_) => {
            if assignment != Some(&intent.desired)
                || state.moves.assignment_generation(intent.position)
                    != intent.expected_assignment_generation.saturating_add(1)
            {
                return Err(ShardError::ControlPlane(format!(
                    "durable move {} committed assignment predicate no longer holds",
                    intent.operation_id
                )));
            }
        }
    }
    Ok(())
}

fn validate_intent_set(state: &ClusterState) -> Result<(), ShardError> {
    if !state
        .moves
        .intents
        .windows(2)
        .all(|pair| pair[0].position < pair[1].position)
    {
        return Err(ShardError::ControlPlane(
            "durable move state has duplicate or non-canonical positions".into(),
        ));
    }
    for (index, intent) in state.moves.intents.iter().enumerate() {
        for other in state.moves.intents.iter().skip(index + 1) {
            if intent.operation_id == other.operation_id
                || intent.members.iter().any(|member| {
                    other
                        .members
                        .iter()
                        .any(|candidate| candidate.endpoint == member.endpoint)
                })
            {
                return Err(ShardError::ControlPlane(
                    "durable move state has duplicate operation ids or overlapping endpoint \
                     reservations"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

impl RecoveryContext<'_> {
    fn connect(&self, intent: &MoveIntent, node: NodeId) -> Result<RemoteShard, ShardError> {
        let member = member_identity(intent, node)?;
        RemoteShard::connect_and_adopt_for_coordinator_with_security(
            &member.endpoint,
            self.handle.clone(),
            self.dict_bytes.clone(),
            self.dict.fingerprint(),
            self.tag_dict_bytes.clone(),
            self.tag_dict.fingerprint(),
            intent.position,
            PlacementGeneration(intent.placement_generation),
            self.num_shards,
            Some(self.coordinator_id),
            self.security,
        )
    }

    fn expected_source(&self, intent: &MoveIntent) -> Result<RemoteShard, ShardError> {
        self.connect(intent, intent.expected.primary)
    }

    fn require_source_fence(
        &self,
        intent: &MoveIntent,
        allow_retained_unfenced: bool,
    ) -> Result<(), ShardError> {
        let source = self.expected_source(intent)?;
        let actual = source.fence(0)?;
        let retained = assignment_nodes(&intent.desired).contains(&intent.expected.primary);
        if actual == intent.live_generation || (allow_retained_unfenced && retained && actual == 0)
        {
            Ok(())
        } else {
            Err(ShardError::ControlPlane(format!(
                "durable move {} expected source fence {}, observed {}; refusing ambiguous \
                 authority",
                intent.operation_id, intent.live_generation, actual
            )))
        }
    }

    fn desired_evidence(&self, intent: &MoveIntent) -> Result<MoveRecoveryEvidence, ShardError> {
        let mut evidence = Vec::new();
        for node in assignment_nodes(&intent.desired) {
            let member = self.connect(intent, node)?;
            let fence = member.fence(0)?;
            if node != intent.expected.primary && fence != 0 {
                return Err(ShardError::ControlPlane(format!(
                    "durable move {} desired member {} remains fenced at generation {}",
                    intent.operation_id, node.0, fence
                )));
            }
            evidence.push(intent::member_evidence(node, &member)?);
        }
        Ok(intent::recovery_evidence(intent.live_generation, evidence))
    }

    fn clear_retained_source(&self, intent: &MoveIntent) -> Result<(), ShardError> {
        if !assignment_nodes(&intent.desired).contains(&intent.expected.primary) {
            return Ok(());
        }
        let current = self
            .expected_source(intent)?
            .unfence(intent.live_generation)?;
        if current != 0 {
            return Err(ShardError::ControlPlane(format!(
                "durable move {} retained source could not be unfenced; generation {} remains",
                intent.operation_id, current
            )));
        }
        Ok(())
    }

    fn abort_expected_preparation(&self, intent: &MoveIntent) -> Result<(), ShardError> {
        let current = self
            .expected_source(intent)?
            .unfence(intent.live_generation)?;
        if current != 0 {
            return Err(ShardError::ControlPlane(format!(
                "durable move {} expected source remains fenced at generation {}; refusing to \
                 discard the intent",
                intent.operation_id, current
            )));
        }
        intent::propose(
            self.control,
            MoveCommand::Abort {
                operation_id: intent.operation_id,
            },
            "startup: abort preparing move",
        )
    }

    fn commit_with_evidence(
        &self,
        intent: &MoveIntent,
        evidence: MoveRecoveryEvidence,
    ) -> Result<(), ShardError> {
        intent::propose(
            self.control,
            MoveCommand::MarkReady {
                operation_id: intent.operation_id,
                evidence,
            },
            "startup: persist recovered move evidence",
        )?;
        intent::propose(
            self.control,
            MoveCommand::Commit {
                operation_id: intent.operation_id,
            },
            "startup: conditionally commit recovered move",
        )
    }

    fn finish(&self, intent: &MoveIntent) -> Result<(), ShardError> {
        intent::propose(
            self.control,
            MoveCommand::Finish {
                operation_id: intent.operation_id,
            },
            "startup: finish recovered move",
        )
    }

    fn recover_one(&self, state: &ClusterState, intent: &MoveIntent) -> Result<(), ShardError> {
        validate_identity(state, intent)?;
        match &intent.phase {
            MoveIntentPhase::Preparing
                if intent.initial_authority == MoveInitialAuthority::Expected =>
            {
                self.abort_expected_preparation(intent)
            }
            MoveIntentPhase::Preparing => {
                self.require_source_fence(intent, false)?;
                let evidence = self.desired_evidence(intent)?;
                self.commit_with_evidence(intent, evidence)?;
                self.clear_retained_source(intent)?;
                self.finish(intent)
            }
            MoveIntentPhase::Ready(expected) => {
                self.require_source_fence(intent, false)?;
                let actual = self.desired_evidence(intent)?;
                if &actual != expected {
                    return Err(ShardError::ControlPlane(format!(
                        "durable move {} target evidence changed after readiness; refusing \
                         ambiguous cutover",
                        intent.operation_id
                    )));
                }
                self.commit_with_evidence(intent, actual)?;
                self.clear_retained_source(intent)?;
                self.finish(intent)
            }
            MoveIntentPhase::Committed(_expected) => {
                self.require_source_fence(intent, true)?;
                // `expected` is the immutable proof that every desired member was complete at the
                // atomic assignment commit. After the live swap, acknowledged writes legitimately
                // advance those fingerprints before a crash. The committed assignment already
                // decides authority, so startup attests every desired endpoint/placement/fence but
                // must not mistake valid post-cutover progress for ambiguity.
                let _current = self.desired_evidence(intent)?;
                self.clear_retained_source(intent)?;
                self.finish(intent)
            }
        }
    }
}

/// Resolve every durable move intent before assignment-routed coordinator assembly. The function
/// uses only recorded authority, exact fence generations, endpoint identities, placement identity,
/// and full member fingerprints. Reachability alone never selects a side. Any mismatch fails the
/// coordinator start, so neither reads nor writes can escape from ambiguous routing.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn recover_durable_moves(
    control: &dyn ControlPlane,
    dict: &Dict,
    tag_dict: &TagDict,
    num_shards: u32,
    handle: &tokio::runtime::Handle,
    coordinator_id: u64,
    security: &ClientSecurity,
) -> Result<usize, ShardError> {
    let context = RecoveryContext {
        control,
        dict,
        tag_dict,
        dict_bytes: crate::storage::serialize_dict(dict),
        tag_dict_bytes: crate::storage::serialize_tagdict(tag_dict),
        num_shards,
        handle,
        coordinator_id,
        security,
    };
    let mut recovered = 0usize;
    for _ in 0..4 {
        let snapshot = control.cluster_state()?;
        validate_intent_set(&snapshot)?;
        let intents = snapshot.moves.intents.clone();
        drop(snapshot);
        if intents.is_empty() {
            return Ok(recovered);
        }
        for original in intents {
            let state = control.cluster_state()?;
            let Some(current) = state
                .moves
                .intents
                .iter()
                .find(|intent| intent.operation_id == original.operation_id)
                .cloned()
            else {
                continue;
            };
            context.recover_one(&state, &current)?;
            recovered = recovered.saturating_add(1);
        }
    }
    Err(ShardError::ControlPlane(
        "durable move intents kept appearing during startup recovery; refusing to serve while \
         control state is churning"
            .into(),
    ))
}
