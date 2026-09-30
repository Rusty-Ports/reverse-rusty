//! Coordinator-startup resolution of a recorded remote-resize intent (ADR-180).

use crate::cluster::control::{MoveCommandOutcome, ResizeCommand, ResizeIntentPhase};

use super::ShardError;

/// What coordinator startup did with a recorded remote-resize intent (ADR-180).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResizeRecovery {
    /// An uncommitted intent was aborted; the previous layout remains authoritative.
    Aborted { operation_id: u64 },
    /// A committed intent was finished; the committed (new) layout is authoritative.
    Finished { operation_id: u64 },
}

/// Resolve a recorded remote-resize intent before routes are assembled (ADR-180). An uncommitted
/// intent is aborted: its staged layout was never routed, and the previous layout is still the
/// committed one. A committed intent is finished: consensus already names the new layout. The
/// retired slots cannot be fenced here without their data-node handles, so a finished recovery
/// reports them for operator decommissioning. Returns `None` when no intent is recorded; any
/// refused or failed transition fails startup rather than serving an ambiguous layout.
pub fn recover_durable_resize(
    control: &dyn crate::cluster::control::ControlPlane,
) -> Result<Option<ResizeRecovery>, ShardError> {
    let state = control
        .cluster_state()
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?;
    let Some(intent) = state.moves.resize.clone() else {
        return Ok(None);
    };
    let operation_id = intent.operation_id;
    let (command, recovery) = match intent.phase {
        ResizeIntentPhase::Preparing | ResizeIntentPhase::Ready(_) => (
            ResizeCommand::Abort { operation_id },
            ResizeRecovery::Aborted { operation_id },
        ),
        ResizeIntentPhase::Committed(_) => (
            ResizeCommand::Finish { operation_id },
            ResizeRecovery::Finished { operation_id },
        ),
    };
    let outcome = control
        .propose_resize(command)
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?
        .outcome;
    match outcome {
        MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(Some(recovery)),
        outcome => Err(ShardError::ControlPlane(format!(
            "could not resolve remote-resize intent {operation_id} at startup ({outcome:?}); \
             inspect the control-plane state before serving"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{recover_durable_resize, ResizeRecovery};
    use crate::cluster::control::{
        ClusterStateChange, ControlPlane, InMemoryControlPlane, MoveMemberIdentity, NodeDescriptor,
        NodeId, NodeRole, ResizeCommand, ResizeIntent, ResizeIntentPhase, ResizeLayout,
        ResizePositionEvidence, ShardAssignment, RESIZE_INTENT_VERSION,
    };

    fn plane_with_intent(operation_id: u64) -> InMemoryControlPlane {
        let cp = InMemoryControlPlane::single_node(1, 64, 1);
        for (id, port) in [(1u64, 1u16), (2, 2)] {
            cp.propose(ClusterStateChange::AddNode(NodeDescriptor {
                id: NodeId(id),
                addr: Some(format!("http://127.0.0.1:{port}")),
                role: NodeRole::Data,
            }))
            .expect("node");
        }
        cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: Vec::new(),
        }))
        .expect("assign");
        let state = cp.cluster_state().expect("state");
        let intent = ResizeIntent {
            intent_version: RESIZE_INTENT_VERSION,
            operation_id,
            expected: ResizeLayout {
                num_shards: 1,
                placement_generation: state.placement_generation,
                assignments: state.assignments.clone(),
            },
            desired: ResizeLayout {
                num_shards: 1,
                placement_generation: state.placement_generation + 1,
                assignments: vec![ShardAssignment {
                    position: 0,
                    primary: NodeId(2),
                    replicas: Vec::new(),
                }],
            },
            members: vec![MoveMemberIdentity {
                node: NodeId(2),
                endpoint: "http://127.0.0.1:2".into(),
            }],
            phase: ResizeIntentPhase::Preparing,
        };
        cp.propose_resize(ResizeCommand::Begin(intent))
            .expect("begin");
        cp
    }

    #[test]
    fn startup_aborts_an_uncommitted_intent_and_finishes_a_committed_one() {
        let empty = InMemoryControlPlane::single_node(1, 64, 1);
        assert_eq!(recover_durable_resize(&empty).expect("none"), None);

        let preparing = plane_with_intent(3);
        assert_eq!(
            recover_durable_resize(&preparing).expect("abort"),
            Some(ResizeRecovery::Aborted { operation_id: 3 })
        );
        let state = preparing.cluster_state().expect("state");
        assert!(state.moves.resize.is_none());
        assert_eq!(state.assignments[0].primary, NodeId(1), "old layout kept");

        let committed = plane_with_intent(4);
        committed
            .propose_resize(ResizeCommand::MarkReady {
                operation_id: 4,
                evidence: vec![ResizePositionEvidence {
                    position: 0,
                    fingerprint_lo: 1,
                    fingerprint_hi: 2,
                    live_count: 3,
                }],
            })
            .expect("ready");
        committed
            .propose_resize(ResizeCommand::Commit { operation_id: 4 })
            .expect("commit");
        assert_eq!(
            recover_durable_resize(&committed).expect("finish"),
            Some(ResizeRecovery::Finished { operation_id: 4 })
        );
        let state = committed.cluster_state().expect("state");
        assert!(state.moves.resize.is_none());
        assert_eq!(state.assignments[0].primary, NodeId(2), "new layout kept");
    }
}
