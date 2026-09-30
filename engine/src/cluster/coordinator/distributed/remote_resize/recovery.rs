//! Coordinator-startup resolution of a recorded remote-resize intent (ADR-180).

use crate::cluster::control::{
    normalized_move_endpoint, ClusterState, ControlPlane, MoveCommandOutcome, ResizeCommand,
    ResizeIntentPhase,
};

use super::{ClusterEngine, ShardError};

impl ClusterEngine {
    /// Resolve a recorded remote-resize intent through this coordinator's control plane. Call it
    /// only after the coordinator has claimed its shards exclusively: a live coordinator that is
    /// still running the resize then holds those claims, so this coordinator fails to connect
    /// instead of aborting the resize underneath it.
    ///
    /// Resolution makes the committed layout authoritative, so it first attests that this
    /// coordinator serves exactly that layout. A coordinator assembled from an older read (for
    /// example, one that connected just before another coordinator committed) fails here instead
    /// of finishing the intent and serving the retired layout.
    pub fn recover_resize_intent(&self) -> Result<Option<ResizeRecovery>, ShardError> {
        let state = self.control_state()?;
        if state.moves.resize.is_none() {
            return Ok(None);
        }
        self.attest_serving_layout(&state)?;
        resolve_resize_intent(self.control.as_ref(), &state)
    }

    /// Fail unless this coordinator serves exactly the committed layout: its shard count,
    /// placement generation, and every position's primary endpoint. An assignment-routed
    /// coordinator checks this before serving, because its topology was read before it connected.
    pub fn attest_committed_layout(&self) -> Result<(), ShardError> {
        self.attest_serving_layout(&self.control_state()?)
    }

    fn attest_serving_layout(&self, state: &ClusterState) -> Result<(), ShardError> {
        let generation = self.placement_generation().0;
        if state.num_shards as usize != self.shards.len()
            || state.placement_generation != generation
        {
            return Err(ShardError::ControlPlane(format!(
                "this coordinator serves placement generation {generation} with {} shards, but \
                 the committed layout is generation {} with {} shards; restart it with \
                 --route-by-assignments to route to the committed layout",
                self.shards.len(),
                state.placement_generation,
                state.num_shards
            )));
        }
        for (position, shard) in self.shards.iter().enumerate() {
            let committed = state
                .assignments
                .iter()
                .find(|assignment| assignment.position as usize == position)
                .and_then(|assignment| {
                    state
                        .nodes
                        .iter()
                        .find(|node| node.id == assignment.primary)
                })
                .and_then(|node| node.addr.as_deref())
                .map(normalized_move_endpoint);
            let serving = shard
                .live_primary_endpoint()
                .as_deref()
                .map(normalized_move_endpoint);
            if committed.is_none() || committed != serving {
                return Err(ShardError::ControlPlane(format!(
                    "position {position} is served from {} but committed to {}; restart the \
                     coordinator with --route-by-assignments to route to the committed layout",
                    serving.as_deref().unwrap_or("no remote node"),
                    committed.as_deref().unwrap_or("no registered node")
                )));
            }
        }
        Ok(())
    }
}

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
/// refused or failed transition fails startup rather than serving an ambiguous layout. A caller
/// that already serves a layout should use [`ClusterEngine::recover_resize_intent`], which first
/// attests that layout against the committed one.
pub fn recover_durable_resize(
    control: &dyn ControlPlane,
) -> Result<Option<ResizeRecovery>, ShardError> {
    let state = control
        .cluster_state()
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?;
    resolve_resize_intent(control, &state)
}

fn resolve_resize_intent(
    control: &dyn ControlPlane,
    state: &ClusterState,
) -> Result<Option<ResizeRecovery>, ShardError> {
    let Some(intent) = state.moves.resize.as_ref() else {
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
