//! Coordinator-startup resolution of remote resize (ADR-180). Runs before the coordinator
//! assembles its routes, like durable-move recovery, because it decides which nodes may serve.

use crate::cluster::control::{
    normalized_move_endpoint, ClusterState, ControlPlane, MoveCommandOutcome, ResizeCommand,
    ResizeIntentPhase, ShardAssignment,
};
use crate::cluster::remote::{claim_retirement, unretire_node};
use crate::cluster::security::ClientSecurity;

use super::{ClusterEngine, ShardError};
use crate::cluster::coordinator::layout::Layout;

impl ClusterEngine {
    /// Fail unless this coordinator serves exactly the committed layout: its shard count,
    /// placement generation, and every position's primary endpoint. An assignment-routed
    /// coordinator checks this before serving, because its topology was read before it connected.
    pub fn attest_committed_layout(&self) -> Result<(), ShardError> {
        let stable = self.stable();
        self.attest_committed_layout_in(&stable.layout)
    }

    pub(in crate::cluster::coordinator) fn attest_committed_layout_in(
        &self,
        layout: &Layout,
    ) -> Result<(), ShardError> {
        let state = self.control_state()?;
        let generation = layout.generation.0;
        if state.num_shards as usize != layout.shards.len()
            || state.placement_generation != generation
        {
            return Err(ShardError::ControlPlane(format!(
                "this coordinator serves placement generation {generation} with {} shards, but \
                 the committed layout is generation {} with {} shards; restart it with \
                 --route-by-assignments to route to the committed layout",
                layout.shards.len(),
                state.placement_generation,
                state.num_shards
            )));
        }
        for (position, shard) in layout.shards.iter().enumerate() {
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

/// The node operations startup resolution needs.
trait RetirementNodes {
    /// Claim `endpoint` for this coordinator, failing while another coordinator's lease is live,
    /// and report which resize retired it: `None` for a node that has adopted nothing, `Some(0)`
    /// for one that is not retired.
    fn claim(&self, endpoint: &str) -> Result<Option<u64>, ShardError>;
    fn unretire(&self, endpoint: &str, operation_id: u64) -> Result<(), ShardError>;
}

struct MeshNodes<'a> {
    handle: &'a tokio::runtime::Handle,
    coordinator_id: u64,
    security: &'a ClientSecurity,
}

impl RetirementNodes for MeshNodes<'_> {
    fn claim(&self, endpoint: &str) -> Result<Option<u64>, ShardError> {
        claim_retirement(endpoint, self.handle, self.security, self.coordinator_id)
    }

    fn unretire(&self, endpoint: &str, operation_id: u64) -> Result<(), ShardError> {
        unretire_node(
            endpoint,
            self.handle,
            self.security,
            self.coordinator_id,
            operation_id,
        )
        .map(|_| ())
    }
}

/// Resolve remote resize before the coordinator assembles its routes (ADR-180).
///
/// 1. An uncommitted intent is aborted, after this coordinator has claimed every node of the
///    layout it would have retired. A coordinator still running that resize holds those claims,
///    so startup fails instead of aborting the resize underneath it.
/// 2. A committed intent is finished: consensus already names its layout, whose old nodes were
///    retired before the commit.
/// 3. Every node of the committed layout must serve. A committed resize retires only nodes
///    outside its layout, so a retired node inside it was retired by a resize that never
///    committed (aborted in step 1, or by an attempt whose unretire failed): its retirement is
///    lifted.
///
/// Returns `None` when no intent was recorded. Any refused transition or unreachable node fails
/// startup rather than serving an ambiguous layout.
pub fn recover_durable_resize(
    control: &dyn ControlPlane,
    handle: &tokio::runtime::Handle,
    coordinator_id: u64,
    security: &ClientSecurity,
) -> Result<Option<ResizeRecovery>, ShardError> {
    resolve(
        control,
        &MeshNodes {
            handle,
            coordinator_id,
            security,
        },
    )
}

fn resolve(
    control: &dyn ControlPlane,
    nodes: &dyn RetirementNodes,
) -> Result<Option<ResizeRecovery>, ShardError> {
    let state = read_state(control)?;
    let recovery = match state.moves.resize.clone() {
        None => None,
        Some(intent) if matches!(intent.phase, ResizeIntentPhase::Committed(_)) => {
            transition(
                control,
                ResizeCommand::Finish {
                    operation_id: intent.operation_id,
                },
            )?;
            Some(ResizeRecovery::Finished {
                operation_id: intent.operation_id,
            })
        }
        Some(intent) => {
            for endpoint in layout_endpoints(&state, &intent.expected.assignments, false)? {
                nodes.claim(&endpoint)?;
            }
            transition(
                control,
                ResizeCommand::Abort {
                    operation_id: intent.operation_id,
                },
            )?;
            Some(ResizeRecovery::Aborted {
                operation_id: intent.operation_id,
            })
        }
    };
    let state = read_state(control)?;
    for endpoint in layout_endpoints(&state, &state.assignments, true)? {
        let Some(operation_id) = nodes.claim(&endpoint)?.filter(|&op| op != 0) else {
            continue;
        };
        // Holding the claim, confirm the node still belongs to the committed layout and no resize
        // is in flight. A resize that committed after the read above legitimately retired the
        // nodes it left; lifting that would let a stale coordinator serve them again.
        let now = read_state(control)?;
        let wanted = normalized_move_endpoint(&endpoint);
        let still_committed = now.moves.resize.is_none()
            && layout_endpoints(&now, &now.assignments, true)?
                .iter()
                .any(|member| normalized_move_endpoint(member) == wanted);
        if !still_committed {
            return Err(ShardError::ControlPlane(format!(
                "the committed layout changed while startup resolved remote resize; {endpoint} \
                 stays retired, restart to resolve again"
            )));
        }
        nodes.unretire(&endpoint, operation_id)?;
    }
    Ok(recovery)
}

fn read_state(control: &dyn ControlPlane) -> Result<std::sync::Arc<ClusterState>, ShardError> {
    control
        .cluster_state()
        .map_err(|error| ShardError::ControlPlane(error.to_string()))
}

fn transition(control: &dyn ControlPlane, command: ResizeCommand) -> Result<(), ShardError> {
    let described = format!("{command:?}");
    match control
        .propose_resize(command)
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?
        .outcome
    {
        MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(()),
        outcome => Err(ShardError::ControlPlane(format!(
            "could not resolve remote resize at startup: {described} was refused ({outcome:?}); \
             inspect the control-plane state before serving"
        ))),
    }
}

/// Every distinct node endpoint (primaries and replicas) of `assignments`. With
/// `skip_unaddressed`, positions still on an address-less placeholder (an unseeded genesis
/// layout) are skipped rather than refused: no such node can have been retired.
fn layout_endpoints(
    state: &ClusterState,
    assignments: &[ShardAssignment],
    skip_unaddressed: bool,
) -> Result<Vec<String>, ShardError> {
    let mut seen = Vec::<String>::new();
    let mut endpoints = Vec::new();
    for assignment in assignments {
        for node_id in
            std::iter::once(assignment.primary).chain(assignment.replicas.iter().copied())
        {
            let Some(endpoint) = state
                .nodes
                .iter()
                .find(|node| node.id == node_id)
                .and_then(|node| node.addr.clone())
            else {
                if skip_unaddressed {
                    continue;
                }
                return Err(ShardError::ControlPlane(format!(
                    "position {} is assigned to node {} with no registered endpoint",
                    assignment.position, node_id.0
                )));
            };
            let normalized = normalized_move_endpoint(&endpoint);
            if !seen.contains(&normalized) {
                seen.push(normalized);
                endpoints.push(endpoint);
            }
        }
    }
    Ok(endpoints)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::{resolve, ResizeRecovery, RetirementNodes};
    use crate::cluster::control::{
        ClusterStateChange, ControlPlane, InMemoryControlPlane, MoveMemberIdentity, NodeDescriptor,
        NodeId, NodeRole, ResizeCommand, ResizeIntent, ResizeIntentPhase, ResizeLayout,
        ResizePositionEvidence, ShardAssignment, RESIZE_INTENT_VERSION,
    };
    use crate::cluster::shard::ShardError;

    const OLD: &str = "http://127.0.0.1:1";
    const NEW: &str = "http://127.0.0.1:2";

    /// Nodes keyed by endpoint: `Some(op)` is adopted (0 = not retired); `claimable` false models
    /// another coordinator's live lease.
    struct FakeNodes {
        retired: RefCell<HashMap<String, u64>>,
        claimable: bool,
        claims: RefCell<Vec<String>>,
        /// Runs once, during the first claim.
        on_claim: RefCell<Option<Box<dyn FnOnce()>>>,
    }

    impl FakeNodes {
        fn new(retired: &[(&str, u64)], claimable: bool) -> Self {
            Self {
                retired: RefCell::new(
                    retired
                        .iter()
                        .map(|(endpoint, op)| ((*endpoint).to_string(), *op))
                        .collect(),
                ),
                claimable,
                claims: RefCell::new(Vec::new()),
                on_claim: RefCell::new(None),
            }
        }

        fn retired_by(&self, endpoint: &str) -> u64 {
            self.retired.borrow().get(endpoint).copied().unwrap_or(0)
        }
    }

    impl RetirementNodes for FakeNodes {
        fn claim(&self, endpoint: &str) -> Result<Option<u64>, ShardError> {
            if !self.claimable {
                return Err(ShardError::Remote(
                    "another coordinator holds the lease".into(),
                ));
            }
            self.claims.borrow_mut().push(endpoint.to_string());
            if let Some(hook) = self.on_claim.borrow_mut().take() {
                hook();
            }
            Ok(Some(self.retired_by(endpoint)))
        }

        fn unretire(&self, endpoint: &str, operation_id: u64) -> Result<(), ShardError> {
            assert_eq!(
                self.retired_by(endpoint),
                operation_id,
                "unretire by its own op"
            );
            self.retired.borrow_mut().insert(endpoint.to_string(), 0);
            Ok(())
        }
    }

    /// Record a resize intent moving position 0 from `OLD` to `NEW`.
    fn begin_intent(cp: &InMemoryControlPlane, operation_id: u64) {
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
                endpoint: NEW.into(),
            }],
            phase: ResizeIntentPhase::Preparing,
        };
        cp.propose_resize(ResizeCommand::Begin(intent))
            .expect("begin");
    }

    fn plane_with_intent(operation_id: u64) -> InMemoryControlPlane {
        let cp = InMemoryControlPlane::single_node(1, 64, 1);
        for (id, endpoint) in [(1u64, OLD), (2, NEW)] {
            cp.propose(ClusterStateChange::AddNode(NodeDescriptor {
                id: NodeId(id),
                addr: Some(endpoint.to_string()),
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
        begin_intent(&cp, operation_id);
        cp
    }

    fn commit(cp: &InMemoryControlPlane, operation_id: u64) {
        cp.propose_resize(ResizeCommand::MarkReady {
            operation_id,
            evidence: vec![ResizePositionEvidence {
                position: 0,
                fingerprint_lo: 1,
                fingerprint_hi: 2,
                live_count: 3,
            }],
        })
        .expect("ready");
        cp.propose_resize(ResizeCommand::Commit { operation_id })
            .expect("commit");
    }

    #[test]
    fn an_uncommitted_intent_is_aborted_and_its_retired_nodes_serve_again() {
        let cp = plane_with_intent(3);
        let nodes = FakeNodes::new(&[(OLD, 3)], true);
        assert_eq!(
            resolve(&cp, &nodes).expect("abort"),
            Some(ResizeRecovery::Aborted { operation_id: 3 })
        );
        let state = cp.cluster_state().expect("state");
        assert!(state.moves.resize.is_none());
        assert_eq!(state.assignments[0].primary, NodeId(1), "old layout kept");
        assert_eq!(nodes.retired_by(OLD), 0, "the old node serves again");
    }

    #[test]
    fn a_live_coordinator_keeps_its_uncommitted_resize() {
        let cp = plane_with_intent(4);
        let nodes = FakeNodes::new(&[(OLD, 4)], false);
        assert!(resolve(&cp, &nodes).is_err());
        assert!(
            cp.cluster_state().expect("state").moves.resize.is_some(),
            "nothing is aborted without the old layout's claims"
        );
        assert_eq!(nodes.retired_by(OLD), 4);
    }

    #[test]
    fn a_committed_intent_is_finished_and_its_old_nodes_stay_retired() {
        let cp = plane_with_intent(5);
        commit(&cp, 5);
        let nodes = FakeNodes::new(&[(OLD, 5), (NEW, 0)], true);
        assert_eq!(
            resolve(&cp, &nodes).expect("finish"),
            Some(ResizeRecovery::Finished { operation_id: 5 })
        );
        let state = cp.cluster_state().expect("state");
        assert!(state.moves.resize.is_none());
        assert_eq!(state.assignments[0].primary, NodeId(2), "new layout kept");
        assert_eq!(
            nodes.retired_by(OLD),
            5,
            "the retired layout never serves again"
        );
        assert_eq!(*nodes.claims.borrow(), vec![NEW.to_string()]);
    }

    #[test]
    fn an_unseeded_genesis_layout_needs_no_node() {
        let genesis = InMemoryControlPlane::single_node(3, 64, 1);
        let nodes = FakeNodes::new(&[], true);
        assert_eq!(resolve(&genesis, &nodes).expect("bootstrap proceeds"), None);
        assert!(nodes.claims.borrow().is_empty());
    }

    #[test]
    fn a_retirement_committed_during_resolution_is_kept() {
        // At the first read the retired node is in the committed layout, but a resize commits
        // before the claim completes: that resize legitimately retired it.
        let cp = std::sync::Arc::new(plane_with_intent(7));
        cp.propose_resize(ResizeCommand::Abort { operation_id: 7 })
            .expect("abort");
        let nodes = FakeNodes::new(&[(OLD, 8)], true);
        let racing = std::sync::Arc::clone(&cp);
        *nodes.on_claim.borrow_mut() = Some(Box::new(move || {
            begin_intent(&racing, 8);
            commit(&racing, 8);
        }));
        assert!(resolve(&*cp, &nodes).is_err());
        assert_eq!(
            nodes.retired_by(OLD),
            8,
            "the committed resize's retirement stands"
        );
    }

    #[test]
    fn a_leftover_retirement_inside_the_committed_layout_is_lifted() {
        // No intent is recorded, yet a committed-layout node is still retired: an attempt aborted
        // its intent but could not unretire it.
        let cp = plane_with_intent(6);
        cp.propose_resize(ResizeCommand::Abort { operation_id: 6 })
            .expect("abort");
        let nodes = FakeNodes::new(&[(OLD, 6)], true);
        assert_eq!(resolve(&cp, &nodes).expect("resolve"), None);
        assert_eq!(nodes.retired_by(OLD), 0);
    }
}
