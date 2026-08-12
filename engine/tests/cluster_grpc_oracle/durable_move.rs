//! Crash-boundary proofs for durable reassignment startup recovery.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use reverse_rusty::cluster::{
    recover_durable_moves, ClientSecurity, ClusterConfig, ClusterEngine, ClusterState,
    ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane, MoveCommand,
    MoveInitialAuthority, MoveIntent, MoveIntentPhase, MoveMemberEvidence, MoveMemberIdentity,
    MoveProposalResult, MoveRecoveryEvidence, NodeDescriptor, NodeId, NodeRole, RemoteShard,
    ShardAssignment, StateVersion, MOVE_INTENT_VERSION,
};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::tagdict::TagDict;

use crate::harness::*;

struct Scenario {
    cluster: ClusterEngine,
    nodes: TwoNode,
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    tags: Arc<TagDict>,
}

/// A deterministic stand-in for losing control-write quorum after target evidence is durable. Reads
/// remain available, while every conditional commit retry fails until the test restores writes.
#[derive(Clone)]
struct CommitWriteGate {
    inner: Arc<InMemoryControlPlane>,
    commits_available: Arc<AtomicBool>,
}

impl ControlPlane for CommitWriteGate {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.inner.cluster_state()
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        self.inner.version()
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        self.inner.propose(change)
    }

    fn propose_move(&self, command: MoveCommand) -> Result<MoveProposalResult, ControlError> {
        if matches!(&command, MoveCommand::Commit { .. })
            && !self.commits_available.load(Ordering::SeqCst)
        {
            return Err(ControlError::Backend(
                "injected control-write quorum loss before move commit".into(),
            ));
        }
        self.inner.propose_move(command)
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        self.inner.leader()
    }
}

fn scenario(tag: &str, rt: &tokio::runtime::Runtime) -> Scenario {
    let queries = vec![
        (11, "+nike +shoe".to_string()),
        (12, "+sony +tv".to_string()),
    ];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tags = empty_tag_dict();
    let nodes = spin_two_servers(rt, &norm, tag);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tags),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source");
    cluster.ingest(&queries).expect("ingest");
    for (id, endpoint) in [(1, &nodes.src_ep), (2, &nodes.tgt_ep)] {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId(id),
                addr: Some(endpoint.clone()),
                role: NodeRole::Data,
            })
            .expect("register node");
    }
    cluster
        .reassign_shard(ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: Vec::new(),
        })
        .expect("seed source assignment");
    Scenario {
        cluster,
        nodes,
        norm,
        dict,
        tags,
    }
}

fn intent(state: &reverse_rusty::cluster::ClusterState, nodes: &TwoNode) -> MoveIntent {
    let expected = state
        .assignments
        .iter()
        .find(|assignment| assignment.position == 0)
        .expect("source assignment")
        .clone();
    MoveIntent {
        intent_version: MOVE_INTENT_VERSION,
        operation_id: 0xD0_0001,
        position: 0,
        expected_assignment_generation: state.moves.assignment_generation(0),
        placement_generation: state.placement_generation,
        expected,
        desired: ShardAssignment {
            position: 0,
            primary: NodeId(2),
            replicas: Vec::new(),
        },
        members: vec![
            MoveMemberIdentity {
                node: NodeId(1),
                endpoint: nodes.src_ep.to_ascii_lowercase(),
            },
            MoveMemberIdentity {
                node: NodeId(2),
                endpoint: nodes.tgt_ep.to_ascii_lowercase(),
            },
        ],
        live_generation: 1,
        source_fence_generation: 1,
        initial_authority: MoveInitialAuthority::Desired,
        phase: MoveIntentPhase::Preparing,
    }
}

fn target_evidence(scenario: &Scenario, rt: &tokio::runtime::Runtime) -> MoveRecoveryEvidence {
    let target = RemoteShard::connect(
        &scenario.nodes.tgt_ep,
        rt.handle().clone(),
        scenario.dict.fingerprint(),
        scenario.tags.fingerprint(),
        0,
    )
    .expect("connect target evidence");
    let (fingerprint_lo, fingerprint_hi, live_count) =
        target.content_fingerprint().expect("target fingerprint");
    MoveRecoveryEvidence {
        live_generation: 1,
        members: vec![MoveMemberEvidence {
            node: NodeId(2),
            fingerprint_lo,
            fingerprint_hi,
            live_count,
        }],
    }
}

fn recover(
    control: &InMemoryControlPlane,
    scenario: &Scenario,
    rt: &tokio::runtime::Runtime,
    coordinator_id: u64,
) -> Result<usize, reverse_rusty::cluster::ShardError> {
    recover_durable_moves(
        control,
        &scenario.dict,
        &scenario.tags,
        1,
        rt.handle(),
        coordinator_id,
        &ClientSecurity::default(),
    )
}

fn cleanup(scenario: &Scenario) {
    let _ = std::fs::remove_dir_all(&scenario.nodes.src_dir);
    let _ = std::fs::remove_dir_all(&scenario.nodes.tgt_dir);
}

#[test]
fn startup_commits_a_preparing_desired_authority_without_stale_recopy() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_preparing_desired", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("raw handoff");
    scenario
        .cluster
        .add_query(13, "+nike")
        .expect("late target-only write");

    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    control
        .propose_move(MoveCommand::Begin(move_intent))
        .expect("begin intent");
    let coordinator_id = RemoteShard::new_coordinator_id();
    assert_eq!(
        recover(&control, &scenario, &rt, coordinator_id).expect("startup recovery"),
        1
    );
    let recovered = control.cluster_state().expect("recovered state");
    assert!(recovered.moves.intents.is_empty());
    assert_eq!(recovered.assignments[0].primary, NodeId(2));

    let restarted = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&scenario.norm),
        Arc::clone(&scenario.dict),
        Arc::clone(&scenario.tags),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        std::slice::from_ref(&scenario.nodes.tgt_ep),
        rt.handle(),
        coordinator_id,
    )
    .expect("connect committed target");
    assert!(
        restarted
            .percolate("nike running shoe")
            .expect("target read")
            .contains(&13),
        "startup must preserve the post-handoff write that exists only on the desired authority"
    );
    cleanup(&scenario);
}

#[test]
fn startup_fences_an_unadopted_map_only_source_after_intent_replay() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_map_only_source", &rt);
    scenario
        .cluster
        .reassign_shard(ShardAssignment {
            position: 0,
            primary: NodeId(2),
            replicas: Vec::new(),
        })
        .expect("craft map-only target while node one remains live");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let state = control.cluster_state().expect("map-only state");
    let mut move_intent = intent(&state, &scenario.nodes);
    move_intent.desired.primary = NodeId(1);
    move_intent.live_generation = 0;
    move_intent.source_fence_generation = 1;
    control
        .propose_move(MoveCommand::Begin(move_intent))
        .expect("persist reconciliation before touching the empty mapped source");

    let coordinator_id = RemoteShard::new_coordinator_id();
    assert_eq!(
        recover(&control, &scenario, &rt, coordinator_id)
            .expect("adopt and fence mapped source, then restore live authority"),
        1
    );
    let recovered = control.cluster_state().expect("recovered state");
    assert_eq!(recovered.assignments[0].primary, NodeId(1));
    assert!(recovered.moves.intents.is_empty());
    let restarted = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&scenario.norm),
        Arc::clone(&scenario.dict),
        Arc::clone(&scenario.tags),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        std::slice::from_ref(&scenario.nodes.src_ep),
        rt.handle(),
        coordinator_id,
    )
    .expect("connect restored live source");
    restarted
        .upsert_query(17, "+nike", 1)
        .expect("restored live source remains writable");
    cleanup(&scenario);
}

#[test]
fn startup_ready_expected_authority_evidence_mismatch_fails_loud() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_ready_mismatch", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("raw handoff");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let mut move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    move_intent.initial_authority = MoveInitialAuthority::Expected;
    control
        .propose_move(MoveCommand::Begin(move_intent.clone()))
        .expect("begin intent");
    control
        .propose_move(MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence: target_evidence(&scenario, &rt),
        })
        .expect("mark ready");
    scenario
        .cluster
        .add_query(14, "+sony")
        .expect("change the non-authoritative target after recorded evidence");

    let error = recover(&control, &scenario, &rt, RemoteShard::new_coordinator_id())
        .expect_err("evidence drift must fail startup");
    assert!(error.to_string().contains("evidence changed"), "{error}");
    let state = control.cluster_state().expect("state after refusal");
    assert!(matches!(
        state.moves.intents[0].phase,
        MoveIntentPhase::Ready(_)
    ));
    assert_eq!(state.assignments[0].primary, NodeId(1));
    cleanup(&scenario);
}

#[test]
fn startup_ready_desired_authority_accepts_post_ready_writes() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_ready_desired_progress", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("make the desired target the live authority");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    control
        .propose_move(MoveCommand::Begin(move_intent.clone()))
        .expect("begin desired-authority intent");
    control
        .propose_move(MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence: target_evidence(&scenario, &rt),
        })
        .expect("mark desired authority ready");
    scenario
        .cluster
        .add_query(14, "+sony")
        .expect("acknowledged write after readiness");

    let coordinator_id = RemoteShard::new_coordinator_id();
    assert_eq!(
        recover(&control, &scenario, &rt, coordinator_id)
            .expect("commit advanced desired authority"),
        1
    );
    let recovered = control.cluster_state().expect("recovered state");
    assert_eq!(recovered.assignments[0].primary, NodeId(2));
    assert!(recovered.moves.intents.is_empty());

    let restarted = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&scenario.norm),
        Arc::clone(&scenario.dict),
        Arc::clone(&scenario.tags),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        std::slice::from_ref(&scenario.nodes.tgt_ep),
        rt.handle(),
        coordinator_id,
    )
    .expect("assemble the committed desired authority");
    assert!(
        restarted
            .percolate("sony television")
            .expect("read post-ready write")
            .contains(&14),
        "the desired authority's acknowledged post-ready write must survive recovery"
    );
    cleanup(&scenario);
}

#[test]
fn startup_commits_exact_ready_evidence_before_serving() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_ready_commit", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("prepare complete target under source fence");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    control
        .propose_move(MoveCommand::Begin(move_intent.clone()))
        .expect("begin intent");
    control
        .propose_move(MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence: target_evidence(&scenario, &rt),
        })
        .expect("mark ready before simulated crash");

    assert_eq!(
        recover(&control, &scenario, &rt, RemoteShard::new_coordinator_id(),)
            .expect("commit exact ready intent"),
        1
    );
    let state = control.cluster_state().expect("committed state");
    assert_eq!(state.assignments[0].primary, NodeId(2));
    assert!(state.moves.intents.is_empty());
    cleanup(&scenario);
}

#[test]
fn startup_recovers_ready_move_after_control_write_quorum_returns_without_request_retry() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_quorum_recovery", &rt);
    let control = Arc::new(InMemoryControlPlane::new(
        scenario.cluster.control_state().expect("state"),
    ));
    let commits_available = Arc::new(AtomicBool::new(false));
    let gate = CommitWriteGate {
        inner: Arc::clone(&control),
        commits_available: Arc::clone(&commits_available),
    };
    let cluster = scenario.cluster.with_control_plane(Box::new(gate.clone()));

    let error = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect_err("conditional commit must fail while control writes are unavailable");
    assert!(error.to_string().contains("quorum loss"), "{error}");
    let interrupted = gate.cluster_state().expect("interrupted state");
    assert_eq!(interrupted.assignments[0].primary, NodeId(1));
    assert!(matches!(
        interrupted.moves.intents[0].phase,
        MoveIntentPhase::Ready(_)
    ));

    // Restore the control path and invoke only cold-start recovery: no second reassignment request.
    commits_available.store(true, Ordering::SeqCst);
    let coordinator_id = RemoteShard::new_coordinator_id();
    assert_eq!(
        recover_durable_moves(
            &gate,
            &scenario.dict,
            &scenario.tags,
            1,
            rt.handle(),
            coordinator_id,
            &ClientSecurity::default(),
        )
        .expect("startup completes the ready transition"),
        1
    );
    let recovered = gate.cluster_state().expect("recovered state");
    assert_eq!(recovered.assignments[0].primary, NodeId(2));
    assert!(recovered.moves.intents.is_empty());

    let restarted = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&scenario.norm),
        Arc::clone(&scenario.dict),
        Arc::clone(&scenario.tags),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        std::slice::from_ref(&scenario.nodes.tgt_ep),
        rt.handle(),
        coordinator_id,
    )
    .expect("assemble from the recovered committed assignment");
    assert!(restarted
        .percolate("nike shoe")
        .expect("target query after recovery")
        .contains(&11));
    assert!(restarted
        .percolate("sony tv")
        .expect("target query after recovery")
        .contains(&12));

    let _ = std::fs::remove_dir_all(&scenario.nodes.src_dir);
    let _ = std::fs::remove_dir_all(&scenario.nodes.tgt_dir);
}

#[test]
fn startup_finishes_a_committed_intent_after_the_live_swap_boundary() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_committed_cleanup", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("raw handoff");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    control
        .propose_move(MoveCommand::Begin(move_intent.clone()))
        .expect("begin intent");
    control
        .propose_move(MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence: target_evidence(&scenario, &rt),
        })
        .expect("mark ready");
    control
        .propose_move(MoveCommand::Commit {
            operation_id: move_intent.operation_id,
        })
        .expect("commit before simulated crash");
    scenario
        .cluster
        .add_query(16, "+sony")
        .expect("acknowledged write after live swap and before cleanup");

    assert_eq!(
        recover(&control, &scenario, &rt, RemoteShard::new_coordinator_id(),)
            .expect("finish committed move"),
        1
    );
    let state = control.cluster_state().expect("clean committed state");
    assert_eq!(state.assignments[0].primary, NodeId(2));
    assert!(state.moves.intents.is_empty());
    cleanup(&scenario);
}

#[test]
fn startup_refuses_ready_intent_when_the_recorded_source_fence_is_gone() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_missing_fence", &rt);
    scenario
        .cluster
        .execute_handoff(
            0,
            &scenario.nodes.src_ep,
            &scenario.nodes.tgt_ep,
            rt.handle(),
        )
        .expect("raw handoff");
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    control
        .propose_move(MoveCommand::Begin(move_intent.clone()))
        .expect("begin intent");
    control
        .propose_move(MoveCommand::MarkReady {
            operation_id: move_intent.operation_id,
            evidence: target_evidence(&scenario, &rt),
        })
        .expect("mark ready");
    let source = RemoteShard::connect(
        &scenario.nodes.src_ep,
        rt.handle().clone(),
        scenario.dict.fingerprint(),
        scenario.tags.fingerprint(),
        0,
    )
    .expect("connect source");
    assert_eq!(source.unfence(1).expect("remove recorded fence"), 0);

    let error = recover(&control, &scenario, &rt, RemoteShard::new_coordinator_id())
        .expect_err("missing authority fence must fail startup");
    assert!(
        error.to_string().contains("expected source fence"),
        "{error}"
    );
    let state = control.cluster_state().expect("state after refusal");
    assert!(matches!(
        state.moves.intents[0].phase,
        MoveIntentPhase::Ready(_)
    ));
    assert_eq!(state.assignments[0].primary, NodeId(1));
    cleanup(&scenario);
}

#[test]
fn startup_preparing_expected_aborts_only_after_source_is_writable() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let scenario = scenario("durable_preparing_expected", &rt);
    let control = InMemoryControlPlane::new(scenario.cluster.control_state().expect("state"));
    let mut move_intent = intent(&control.cluster_state().expect("state"), &scenario.nodes);
    move_intent.initial_authority = MoveInitialAuthority::Expected;
    control
        .propose_move(MoveCommand::Begin(move_intent))
        .expect("begin intent");

    let coordinator_id = RemoteShard::new_coordinator_id();
    assert_eq!(
        recover(&control, &scenario, &rt, coordinator_id).expect("clean abort"),
        1
    );
    let state = control.cluster_state().expect("state after abort");
    assert!(state.moves.intents.is_empty());
    assert_eq!(state.assignments[0].primary, NodeId(1));
    let restarted = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&scenario.norm),
        Arc::clone(&scenario.dict),
        Arc::clone(&scenario.tags),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        std::slice::from_ref(&scenario.nodes.src_ep),
        rt.handle(),
        coordinator_id,
    )
    .expect("connect restored expected source");
    restarted
        .upsert_query(15, "+nike", 1)
        .expect("expected source remains writable");
    cleanup(&scenario);
}
