use super::super::{
    ClusterState, ClusterStateChange, ControlPlane, InMemoryControlPlane, MoveCommand,
    MoveCommandOutcome, MoveIntent, MoveIntentPhase, MoveMemberIdentity, NodeDescriptor, NodeId,
    NodeRole, ShardAssignment, MOVE_CONTROL_FORMAT_CURRENT, MOVE_INTENT_VERSION,
};
use super::{
    ResizeCommand, ResizeIntent, ResizeIntentPhase, ResizeLayout, ResizePositionEvidence,
    RESIZE_CONTROL_FORMAT, RESIZE_INTENT_VERSION,
};

fn endpoint(id: u64) -> String {
    format!("http://127.0.0.1:{}", 51000 + id)
}

fn data_node(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(endpoint(id)),
        role: NodeRole::Data,
    }
}

fn assignment(position: u32, node: u64) -> ShardAssignment {
    ShardAssignment {
        position,
        primary: NodeId(node),
        replicas: Vec::new(),
    }
}

/// Three positions on nodes 1..=3, plus target nodes 11..=15 registered.
fn plane() -> InMemoryControlPlane {
    let cp = InMemoryControlPlane::single_node(3, 64, 0xF00D);
    for id in [1, 2, 3, 11, 12, 13, 14, 15] {
        cp.propose(ClusterStateChange::AddNode(data_node(id)))
            .expect("add node");
    }
    for position in 0..3 {
        cp.propose(ClusterStateChange::AssignShard(assignment(
            position,
            u64::from(position) + 1,
        )))
        .expect("assign");
    }
    cp
}

fn intent(cp: &InMemoryControlPlane, operation_id: u64, targets: &[u64]) -> ResizeIntent {
    let state = cp.cluster_state().expect("state");
    let desired_assignments: Vec<ShardAssignment> = targets
        .iter()
        .enumerate()
        .map(|(position, &node)| assignment(position as u32, node))
        .collect();
    let mut members: Vec<MoveMemberIdentity> = targets
        .iter()
        .map(|&node| MoveMemberIdentity {
            node: NodeId(node),
            endpoint: endpoint(node),
        })
        .collect();
    members.sort_by_key(|m| m.node);
    members.dedup_by_key(|m| m.node);
    ResizeIntent {
        intent_version: RESIZE_INTENT_VERSION,
        operation_id,
        expected: ResizeLayout {
            num_shards: state.num_shards,
            placement_generation: state.placement_generation,
            assignments: state.assignments.clone(),
        },
        desired: ResizeLayout {
            num_shards: targets.len() as u32,
            placement_generation: state.placement_generation + 1,
            assignments: desired_assignments,
        },
        members,
        phase: ResizeIntentPhase::Preparing,
    }
}

fn evidence(num_shards: u32) -> Vec<ResizePositionEvidence> {
    (0..num_shards)
        .map(|position| ResizePositionEvidence {
            position,
            fingerprint_lo: u64::from(position) * 7 + 1,
            fingerprint_hi: 9,
            live_count: 10 + u64::from(position),
        })
        .collect()
}

fn resize(cp: &InMemoryControlPlane, command: ResizeCommand) -> MoveCommandOutcome {
    cp.propose_resize(command).expect("propose").outcome
}

#[test]
fn a_resize_commits_the_complete_layout_atomically_and_idempotently() {
    let cp = plane();
    let before = cp.cluster_state().expect("state");
    let begin = intent(&cp, 42, &[11, 12, 13, 14, 15]);
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(begin.clone())),
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(begin.clone())),
        MoveCommandOutcome::AlreadyApplied
    );
    let preparing = cp.cluster_state().expect("state");
    assert_eq!(preparing.num_shards, 3, "Begin changes no routing");
    assert_eq!(preparing.moves.format_version, RESIZE_CONTROL_FORMAT);

    assert_eq!(
        resize(&cp, ResizeCommand::Commit { operation_id: 42 }),
        MoveCommandOutcome::Conflict,
        "a preparing intent cannot commit"
    );
    let ready = ResizeCommand::MarkReady {
        operation_id: 42,
        evidence: evidence(5),
    };
    assert_eq!(resize(&cp, ready.clone()), MoveCommandOutcome::Applied);
    assert_eq!(resize(&cp, ready), MoveCommandOutcome::AlreadyApplied);
    assert_eq!(
        resize(
            &cp,
            ResizeCommand::MarkReady {
                operation_id: 42,
                evidence: evidence(4),
            }
        ),
        MoveCommandOutcome::Invalid,
        "evidence must cover every desired position"
    );

    assert_eq!(
        resize(&cp, ResizeCommand::Commit { operation_id: 42 }),
        MoveCommandOutcome::Applied
    );
    let committed = cp.cluster_state().expect("state");
    assert_eq!(committed.num_shards, 5);
    assert_eq!(
        committed.placement_generation,
        before.placement_generation + 1
    );
    assert_eq!(committed.assignments, begin.desired.assignments);
    let registered: Vec<u64> = committed.nodes.iter().map(|node| node.id.0).collect();
    for retired in [1, 2, 3] {
        assert!(
            !registered.contains(&retired),
            "retired node {retired} leaves membership with the commit"
        );
    }
    for target in [11, 12, 13, 14, 15] {
        assert!(registered.contains(&target));
    }
    for position in 0..5 {
        assert!(committed.moves.assignment_generation(position) > 0);
    }
    assert_eq!(
        resize(&cp, ResizeCommand::Commit { operation_id: 42 }),
        MoveCommandOutcome::AlreadyApplied
    );
    assert_eq!(
        resize(&cp, ResizeCommand::Abort { operation_id: 42 }),
        MoveCommandOutcome::Conflict,
        "a committed resize can only finish"
    );
    assert_eq!(
        resize(&cp, ResizeCommand::Finish { operation_id: 42 }),
        MoveCommandOutcome::Applied
    );
    assert!(cp.cluster_state().expect("state").moves.resize.is_none());
    assert_eq!(
        resize(&cp, ResizeCommand::Finish { operation_id: 42 }),
        MoveCommandOutcome::AlreadyApplied
    );
}

#[test]
fn malformed_or_colocated_intents_are_invalid() {
    let cp = plane();
    let mut cases: Vec<(&str, ResizeIntent)> = Vec::new();

    let mut zero = intent(&cp, 42, &[11, 12]);
    zero.operation_id = 0;
    cases.push(("zero operation id", zero));

    let mut skipped = intent(&cp, 42, &[11, 12]);
    skipped.desired.placement_generation += 1;
    cases.push(("generation must advance by exactly one", skipped));

    let mut gap = intent(&cp, 42, &[11, 12]);
    gap.desired.assignments[1].position = 2;
    cases.push(("positions must be gap-free", gap));

    let mut count = intent(&cp, 42, &[11, 12]);
    count.desired.num_shards = 3;
    cases.push(("count must match assignments", count));

    cases.push((
        "target shares a node with the current layout",
        intent(&cp, 42, &[11, 2]),
    ));

    let mut unregistered = intent(&cp, 42, &[11, 12]);
    unregistered.members[0].endpoint = "http://127.0.0.1:1".into();
    cases.push(("member endpoint must match registration", unregistered));

    let mut unnormalized = intent(&cp, 42, &[11, 12]);
    unnormalized.members[0].endpoint.push('/');
    cases.push(("member endpoint must be normalized", unnormalized));

    let mut missing_member = intent(&cp, 42, &[11, 12]);
    missing_member.members.pop();
    cases.push(("every desired node must be a member", missing_member));

    let mut started = intent(&cp, 42, &[11, 12]);
    started.phase = ResizeIntentPhase::Ready(evidence(2));
    cases.push(("Begin must be preparing", started));

    for (name, bad) in cases {
        assert_eq!(
            resize(&cp, ResizeCommand::Begin(bad)),
            MoveCommandOutcome::Invalid,
            "{name}"
        );
    }
    assert!(cp.cluster_state().expect("state").moves.resize.is_none());
}

#[test]
fn a_topology_change_during_the_resize_blocks_its_commit() {
    let cp = plane();
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(intent(&cp, 7, &[11, 12, 13, 14]))),
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        resize(
            &cp,
            ResizeCommand::MarkReady {
                operation_id: 7,
                evidence: evidence(4),
            }
        ),
        MoveCommandOutcome::Applied
    );
    // An ordinary assignment write lands between the evidence and the commit.
    cp.propose(ClusterStateChange::AssignShard(assignment(1, 3)))
        .expect("assign");
    assert_eq!(
        resize(&cp, ResizeCommand::Commit { operation_id: 7 }),
        MoveCommandOutcome::Conflict
    );
    assert_eq!(cp.cluster_state().expect("state").num_shards, 3);
    assert_eq!(
        resize(&cp, ResizeCommand::Abort { operation_id: 7 }),
        MoveCommandOutcome::Applied,
        "an uncommitted resize can be abandoned"
    );
}

#[test]
fn only_one_resize_and_no_concurrent_moves() {
    let cp = plane();
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(intent(&cp, 1, &[11, 12]))),
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(intent(&cp, 2, &[13, 14]))),
        MoveCommandOutcome::Conflict
    );

    let state = cp.cluster_state().expect("state");
    let expected = state.assignments[0].clone();
    let desired = assignment(0, 15);
    let mut members = vec![
        MoveMemberIdentity {
            node: NodeId(1),
            endpoint: endpoint(1),
        },
        MoveMemberIdentity {
            node: NodeId(15),
            endpoint: endpoint(15),
        },
    ];
    members.sort_by_key(|m| m.node);
    let move_intent = MoveIntent {
        intent_version: MOVE_INTENT_VERSION,
        operation_id: 99,
        position: 0,
        expected_assignment_generation: state.moves.assignment_generation(0),
        placement_generation: state.placement_generation,
        expected,
        desired,
        members,
        live_generation: 5,
        source_fence_generation: 5,
        initial_authority: super::super::MoveInitialAuthority::Expected,
        phase: MoveIntentPhase::Preparing,
    };
    let outcome = cp
        .propose_move(MoveCommand::Begin(move_intent))
        .expect("propose")
        .outcome;
    assert_eq!(
        outcome,
        MoveCommandOutcome::Conflict,
        "no move under a resize"
    );
    assert_eq!(
        cp.cluster_state().expect("state").moves.format_version,
        RESIZE_CONTROL_FORMAT,
        "a move command must not downgrade the resize format fence"
    );
}

#[test]
fn resize_state_is_fenced_against_older_formats() {
    let cp = plane();
    assert_eq!(
        resize(&cp, ResizeCommand::Begin(intent(&cp, 5, &[11, 12]))),
        MoveCommandOutcome::Applied
    );
    let state = cp.cluster_state().expect("state");
    let encoded = serde_json::to_value(state.as_ref()).expect("encode");
    assert_eq!(
        encoded["epoch"]["control_format_version"],
        RESIZE_CONTROL_FORMAT
    );
    let round_trip: ClusterState = serde_json::from_value(encoded.clone()).expect("decode");
    assert_eq!(&round_trip, state.as_ref());

    // A state claiming the move-only format cannot smuggle a resize intent.
    let mut downgraded = encoded;
    downgraded["epoch"]["control_format_version"] = serde_json::json!(MOVE_CONTROL_FORMAT_CURRENT);
    downgraded["moves"]["format_version"] = serde_json::json!(MOVE_CONTROL_FORMAT_CURRENT);
    assert!(serde_json::from_value::<ClusterState>(downgraded).is_err());

    // Finishing leaves the fence in place.
    resize(&cp, ResizeCommand::Abort { operation_id: 5 });
    assert_eq!(
        cp.cluster_state().expect("state").moves.format_version,
        RESIZE_CONTROL_FORMAT
    );
}
