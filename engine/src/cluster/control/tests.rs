use super::*;

fn node(id: u64, role: NodeRole) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role,
    }
}

#[test]
fn single_node_is_one_manager_owning_every_position() {
    let cp = InMemoryControlPlane::single_node(4, 128, 0xABCD);
    let st = cp.cluster_state().unwrap();
    assert_eq!(st.epoch, 0);
    assert_eq!(st.num_shards, 4);
    assert_eq!(st.nodes.len(), 1);
    assert_eq!(st.voters, vec![NodeId(0)]);
    assert_eq!(st.assignments.len(), 4);
    assert!(st
        .assignments
        .iter()
        .all(|a| a.primary == NodeId(0) && a.replicas.is_empty()));
    assert_eq!(cp.leader().unwrap(), Some(NodeId(0)));
}

#[test]
fn add_node_is_idempotent_and_sorted_and_bumps_version() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    let v0 = cp.version().unwrap();
    cp.propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
        .unwrap();
    let v1 = cp
        .propose(ClusterStateChange::AddNode(node(1, NodeRole::Data)))
        .unwrap();
    assert!(v1 > v0, "each commit advances the version");
    // Re-adding the same id replaces, never duplicates.
    cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
        .unwrap();
    let st = cp.cluster_state().unwrap();
    let ids: Vec<u64> = st.nodes.iter().map(|n| n.id.0).collect();
    assert_eq!(ids, vec![0, 1, 2], "no dups, sorted by id");
    assert_eq!(
        st.nodes.iter().find(|n| n.id == NodeId(1)).unwrap().role,
        NodeRole::Manager,
        "the last add wins"
    );
}

#[test]
fn remove_node_is_idempotent() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    cp.propose(ClusterStateChange::AddNode(node(5, NodeRole::Data)))
        .unwrap();
    cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
        .unwrap();
    cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
        .unwrap(); // no-op, no panic
    assert!(cp
        .cluster_state()
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.id != NodeId(5)));
}

#[test]
fn assign_shard_replaces_position_kept_sorted() {
    let cp = InMemoryControlPlane::single_node(3, 64, 0);
    cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
        position: 2,
        primary: NodeId(7),
        replicas: vec![NodeId(8)],
    }))
    .unwrap();
    // Replace the same position rather than appending a second entry for it.
    cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
        position: 2,
        primary: NodeId(9),
        replicas: vec![],
    }))
    .unwrap();
    let st = cp.cluster_state().unwrap();
    let positions: Vec<u32> = st.assignments.iter().map(|a| a.position).collect();
    assert_eq!(positions, vec![0, 1, 2], "one entry per position, sorted");
    let p2 = st.assignments.iter().find(|a| a.position == 2).unwrap();
    assert_eq!(p2.primary, NodeId(9));
    assert!(p2.replicas.is_empty(), "the last assignment wins");
}

#[test]
fn bump_model_version_advances_fingerprint_and_counter() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0x1111);
    cp.propose(ClusterStateChange::BumpModelVersion {
        dict_fingerprint: 0x2222,
    })
    .unwrap();
    let st = cp.cluster_state().unwrap();
    assert_eq!(st.dict_fingerprint, 0x2222);
    assert_eq!(st.model_version, 1);
}

#[test]
fn change_membership_sorts_dedups_and_is_distinct_from_propose() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    cp.change_membership(vec![NodeId(3), NodeId(1), NodeId(3), NodeId(2)])
        .unwrap();
    assert_eq!(
        cp.cluster_state().unwrap().voters,
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
    // Leader is the first voter.
    assert_eq!(cp.leader().unwrap(), Some(NodeId(1)));
}

#[test]
fn proposals_are_deterministic_regardless_of_order() {
    // Two backends fed the same change SET in different orders converge to the same
    // canonical document — the property the two-backend differential relies on.
    let mk = || InMemoryControlPlane::single_node(2, 64, 0);
    let (a, b) = (mk(), mk());
    let changes = [
        ClusterStateChange::AddNode(node(3, NodeRole::Data)),
        ClusterStateChange::AddNode(node(1, NodeRole::Data)),
        ClusterStateChange::AssignShard(ShardAssignment {
            position: 1,
            primary: NodeId(3),
            replicas: vec![NodeId(1)],
        }),
    ];
    for c in &changes {
        a.propose(c.clone()).unwrap();
    }
    for c in changes.iter().rev() {
        b.propose(c.clone()).unwrap();
    }
    // Same membership + assignments (epoch differs only if order changed counts — here
    // both applied 3 changes, so epochs match too).
    let (sa, sb) = (a.cluster_state().unwrap(), b.cluster_state().unwrap());
    assert_eq!(sa.nodes, sb.nodes);
    assert_eq!(sa.assignments, sb.assignments);
    assert_eq!(sa.epoch, sb.epoch);
}

#[test]
fn broken_backend_fails_closed() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    let before = cp.cluster_state().unwrap();
    cp.break_proposals_for_test();
    assert!(matches!(
        cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Data))),
        Err(ControlError::Backend(_))
    ));
    // State is unchanged — fail-closed (no partial mutation).
    assert_eq!(*cp.cluster_state().unwrap(), *before);
}

#[test]
fn control_error_folds_into_shard_error() {
    let e: ShardError = ControlError::NoQuorum.into();
    assert!(matches!(e, ShardError::ControlPlane(_)));
}

fn move_control_with_target(target: u64, operation_id: u64) -> (InMemoryControlPlane, MoveIntent) {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    for id in [1, 2, 3] {
        cp.propose(ClusterStateChange::AddNode(node(id, NodeRole::Data)))
            .unwrap();
    }
    let expected = ShardAssignment {
        position: 0,
        primary: NodeId(1),
        replicas: Vec::new(),
    };
    cp.propose(ClusterStateChange::AssignShard(expected.clone()))
        .unwrap();
    let state = cp.cluster_state().unwrap();
    let desired = ShardAssignment {
        position: 0,
        primary: NodeId(target),
        replicas: Vec::new(),
    };
    let members = [1, target]
        .into_iter()
        .map(|id| MoveMemberIdentity {
            node: NodeId(id),
            endpoint: format!("http://127.0.0.1:{}", 50050 + id),
        })
        .collect();
    let intent = MoveIntent {
        intent_version: MOVE_INTENT_VERSION,
        operation_id,
        position: 0,
        expected_assignment_generation: state.moves.assignment_generation(0),
        placement_generation: state.placement_generation,
        expected,
        desired,
        members,
        live_generation: 17,
        initial_authority: MoveInitialAuthority::Expected,
        phase: MoveIntentPhase::Preparing,
    };
    drop(state);
    (cp, intent)
}

fn recovery_evidence(target: u64) -> MoveRecoveryEvidence {
    MoveRecoveryEvidence {
        live_generation: 17,
        members: vec![MoveMemberEvidence {
            node: NodeId(target),
            fingerprint_lo: 11,
            fingerprint_hi: 22,
            live_count: 3,
        }],
    }
}

#[test]
fn durable_move_is_idempotent_and_commits_assignment_conditionally() {
    let (cp, intent) = move_control_with_target(2, 41);
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent.clone()))
            .unwrap()
            .outcome,
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent.clone()))
            .unwrap()
            .outcome,
        MoveCommandOutcome::AlreadyApplied
    );

    let evidence = recovery_evidence(2);
    assert_eq!(
        cp.propose_move(MoveCommand::MarkReady {
            operation_id: 41,
            evidence: evidence.clone(),
        })
        .unwrap()
        .outcome,
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent.clone()))
            .unwrap()
            .outcome,
        MoveCommandOutcome::AlreadyApplied,
        "Begin retries must remain idempotent after the phase advances"
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Commit { operation_id: 41 })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Commit { operation_id: 41 })
            .unwrap()
            .outcome,
        MoveCommandOutcome::AlreadyApplied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent.clone()))
            .unwrap()
            .outcome,
        MoveCommandOutcome::AlreadyApplied,
        "a lost Commit response must not make the original Begin conflict"
    );
    let state = cp.cluster_state().unwrap();
    assert_eq!(state.assignments, vec![intent.desired]);
    assert_eq!(state.moves.assignment_generation(0), 2);
    assert!(matches!(
        state.moves.intents[0].phase,
        MoveIntentPhase::Committed(ref stored) if stored == &evidence
    ));
    drop(state);

    assert_eq!(
        cp.propose_move(MoveCommand::Finish { operation_id: 41 })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Applied
    );
    assert!(cp.cluster_state().unwrap().moves.intents.is_empty());
}

#[test]
fn assignment_generation_invalidates_a_stale_move() {
    let (cp, intent) = move_control_with_target(2, 42);
    cp.propose(ClusterStateChange::AssignShard(intent.expected.clone()))
        .unwrap();
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent)).unwrap().outcome,
        MoveCommandOutcome::Conflict
    );
    assert!(cp.cluster_state().unwrap().moves.intents.is_empty());
}

#[test]
fn racing_move_begins_have_exactly_one_winner() {
    use std::sync::Barrier;

    let (cp, first) = move_control_with_target(2, 51);
    let cp = Arc::new(cp);
    let mut second = first.clone();
    second.operation_id = 52;
    second.desired.primary = NodeId(3);
    second.members[1] = MoveMemberIdentity {
        node: NodeId(3),
        endpoint: "http://127.0.0.1:50053".into(),
    };
    let barrier = Arc::new(Barrier::new(3));
    let run = |intent: MoveIntent| {
        let cp = Arc::clone(&cp);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            cp.propose_move(MoveCommand::Begin(intent)).unwrap().outcome
        })
    };
    let a = run(first);
    let b = run(second);
    barrier.wait();
    let mut outcomes = [a.join().unwrap(), b.join().unwrap()];
    outcomes.sort_unstable_by_key(|outcome| match outcome {
        MoveCommandOutcome::Applied => 0,
        MoveCommandOutcome::Conflict => 1,
        MoveCommandOutcome::AlreadyApplied => 2,
        MoveCommandOutcome::Invalid => 3,
    });
    assert_eq!(
        outcomes,
        [MoveCommandOutcome::Applied, MoveCommandOutcome::Conflict]
    );
    assert_eq!(cp.cluster_state().unwrap().moves.intents.len(), 1);
}

#[test]
fn move_state_uses_a_fail_loud_epoch_encoding_after_upgrade() {
    #[derive(Deserialize)]
    struct LegacyReader {
        #[allow(dead_code)]
        epoch: u64,
    }

    let (cp, _) = move_control_with_target(2, 61);
    let legacy = serde_json::to_vec(cp.cluster_state().unwrap().as_ref()).unwrap();
    assert!(serde_json::from_slice::<LegacyReader>(&legacy).is_ok());

    let result = cp
        .propose_move(MoveCommand::Abort { operation_id: 999 })
        .unwrap();
    assert_eq!(result.outcome, MoveCommandOutcome::AlreadyApplied);
    let current = serde_json::to_vec(cp.cluster_state().unwrap().as_ref()).unwrap();
    assert!(
        serde_json::from_slice::<LegacyReader>(&current).is_err(),
        "an old binary must reject a state that has observed the move protocol"
    );
    let round_trip: ClusterState = serde_json::from_slice(&current).unwrap();
    assert_eq!(round_trip.moves.format_version, MOVE_CONTROL_FORMAT_CURRENT);
}

#[test]
fn placement_bound_move_schema_rejects_the_predecessor_format() {
    let (cp, intent) = move_control_with_target(2, 610);
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent.clone()))
            .unwrap()
            .outcome,
        MoveCommandOutcome::Applied
    );

    let current_command = serde_json::to_value(MoveCommand::Begin(intent.clone())).unwrap();
    assert!(current_command.get("BeginV2").is_some());
    assert!(
        serde_json::from_value::<MoveCommand>(serde_json::json!({ "Begin": intent })).is_err(),
        "the predecessor wire variant must not enter a mixed-version state machine"
    );

    let mut predecessor = serde_json::to_value(cp.cluster_state().unwrap().as_ref()).unwrap();
    let mut malformed_current = predecessor.clone();
    malformed_current["moves"]["intents"][0]
        .as_object_mut()
        .unwrap()
        .remove("placement_generation");
    assert!(
        serde_json::from_value::<ClusterState>(malformed_current)
            .unwrap_err()
            .to_string()
            .contains("placement_generation"),
        "current move state must not default a missing placement predicate"
    );

    predecessor["epoch"]["control_format_version"] = serde_json::json!(2);
    predecessor["moves"]["format_version"] = serde_json::json!(2);
    let error = serde_json::from_value::<ClusterState>(predecessor).unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported move control format 2"));
}

#[test]
fn generic_propose_refuses_to_hide_a_move_outcome() {
    let (cp, _) = move_control_with_target(2, 62);
    let before = cp.cluster_state().unwrap();
    assert!(matches!(
        cp.propose(ClusterStateChange::Move(MoveCommand::Abort {
            operation_id: 999,
        })),
        Err(ControlError::Backend(_))
    ));
    assert_eq!(*cp.cluster_state().unwrap(), *before);
}

#[test]
fn desired_authority_and_ready_intents_cannot_be_aborted() {
    let (cp, mut desired) = move_control_with_target(2, 63);
    desired.initial_authority = MoveInitialAuthority::Desired;
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(desired))
            .unwrap()
            .outcome,
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Abort { operation_id: 63 })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Conflict
    );
    assert_eq!(cp.cluster_state().unwrap().moves.intents.len(), 1);

    let (cp, expected) = move_control_with_target(2, 64);
    cp.propose_move(MoveCommand::Begin(expected)).unwrap();
    cp.propose_move(MoveCommand::MarkReady {
        operation_id: 64,
        evidence: recovery_evidence(2),
    })
    .unwrap();
    assert_eq!(
        cp.propose_move(MoveCommand::Abort { operation_id: 64 })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Conflict
    );
}

#[test]
fn abort_and_finish_preserve_intent_after_identity_drift() {
    let (cp, intent) = move_control_with_target(2, 631);
    let operation_id = intent.operation_id;
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(intent)).unwrap().outcome,
        MoveCommandOutcome::Applied
    );
    cp.propose(ClusterStateChange::AddNode(NodeDescriptor {
        id: NodeId(1),
        addr: Some("http://127.0.0.1:59999".into()),
        role: NodeRole::Data,
    }))
    .unwrap();
    assert_eq!(
        cp.propose_move(MoveCommand::Abort { operation_id })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Conflict
    );
    assert_eq!(cp.cluster_state().unwrap().moves.intents.len(), 1);

    let (cp, intent) = move_control_with_target(2, 632);
    let operation_id = intent.operation_id;
    let evidence = recovery_evidence(2);
    cp.propose_move(MoveCommand::Begin(intent)).unwrap();
    cp.propose_move(MoveCommand::MarkReady {
        operation_id,
        evidence,
    })
    .unwrap();
    cp.propose_move(MoveCommand::Commit { operation_id })
        .unwrap();
    cp.propose(ClusterStateChange::AddNode(NodeDescriptor {
        id: NodeId(2),
        addr: Some("http://127.0.0.1:59998".into()),
        role: NodeRole::Data,
    }))
    .unwrap();
    assert_eq!(
        cp.propose_move(MoveCommand::Finish { operation_id })
            .unwrap()
            .outcome,
        MoveCommandOutcome::Conflict
    );
    assert_eq!(cp.cluster_state().unwrap().moves.intents.len(), 1);
}

#[test]
fn active_intents_reserve_their_physical_endpoint_footprints() {
    let cp = InMemoryControlPlane::single_node(2, 64, 0);
    for id in [1, 2, 3] {
        cp.propose(ClusterStateChange::AddNode(node(id, NodeRole::Data)))
            .unwrap();
    }
    for position in 0..2 {
        cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
            position,
            primary: NodeId(1),
            replicas: Vec::new(),
        }))
        .unwrap();
    }
    let state = cp.cluster_state().unwrap();
    let make_intent = |position, target, operation_id| MoveIntent {
        intent_version: MOVE_INTENT_VERSION,
        operation_id,
        position,
        expected_assignment_generation: state.moves.assignment_generation(position),
        placement_generation: state.placement_generation,
        expected: ShardAssignment {
            position,
            primary: NodeId(1),
            replicas: Vec::new(),
        },
        desired: ShardAssignment {
            position,
            primary: NodeId(target),
            replicas: Vec::new(),
        },
        members: [1, target]
            .into_iter()
            .map(|id| MoveMemberIdentity {
                node: NodeId(id),
                endpoint: format!("http://127.0.0.1:{}", 50050 + id),
            })
            .collect(),
        live_generation: 21,
        initial_authority: MoveInitialAuthority::Expected,
        phase: MoveIntentPhase::Preparing,
    };
    let first = make_intent(0, 2, 71);
    let overlapping = make_intent(1, 3, 72);
    drop(state);
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(first)).unwrap().outcome,
        MoveCommandOutcome::Applied
    );
    assert_eq!(
        cp.propose_move(MoveCommand::Begin(overlapping))
            .unwrap()
            .outcome,
        MoveCommandOutcome::Conflict
    );
}

#[test]
fn placement_generation_change_invalidates_recovery_evidence() {
    let (cp, intent) = move_control_with_target(2, 73);
    cp.propose_move(MoveCommand::Begin(intent)).unwrap();
    cp.propose(ClusterStateChange::BumpModelVersion {
        dict_fingerprint: 99,
    })
    .unwrap();
    assert_eq!(
        cp.propose_move(MoveCommand::MarkReady {
            operation_id: 73,
            evidence: recovery_evidence(2),
        })
        .unwrap()
        .outcome,
        MoveCommandOutcome::Conflict
    );
}
