use super::{
    valid_operation_id, ResizeAdmission, ResizeFailure, ResizeOperations, ResizeOrigin,
    ResizeOutcome, ResizeState, MAX_RETAINED_RESIZE_OPERATIONS,
};

fn outcome(num_shards: usize) -> ResizeOutcome {
    ResizeOutcome {
        old_num_shards: 4,
        num_shards,
        rebuilt: 10,
        version: 3,
        placement_generation: 2,
    }
}

fn failure() -> ResizeFailure {
    ResizeFailure {
        error_type: "control_plane_error".into(),
        reason: "injected".into(),
    }
}

fn execute(admission: ResizeAdmission) -> String {
    match admission {
        ResizeAdmission::Execute(id) => id,
        other => panic!("expected Execute, got {other:?}"),
    }
}

#[test]
fn operation_ids_are_bounded_and_restricted() {
    assert!(valid_operation_id("deploy-42.a:b_c"));
    assert!(!valid_operation_id(""));
    assert!(!valid_operation_id("has space"));
    assert!(!valid_operation_id("slash/id"));
    assert!(!valid_operation_id(&"x".repeat(65)));
    assert!(valid_operation_id(&"x".repeat(64)));
}

#[test]
fn generated_ids_are_unique_and_prefixed_by_origin() {
    let ops = ResizeOperations::new(false);
    let a = execute(ops.admit(None, ResizeOrigin::Api, 8, None));
    let b = execute(ops.admit(None, ResizeOrigin::Autoscaler, 8, None));
    assert_ne!(a, b);
    assert!(a.starts_with("resize-"), "{a}");
    assert!(b.starts_with("autoscale-"), "{b}");
}

#[test]
fn a_succeeded_request_replays_and_an_active_one_reports_progress() {
    let ops = ResizeOperations::new(false);
    let id = execute(ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, Some(2)));
    assert!(matches!(
        ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, Some(2)),
        ResizeAdmission::InProgress(_)
    ));
    ops.mark_running(&id);
    assert!(matches!(
        ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, Some(2)),
        ResizeAdmission::InProgress(record) if record.state == ResizeState::Running
    ));
    ops.mark_succeeded(&id, outcome(8));
    match ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, Some(2)) {
        ResizeAdmission::Replay(record) => {
            assert_eq!(record.state, ResizeState::Succeeded);
            assert_eq!(record.outcome, Some(outcome(8)));
        }
        other => panic!("expected Replay, got {other:?}"),
    }
}

#[test]
fn different_parameters_under_one_id_conflict() {
    let ops = ResizeOperations::new(false);
    let id = execute(ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, None));
    ops.mark_succeeded(&id, outcome(8));
    for (origin, target, generation) in [
        (ResizeOrigin::Api, 4, None),
        (ResizeOrigin::Api, 8, Some(2)),
        (ResizeOrigin::Autoscaler, 8, None),
    ] {
        assert!(matches!(
            ops.admit(Some("op-1".into()), origin, target, generation),
            ResizeAdmission::Conflict(_)
        ));
    }
}

#[test]
fn a_failed_or_not_started_request_re_executes_under_its_id() {
    let ops = ResizeOperations::new(false);
    let id = execute(ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, None));
    ops.mark_running(&id);
    ops.mark_failed(&id, failure());
    assert_eq!(
        execute(ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, None)),
        "op-1"
    );
    let record = ops.get("op-1").expect("retained");
    assert_eq!(record.state, ResizeState::Queued);
    assert!(record.error.is_none() && record.started_at_ms.is_none());

    ops.mark_not_started(&id, failure());
    assert_eq!(
        execute(ops.admit(Some("op-1".into()), ResizeOrigin::Api, 8, None)),
        "op-1"
    );
    assert_eq!(ops.list().len(), 1, "a retry reuses the retained record");
}

#[test]
fn retention_evicts_terminal_records_and_never_active_ones() {
    let ops = ResizeOperations::new(false);
    let first = execute(ops.admit(Some("first".into()), ResizeOrigin::Api, 2, None));
    ops.mark_succeeded(&first, outcome(2));
    for i in 1..MAX_RETAINED_RESIZE_OPERATIONS {
        execute(ops.admit(Some(format!("active-{i}")), ResizeOrigin::Api, 2, None));
    }
    // Full: the only terminal record is evicted to admit one more.
    execute(ops.admit(Some("next".into()), ResizeOrigin::Api, 2, None));
    assert!(ops.get("first").is_none());
    assert_eq!(ops.list().len(), MAX_RETAINED_RESIZE_OPERATIONS);
    // Every retained record is now active.
    assert!(matches!(
        ops.admit(Some("overflow".into()), ResizeOrigin::Api, 2, None),
        ResizeAdmission::Full
    ));
}

#[test]
fn list_is_newest_first() {
    let ops = ResizeOperations::new(false);
    for id in ["a", "b", "c"] {
        execute(ops.admit(Some(id.into()), ResizeOrigin::Api, 2, None));
    }
    let ids: Vec<String> = ops.list().into_iter().map(|r| r.operation_id).collect();
    assert_eq!(ids, ["c", "b", "a"]);
}

#[test]
fn autoscale_status_serializes_every_verdict_without_duplicate_keys() {
    use reverse_rusty::cluster::ResizeVerdict;
    let verdicts = [
        ResizeVerdict::Idle,
        ResizeVerdict::AtCeiling { max_shards: 16 },
        ResizeVerdict::Deferred {
            target: 5,
            observations: 1,
            required: 3,
        },
        ResizeVerdict::CoolingDown {
            target: 5,
            remaining_ms: 10,
        },
        ResizeVerdict::Ineffective {
            before_max_selective: 10,
            after_max_selective: 9,
            min_relief_percent: 10,
        },
        ResizeVerdict::Accept {
            from: 4,
            to: 5,
            if_placement_generation: 2,
        },
    ];
    for verdict in verdicts {
        let status = super::AutoscaleStatus {
            observed_at_ms: 1,
            num_shards: 4,
            placement_generation: 2,
            recommended: Some(5),
            max_selective_corpus: 10,
            verdict,
        };
        let text = serde_json::to_string(&status).expect("serialize");
        let value: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&text).expect("object");
        let keys = text.matches("\":").count();
        assert_eq!(keys, value.len(), "duplicate keys in {text}");
        assert!(value["verdict"].is_string(), "{text}");
    }
}

#[test]
fn an_uncommitted_failed_record_is_pinned_against_eviction() {
    let ops = ResizeOperations::new(false);
    let pinned = execute(ops.admit(Some("pinned".into()), ResizeOrigin::Autoscaler, 6, Some(3)));
    ops.mark_failed_uncommitted(&pinned, failure(), 4);
    for i in 0..(2 * MAX_RETAINED_RESIZE_OPERATIONS) {
        let id = execute(ops.admit(Some(format!("churn-{i}")), ResizeOrigin::Api, 2, None));
        ops.mark_not_started(&id, failure());
    }
    let record = ops
        .get("pinned")
        .expect("an uncommitted swap must stay retryable");
    assert_eq!(record.uncommitted_generation, Some(4));
    assert_eq!(
        execute(ops.admit(Some("pinned".into()), ResizeOrigin::Autoscaler, 6, Some(3))),
        "pinned"
    );
    assert_eq!(
        ops.get("pinned").expect("retained").uncommitted_generation,
        Some(4),
        "re-admission keeps the uncommitted generation for the worker's precondition"
    );
}

#[test]
fn superseded_or_healed_pins_are_released() {
    let ops = ResizeOperations::new(false);
    let old = execute(ops.admit(Some("old".into()), ResizeOrigin::Api, 6, Some(3)));
    ops.mark_failed_uncommitted(&old, failure(), 4);
    let newer = execute(ops.admit(Some("newer".into()), ResizeOrigin::Api, 8, None));
    ops.mark_failed_uncommitted(&newer, failure(), 5);
    let healed = execute(ops.admit(Some("healer".into()), ResizeOrigin::Api, 8, None));
    ops.mark_succeeded(
        &healed,
        ResizeOutcome {
            placement_generation: 5,
            ..outcome(8)
        },
    );
    // Every retained record is now evictable: the gen-4 pin is superseded by gen 5, and the gen-5
    // pin is healed by a success that committed generation 5.
    let mut last = super::ResizeAdmission::Full;
    for i in 0..MAX_RETAINED_RESIZE_OPERATIONS {
        last = ops.admit(Some(format!("fill-{i}")), ResizeOrigin::Api, 2, None);
        if let super::ResizeAdmission::Execute(id) = &last {
            ops.mark_not_started(id, failure());
        }
    }
    assert!(
        matches!(last, super::ResizeAdmission::Execute(_)),
        "{last:?}"
    );
    assert!(ops.get("old").is_none() && ops.get("newer").is_none());
}

#[test]
fn a_generated_id_never_aliases_a_caller_supplied_one() {
    let ops = ResizeOperations::new(false);
    let now = super::unix_ms_now();
    // Occupy a window of plausible generated names.
    for ms in now..now + 50 {
        for seq in 1..4 {
            let id = format!("resize-{ms}-{seq}");
            if let super::ResizeAdmission::Execute(id) =
                ops.admit(Some(id), ResizeOrigin::Api, 2, None)
            {
                ops.mark_not_started(&id, failure());
            }
        }
    }
    let generated = execute(ops.admit(None, ResizeOrigin::Api, 9, None));
    let matching: Vec<_> = ops
        .list()
        .into_iter()
        .filter(|r| r.operation_id == generated)
        .collect();
    assert_eq!(matching.len(), 1, "exactly one record per id");
    assert_eq!(matching[0].num_shards, 9);
}
