//! ADR-179 resize operation records: idempotent operation IDs, placement-generation
//! preconditions, and the status reads.

use super::*;

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

async fn post_json(state: &Arc<ClusterAppState>, body: &str) -> (StatusCode, serde_json::Value) {
    let (status, _, bytes) =
        send_raw(state, resize_request("/_cluster/resize", body.to_string())).await;
    let value = serde_json::from_slice(&bytes).expect("JSON body");
    (status, value)
}

async fn get_json(state: &Arc<ClusterAppState>, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, _, bytes) = send_raw(state, get(uri)).await;
    let value = serde_json::from_slice(&bytes).expect("JSON body");
    (status, value)
}

/// Hold the exclusive topology guard on a dedicated thread (never across an `await`) until the
/// returned release function runs.
fn hold_topology(state: &Arc<ClusterAppState>) -> impl FnOnce() {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _topology = holder_state.topology_guard.write();
        locked_sender.send(()).expect("signal topology lock");
        let _released = release_receiver.recv();
    });
    locked_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("topology lock held");
    move || {
        release_sender.send(()).expect("release topology lock");
        holder.join().expect("topology holder");
    }
}

fn generation(state: &Arc<ClusterAppState>) -> u64 {
    state.cluster.read().placement_generation().0
}

#[tokio::test]
async fn status_reads_start_empty_and_are_strict() {
    let state = test_state(&seed());
    let (status, body) = get_json(&state, "/_cluster/resize").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["operations"], serde_json::json!([]));
    assert_eq!(body["autoscaler"]["enabled"], false);
    assert!(body["autoscaler"].get("last_observation").is_none());

    let (status, _, bytes) = send_raw(&state, get("/_cluster/resize?pretty=true")).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let (status, _, bytes) = send_raw(&state, get("/_cluster/resize/unknown-op")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_FOUND,
        "resize_operation_not_found",
    );

    let (status, headers, bytes) = send_raw(
        &state,
        resize_request("/_cluster/resize/unknown-op", r#"{"num_shards":3}"#),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("no-store"),
        "no-store"
    );
}

#[tokio::test]
async fn an_operation_id_replays_its_recorded_success_without_rebuilding() {
    let state = test_state(&seed());
    let before = generation(&state);
    let (status, first) = post_json(&state, r#"{"num_shards":4,"operation_id":"grow-4"}"#).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["operation_id"], "grow-4");
    assert_eq!(first["num_shards"], 4);
    assert_eq!(first["placement_generation"], before + 1);
    assert!(first.get("replayed").is_none(), "{first}");

    let (status, record) = get_json(&state, "/_cluster/resize/grow-4").await;
    assert_eq!(status, StatusCode::OK, "{record}");
    assert_eq!(record["state"], "succeeded");
    assert_eq!(record["origin"], "api");
    assert_eq!(record["outcome"]["num_shards"], 4);
    assert!(record["started_at_ms"].as_u64().is_some());
    assert!(record["finished_at_ms"].as_u64().is_some());

    let (status, replay) = post_json(&state, r#"{"num_shards":4,"operation_id":"grow-4"}"#).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["rebuilt"], first["rebuilt"]);
    assert_eq!(replay["version"], first["version"]);
    assert_eq!(generation(&state), before + 1, "a replay must not rebuild");

    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            r#"{"num_shards":5,"operation_id":"grow-4"}"#,
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::CONFLICT,
        "operation_id_conflict",
    );
    assert_eq!(state.cluster.read().num_shards(), 4);
}

#[tokio::test]
async fn a_stale_retry_cannot_undo_a_later_resize() {
    let state = test_state(&seed());
    let (status, body) = post_json(&state, r#"{"num_shards":4,"operation_id":"to-4"}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = post_json(&state, r#"{"num_shards":2,"operation_id":"to-2"}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A delayed retry of the first operation replays its outcome; it does not grow back to 4.
    let (status, replay) = post_json(&state, r#"{"num_shards":4,"operation_id":"to-4"}"#).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["replayed"], true);
    assert_eq!(state.cluster.read().num_shards(), 2);

    let (status, list) = get_json(&state, "/_cluster/resize").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let ids: Vec<&str> = list["operations"]
        .as_array()
        .expect("operations")
        .iter()
        .map(|r| r["operation_id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids, ["to-2", "to-4"], "newest first");
}

#[tokio::test]
async fn a_placement_generation_precondition_guards_the_start() {
    let state = test_state(&seed());
    let current = generation(&state);
    let stale = current + 7;
    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            format!(
                r#"{{"num_shards":4,"operation_id":"guarded","if_placement_generation":{stale}}}"#
            ),
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::CONFLICT,
        "placement_generation_mismatch",
    );
    assert_eq!(state.cluster.read().num_shards(), 3, "nothing started");
    assert_eq!(generation(&state), current);
    let (_, record) = get_json(&state, "/_cluster/resize/guarded").await;
    assert_eq!(record["state"], "failed");
    assert_eq!(record["error"]["type"], "placement_generation_mismatch");
    assert!(record.get("started_at_ms").is_none(), "{record}");

    let (status, body) = post_json(
        &state,
        &format!(r#"{{"num_shards":4,"if_placement_generation":{current}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["operation_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("resize-")),
        "a generated id is returned: {body}"
    );
    assert_eq!(state.cluster.read().num_shards(), 4);
}

#[tokio::test]
async fn a_failed_operation_re_executes_under_the_same_id() {
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let state = state_with_control(Box::new(retry::FailResizeProposals {
        inner: InMemoryControlPlane::new(initial),
        remaining: AtomicUsize::new(1),
    }));

    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            r#"{"num_shards":4,"operation_id":"heal"}"#,
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "control_plane_error",
    );
    let (_, record) = get_json(&state, "/_cluster/resize/heal").await;
    assert_eq!(record["state"], "failed", "{record}");
    assert_eq!(record["error"]["type"], "control_plane_error");

    let (status, body) = post_json(&state, r#"{"num_shards":4,"operation_id":"heal"}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("replayed").is_none(),
        "a failed record re-executes: {body}"
    );
    let (_, record) = get_json(&state, "/_cluster/resize/heal").await;
    assert_eq!(record["state"], "succeeded", "{record}");
    assert!(record.get("error").is_none(), "{record}");
    assert_eq!(state.cluster.read().num_shards(), 4);
}

#[tokio::test]
async fn an_active_operation_reports_in_progress_to_a_duplicate() {
    let state = test_state(&seed());
    let release = hold_topology(&state);

    let background = Arc::clone(&state);
    let first = tokio::spawn(async move {
        send_raw(
            &background,
            resize_request(
                "/_cluster/resize?cluster_manager_timeout=5s",
                r#"{"num_shards":4,"operation_id":"busy"}"#,
            ),
        )
        .await
    });
    let mut queued = false;
    for _ in 0..200 {
        if state
            .resize_operations
            .get("busy")
            .is_some_and(|r| r.state == crate::resize_ops::ResizeState::Queued)
        {
            queued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(queued, "the first request must be admitted and waiting");

    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            r#"{"num_shards":4,"operation_id":"busy"}"#,
        ),
    )
    .await;
    assert_error(status, &bytes, StatusCode::CONFLICT, "resize_in_progress");

    release();
    let (status, _, bytes) = first.await.expect("first request");
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(
        state.resize_operations.get("busy").expect("record").state,
        crate::resize_ops::ResizeState::Succeeded
    );
}

#[tokio::test]
async fn a_request_that_never_starts_is_recorded_not_started() {
    let state = test_state(&seed());
    let release = hold_topology(&state);
    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize?cluster_manager_timeout=0",
            r#"{"num_shards":4,"operation_id":"blocked"}"#,
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "resize_timeout",
    );
    release();
    let record = state.resize_operations.get("blocked").expect("record");
    assert_eq!(record.state, crate::resize_ops::ResizeState::NotStarted);
    assert_eq!(state.cluster.read().num_shards(), 3);
}

#[tokio::test]
async fn a_conditional_operation_heals_its_own_uncommitted_swap() {
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let state = state_with_control(Box::new(retry::FailResizeProposals {
        inner: InMemoryControlPlane::new(initial),
        remaining: AtomicUsize::new(1),
    }));
    let before = generation(&state);
    let body = format!(
        r#"{{"num_shards":4,"operation_id":"cas-heal","if_placement_generation":{before}}}"#
    );

    let (status, _, bytes) =
        send_raw(&state, resize_request("/_cluster/resize", body.clone())).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "control_plane_error",
    );
    assert_eq!(generation(&state), before + 1, "the serving swap happened");
    let (_, record) = get_json(&state, "/_cluster/resize/cas-heal").await;
    assert_eq!(record["state"], "failed", "{record}");
    assert_eq!(record["uncommitted_generation"], before + 1, "{record}");

    // The identical request's original precondition is now stale, but it names this
    // operation's own uncommitted swap, so the retry finishes the commit.
    let (status, healed) = post_json(&state, &body).await;
    assert_eq!(status, StatusCode::OK, "{healed}");
    assert_eq!(healed["num_shards"], 4);
    assert_eq!(healed["placement_generation"], before + 1);
    let control = state.cluster.read().control_state().expect("state");
    assert_eq!(control.num_shards, 4);
    assert_eq!(control.placement_generation, before + 1);
    let (_, record) = get_json(&state, "/_cluster/resize/cas-heal").await;
    assert_eq!(record["state"], "succeeded", "{record}");
    assert!(record.get("uncommitted_generation").is_none(), "{record}");

    // An unrelated conditional request at the pre-swap generation is still refused.
    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            format!(r#"{{"num_shards":5,"if_placement_generation":{before}}}"#),
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::CONFLICT,
        "placement_generation_mismatch",
    );
}

#[tokio::test]
async fn a_failure_names_the_generated_operation_so_it_can_heal() {
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let state = state_with_control(Box::new(retry::FailResizeProposals {
        inner: InMemoryControlPlane::new(initial),
        remaining: AtomicUsize::new(1),
    }));
    let before = generation(&state);
    let (status, failed) = post_json(
        &state,
        &format!(r#"{{"num_shards":4,"if_placement_generation":{before}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{failed}");
    assert_eq!(failed["error"]["type"], "control_plane_error");
    let id = failed["operation_id"]
        .as_str()
        .expect("a failed response names its generated operation")
        .to_string();
    assert!(id.starts_with("resize-"), "{id}");

    let (status, healed) = post_json(
        &state,
        &format!(r#"{{"num_shards":4,"operation_id":"{id}","if_placement_generation":{before}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{healed}");
    assert_eq!(healed["operation_id"], id.as_str());
    assert_eq!(
        state
            .cluster
            .read()
            .control_state()
            .expect("state")
            .num_shards,
        4
    );
}

#[tokio::test]
async fn targets_are_validated_against_the_topology() {
    let state = test_state(&seed());
    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize",
            r#"{"num_shards":4,"targets":[{"id":11,"endpoint":"http://127.0.0.1:1"}]}"#,
        ),
    )
    .await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    for body in [
        r#"{"num_shards":4,"targets":[]}"#,
        r#"{"num_shards":4,"targets":null}"#,
        r#"{"num_shards":4,"targets":[{"id":1,"endpoint":""}]}"#,
        r#"{"num_shards":4,"targets":[{"id":1,"endpoint":"a"},{"id":1,"endpoint":"b"}]}"#,
        r#"{"num_shards":4,"targets":[{"id":1,"endpoint":"a","extra":true}]}"#,
    ] {
        let (status, _, bytes) = send_raw(&state, resize_request("/_cluster/resize", body)).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }
    assert_eq!(state.cluster.read().num_shards(), 3);
}

#[cfg(feature = "distributed")]
fn resolve_only_state(config: &ClusterConfig) -> Arc<ClusterAppState> {
    let cluster =
        ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), config, &seed())
            .expect("cluster");
    state_from_cluster_with_rebalance_topology(
        cluster,
        crate::state::ClusterRebalanceTopology::ResolveOnlyRemote,
    )
}

#[cfg(feature = "distributed")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_resize_frees_health_admission_but_stays_joinable_by_shutdown() {
    let state = resolve_only_state(&ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    });
    // Hold the exclusive cluster lock on a dedicated thread so the remote worker parks after it
    // has taken the topology and write guards.
    let holder_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _cluster = holder_state.cluster.write();
        locked_sender.send(()).expect("signal cluster lock");
        release_receiver.recv().expect("release cluster lock");
    });
    locked_receiver.recv().expect("cluster locked");
    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        post_json(
            &request_state,
            r#"{"num_shards":4,"operation_id":"remote-health","targets":[{"id":11,"endpoint":"http://127.0.0.1:1"}]}"#,
        )
        .await
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !state.write_serial.is_locked() {
        assert!(
            std::time::Instant::now() < deadline,
            "the worker never took the write guard"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A remote copy keeps reads serving, so it must return the single administrative permit
    // that `/_health` also waits for; otherwise probes time out for the whole copy.
    while state.stats_permits.available_permits() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the remote resize kept the permit health probes need"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Shutdown joins detached administration by acquiring its permits, so the copy must still
    // hold one of its own.
    assert_eq!(
        state.remote_resize_permits.available_permits(),
        0,
        "shutdown must be able to wait for the running copy"
    );
    release_sender.send(()).expect("release");
    holder.join().expect("holder");
    let (status, failed) = request.await.expect("request task");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{failed}");
    assert_eq!(state.remote_resize_permits.available_permits(), 1);
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn a_resolve_only_remote_resize_requires_targets() {
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let state = resolve_only_state(&config);
    let (status, _, bytes) = send_raw(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":4}"#),
    )
    .await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    // Unknown targets are registered as data nodes, so each endpoint must be a mesh origin; a
    // malformed one is refused before any operation starts or any node is registered.
    for endpoint in ["not-a-uri", "ftp://127.0.0.1:1", "http://127.0.0.1:1/path"] {
        let body = format!(
            r#"{{"num_shards":4,"operation_id":"bad-origin","targets":[{{"id":11,"endpoint":"{endpoint}"}}]}}"#
        );
        let (status, _, bytes) = send_raw(&state, resize_request("/_cluster/resize", body)).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }
    let (status, _) = get_json(&state, "/_cluster/resize/bad-origin").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(state
        .cluster
        .read()
        .control_state()
        .expect("state")
        .nodes
        .iter()
        .all(|node| node.id.0 != 11));

    // With targets, the worker reaches the engine, which refuses a cluster that is not
    // remote and assignment-routed; the refusal is recorded and nothing changes.
    let (status, failed) = post_json(
        &state,
        r#"{"num_shards":4,"operation_id":"remote-1","targets":[{"id":11,"endpoint":"http://127.0.0.1:1"}]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{failed}");
    assert_eq!(failed["operation_id"], "remote-1");
    let (_, record) = get_json(&state, "/_cluster/resize/remote-1").await;
    assert_eq!(record["state"], "failed", "{record}");
    assert_eq!(record["targets"][0]["id"], 11);
    assert_eq!(state.cluster.read().num_shards(), 3);

    let static_state = state_from_cluster_with_rebalance_topology(
        ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &seed(),
        )
        .expect("cluster"),
        crate::state::ClusterRebalanceTopology::StaticRemote,
    );
    let (status, _, bytes) = send_raw(
        &static_state,
        resize_request(
            "/_cluster/resize",
            r#"{"num_shards":4,"targets":[{"id":11,"endpoint":"http://127.0.0.1:1"}]}"#,
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
    );
}
