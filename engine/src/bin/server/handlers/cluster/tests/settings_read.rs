use super::*;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{header, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_settings_are_complete_default_aware_uncacheable_and_observed() {
    let state = test_state(&seed());
    let uri = "/_settings?include_defaults=true&flat_settings=true";
    let (status, get_headers, bytes) = send_raw(&state, req_empty("GET", uri)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        get_headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        get_headers
            .get(header::CONTENT_LENGTH)
            .expect("content length")
            .to_str()
            .expect("ASCII length"),
        bytes.len().to_string()
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("settings JSON");
    assert_eq!(body["mode"], "cluster", "{body}");
    assert_eq!(body["shards"], 3, "{body}");
    assert_eq!(body["replication_factor"], 1, "{body}");
    assert_eq!(body["include_broad"], true, "{body}");
    assert_eq!(body["durable"], false, "{body}");
    assert!(body["per_shard"].is_object(), "{body}");
    assert_eq!(body["per_shard"]["max_segments"], 8, "{body}");
    assert_eq!(body["defaults"]["max_segments"], 8, "{body}");
    assert!(
        body.get("shard_local").is_none(),
        "in-process, per_shard is every shard's configuration: {body}"
    );

    let (status, head_headers, head_body) = send_raw(&state, req_empty("HEAD", uri)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(head_body.is_empty());
    assert_eq!(
        head_headers.get(header::CONTENT_LENGTH),
        get_headers.get(header::CONTENT_LENGTH)
    );

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["settings_get", "200"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["settings_get"])
            .get_sample_count(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_transport_rejects_invalid_cluster_reads_before_locking() {
    let state = test_state(&seed());
    for request in [
        req_empty("GET", "/_settings?unknown=true"),
        req_empty(
            "GET",
            "/_settings?include_defaults=true&include_defaults=false",
        ),
        Request::builder()
            .method("GET")
            .uri("/_settings")
            .body(Body::from("{}"))
            .expect("request"),
    ] {
        let (status, headers, bytes) = send_raw(&state, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
    }

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_settings")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD, PUT");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed", "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_lock_wait_is_off_runtime_and_keeps_admission() {
    let state = test_state(&seed());
    let lock_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _guard = lock_state.cluster.write();
        locked_tx.send(()).expect("announce held lock");
        std::thread::sleep(Duration::from_millis(500));
    });
    locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cluster write lock held");

    let request_state = Arc::clone(&state);
    let request =
        tokio::spawn(async move { send_raw(&request_state, req_empty("GET", "/_settings")).await });
    let started = Instant::now();
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "cluster-lock contention escaped spawn_blocking and stalled the async runtime"
    );
    assert!(
        !request.is_finished(),
        "request should still be waiting for the deliberately held cluster lock"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            Arc::clone(&state.stats_permits).acquire_owned()
        )
        .await
        .is_err(),
        "the blocking worker must retain administrative admission while waiting"
    );

    blocker.join().expect("lock holder");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    state.stats_permits.close();
    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_settings")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "settings_unavailable", "{body}");
}

/// Against remote shard nodes `per_shard` is only the coordinator's own configuration. The
/// response names the keys each node sets for itself, so a coordinator's values are not read
/// as the shards' (ADR-192).
#[cfg(feature = "distributed")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_settings_name_the_keys_each_shard_node_sets_itself() {
    let norm = Arc::new(Normalizer::default_vocab().expect("vocab"));
    let config = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    let template = ClusterEngine::build(
        Normalizer::default_vocab().expect("template vocab"),
        &config,
        &seed(),
    )
    .expect("frozen dict");
    let dict = Arc::new(template.dict().clone());
    let mut tags = reverse_rusty::tagdict::TagDict::new();
    tags.mark_finalized();
    let tags = Arc::new(tags);
    let incoming =
        tonic::transport::server::TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
    let endpoint = format!("http://{}", incoming.local_addr().expect("address"));
    let server = reverse_rusty::cluster::ShardServer::pending(
        Arc::clone(&norm),
        reverse_rusty::config::EngineConfig::default(),
    );
    let task = tokio::spawn(server.serve_with_incoming(incoming));
    let handle = tokio::runtime::Handle::current();
    let cluster = tokio::task::spawn_blocking(move || {
        ClusterEngine::connect_remote(norm, dict, tags, &config, &[endpoint], &handle)
            .expect("remote coordinator")
    })
    .await
    .expect("connect worker");
    let state = state_from_cluster(cluster);

    let (status, _, bytes) = send_raw(&state, req_empty("GET", "/_settings")).await;
    task.abort();
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("settings JSON");
    let shard_local: Vec<&str> = body["shard_local"]
        .as_array()
        .unwrap_or_else(|| panic!("a remote coordinator lists shard-local keys: {body}"))
        .iter()
        .map(|key| key.as_str().expect("key"))
        .collect();
    for key in [
        "wal_sync_on_write",
        "retain_source",
        "max_segments",
        "memtable_flush_threshold",
    ] {
        assert!(shard_local.contains(&key), "{key}: {body}");
        assert!(
            body["per_shard"].get(key).is_some(),
            "{key} is a per_shard key: {body}"
        );
    }
}
