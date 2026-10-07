use super::*;

use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn has_standalone_parity_and_true_noop_semantics() {
    let state = test_state(&[(7, "package adapter".to_string())]);
    let document = serde_json::json!({
        "synonyms_set": [
            {"id": "package-rule", "synonyms": "package, pkg"},
            {"id": "adapter-rule", "synonyms": "adapter, adaptor"}
        ]
    });

    let (status, headers, bytes) = send_raw(
        &state,
        req("POST", "/_vocab/aliases/import?refresh=true", &document),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["result"], "updated", "{body}");
    assert_eq!(body["rules"], 2, "{body}");
    assert_eq!(body["activated"], 2, "{body}");
    assert_eq!(body["recompiled"], 1, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");
    assert!(state
        .cluster
        .percolate("pkg adaptor")
        .expect("percolate")
        .contains(&7));

    let (status, _, bytes) =
        send_raw(&state, req("POST", "/_vocab/aliases/import", &document)).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["result"], "noop", "{body}");
    assert_eq!(body["activated"], 0, "{body}");
    assert_eq!(body["recompiled"], 0, "{body}");

    let (status, headers, bytes) =
        send_raw(&state, req_empty("GET", "/_vocab/aliases/import")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed", "{body}");

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_import", "200"])
            .get(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn waits_for_admission_and_a_running_rebuild_off_runtime() {
    let state = test_state(&[(7, "package adapter".to_string())]);
    let document = serde_json::json!({"synonyms": "package, pkg"});
    let held = Arc::clone(&state.admin_change_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let queued = document.clone();
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req("POST", "/_vocab/aliases/import", &queued),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "cluster alias import must wait asynchronously for admission"
    );
    drop(held);
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    // Another rebuild is running: the import waits for it inside the engine.
    let other_rebuild = pause_a_rebuild(&state);

    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req("POST", "/_vocab/aliases/import", &document),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "the import should wait for the rebuild that is running"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    let rebuilt = other_rebuild.finish();
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);
    rebuilt.expect("the other rebuild");
}
