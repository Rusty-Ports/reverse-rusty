//! The coordinator's `--include-broad` sets the scope of every request that names none, on
//! every surface (ADR-201). The single-node twin, which also compares result sets, is
//! `handlers::search::tests::default_scope`.

use super::*;

fn state_with_default(include_broad: bool) -> Arc<ClusterAppState> {
    let mut state = test_state(&seed());
    Arc::get_mut(&mut state)
        .expect("a state nothing else holds yet")
        .include_broad = include_broad;
    state
}

async fn scope_of(
    state: &Arc<ClusterAppState>,
    path: &str,
    body: &serde_json::Value,
) -> serde_json::Value {
    let (status, json) = send(state, req("POST", path, body)).await;
    assert!(status.is_success(), "{path}: {status} {json}");
    json["query_scope"].clone()
}

async fn job_scope_of(state: &Arc<ClusterAppState>, body: &serde_json::Value) -> serde_json::Value {
    let (status, created) = send(state, req("POST", "/_percolate/jobs", body)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{created}");
    let id = created["job_id"].as_str().expect("job id");
    let (status, job) = send(
        state,
        req_empty(
            "GET",
            &format!("/_percolate/jobs/{id}?wait_for_completion_timeout=0s"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{job}");
    state.exhaustive_jobs.cancel(id);
    job["query_scope"].clone()
}

#[tokio::test]
async fn v2_search_batch_and_jobs_without_a_scope_use_the_server_default() {
    for (include_broad, scope) in [(true, "with_broad"), (false, "standard")] {
        let state = state_with_default(include_broad);
        let one = serde_json::json!({"document": {"title": "1994 acme"}});
        let many = serde_json::json!({"documents": [{"title": "1994 acme"}]});

        assert_eq!(scope_of(&state, "/v2/_search", &one).await, scope);
        assert_eq!(scope_of(&state, "/v2/_mpercolate", &many).await, scope);
        assert_eq!(job_scope_of(&state, &one).await, scope);
    }
}

#[tokio::test]
async fn a_request_that_names_a_scope_gets_that_scope_whatever_the_server_default() {
    for (include_broad, named) in [(true, "standard"), (false, "with_broad")] {
        let state = state_with_default(include_broad);
        let one = serde_json::json!({"document": {"title": "1994 acme"}, "query_scope": named});
        let many = serde_json::json!({"documents": [{"title": "1994 acme"}], "query_scope": named});

        assert_eq!(scope_of(&state, "/v2/_search", &one).await, named);
        assert_eq!(scope_of(&state, "/v2/_mpercolate", &many).await, named);
        assert_eq!(job_scope_of(&state, &one).await, named);
    }
}

/// The coordinator's compatibility responses say which scope ran, as the single-node ones do.
#[tokio::test]
async fn compatibility_responses_say_which_scope_ran() {
    use tower::ServiceExt;

    for (server_default, default_scope) in [(true, "with_broad"), (false, "standard")] {
        let state = state_with_default(server_default);
        let router = crate::cluster_mode::router::build_cluster_router(Arc::clone(&state), 8);
        let one = serde_json::json!({"document": {"title": "1994 acme"}});
        let many = serde_json::json!({"documents": [{"title": "1994 acme"}]});
        for (path, body) in [("/_search", &one), ("/_mpercolate", &many)] {
            let cases = [
                (None, default_scope),
                (Some(true), "with_broad"),
                (Some(false), "standard"),
            ];
            for (named, scope) in cases {
                let mut body = body.clone();
                if let Some(named) = named {
                    body["include_broad"] = named.into();
                }
                let response = router
                    .clone()
                    .oneshot(req("POST", path, &body))
                    .await
                    .expect("response");
                assert!(response.status().is_success(), "{path} {named:?}");
                assert_eq!(
                    response
                        .headers()
                        .get("x-rr-query-scope")
                        .map(axum::http::HeaderValue::as_bytes),
                    Some(scope.as_bytes()),
                    "{path} {named:?}"
                );
            }
        }
    }
}
