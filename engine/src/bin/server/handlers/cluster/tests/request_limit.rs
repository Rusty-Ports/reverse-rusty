//! The request limit on the real coordinator router: one limit per endpoint (ADR-199). The
//! single-node twin is `crate::router::tests`.

use std::time::Duration;

use axum::http::StatusCode;
use tower::ServiceExt;

use super::test_state;
use crate::auth::AuthConfig;
use crate::cluster_mode::router::build_cluster_router;
use crate::router::held::{bare, hold, hold_as, release_all, send};

/// Long enough for a request that is free to run to finish, by a wide margin.
const SETTLE: Duration = Duration::from_millis(300);

/// An endpoint works on its limit and no more: with a limit of one and one search in
/// flight, a second search waits until the first has answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_endpoint_works_on_its_limit_and_no_more() {
    let router = build_cluster_router(test_state(&[]), 1);
    let first = hold(&router, "POST", "/_search").await;
    let mut second = send(&router, "POST", "/_search");
    let ran_beside_the_first = second.is_admitted_within(SETTLE).await;

    first.release().await;
    let ran_after_it = second.is_admitted_within(SETTLE * 10).await;
    second.release().await;

    assert!(
        !ran_beside_the_first,
        "two searches ran at once under a limit of one"
    );
    assert!(
        ran_after_it,
        "the waiting search runs once the slot is free"
    );
}

/// The limit is per endpoint, and it has to be. A full endpoint holds back no other: not
/// another search route, not a write, not a read, not administration, not the probes. Some
/// requests wait in flight for a request on another endpoint (a job-status poll for the
/// job's stream; a search or a write for a lock a job holds until its stream is read), and
/// if they shared slots with it, the waiters would fill the pool and it would never run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_endpoint_holds_back_no_other_endpoint() {
    let router = build_cluster_router(test_state(&[]), 1);
    let search = hold(&router, "POST", "/_search").await;

    let mut held = Vec::new();
    let mut admitted = Vec::new();
    for (method, path) in [("POST", "/_mpercolate"), ("PUT", "/_doc/1")] {
        let mut other = send(&router, method, path);
        admitted.push((path, other.is_admitted_within(SETTLE * 10).await));
        held.push(other);
    }
    let mut answered = Vec::new();
    for path in ["/", "/_stats", "/_health", "/_metrics"] {
        let answer =
            tokio::time::timeout(SETTLE * 10, router.clone().oneshot(bare("GET", path))).await;
        answered.push((
            path,
            answer.map(|response| response.expect("router").status()),
        ));
    }
    // Free everything before asserting, so a failure does not leave requests in flight.
    release_all(held.into_iter().chain([search])).await;

    for (path, ran) in admitted {
        assert!(ran, "{path} waited for the search endpoint's slot");
    }
    for (path, status) in answered {
        assert_eq!(
            status.ok(),
            Some(StatusCode::OK),
            "{path} waited for the search endpoint's slot"
        );
    }
}

/// Auth is outside the limiter: a request without the token is refused at once, also when
/// its endpoint is full, so a flood of them cannot queue in front of real traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_without_the_token_is_refused_without_waiting_for_a_slot() {
    let auth = AuthConfig::resolve(
        Some("s3cret".into()),
        Err(std::env::VarError::NotPresent),
        false,
    )
    .expect("token");
    let mut state = test_state(&[]);
    std::sync::Arc::get_mut(&mut state)
        .expect("a state nothing else holds yet")
        .auth = auth;
    let router = build_cluster_router(state, 1);
    let write = hold_as(&router, "PUT", "/_doc/1", Some("s3cret")).await;

    let refused =
        tokio::time::timeout(SETTLE * 10, router.clone().oneshot(bare("PUT", "/_doc/2"))).await;
    write.release().await;

    let response = refused
        .expect("the refusal does not wait for the endpoint's slot")
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
