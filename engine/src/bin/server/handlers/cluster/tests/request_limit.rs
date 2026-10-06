//! The request pool of the real coordinator router (ADR-199). The single-node twin is
//! `crate::router::tests`.

use std::time::Duration;

use axum::http::StatusCode;
use tower::ServiceExt;

use super::test_state;
use crate::auth::AuthConfig;
use crate::cluster_mode::router::build_cluster_router;
use crate::router::held::{bare, hold, release_all, send};

const SETTLE: Duration = Duration::from_millis(300);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_pool_holds_back_a_request_on_any_other_route() {
    let router = build_cluster_router(test_state(&[]), 2);
    let search = hold(&router, "POST", "/_search").await;
    let write = hold(&router, "PUT", "/_doc/7").await;

    let mut waiting = tokio::spawn(router.clone().oneshot(bare("GET", "/")));
    let ran = tokio::time::timeout(SETTLE, &mut waiting).await.is_ok();

    // Free the slots before asserting, so a failure does not leave requests in flight.
    search.release().await;
    let answered = tokio::time::timeout(Duration::from_secs(30), waiting).await;
    write.release().await;

    assert!(
        !ran,
        "a request ran on a third route while two others held a pool of two"
    );
    let response = answered
        .expect("the waiting request runs once a slot is free")
        .expect("request task")
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_probes_answer_while_the_pool_is_full() {
    let router = build_cluster_router(test_state(&[]), 1);
    let search = hold(&router, "POST", "/_search").await;

    let mut statuses = Vec::new();
    for path in ["/_health", "/_metrics"] {
        let answer =
            tokio::time::timeout(SETTLE * 10, router.clone().oneshot(bare("GET", path))).await;
        statuses.push((
            path,
            answer.map(|response| response.expect("router").status()),
        ));
    }
    search.release().await;

    for (path, status) in statuses {
        assert_eq!(
            status.ok(),
            Some(StatusCode::OK),
            "{path} waited for a slot"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_without_the_token_is_refused_without_waiting_for_a_slot() {
    let mut state = test_state(&[]);
    std::sync::Arc::get_mut(&mut state)
        .expect("a state nothing else holds yet")
        .auth = AuthConfig::resolve(
        Some("s3cret".into()),
        Err(std::env::VarError::NotPresent),
        false,
    )
    .expect("token");
    let router = build_cluster_router(state, 1);
    let search = hold(&router, "POST", "/_search").await;

    let refused =
        tokio::time::timeout(SETTLE * 10, router.clone().oneshot(bare("PUT", "/_doc/7"))).await;
    search.release().await;

    let response = refused
        .expect("the refusal does not wait for a slot")
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Document writes hold at most their share of the pool, and a write that waits for the
/// share holds no request slot. A compaction or backup keeps the engine lock for as long as
/// it takes and every write that arrives meanwhile waits in flight; searches need no lock
/// and must still find a slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_waiting_for_their_share_leave_the_other_slots_to_searches() {
    // A pool of four: one slot for writes, and three more.
    let router = build_cluster_router(test_state(&[]), 4);
    let write = hold(&router, "PUT", "/_doc/1").await;
    let mut queued: Vec<_> = (2..=6)
        .map(|id| send(&router, "PUT", &format!("/_doc/{id}")))
        .collect();
    let mut admitted_writes = 0;
    for write in &mut queued {
        if write.is_admitted_within(SETTLE / 3).await {
            admitted_writes += 1;
        }
    }

    // Three searches take the three slots the queued writes must not be holding.
    let mut searches = Vec::new();
    for _ in 0..3 {
        let mut search = send(&router, "POST", "/_search");
        let admitted = search.is_admitted_within(SETTLE * 10).await;
        searches.push((search, admitted));
    }

    // Free everything before asserting, so a failure does not leave requests in flight.
    let all_searches_ran = searches.iter().all(|(_, admitted)| *admitted);
    let searches = searches.into_iter().map(|(search, _)| search);
    release_all(searches.chain([write]).chain(queued)).await;

    assert_eq!(
        admitted_writes, 0,
        "a write ran beyond the write share of a pool of four"
    );
    assert!(
        all_searches_ran,
        "a search waited behind writes that were only waiting for their share"
    );
}
