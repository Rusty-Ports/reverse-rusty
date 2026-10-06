//! Request admission on the real coordinator router (ADR-199). The single-node twin is
//! `crate::router::tests`; both use one admission function.

use std::time::Duration;

use axum::http::StatusCode;
use tower::ServiceExt;

use super::test_state;
use crate::auth::AuthConfig;
use crate::cluster_mode::router::build_cluster_router;
use crate::router::held::{bare, hold, release_all, send};
use crate::router::RequestPools;

/// Long enough for a request that is free to run to finish, by a wide margin.
const SETTLE: Duration = Duration::from_millis(300);

/// A pool is one pool for every route of its class. Two reads on two routes fill a read
/// pool of two, and a read on a third route waits for one of them. With a pool per route,
/// which is what `ConcurrencyLimitLayer` under `Router::layer` gave, the third ran at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_read_pool_holds_back_a_read_on_any_other_route() {
    let router = build_cluster_router(test_state(&[]), RequestPools::sized(2));
    let search = hold(&router, "POST", "/_search").await;
    let percolate = hold(&router, "POST", "/_mpercolate").await;

    let mut waiting = tokio::spawn(router.clone().oneshot(bare("GET", "/")));
    let ran = tokio::time::timeout(SETTLE, &mut waiting).await.is_ok();

    // Free the slots before asserting, so a failure does not leave requests in flight.
    search.release().await;
    let answered = tokio::time::timeout(Duration::from_secs(30), waiting).await;
    percolate.release().await;

    assert!(
        !ran,
        "a read ran on a third route while two others held a read pool of two"
    );
    let response = answered
        .expect("the waiting request runs once a slot is free")
        .expect("request task")
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
}

/// The probes take no request slot: an orchestrator and a scraper get their answer from a
/// server whose read pool is full.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_probes_answer_while_the_read_pool_is_full() {
    let router = build_cluster_router(test_state(&[]), RequestPools::sized(1));
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

/// Auth is outside admission: a request without the token is refused at once, also when
/// the pool it would use is full, so a flood of them cannot queue in front of real traffic.
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
    let router = build_cluster_router(state, RequestPools::sized(1));
    // An open read fills the read pool; `/v2/_mpercolate` is a read that needs the token.
    let search = hold(&router, "POST", "/_search").await;

    let refused = tokio::time::timeout(
        SETTLE * 10,
        router.clone().oneshot(bare("POST", "/v2/_mpercolate")),
    )
    .await;
    search.release().await;

    let response = refused
        .expect("the refusal does not wait for a slot")
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Document writes have a pool of their own. A compaction or backup keeps the engine lock
/// for as long as it takes and every write that arrives meanwhile waits in flight; those
/// waiting writes hold no slot a search could use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_waiting_for_their_pool_take_no_slot_from_searches() {
    // Four read slots, one write slot.
    let router = build_cluster_router(test_state(&[]), RequestPools::sized(4));
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

    let mut searches = Vec::new();
    for _ in 0..4 {
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
        "a second write ran in a write pool of one"
    );
    assert!(
        all_searches_ran,
        "a search waited behind writes that were only waiting for their own pool"
    );
}

/// The other direction: a full read pool holds back neither a write nor an administrative
/// request, so an operator can still act on a server that is saturated with searches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_read_pool_holds_back_neither_writes_nor_administration() {
    let router = build_cluster_router(test_state(&[]), RequestPools::sized(1));
    let search = hold(&router, "POST", "/_search").await;

    let mut write = send(&router, "PUT", "/_doc/1");
    let write_ran = write.is_admitted_within(SETTLE * 10).await;
    let stats =
        tokio::time::timeout(SETTLE * 10, router.clone().oneshot(bare("GET", "/_stats"))).await;
    release_all([search, write]).await;

    assert!(write_ran, "a write waited for a read slot");
    let stats = stats
        .expect("an administrative read does not wait for a read slot")
        .expect("router response");
    assert_eq!(stats.status(), StatusCode::OK);
}
