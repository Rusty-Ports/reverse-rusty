//! Brief cluster-lock reads wait on blocking threads under bounded admission (ADR-191).

use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use super::*;
use crate::state::MAX_QUEUED_CLUSTER_READS;

/// Hold the exclusive cluster lock on a helper thread until the returned sender is used or
/// dropped, the way a vocabulary rebuild or an in-process resize does. Tests drop it before
/// asserting, so a failed assertion cannot leave readers parked behind it.
fn hold_cluster_exclusively(state: &Arc<ClusterAppState>) -> (JoinHandle<()>, SyncSender<()>) {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = sync_channel(1);
    let (release_sender, release_receiver) = sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _cluster = holder_state.cluster.write();
        locked_sender.send(()).expect("signal the held cluster");
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("cluster held");
    (holder, release_sender)
}

/// Every data-plane route that only needs a brief read of the cluster must wait for it on a
/// blocking thread. With the exclusive lock held and the request queued, a timer still fires on
/// a single-threaded runtime; a request that took the lock on the async worker would stop it.
#[test]
fn a_read_waiting_for_the_cluster_lock_never_parks_the_async_runtime() {
    type Case = (&'static str, fn() -> Request<Body>, StatusCode);
    let cases: [Case; 6] = [
        ("GET /_doc", || req_empty("GET", "/_doc/1"), StatusCode::OK),
        (
            "HEAD /_doc",
            || req_empty("HEAD", "/_doc/1"),
            StatusCode::OK,
        ),
        ("GET /", || req_empty("GET", "/"), StatusCode::OK),
        (
            "POST /v2/_search",
            || {
                req(
                    "POST",
                    "/v2/_search",
                    &serde_json::json!({"document": {"title": "1994 acme"}}),
                )
            },
            StatusCode::OK,
        ),
        (
            "POST /v2/_mpercolate",
            || {
                req(
                    "POST",
                    "/v2/_mpercolate",
                    &serde_json::json!({"documents": [{"title": "1994 acme"}]}),
                )
            },
            StatusCode::OK,
        ),
        (
            "POST /_percolate/jobs",
            || {
                req(
                    "POST",
                    "/_percolate/jobs",
                    &serde_json::json!({
                        "document": {"title": "1994 acme"},
                        "rank": {"boosts": [{"key": "tier", "value": "gold", "boost": 10}]}
                    }),
                )
            },
            StatusCode::ACCEPTED,
        ),
    ];
    for (route, request, expected) in cases {
        let state = test_state(&seed());
        let (holder, release) = hold_cluster_exclusively(&state);
        let (alive_sender, alive_receiver) = std::sync::mpsc::channel();
        let server_state = Arc::clone(&state);
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let read_state = Arc::clone(&server_state);
                let read = tokio::spawn(async move { send(&read_state, request()).await });
                // Let the request start and queue behind the held lock.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                alive_sender.send(()).expect("report a live runtime");
                read.await.expect("read task")
            })
        });
        let alive = alive_receiver.recv_timeout(Duration::from_secs(10));
        drop(release);
        holder.join().expect("holder");
        let (status, body) = server.join().expect("server");
        assert!(
            alive.is_ok(),
            "{route}: the runtime must keep running while the request waits for the cluster lock"
        );
        assert_eq!(status, expected, "{route}: {body}");
    }
}

/// A read whose client disconnects keeps its admission permit until its blocking worker
/// finishes, so the threads parked behind one long exclusive holder stay bounded.
#[test]
fn a_cancelled_read_keeps_its_admission_until_the_worker_finishes() {
    let admitted = MAX_QUEUED_CLUSTER_READS;
    let requested = admitted + 4;
    let state = test_state(&seed());
    let (holder, release) = hold_cluster_exclusively(&state);
    let run_state = Arc::clone(&state);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let (held_after_cancel, restored) = runtime.block_on(async move {
        let wait_until = |target: usize, state: Arc<ClusterAppState>| async move {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while state.read_permits.available_permits() != target
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let reads: Vec<_> = (0..requested)
            .map(|_| {
                let state = Arc::clone(&run_state);
                tokio::spawn(async move { send(&state, req_empty("GET", "/_doc/1")).await })
            })
            .collect();
        wait_until(0, Arc::clone(&run_state)).await;
        for read in &reads {
            read.abort();
        }
        // Not awaited yet: a handler that parked a worker cannot be cancelled, and waiting
        // for it here would hang the test instead of failing it.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !reads.iter().all(tokio::task::JoinHandle::is_finished)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let held_after_cancel = admitted - run_state.read_permits.available_permits();
        drop(release);
        for read in reads {
            let _ = read.await;
        }
        wait_until(admitted, Arc::clone(&run_state)).await;
        (
            held_after_cancel,
            run_state.read_permits.available_permits(),
        )
    });
    holder.join().expect("holder");
    assert_eq!(
        held_after_cancel, admitted,
        "cancelled reads must keep their admission while their workers are parked"
    );
    assert_eq!(
        restored, admitted,
        "every permit returns once the lock frees"
    );
}

/// A ranked request's timeout covers its compile step. That step waits for read admission, a
/// blocking thread and the cluster lock; while the exclusive lock is held, the request must
/// answer 408 at its deadline instead of waiting for the rebuild to finish.
#[test]
fn a_ranked_request_times_out_while_its_compile_waits_for_the_cluster_lock() {
    type Case = (&'static str, fn() -> Request<Body>);
    let cases: [Case; 2] = [
        ("POST /v2/_search", || {
            req(
                "POST",
                "/v2/_search",
                &serde_json::json!({"document": {"title": "1994 acme"}, "timeout_ms": 20}),
            )
        }),
        ("POST /v2/_mpercolate", || {
            req(
                "POST",
                "/v2/_mpercolate",
                &serde_json::json!({"documents": [{"title": "1994 acme"}], "timeout_ms": 20}),
            )
        }),
    ];
    for (route, request) in cases {
        let state = test_state(&seed());
        let (holder, release) = hold_cluster_exclusively(&state);
        let run_state = Arc::clone(&state);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let answer = runtime.block_on(async move {
            tokio::time::timeout(Duration::from_secs(5), send(&run_state, request())).await
        });
        drop(release);
        holder.join().expect("holder");
        drop(runtime);
        let (status, body) =
            answer.unwrap_or_else(|_| panic!("{route}: the request ignored its timeout"));
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{route}: {body}");
    }
}

fn ranked_outcomes(state: &ClusterAppState, outcome: &str) -> u64 {
    state
        .prom
        .ranked_requests_total
        .with_label_values(&[outcome, "standard"])
        .get()
}

/// A request that timed out is counted once, as a timeout. Its compile worker is still
/// queued when the request answers; when the lock frees, the worker finds the rank program
/// invalid, and that result has no request left to belong to.
#[test]
fn a_timed_out_ranked_request_is_counted_once() {
    let state = test_state(&seed());
    let (holder, release) = hold_cluster_exclusively(&state);
    let run_state = Arc::clone(&state);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let (answer, after_release) = runtime.block_on(async move {
        let request = req(
            "POST",
            "/v2/_search?query_scope=standard",
            &serde_json::json!({
                "document": {"title": "1994 acme"},
                "rank": {"profile": "no-such-profile"},
                "timeout_ms": 20
            }),
        );
        let answer = tokio::time::timeout(Duration::from_secs(5), send(&run_state, request)).await;
        drop(release);
        // The detached worker now runs to completion and frees its read permit.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while run_state.read_permits.available_permits() != MAX_QUEUED_CLUSTER_READS
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        (
            answer,
            (
                ranked_outcomes(&run_state, "timeout"),
                ranked_outcomes(&run_state, "validation"),
                run_state.read_permits.available_permits(),
            ),
        )
    });
    holder.join().expect("holder");
    drop(runtime);
    let (status, body) = answer.expect("the request answers at its deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{body}");
    let (timeouts, validations, permits) = after_release;
    assert_eq!(permits, MAX_QUEUED_CLUSTER_READS, "the worker finished");
    assert_eq!(
        (timeouts, validations),
        (1, 0),
        "one request, one outcome: the late worker must not add a second"
    );
}
