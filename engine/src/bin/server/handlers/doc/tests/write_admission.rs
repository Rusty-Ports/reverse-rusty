//! Standalone writes wait on blocking threads under bounded admission (ADR-191).

use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use super::*;
use crate::state::MAX_QUEUED_WRITES;

/// Hold the engine mutex on a helper thread until the returned sender is used or dropped, the
/// way a compaction, backup or vocabulary rebuild does. Tests drop it before asserting: a panic
/// that unwound past a held mutex would drop the runtime first, and a runtime drop waits forever
/// for the blocking writers queued behind it.
fn hold_engine(state: &Arc<AppState>) -> (JoinHandle<()>, SyncSender<()>) {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = sync_channel(1);
    let (release_sender, release_receiver) = sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _engine = holder_state.engine.lock();
        locked_sender.send(()).expect("signal the held engine");
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("engine held");
    (holder, release_sender)
}

fn write_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_doc/{id}", get(get_doc).put(put_doc).delete(delete_doc))
        .route("/_bulk", axum::routing::post(bulk_route))
        .with_state(Arc::clone(state))
}

async fn send(state: &Arc<AppState>, request: Request<Body>) -> StatusCode {
    write_router(state)
        .oneshot(request)
        .await
        .expect("router response")
        .status()
}

fn put(id: u64) -> Request<Body> {
    put_request(
        &format!("/_doc/{id}"),
        &serde_json::json!({"query": "acme chrome"}),
    )
}

fn delete(id: u64) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(format!("/_doc/{id}"))
        .body(Body::empty())
        .expect("DELETE request")
}

fn bulk(id: u64) -> Request<Body> {
    Request::post("/_bulk")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(format!(
            "{{\"index\":{{\"_id\":\"{id}\"}}}}\n{{\"query\":\"acme chrome\"}}\n"
        )))
        .expect("bulk request")
}

fn read(id: u64) -> Request<Body> {
    Request::get(format!("/_doc/{id}"))
        .body(Body::empty())
        .expect("GET request")
}

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

async fn wait_until(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A seeded state: document 1 is live, so a read has something to find and a delete something
/// to remove.
fn seeded() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("1994 acme", 1, 1).expect("seed");
    state_with_engine(engine)
}

/// A write waiting on the engine mutex must wait on a blocking thread, never on an async worker.
/// On a single-threaded runtime a parked worker stalls every other request, so a lock-free read
/// has to be served while the write is still queued behind a long holder.
#[test]
fn a_queued_write_never_parks_the_async_runtime() {
    type Case = (&'static str, fn() -> Request<Body>, StatusCode);
    let cases: [Case; 3] = [
        ("PUT /_doc", || put(20), StatusCode::CREATED),
        ("DELETE /_doc", || delete(1), StatusCode::OK),
        ("POST /_bulk", || bulk(21), StatusCode::OK),
    ];
    for (route, request, expected) in cases {
        let state = seeded();
        let (holder, release) = hold_engine(&state);
        let (read_sender, read_receiver) = std::sync::mpsc::channel();
        let server_state = Arc::clone(&state);
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let write_state = Arc::clone(&server_state);
                let write = tokio::spawn(async move { send(&write_state, request()).await });
                // Let the write start and queue behind the held engine.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                let status = send(&server_state, read(1)).await;
                read_sender.send(status).expect("report the read");
                write.await.expect("write task")
            })
        });
        let read_status = read_receiver.recv_timeout(Duration::from_secs(10));
        drop(release);
        holder.join().expect("holder");
        let write_status = server.join().expect("server");
        assert_eq!(
            read_status.unwrap_or_else(|_| panic!(
                "{route}: a read must be served while the write waits for the engine"
            )),
            StatusCode::OK,
            "{route}"
        );
        assert_eq!(write_status, expected, "{route}");
    }
}

/// A write whose client disconnects keeps its admission permit until its blocking worker
/// finishes, and a write cancelled before admission never runs. Otherwise every disconnect
/// would free a request slot while leaving a detached writer behind, and queued writers would
/// grow without bound.
#[test]
fn a_cancelled_write_keeps_its_admission_until_the_worker_finishes() {
    type Case = (&'static str, fn(u64) -> Request<Body>);
    let cases: [Case; 2] = [("PUT /_doc", put), ("POST /_bulk", bulk)];
    for (route, request) in cases {
        let admitted = MAX_QUEUED_WRITES;
        let requested = admitted + 4;
        let state = seeded();
        let (holder, release) = hold_engine(&state);
        let run_state = Arc::clone(&state);
        let (held_after_cancel, applied) = multi_thread_runtime().block_on(async move {
            let writes: Vec<_> = (0..requested)
                .map(|index| {
                    let state = Arc::clone(&run_state);
                    tokio::spawn(async move { send(&state, request(100 + index as u64)).await })
                })
                .collect();
            let permits = &run_state.write_permits;
            wait_until(|| permits.available_permits() == 0).await;
            for write in &writes {
                write.abort();
            }
            // Not awaited yet: a handler that parked a worker cannot be cancelled, and
            // waiting for it here would hang the test instead of failing it.
            wait_until(|| writes.iter().all(tokio::task::JoinHandle::is_finished)).await;
            let held_after_cancel = admitted - permits.available_permits();
            drop(release);
            for write in writes {
                let _ = write.await;
            }
            wait_until(|| permits.available_permits() == admitted).await;
            let mut applied = 0;
            for index in 0..requested {
                applied +=
                    usize::from(send(&run_state, read(100 + index as u64)).await == StatusCode::OK);
            }
            (held_after_cancel, applied)
        });
        holder.join().expect("holder");
        assert_eq!(
            held_after_cancel, admitted,
            "{route}: cancelled writes must keep their admission while their workers are queued"
        );
        assert_eq!(
            applied, admitted,
            "{route}: exactly the admitted writes run; the rest were cancelled before starting"
        );
    }
}

/// A write whose request is dropped after admission still completes and is published: the
/// worker publishes the snapshot, not the request that may no longer exist.
#[test]
fn a_write_whose_request_is_dropped_is_still_published() {
    let state = seeded();
    let (holder, release) = hold_engine(&state);
    let run_state = Arc::clone(&state);
    let published = multi_thread_runtime().block_on(async move {
        let write_state = Arc::clone(&run_state);
        let write = tokio::spawn(async move { send(&write_state, put(40)).await });
        let permits = &run_state.write_permits;
        wait_until(|| permits.available_permits() < MAX_QUEUED_WRITES).await;
        write.abort();
        let _ = write.await;
        drop(release);
        wait_until(|| matches_in_snapshot(&run_state, "acme chrome").contains(&40)).await;
        matches_in_snapshot(&run_state, "acme chrome")
    });
    holder.join().expect("holder");
    assert!(
        published.contains(&40),
        "the write was admitted, so it lands and is visible to lock-free reads: {published:?}"
    );
}

/// Shutdown joins a write that outlived its request before its final flush, then keeps write
/// admission closed, so no late write lands after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_quiescence_joins_detached_writes() {
    let state = seeded();
    let detached = crate::state::admit_write(&state)
        .await
        .expect("admitted write");
    let mut shutdown = Box::pin(crate::state::quiesce_writes(&state));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for an admitted write to finish"
    );
    drop(detached);
    let guard = tokio::time::timeout(Duration::from_secs(1), &mut shutdown)
        .await
        .expect("shutdown quiescence completed")
        .expect("write admission stayed open");
    assert_eq!(
        state.write_permits.available_permits(),
        0,
        "shutdown must retain write admission through its final flush"
    );
    drop(guard);
    assert_eq!(state.write_permits.available_permits(), MAX_QUEUED_WRITES);
}
