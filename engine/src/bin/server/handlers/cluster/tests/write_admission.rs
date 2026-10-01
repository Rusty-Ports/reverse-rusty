//! Cluster writes wait on blocking threads under bounded admission (ADR-183).

use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use super::*;
use crate::state::MAX_QUEUED_CLUSTER_WRITES;

/// Hold `write_serial` on a helper thread until the returned sender is used or dropped. Tests drop
/// it before asserting: a panic that unwound past a held serializer would drop the runtime first,
/// and a runtime drop waits forever for the blocking writers queued behind it.
fn hold_write_serial(state: &Arc<ClusterAppState>) -> (JoinHandle<()>, SyncSender<()>) {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = sync_channel(1);
    let (release_sender, release_receiver) = sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _writes = holder_state.write_serial.lock();
        locked_sender.send(()).expect("signal the held serializer");
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("serializer held");
    (holder, release_sender)
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

fn spawn_put(
    state: &Arc<ClusterAppState>,
    id: usize,
) -> tokio::task::JoinHandle<(StatusCode, serde_json::Value)> {
    let state = Arc::clone(state);
    tokio::spawn(async move {
        let body = serde_json::json!({"query": "1997 acme"});
        send(&state, req("PUT", &format!("/_doc/{id}"), &body)).await
    })
}

/// A write waiting on `write_serial` must wait on a blocking thread, never on an async worker: on
/// a single-threaded runtime a parked worker stalls every other request (and, with remote shards,
/// the RPCs the serializer's holder needs to finish).
#[test]
fn a_queued_write_never_parks_the_async_runtime() {
    let state = test_state(&seed());
    let (holder, release) = hold_write_serial(&state);
    let (read_sender, read_receiver) = std::sync::mpsc::channel();
    let server_state = Arc::clone(&state);
    let server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let put = spawn_put(&server_state, 20);
            // Let the PUT start and queue behind the held serializer.
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            let (status, _) = send(&server_state, req_empty("GET", "/")).await;
            read_sender.send(status).expect("report the read");
            put.await.expect("put task")
        })
    });
    let read = read_receiver.recv_timeout(Duration::from_secs(10));
    drop(release);
    holder.join().expect("holder");
    let (put_status, body) = server.join().expect("server");
    assert_eq!(
        read.expect("a read must be served while a write waits on the serializer"),
        StatusCode::OK
    );
    assert_eq!(put_status, StatusCode::CREATED, "{body}");
}

/// A write whose client disconnects keeps its admission permit until its blocking worker finishes,
/// and a write cancelled before admission never runs. Otherwise every disconnect would free a
/// request slot while leaving a detached writer behind, and queued writers would grow unbounded.
#[test]
fn a_cancelled_write_keeps_its_admission_until_the_worker_finishes() {
    let admitted = MAX_QUEUED_CLUSTER_WRITES;
    let requested = admitted + 4;
    let state = test_state(&seed());
    let (holder, release) = hold_write_serial(&state);
    let run_state = Arc::clone(&state);
    let (held_after_cancel, applied) = multi_thread_runtime().block_on(async move {
        let writes: Vec<_> = (0..requested)
            .map(|index| spawn_put(&run_state, 100 + index))
            .collect();
        let permits = &run_state.write_permits;
        wait_until(|| permits.available_permits() == 0).await;
        for write in &writes {
            write.abort();
        }
        for write in writes {
            let _ = write.await;
        }
        let held_after_cancel = admitted - permits.available_permits();
        drop(release);
        wait_until(|| permits.available_permits() == admitted).await;
        let mut applied = 0;
        for index in 0..requested {
            let get = req_empty("GET", &format!("/_doc/{}", 100 + index));
            applied += usize::from(send(&run_state, get).await.0 == StatusCode::OK);
        }
        (held_after_cancel, applied)
    });
    holder.join().expect("holder");
    assert_eq!(
        held_after_cancel, admitted,
        "cancelled writes must keep their admission while their workers are queued"
    );
    assert_eq!(
        applied, admitted,
        "exactly the admitted writes run; the rest were cancelled before starting"
    );
}

/// Shutdown joins a write that outlived its request before durability cleanup, then keeps write
/// admission closed, so no late write lands after the shutdown checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_quiescence_joins_detached_writes() {
    let state = test_state(&seed());
    let detached = super::super::admit_cluster_write(&state)
        .await
        .expect("admitted write");
    let mut shutdown = Box::pin(crate::cluster_mode::shutdown::quiesce_detached_work(&state));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for an admitted write to finish"
    );
    drop(detached);
    let guards = tokio::time::timeout(Duration::from_secs(1), &mut shutdown)
        .await
        .expect("shutdown quiescence completed");
    assert_eq!(
        state.write_permits.available_permits(),
        0,
        "shutdown must retain write admission through durability cleanup"
    );
    drop(guards);
    assert_eq!(
        state.write_permits.available_permits(),
        MAX_QUEUED_CLUSTER_WRITES
    );
}

/// `wait_if_ongoing=false` reports a flush already in progress even when that flush and queued
/// writes hold every admission permit, instead of queueing behind them and acknowledging later.
#[test]
fn a_non_waiting_flush_reports_an_ongoing_flush_without_queueing() {
    let state = test_state(&seed());
    let (holder, release) = hold_write_serial(&state);
    let run_state = Arc::clone(&state);
    let probe = multi_thread_runtime().block_on(async move {
        let flush_state = Arc::clone(&run_state);
        let mut queued = vec![tokio::spawn(async move {
            send(&flush_state, req_empty("POST", "/_flush")).await
        })];
        wait_until(|| run_state.flush_serial.is_locked()).await;
        queued
            .extend((1..MAX_QUEUED_CLUSTER_WRITES).map(|index| spawn_put(&run_state, 200 + index)));
        wait_until(|| run_state.write_permits.available_permits() == 0).await;
        let probe = tokio::time::timeout(
            Duration::from_secs(2),
            send(
                &run_state,
                req_empty("POST", "/_flush?wait_if_ongoing=false"),
            ),
        )
        .await;
        drop(release);
        for task in queued {
            let _ = task.await;
        }
        probe
    });
    holder.join().expect("holder");
    let (status, body) = probe.expect("a non-waiting flush must not queue behind write admission");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["type"], "flush_in_progress_exception",
        "{body}"
    );
}
