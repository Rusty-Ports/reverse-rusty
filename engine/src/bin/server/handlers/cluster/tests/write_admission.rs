//! Cluster writes wait on blocking threads under bounded admission (ADR-183), share that
//! admission with each other, and leave it to whole-cluster operations when one needs it alone
//! (ADR-206).

use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use super::*;
use crate::state::MAX_QUEUED_CLUSTER_WRITES;

/// Which side of write admission a helper thread holds.
#[derive(Clone, Copy)]
enum Held {
    /// As an ordinary write in flight holds it.
    Shared,
    /// As a whole-cluster operation holds it: a checkpoint, a backup, a resize, a job.
    Alone,
}

/// Hold write admission on a helper thread until the returned sender is used or dropped. Tests
/// drop it before asserting: a panic that unwound past held admission would drop the runtime
/// first, and a runtime drop waits forever for the blocking writers queued behind it.
fn hold_admission(state: &Arc<ClusterAppState>, held: Held) -> (JoinHandle<()>, SyncSender<()>) {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = sync_channel(1);
    let (release_sender, release_receiver) = sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let (_shared, _alone);
        match held {
            Held::Shared => _shared = holder_state.write_admission.read(),
            Held::Alone => _alone = holder_state.write_admission.write(),
        }
        locked_sender.send(()).expect("signal held admission");
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("admission held");
    (holder, release_sender)
}

/// Hold write admission the way a whole-cluster operation does, so that writes wait.
fn hold_write_serial(state: &Arc<ClusterAppState>) -> (JoinHandle<()>, SyncSender<()>) {
    hold_admission(state, Held::Alone)
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

/// A write waiting for admission must wait on a blocking thread, never on an async worker: on
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

/// Two writes do not wait for each other. With one write in flight (its shared admission held
/// here), a second write to another id is applied; before ADR-206 it queued behind the first
/// for as long as the first took, including a full write deadline on a dead remote shard.
#[test]
fn a_write_does_not_wait_for_another_write() {
    let state = test_state(&seed());
    let (holder, release) = hold_admission(&state, Held::Shared);
    let run_state = Arc::clone(&state);
    let outcome = multi_thread_runtime().block_on(async move {
        let put = tokio::time::timeout(Duration::from_secs(5), spawn_put(&run_state, 30)).await;
        let delete = tokio::time::timeout(
            Duration::from_secs(5),
            send(&run_state, req_empty("DELETE", "/_doc/1")),
        )
        .await;
        let bulk = tokio::time::timeout(
            Duration::from_secs(5),
            send(
                &run_state,
                Request::post("/_bulk")
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from(
                        "{\"index\":{\"_id\":31}}\n{\"query\":\"1998 acme\"}\n",
                    ))
                    .expect("request"),
            ),
        )
        .await;
        // Release before leaving the runtime: a write that did wait is parked on a blocking
        // thread, and the runtime does not shut down until that thread is done.
        drop(release);
        (put, delete, bulk)
    });
    holder.join().expect("holder");
    let (put, delete, bulk) = outcome;
    let (status, body) = put
        .expect("a write must not wait for another write")
        .expect("put task");
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = delete.expect("a delete must not wait for another write");
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = bulk.expect("a bulk batch must not wait for another write");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false, "{body}");
}

/// A write still waits for an operation that needs the corpus still, and is applied once that
/// operation is done.
#[test]
fn a_write_waits_for_an_operation_that_holds_admission_alone() {
    let state = test_state(&seed());
    let (holder, release) = hold_admission(&state, Held::Alone);
    let run_state = Arc::clone(&state);
    let (early, late) = multi_thread_runtime().block_on(async move {
        let mut put = spawn_put(&run_state, 40);
        let early = tokio::time::timeout(Duration::from_millis(200), &mut put)
            .await
            .is_ok();
        drop(release);
        let late = tokio::time::timeout(Duration::from_secs(10), put).await;
        (early, late)
    });
    holder.join().expect("holder");
    assert!(
        !early,
        "the write ran beside an operation that needs it out"
    );
    let (status, body) = late
        .expect("the write runs once admission is free")
        .expect("put");
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

/// The three surfaces that return sources or an explanation by default or on request.
fn searches_with_sources() -> [(&'static str, serde_json::Value); 3] {
    [
        (
            "/v2/_search",
            serde_json::json!({ "document": { "title": "1994 acme" } }),
        ),
        (
            "/v2/_mpercolate",
            serde_json::json!({ "documents": [{ "title": "1994 acme" }] }),
        ),
        (
            "/_search",
            serde_json::json!({
                "document": { "title": "1994 acme" },
                "include_broad": true,
                "_source": true
            }),
        ),
    ]
}

/// A search that returns sources takes no write admission (ADR-210). What it has to wait
/// for, a write that is being applied or the moment a rebuild swaps its layout in, it waits
/// for at the cluster's frozen view. So it answers while admission is held: shared, as by a
/// write or a bulk batch in flight (before ADR-206 it queued behind those), and alone, as by
/// a vocabulary change or a resize for its whole run.
#[test]
fn a_search_with_sources_does_not_wait_for_write_admission() {
    for held in [Held::Shared, Held::Alone] {
        let state = test_state(&seed());
        let (holder, release) = hold_admission(&state, held);
        let run_state = Arc::clone(&state);
        let answers = multi_thread_runtime().block_on(async move {
            let mut answers = Vec::new();
            for (path, body) in searches_with_sources() {
                let answer = tokio::time::timeout(
                    Duration::from_secs(5),
                    send(&run_state, req("POST", path, &body)),
                )
                .await;
                answers.push((path, answer));
            }
            // Release before leaving the runtime, as above.
            drop(release);
            answers
        });
        holder.join().expect("holder");
        let how = match held {
            Held::Shared => "shared",
            Held::Alone => "alone",
        };
        for (path, answer) in answers {
            let (status, body) =
                answer.unwrap_or_else(|_| panic!("{path} waited for write admission (held {how})"));
            assert_eq!(status, StatusCode::OK, "{path}: {body}");
        }
    }
}

type MakeRequest = fn() -> Request<Body>;

/// Every operation that needs the corpus still waits for a write in flight, and runs once the
/// write is done. A write shares admission; each of these takes it alone.
#[test]
fn an_operation_that_needs_the_corpus_still_waits_for_a_write_in_flight() {
    let operations: [(&str, MakeRequest); 6] = [
        ("flush", || req_empty("POST", "/_flush")),
        ("checkpoint", || req_empty("POST", "/_checkpoint")),
        ("replace the vocabulary", || {
            req("PUT", "/_vocab", &serde_json::json!({}))
        }),
        ("learn and apply a vocabulary", || {
            req_empty("POST", "/_vocab/learn_and_apply?min_count=2")
        }),
        ("import aliases", || {
            req(
                "POST",
                "/_vocab/aliases/import",
                &serde_json::json!({
                    "synonyms_set": [{ "id": "zz-rule", "synonyms": "zzcouch, zzsofa" }]
                }),
            )
        }),
        ("learn and apply aliases", || {
            req_empty("POST", "/_vocab/aliases/learn_and_apply?min_count=2")
        }),
    ];
    for (name, request) in operations {
        let state = test_state(&seed());
        let (holder, release) = hold_admission(&state, Held::Shared);
        let run_state = Arc::clone(&state);
        let (early, late) = multi_thread_runtime().block_on(async move {
            let task_state = Arc::clone(&run_state);
            let mut operation = tokio::spawn(async move { send_raw(&task_state, request()).await });
            let early = tokio::time::timeout(Duration::from_millis(200), &mut operation).await;
            drop(release);
            match early {
                Ok(answer) => (true, Ok(answer)),
                Err(_) => (
                    false,
                    tokio::time::timeout(Duration::from_secs(20), operation).await,
                ),
            }
        });
        holder.join().expect("holder");
        let (status, _, bytes) = late
            .unwrap_or_else(|_| panic!("{name} never ran"))
            .expect("operation task");
        assert!(
            !early,
            "{name} ran beside a write in flight: {status} {}",
            String::from_utf8_lossy(&bytes)
        );
        assert!(
            status.is_success(),
            "{name}: {status} {}",
            String::from_utf8_lossy(&bytes)
        );
    }
}
