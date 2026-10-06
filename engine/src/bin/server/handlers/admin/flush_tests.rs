use super::flush_route;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{header, Method, Request, StatusCode};
use axum::routing::any;
use axum::Router;
use parking_lot::Mutex;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use reverse_rusty::Normalizer;
use std::sync::Arc;
use tower::ServiceExt;

fn state_with_engine(engine: Engine) -> Arc<AppState> {
    let snapshot = Arc::new(engine.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    Arc::new(AppState {
        engine: Mutex::new(engine),
        flush_serial: Mutex::new(()),
        write_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_QUEUED_WRITES,
        )),
        backup_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_BACKUPS,
        )),
        health_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        snapshot: ArcSwap::new(snapshot),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        rank_profiles: Arc::new(reverse_rusty::RankProfiles::default()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: false,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn state_with_memtable() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("1994 acme", 7, 1).expect("insert");
    state_with_engine(engine)
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route("/_flush", any(flush_route))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let response = router(state, 64 * 1024)
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(body.into())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, headers, body)
}

#[tokio::test]
async fn get_and_post_flush_are_es_familiar_and_idempotent() {
    let state = state_with_memtable();
    let (status, _, body) = send(
        &state,
        Method::GET,
        "/_flush?force=true&wait_if_ongoing=true",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 1, "failed": 0})
    );
    assert_eq!(body["total_queries"], 1);
    assert_eq!(body["base_segments"], 1);
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 0);

    let (status, _, body) = send(
        &state,
        Method::POST,
        "/_flush?force=false&wait_if_ongoing=false",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["base_segments"], 1, "a clean flush is a no-op");
}

#[tokio::test]
async fn transport_controls_are_strict_and_precede_mutation() {
    let state = state_with_memtable();
    for uri in [
        "/_flush?routing=one",
        "/_flush?force=maybe",
        "/_flush?wait_if_ongoing=true&wait_if_ongoing=false",
    ] {
        let (status, _, body) = send(&state, Method::POST, uri, Body::empty()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
        assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
    }

    let (status, _, body) = send(&state, Method::POST, "/_flush", "{}").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);

    let (status, headers, body) = send(&state, Method::PUT, "/_flush", Body::empty()).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, POST");
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);

    let response = router(&state, 4)
        .oneshot(
            Request::post("/_flush")
                .body(Body::from("12345"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
}

#[tokio::test]
async fn nonwaiting_flush_conflicts_only_with_an_explicit_flush() {
    let state = state_with_memtable();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_state = Arc::clone(&state);
    let holder = std::thread::spawn(move || {
        let _held = lock_state.flush_serial.lock();
        locked_tx.send(()).expect("report held lock");
        release_rx.recv().expect("release held lock");
    });
    locked_rx.recv().expect("wait for held lock");
    let (status, _, body) = send(
        &state,
        Method::GET,
        "/_flush?wait_if_ongoing=false",
        Body::empty(),
    )
    .await;
    release_tx.send(()).expect("release held lock");
    holder.join().expect("lock holder");

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["type"], "flush_in_progress_exception",
        "{body}"
    );
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn durable_failure_is_a_failed_shard_and_never_acknowledged() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("rr-flush-api-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
    engine.try_insert_live("1994 acme", 7, 1).expect("insert");
    let state = state_with_engine(engine);

    let segments = dir.join("segments");
    let original = std::fs::metadata(&segments)
        .expect("segments")
        .permissions();
    std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(0o555))
        .expect("make segments read-only");
    let (status, _, body) = send(&state, Method::POST, "/_flush", Body::empty()).await;
    std::fs::set_permissions(&segments, original).expect("restore permissions");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["acknowledged"], false);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 0, "failed": 1})
    );
    assert!(
        state.snapshot.load().has_live_query(7),
        "the in-memory fallback remains readable"
    );

    drop(state);
    let _ = std::fs::remove_dir_all(dir);
}

/// A lock a flush waits on.
#[derive(Clone, Copy)]
enum Held {
    /// What a compaction, backup or vocabulary rebuild holds.
    Engine,
    /// What another explicit flush holds.
    FlushSerial,
}

/// Hold `lock` on a helper thread until the returned sender is used or dropped.
fn hold(
    state: &Arc<AppState>,
    lock: Held,
) -> (std::thread::JoinHandle<()>, std::sync::mpsc::SyncSender<()>) {
    let holder_state = Arc::clone(state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let (_engine, _flush) = match lock {
            Held::Engine => (Some(holder_state.engine.lock()), None),
            Held::FlushSerial => (None, Some(holder_state.flush_serial.lock())),
        };
        locked_sender.send(()).expect("signal the held lock");
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("lock held");
    (holder, release_sender)
}

/// A flush waits for the engine mutex (behind a compaction or backup) and, with
/// `wait_if_ongoing`, for another flush. Both waits happen on a blocking thread: on a
/// single-threaded runtime a timer still fires while the flush is queued (ADR-191).
#[test]
fn a_queued_flush_never_parks_the_async_runtime() {
    let cases = [
        ("the engine mutex", Held::Engine),
        ("another flush", Held::FlushSerial),
    ];
    for (held, lock) in cases {
        let state = state_with_memtable();
        let (holder, release) = hold(&state, lock);
        let (alive_sender, alive_receiver) = std::sync::mpsc::channel();
        let server_state = Arc::clone(&state);
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let flush_state = Arc::clone(&server_state);
                let flush = tokio::spawn(async move {
                    send(&flush_state, Method::POST, "/_flush", Body::empty()).await
                });
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                alive_sender.send(()).expect("report a live runtime");
                flush.await.expect("flush task")
            })
        });
        let alive = alive_receiver.recv_timeout(std::time::Duration::from_secs(10));
        drop(release);
        holder.join().expect("holder");
        let (status, _, body) = server.join().expect("server");
        assert!(
            alive.is_ok(),
            "the runtime must keep running while a flush waits for {held}"
        );
        assert_eq!(status, StatusCode::OK, "waiting for {held}: {body}");
        assert_eq!(state.snapshot.load().metrics().memtable_entries, 0);
    }
}

/// `wait_if_ongoing=false` reports a flush already in progress even when that flush and queued
/// writes hold every admission permit, instead of queueing behind them and acknowledging later.
#[test]
fn a_non_waiting_flush_reports_an_ongoing_flush_without_queueing() {
    use crate::state::MAX_QUEUED_WRITES;
    let state = state_with_memtable();
    let (holder, release) = hold(&state, Held::Engine);
    let run_state = Arc::clone(&state);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let probe = runtime.block_on(async move {
        let wait_until = |done: fn(&AppState) -> bool, state: Arc<AppState>| async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !done(&state) && std::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        };
        let flush_state = Arc::clone(&run_state);
        let queued = tokio::spawn(async move {
            send(&flush_state, Method::POST, "/_flush", Body::empty()).await
        });
        wait_until(
            |state| state.flush_serial.is_locked(),
            Arc::clone(&run_state),
        )
        .await;
        // Queued writers take every remaining permit.
        let mut writers = Vec::new();
        for _ in 1..MAX_QUEUED_WRITES {
            writers.push(
                crate::state::admit_write(&run_state)
                    .await
                    .expect("admitted writer"),
            );
        }
        let probe = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            send(
                &run_state,
                Method::POST,
                "/_flush?wait_if_ongoing=false",
                Body::empty(),
            ),
        )
        .await;
        drop(release);
        drop(writers);
        let _ = queued.await;
        probe
    });
    holder.join().expect("holder");
    let (status, _, body) =
        probe.expect("a non-waiting flush must not queue behind write admission");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["type"], "flush_in_progress_exception",
        "{body}"
    );
}
