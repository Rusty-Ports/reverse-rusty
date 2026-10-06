//! A search that returns sources needs the cluster lock for its whole run and runs in the
//! search pool. It takes them in that order's reverse: a worker first, the lock inside it
//! (ADR-206).
//!
//! The pool's workers take the cluster lock for each title they match, and they wait when a
//! writer is queued for it. A request that held the lock and then waited for a worker would
//! complete a cycle with a vocabulary rebuild or a resize queued behind it: the workers wait
//! for the writer, the writer for the request, the request for a worker.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::Duration;

use super::*;

/// Every worker of the search pool, held until [`Occupied::release`].
struct Occupied {
    released: Arc<(StdMutex<bool>, Condvar)>,
}

impl Occupied {
    fn every_worker_of(state: &Arc<ClusterAppState>) -> Self {
        let workers = state.pool.current_num_threads();
        let released = Arc::new((StdMutex::new(false), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let (released, started) = (Arc::clone(&released), Arc::clone(&started));
            state.pool.spawn(move || {
                started.fetch_add(1, Ordering::SeqCst);
                let (flag, wake) = &*released;
                let mut done = flag.lock().expect("flag");
                while !*done {
                    done = wake.wait(done).expect("wait");
                }
            });
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while started.load(Ordering::SeqCst) < workers {
            assert!(
                std::time::Instant::now() < deadline,
                "the pool never filled"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        Occupied { released }
    }

    fn release(&self) {
        let (flag, wake) = &*self.released;
        *flag.lock().expect("flag") = true;
        wake.notify_all();
    }
}

/// What one request did while every worker of the pool was occupied.
struct Observed {
    took_its_turn: bool,
    answered_without_a_worker: bool,
    cluster_lock_was_free: bool,
    answer: Option<(StatusCode, serde_json::Value)>,
}

#[test]
fn a_search_with_sources_waits_for_a_worker_without_holding_the_cluster_lock() {
    let requests = [
        (
            "/v2/_search",
            serde_json::json!({ "document": { "title": "1994 acme" } }),
        ),
        (
            "/v2/_mpercolate",
            serde_json::json!({ "documents": [{ "title": "1994 acme" }, { "title": "1995 vertex" }] }),
        ),
        (
            "/_search",
            serde_json::json!({
                "documents": [{ "title": "1994 acme" }, { "title": "1995 vertex" }],
                "include_broad": true,
                "_source": true
            }),
        ),
    ];
    for (path, body) in requests {
        let state = test_state(&seed());
        let occupied = Occupied::every_worker_of(&state);
        let run_state = Arc::clone(&state);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let observed = runtime.block_on(async move {
            let request_state = Arc::clone(&run_state);
            let mut request =
                tokio::spawn(async move { send(&request_state, req("POST", path, &body)).await });
            // It takes its turn, and then waits for a worker.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !run_state.stable_view_turn.is_locked() && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let took_its_turn = run_state.stable_view_turn.is_locked();
            let early = tokio::time::timeout(Duration::from_millis(150), &mut request).await;
            // While it waits it must not hold the cluster lock: a vocabulary rebuild or a
            // resize that asks for the write lock now gets it.
            let lock_state = Arc::clone(&run_state);
            let cluster_lock_was_free = tokio::task::spawn_blocking(move || {
                lock_state
                    .cluster
                    .try_write_for(Duration::from_secs(2))
                    .is_some()
            })
            .await
            .expect("lock probe");
            // Free the pool before leaving the runtime: the request is parked on a blocking
            // thread, and the runtime does not shut down until it is done.
            occupied.release();
            let (answered_without_a_worker, answer) = match early {
                Ok(answer) => (true, answer.ok()),
                Err(_) => (
                    false,
                    tokio::time::timeout(Duration::from_secs(10), request)
                        .await
                        .ok()
                        .and_then(Result::ok),
                ),
            };
            Observed {
                took_its_turn,
                answered_without_a_worker,
                cluster_lock_was_free,
                answer,
            }
        });
        assert!(observed.took_its_turn, "{path} did not take its turn");
        assert!(
            !observed.answered_without_a_worker,
            "{path} ran outside the search pool, so outside the configured thread budget"
        );
        assert!(
            observed.cluster_lock_was_free,
            "{path} held the cluster lock while it waited for a pool worker"
        );
        let (status, body) = observed
            .answer
            .unwrap_or_else(|| panic!("{path} never answered once the pool was free"));
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}
