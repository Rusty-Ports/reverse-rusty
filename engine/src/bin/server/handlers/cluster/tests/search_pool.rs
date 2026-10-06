//! A search that returns sources holds the cluster lock for its whole run, so it does not
//! wait for the search pool (ADR-206).
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

#[test]
fn a_search_with_sources_does_not_wait_for_the_search_pool() {
    let state = test_state(&seed());
    let occupied = Occupied::every_worker_of(&state);
    let run_state = Arc::clone(&state);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let (source_free, answers) = runtime.block_on(async move {
        // The pool really is full: a search that returns ids only runs in it, and waits.
        let ids_only = serde_json::json!({
            "document": { "title": "1994 acme" },
            "include_source": false
        });
        let source_free = tokio::time::timeout(
            Duration::from_millis(200),
            send(&run_state, req("POST", "/v2/_search", &ids_only)),
        )
        .await
        .is_err();

        let mut answers = Vec::new();
        for (path, body) in [
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
        ] {
            let answer = tokio::time::timeout(
                Duration::from_secs(5),
                send(&run_state, req("POST", path, &body)),
            )
            .await;
            answers.push((path, answer));
        }
        // Free the pool before leaving the runtime: a request that did wait for a worker is
        // parked on a blocking thread, and the runtime does not shut down until it is done.
        occupied.release();
        (source_free, answers)
    });
    assert!(
        source_free,
        "the pool was not full: a search that runs in it answered"
    );
    for (path, answer) in answers {
        let (status, body) = answer.unwrap_or_else(|_| panic!("{path} waited for the search pool"));
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}
