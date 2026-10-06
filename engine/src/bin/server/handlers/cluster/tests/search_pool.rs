//! A search that returns sources holds the cluster lock while it waits for a worker of the
//! search pool. That is safe only because nothing can queue for the cluster's write lock
//! behind it: it shares write admission, and whoever takes the write lock holds admission
//! alone (ADR-206).
//!
//! The pool's workers take the cluster lock for each title they match, and they wait when a
//! writer is queued for it. With a vocabulary rebuild queued behind the search, the workers
//! would wait for the rebuild, the rebuild for the search, and the search for a worker.

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

/// What happened while every worker of the pool was occupied.
struct Observed {
    /// What should not have happened, if anything did.
    violations: Vec<&'static str>,
    answer: Option<(StatusCode, serde_json::Value)>,
    rebuild: Option<StatusCode>,
}

#[test]
fn nothing_queues_for_the_cluster_lock_behind_a_search_that_waits_for_a_worker() {
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
            // The search shares write admission, takes the cluster lock and its view, and
            // then waits for a worker.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !run_state.write_admission.is_locked() && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let shared_admission = run_state.write_admission.is_locked()
                && !run_state.write_admission.is_locked_exclusive();
            let early = tokio::time::timeout(Duration::from_millis(150), &mut request).await;

            // A vocabulary change arrives. It needs admission alone, so it waits there and
            // does not queue for the cluster's write lock: the cluster lock is still free
            // to read, which is what the pool's workers need.
            let rebuild_state = Arc::clone(&run_state);
            let mut rebuild = tokio::spawn(async move {
                send_raw(
                    &rebuild_state,
                    req("PUT", "/_vocab", &serde_json::json!({})),
                )
                .await
                .0
            });
            let rebuild_early =
                tokio::time::timeout(Duration::from_millis(150), &mut rebuild).await;
            let probe_state = Arc::clone(&run_state);
            let no_writer_queued_for_the_cluster =
                tokio::task::spawn_blocking(move || probe_state.cluster.try_read().is_some())
                    .await
                    .expect("lock probe");

            // Free the pool before leaving the runtime: the requests are parked on blocking
            // threads, and the runtime does not shut down until they are done.
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
            let (rebuild_ran_beside_it, rebuild) = match rebuild_early {
                Ok(status) => (true, status.ok()),
                Err(_) => (
                    false,
                    tokio::time::timeout(Duration::from_secs(20), rebuild)
                        .await
                        .ok()
                        .and_then(Result::ok),
                ),
            };
            let mut violations = Vec::new();
            if !shared_admission {
                violations.push("the search does not share write admission");
            }
            if answered_without_a_worker {
                violations.push("the search ran outside the search pool and its thread budget");
            }
            if rebuild_ran_beside_it {
                violations
                    .push("a vocabulary change ran beside a search that holds the cluster lock");
            }
            if !no_writer_queued_for_the_cluster {
                violations.push(
                    "a writer queued for the cluster lock behind a search waiting for a worker",
                );
            }
            Observed {
                violations,
                answer,
                rebuild,
            }
        });
        assert!(
            observed.violations.is_empty(),
            "{path}: {:?}",
            observed.violations
        );
        let (status, body) = observed
            .answer
            .unwrap_or_else(|| panic!("{path} never answered once the pool was free"));
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(
            observed.rebuild,
            Some(StatusCode::OK),
            "{path}: the vocabulary change runs once the search is done"
        );
    }
}
