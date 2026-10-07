//! The gate of the search pool (ADR-207): a writer of the cluster lock is queued for it, or
//! holds it, only while the pool is empty.
//!
//! The pool's workers take the cluster lock for each title they match, and a reader waits
//! when a writer is queued. A worker that holds the lock for one title waits for the workers
//! running that title's shard fan-out, and those pick up other titles meanwhile. Before the
//! gate, one of them asking for the lock behind a queued vocabulary rebuild stopped the
//! coordinator: the rebuild waited for the first worker, and the first worker for this one.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use super::*;

fn wait_until(mut reached: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !reached() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

#[test]
fn a_cluster_writer_waits_at_the_gate_while_work_is_in_the_pool() {
    let state = test_state(&seed());
    let (report, reported) = mpsc::channel();
    let request_state = Arc::clone(&state);
    // Detached on purpose: where a worker does wait for the writer, this thread never
    // finishes, and the test must fail instead of joining it.
    std::thread::spawn(move || {
        let state = request_state;
        let (order, ordered) = mpsc::channel();
        let wrote = order.clone();
        let entered = state.pool.enter();
        let (writer_at_the_gate, lock_free_to_read, reads_in_the_pool) = entered.run(|| {
            // One title in flight: a worker holds the lock and waits for its fan-out.
            let in_flight = state.cluster.read();
            let writer_state = Arc::clone(&state);
            std::thread::spawn(move || {
                let admission = writer_state.write_admission.write();
                let writer = writer_state.write_cluster(&admission);
                // Said while the gate is still closed, so the order is the gate's and not
                // the scheduler's.
                let _ = wrote.send("the writer");
                drop(writer);
            });
            // The writer shows up at the gate, or (without one) queued for the lock.
            wait_until(|| state.pool.try_enter().is_none() || state.cluster.try_read().is_none());
            let at_the_gate = state.pool.try_enter().is_none();
            let free_to_read = state.cluster.try_read().is_some();
            // Other titles of the pool take the lock now, on this worker and on the others.
            let reads = (0..16)
                .into_par_iter()
                .map(|_| state.cluster.read().num_shards())
                .count();
            drop(in_flight);
            (at_the_gate, free_to_read, reads)
        });
        // A request that arrives now waits its turn behind the writer.
        let later_state = Arc::clone(&state);
        let later = order.clone();
        std::thread::spawn(move || {
            later_state.pool.run(|| ());
            let _ = later.send("a later request");
        });
        std::thread::sleep(Duration::from_millis(100));
        let wrote_beside_the_request = ordered.try_recv().is_ok();
        drop(entered);
        let turns: Vec<&str> = (0..2)
            .filter_map(|_| ordered.recv_timeout(Duration::from_secs(5)).ok())
            .collect();
        let mut violations = Vec::new();
        if !writer_at_the_gate {
            violations.push("the writer did not wait at the gate");
        }
        if !lock_free_to_read {
            violations.push("the writer queued for the cluster lock while work was in the pool");
        }
        if reads_in_the_pool != 16 {
            violations.push("a title was not matched");
        }
        if wrote_beside_the_request {
            violations.push("the writer, or a request behind it, ran beside the first request");
        }
        if turns != ["the writer", "a later request"] {
            violations.push("the writer did not run next, with the later request after it");
        }
        let _ = report.send(violations);
    });
    let violations = reported
        .recv_timeout(Duration::from_secs(15))
        .expect("a worker of the search pool waited for a writer that was waiting for the pool");
    assert!(violations.is_empty(), "{violations:?}");
}

/// The gate prefers a waiting writer, like the cluster lock. Work that is already in the
/// pool must not take it a second time, or the writer waits for the first hold and the
/// second hold for the writer.
#[test]
fn work_in_the_pool_enters_again_past_a_waiting_writer() {
    let state = test_state(&seed());
    let (report, reported) = mpsc::channel();
    let request_state = Arc::clone(&state);
    // Detached on purpose: where the second entry waits for the writer, this thread never
    // finishes, and the test must fail instead of joining it.
    std::thread::spawn(move || {
        let state = request_state;
        let (wrote, written) = mpsc::channel();
        let (writer_waits, shards) = state.pool.run(|| {
            let writer_state = Arc::clone(&state);
            std::thread::spawn(move || {
                let admission = writer_state.write_admission.write();
                drop(writer_state.write_cluster(&admission));
                let _ = wrote.send(());
            });
            let writer_waits = wait_until(|| state.pool.try_enter().is_none());
            // On this worker and on the others.
            let shards: Vec<usize> = (0..8)
                .into_par_iter()
                .map(|_| state.pool.run(|| state.cluster.read().num_shards()))
                .collect();
            (writer_waits, shards)
        });
        let wrote = written.recv_timeout(Duration::from_secs(5)).is_ok();
        let _ = report.send((writer_waits, shards, wrote));
    });
    let (writer_waits, shards, wrote) = reported
        .recv_timeout(Duration::from_secs(15))
        .expect("work in the pool waited at the gate behind a writer that was waiting for it");
    assert!(writer_waits, "the writer never came to the gate");
    assert_eq!(shards, vec![3; 8]);
    assert!(
        wrote,
        "the writer never got the lock once the pool was empty"
    );
}

#[test]
fn no_request_enters_the_pool_while_a_writer_has_the_cluster_lock() {
    let state = test_state(&seed());
    let admission = state.write_admission.write();
    let writer = state.write_cluster(&admission);
    let in_the_pool = Arc::new(AtomicBool::new(false));
    let (answer, answered) = mpsc::channel();
    let (request_state, request_in_the_pool) = (Arc::clone(&state), Arc::clone(&in_the_pool));
    let request = std::thread::spawn(move || {
        let shards = request_state.pool.run(|| {
            request_in_the_pool.store(true, Ordering::SeqCst);
            request_state.cluster.read().num_shards()
        });
        let _ = answer.send(shards);
    });
    std::thread::sleep(Duration::from_millis(150));
    let entered_beside_the_writer = in_the_pool.load(Ordering::SeqCst);
    let closed = state.pool.try_enter().is_none();
    // Released before anything can fail: the request thread is waiting for these.
    drop(writer);
    drop(admission);
    let shards = answered.recv_timeout(Duration::from_secs(5)).ok();
    assert!(
        !entered_beside_the_writer,
        "a request's work reached the pool while a writer had the cluster lock"
    );
    assert!(
        closed,
        "the gate was open while a writer had the cluster lock"
    );
    assert_eq!(shards, Some(3), "the request runs once the writer is done");
    request.join().expect("request thread");
}

#[test]
fn a_timed_cluster_writer_gives_up_at_the_gate_and_reopens_it() {
    let state = test_state(&seed());
    let admission = state.write_admission.write();

    let entered = state.pool.enter();
    assert!(
        state.try_write_cluster_until(&admission, None).is_none(),
        "work is in the pool"
    );
    assert!(
        state.cluster.try_read().is_some(),
        "giving up at the gate leaves the cluster lock alone"
    );
    drop(entered);

    let writer = state
        .try_write_cluster_until(&admission, None)
        .expect("the pool is empty");
    assert!(
        state.pool.try_enter().is_none(),
        "the gate is closed for as long as the writer has the lock"
    );
    drop(writer);
    assert!(state.pool.try_enter().is_some());

    // A reader outside the pool: the writer gets the gate, gives up at the lock, and must
    // not leave the gate closed behind it.
    let reader = state.cluster.read();
    assert!(state.try_write_cluster_until(&admission, None).is_none());
    assert!(state
        .try_write_cluster_until(&admission, Some(Instant::now() + Duration::from_millis(20)))
        .is_none());
    assert!(
        state.pool.try_enter().is_some(),
        "giving up at the lock reopens the gate"
    );
    drop(reader);
    assert!(state
        .try_write_cluster_until(&admission, Some(Instant::now() + Duration::from_secs(5)))
        .is_some());
    assert!(
        state
            .try_write_cluster_until(
                &admission,
                Some(
                    Instant::now()
                        .checked_sub(Duration::from_millis(1))
                        .expect("past")
                )
            )
            .is_none(),
        "a deadline already passed is not a try without waiting"
    );
}

/// A timed writer that held the cluster lock while it waited at the gate would stop the
/// pool's workers for as long as its budget.
#[test]
fn a_timed_cluster_writer_waits_at_the_gate_without_the_cluster_lock() {
    // Long enough that this thread sees the writer waiting on a busy machine.
    const BUDGET: Duration = Duration::from_secs(1);
    let state = test_state(&seed());
    let entered = state.pool.enter();
    let (tell, told) = mpsc::channel();
    let writer_state = Arc::clone(&state);
    let writer = std::thread::spawn(move || {
        let admission = writer_state.write_admission.write();
        let started = Instant::now();
        let wrote = writer_state
            .try_write_cluster_until(&admission, Some(started + BUDGET))
            .is_some();
        let _ = tell.send((wrote, started.elapsed()));
    });
    let writer_waits = wait_until(|| state.pool.try_enter().is_none());
    let lock_free_to_read = state.cluster.try_read().is_some();
    let gave_up = told.recv_timeout(BUDGET + Duration::from_secs(3)).ok();
    // A writer that ignored its deadline is still at the gate: this lets it finish.
    drop(entered);
    writer.join().expect("writer thread");
    assert!(writer_waits, "the writer never came to the gate");
    assert!(
        lock_free_to_read,
        "the writer held or queued for the cluster lock while it waited at the gate"
    );
    let (wrote, waited) = gave_up.expect("the writer waited past its deadline");
    assert!(
        !wrote,
        "the writer got the lock beside a request in the pool"
    );
    assert!(waited >= BUDGET.mul_f32(0.9), "it waits for its budget");
}

/// Within its budget a timed writer waits for a request to leave the pool, and for a reader
/// outside the pool to let go of the lock.
#[test]
fn a_timed_cluster_writer_waits_for_its_turn() {
    for in_the_pool in [true, false] {
        let state = test_state(&seed());
        let (holding, held) = mpsc::channel();
        let holder_state = Arc::clone(&state);
        // The holder lets go by itself, so nothing here can leave it holding a lock.
        let holder = std::thread::spawn(move || {
            let _entered = in_the_pool.then(|| holder_state.pool.enter());
            let _reader = (!in_the_pool).then(|| holder_state.cluster.read());
            let _ = holding.send(());
            std::thread::sleep(Duration::from_millis(150));
        });
        held.recv().expect("holder");
        let admission = state.write_admission.write();
        let wrote = state
            .try_write_cluster_until(&admission, Some(Instant::now() + Duration::from_secs(5)))
            .is_some();
        holder.join().expect("holder thread");
        assert!(wrote, "in the pool: {in_the_pool}");
    }
}

/// ADR-206's rule, kept where the lock is taken: the caller shows the exclusive guard of
/// this coordinator's write admission, not of some other lock.
#[test]
#[should_panic(expected = "under this coordinator's write admission")]
fn the_cluster_write_lock_is_refused_without_write_admission() {
    let state = test_state(&seed());
    let other = state.topology_guard.write();
    drop(state.write_cluster(&other));
}

/// The served path: searches whose titles route to several shards, in a small pool, while
/// a writer takes and releases the cluster's write lock as a vocabulary rebuild does.
/// Before ADR-207 this stopped within seconds, with every worker of the pool waiting for
/// the writer and the writer for one of them.
#[test]
fn searches_and_a_cluster_writer_keep_each_other_moving() {
    let queries: Vec<(u64, String)> = (0..400u64)
        .map(|id| (id + 1, format!("zzq{id} zzr{}", id % 37)))
        .collect();
    let config = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..Default::default()
    };
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        &queries,
    )
    .expect("cluster");
    let state = state_from_cluster(cluster);
    // Each title names a dozen queries, so it routes to most of the shards.
    let titles: Vec<serde_json::Value> = (0..200)
        .map(|at| {
            let words: Vec<String> = (0..12)
                .map(|k| format!("zzq{}", (at * 7 + k * 13) % 400))
                .collect();
            serde_json::json!({ "title": words.join(" ") })
        })
        .collect();
    // Requests that return ids only: they take nothing but their place in the pool, so
    // nothing else keeps the writer away from them.
    let requests = [
        (
            "/_search",
            serde_json::json!({ "documents": titles, "include_broad": true, "size": 5 }),
        ),
        (
            "/v2/_mpercolate",
            serde_json::json!({ "documents": titles[..40].to_vec(), "_source": false }),
        ),
    ];

    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    {
        // Detached: where the pool and the writer deadlock, this thread never returns.
        let (state, stop, writes) = (Arc::clone(&state), Arc::clone(&stop), Arc::clone(&writes));
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                {
                    let admission = state.write_admission.write();
                    drop(state.write_cluster(&admission));
                }
                writes.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_micros(200));
            }
        });
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let (run_state, run_writes) = (Arc::clone(&state), Arc::clone(&writes));
    let outcome = runtime.block_on(async move {
        let started = Instant::now();
        let mut answered = 0usize;
        while started.elapsed() < Duration::from_secs(2) {
            let writes_before = run_writes.load(Ordering::SeqCst);
            let round: Vec<_> = requests
                .iter()
                .cycle()
                .take(6)
                .map(|(path, body)| {
                    let (state, path, body) = (Arc::clone(&run_state), *path, body.clone());
                    tokio::spawn(async move { send(&state, req("POST", path, &body)).await })
                })
                .collect();
            for search in round {
                match tokio::time::timeout(Duration::from_secs(10), search).await {
                    Ok(Ok((StatusCode::OK, _))) => answered += 1,
                    Ok(other) => return Err(format!("a search answered {other:?}")),
                    Err(_) => return Err(format!("a search never answered ({answered} had)")),
                }
            }
            // The writer waits at the gate while a round is in the pool and takes its turn
            // when the round leaves.
            let turn_by = Instant::now() + Duration::from_secs(5);
            while run_writes.load(Ordering::SeqCst) == writes_before {
                if Instant::now() >= turn_by {
                    return Err(format!(
                        "the writer got no turn after a round ({writes_before} turns so far)"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        Ok(answered)
    });
    stop.store(true, Ordering::SeqCst);
    // On failure the pool's workers and the request threads are stuck for good. Leave them:
    // waiting for the runtime would hang the test instead of failing it.
    runtime.shutdown_background();
    let answered = outcome.unwrap_or_else(|stuck| panic!("{stuck}"));
    assert!(answered > 0);
    assert!(writes.load(Ordering::SeqCst) > 0);
}
