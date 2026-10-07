//! Coordinator writes that run at the same time (ADR-206).
//!
//! Writes share admission, so the served coordinator now runs them beside each other. The
//! cluster orders the writes to one id by its log, under a lock held across the append and the
//! complete shard fan-out (ADR-177). These tests drive that through the HTTP surface: whatever
//! the interleaving, every id ends on a body some request wrote for it, the index and the
//! stored source agree, and a reopen replays the log to the same state.

use std::collections::BTreeMap;

use super::checkpoint::durable_state;
use super::*;

fn runtime(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime")
}

/// The live source of every id in `ids`, and a check that a title made of the source's own
/// words is matched by that id: the index holds the version the source store holds.
fn live_bodies(
    cluster: &ClusterEngine,
    ids: impl Iterator<Item = u64>,
) -> BTreeMap<u64, Option<String>> {
    ids.map(|id| {
        let body = cluster.get_source(id).expect("source");
        if let Some(body) = &body {
            let matched = cluster.percolate_with_broad(body, true).expect("percolate");
            assert!(
                matched.contains(&id),
                "id {id} holds {body:?} and does not match it"
            );
        }
        (id, body)
    })
    .collect()
}

fn reopened(root: &std::path::Path) -> ClusterEngine {
    ClusterEngine::open(
        root.join("data"),
        Normalizer::default_vocab().expect("vocab"),
        None,
    )
    .expect("reopen")
}

#[test]
fn overlapping_bulk_batches_leave_what_the_log_replays() {
    const WRITERS: u64 = 8;
    const ROUNDS: u64 = 5;
    const IDS: std::ops::Range<u64> = 1_000..1_032;
    let (state, root) = durable_state("concurrent-bulk");
    let run_state = Arc::clone(&state);
    runtime(4).block_on(async move {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let state = Arc::clone(&run_state);
                tokio::spawn(async move {
                    for round in 0..ROUNDS {
                        // Each writer walks the ids from its own starting point, so the
                        // batches collide on every id in every order.
                        let mut body = String::new();
                        for step in 0..IDS.end - IDS.start {
                            let id = IDS.start + (step + writer * 5) % (IDS.end - IDS.start);
                            body.push_str(&format!("{{\"index\":{{\"_id\":{id}}}}}\n"));
                            body.push_str(&format!(
                                "{{\"query\":\"zzw{writer}r{round} zzid{id}\",\"version\":{}}}\n",
                                round + 1
                            ));
                        }
                        let request = Request::post("/_bulk")
                            .header("content-type", "application/x-ndjson")
                            .body(Body::from(body))
                            .expect("request");
                        let (status, response) = send(&state, request).await;
                        assert_eq!(status, StatusCode::OK, "{response}");
                        assert_eq!(response["errors"], false, "{response}");
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.await.expect("writer");
        }
    });

    let live = live_bodies(&state.cluster.read(), IDS);
    for (id, body) in &live {
        let body = body
            .as_deref()
            .unwrap_or_else(|| panic!("id {id} was lost"));
        let (stamp, tail) = body.split_once(' ').expect("two words");
        assert_eq!(tail, format!("zzid{id}"), "id {id} holds another id's body");
        assert!(stamp.starts_with("zzw"), "id {id}: {body:?}");
    }

    // The log is the order of record: a reopen that replays it holds the same bodies.
    drop(state);
    let replayed = live_bodies(&reopened(&root), IDS);
    assert_eq!(replayed, live);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn concurrent_puts_and_deletes_on_distinct_ids_all_land() {
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 40;
    let (state, root) = durable_state("concurrent-put");
    let run_state = Arc::clone(&state);
    runtime(4).block_on(async move {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let state = Arc::clone(&run_state);
                tokio::spawn(async move {
                    for step in 0..PER_WRITER {
                        let id = 10_000 + writer * 1_000 + step;
                        let body = serde_json::json!({ "query": format!("zzput{writer} zzn{id}") });
                        let (status, response) =
                            send(&state, req("PUT", &format!("/_doc/{id}"), &body)).await;
                        assert_eq!(status, StatusCode::CREATED, "{response}");
                        // Every fourth document is replaced, every fifth removed.
                        if step % 4 == 0 {
                            let body =
                                serde_json::json!({ "query": format!("zzagain{writer} zzn{id}") });
                            let (status, response) =
                                send(&state, req("PUT", &format!("/_doc/{id}"), &body)).await;
                            assert_eq!(status, StatusCode::OK, "{response}");
                        }
                        if step % 5 == 0 {
                            let (status, response) =
                                send(&state, req_empty("DELETE", &format!("/_doc/{id}"))).await;
                            assert_eq!(status, StatusCode::OK, "{response}");
                        }
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.await.expect("writer");
        }
    });

    let ids = || (0..WRITERS).flat_map(|writer| (0..PER_WRITER).map(move |step| (writer, step)));
    let live = live_bodies(
        &state.cluster.read(),
        ids().map(|(writer, step)| 10_000 + writer * 1_000 + step),
    );
    for (writer, step) in ids() {
        let id = 10_000 + writer * 1_000 + step;
        let expected = if step % 5 == 0 {
            None
        } else if step % 4 == 0 {
            Some(format!("zzagain{writer} zzn{id}"))
        } else {
            Some(format!("zzput{writer} zzn{id}"))
        };
        assert_eq!(live[&id], expected, "id {id}");
    }
    drop(state);
    let replayed = live_bodies(
        &reopened(&root),
        ids().map(|(writer, step)| 10_000 + writer * 1_000 + step),
    );
    assert_eq!(replayed, live);
    let _ = std::fs::remove_dir_all(root);
}
