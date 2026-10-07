//! What answers while a vocabulary change or a resize is rebuilding the cluster (ADR-210).
//!
//! Each test stops a real rebuild half-way, at the point where it holds the engine's layout
//! lock alone, and asks the server for things. A search and a plain read answer, with the old
//! layout's result. A write waits. An operation with a time budget gives up within it. When
//! the rebuild goes on, everything that waited finishes and a search sees the new layout.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::http::header;

use super::*;

/// The queries of these tests. `widget package` matches the title `widget pkg` only once
/// `pkg` is a synonym of `package`.
fn queries() -> Vec<(u64, String)> {
    let mut queries = seed();
    queries.push((4, "widget package".to_string()));
    queries
}

fn pkg_vocab() -> serde_json::Value {
    serde_json::json!({
        "synonyms": [{"token": "pkg", "canonical": "term:package", "kind": "generic"}]
    })
}

type Request0 = fn() -> Request<Body>;

/// Requests that must answer `200 OK` while a rebuild is half-way: every search route, with
/// and without sources, and every plain read.
fn must_answer() -> Vec<(&'static str, Request0)> {
    vec![
        ("POST /_search", || {
            req(
                "POST",
                "/_search",
                &serde_json::json!({"documents": [{"title": "1994 acme"}, {"title": "widget pkg"}]}),
            )
        }),
        ("POST /_search with sources", || {
            req(
                "POST",
                "/_search",
                &serde_json::json!({"document": {"title": "1994 acme"}, "_source": true}),
            )
        }),
        ("POST /v2/_search", || {
            req(
                "POST",
                "/v2/_search",
                &serde_json::json!({"document": {"title": "1994 acme"}}),
            )
        }),
        ("POST /v2/_search without sources", || {
            req(
                "POST",
                "/v2/_search",
                &serde_json::json!({"document": {"title": "1994 acme"}, "_source": false}),
            )
        }),
        ("POST /v2/_mpercolate", || {
            req(
                "POST",
                "/v2/_mpercolate",
                &serde_json::json!({"documents": [{"title": "1994 acme"}, {"title": "1995 vertex"}]}),
            )
        }),
        ("GET /_doc/1", || req_empty("GET", "/_doc/1")),
        ("HEAD /_doc/1", || req_empty("HEAD", "/_doc/1")),
        ("GET /", || req_empty("GET", "/")),
        ("GET /_health", || req_empty("GET", "/_health")),
        ("GET /_stats", || req_empty("GET", "/_stats")),
        ("GET /_cat/shards", || req_empty("GET", "/_cat/shards")),
        ("GET /_metrics", || req_empty("GET", "/_metrics")),
        ("GET /_settings", || req_empty("GET", "/_settings")),
        ("GET /_vocab", || req_empty("GET", "/_vocab")),
        ("GET /_vocab/aliases", || {
            req_empty("GET", "/_vocab/aliases")
        }),
        ("GET /_cluster/state", || {
            req_empty("GET", "/_cluster/state")
        }),
    ]
}

/// The ids `/_search` returns for one title, in the default scope.
async fn matches(state: &Arc<ClusterAppState>, title: &str) -> Result<Vec<u64>, String> {
    let request = req(
        "POST",
        "/_search",
        &serde_json::json!({"document": {"title": title}, "include_broad": false}),
    );
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), send(state, request))
        .await
        .map_err(|_| format!("a search for {title:?} waited for the rebuild"))?;
    if status != StatusCode::OK {
        return Err(format!("a search for {title:?} answered {status}: {body}"));
    }
    let mut ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits in {body}"))?
        .iter()
        .filter_map(|hit| hit["_id"].as_u64())
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Ask for everything in [`must_answer`] and say what did not answer `200 OK` in time.
async fn unanswered(state: &Arc<ClusterAppState>) -> Vec<String> {
    let mut failures = Vec::new();
    for (route, request) in must_answer() {
        match tokio::time::timeout(Duration::from_secs(2), send_raw(state, request())).await {
            Ok((StatusCode::OK, _, _)) => {}
            Ok((status, _, bytes)) => failures.push(format!(
                "{route} answered {status}: {}",
                String::from_utf8_lossy(&bytes)
            )),
            Err(_) => failures.push(format!("{route} waited for the rebuild")),
        }
    }
    failures
}

/// A rebalance with a budget of 25 ms: `(status, error type)`, or `None` if it did not
/// answer within five seconds.
async fn budgeted_rebalance(state: &Arc<ClusterAppState>) -> Option<(StatusCode, String)> {
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/rebalance?master_timeout=25ms")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .expect("request");
    let (status, _, bytes) = tokio::time::timeout(Duration::from_secs(5), send_raw(state, request))
        .await
        .ok()?;
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    Some((
        status,
        body["error"]["type"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
    ))
}

/// `/_health`: its HTTP status, its colour, and whether its reason says a rebuild is running.
async fn health(state: &Arc<ClusterAppState>) -> (StatusCode, String, bool) {
    let (status, body) = send(state, req_empty("GET", "/_health")).await;
    (
        status,
        body["status"].as_str().unwrap_or_default().to_string(),
        body["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("rebuilding the cluster")),
    )
}

async fn wait_for(stopped: std::sync::mpsc::Receiver<()>) {
    tokio::task::spawn_blocking(move || stopped.recv_timeout(Duration::from_secs(10)))
        .await
        .expect("wait")
        .expect("the rebuild reached its stopping point");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn searches_and_reads_answer_while_a_vocabulary_change_is_half_done() {
    let state = test_state(&queries());
    let (stopped, release) = stop_the_next_rebuild(&state);
    let rebuild_state = Arc::clone(&state);
    let rebuild =
        tokio::spawn(
            async move { send(&rebuild_state, req("PUT", "/_vocab", &pkg_vocab())).await },
        );
    wait_for(stopped).await;

    // Everything here is observed first and judged after the rebuild has been let go.
    // A resync with the longest budget waits for the rebuild. It must not sit on anything the
    // reads below need while it waits.
    let resync_state = Arc::clone(&state);
    let resync = tokio::spawn(async move {
        let request = req_empty("POST", "/_cluster/resync?cluster_manager_timeout=30s");
        send(&resync_state, request).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let failures = unanswered(&state).await;
    let resync_waited = !resync.is_finished();
    let health_meanwhile = health(&state).await;
    let old_answer = matches(&state, "widget pkg").await;
    let write_state = Arc::clone(&state);
    let mut write = tokio::spawn(async move {
        send(
            &write_state,
            req(
                "PUT",
                "/_doc/900",
                &serde_json::json!({"query": "zzlate arrival"}),
            ),
        )
        .await
    });
    let write_waited = tokio::time::timeout(Duration::from_millis(200), &mut write)
        .await
        .is_err();
    let budgeted = budgeted_rebalance(&state).await;

    drop(release);
    let (rebuilt, body) = tokio::time::timeout(Duration::from_secs(20), rebuild)
        .await
        .expect("the vocabulary change finished")
        .expect("rebuild task");
    let (written, write_body) = tokio::time::timeout(Duration::from_secs(20), write)
        .await
        .expect("the write finished")
        .expect("write task");
    let (resynced, resync_body) = tokio::time::timeout(Duration::from_secs(20), resync)
        .await
        .expect("the resync finished")
        .expect("resync task");

    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(
        health_meanwhile,
        (StatusCode::OK, "yellow".to_string(), true),
        "health says the cluster serves and is rebuilding, and does not compare a topology \
         that is being replaced"
    );
    assert_eq!(
        health(&state).await,
        (StatusCode::OK, "green".to_string(), false),
        "health is green again once the rebuild is done"
    );
    assert!(resync_waited, "a resync ran beside a vocabulary change");
    assert_eq!(resynced, StatusCode::OK, "{resync_body}");
    assert_eq!(
        old_answer,
        Ok(vec![]),
        "a search beside the rebuild answers under the vocabulary it replaces"
    );
    assert!(write_waited, "a write ran beside a vocabulary change");
    assert_eq!(
        budgeted,
        Some((StatusCode::REQUEST_TIMEOUT, "rebalance_timeout".to_string())),
        "an operation with a time budget gives up within it"
    );
    assert_eq!(rebuilt, StatusCode::OK, "{body}");
    assert!(written.is_success(), "{written}: {write_body}");
    assert_eq!(
        matches(&state, "widget pkg").await,
        Ok(vec![4]),
        "a search after the swap answers under the new vocabulary"
    );
    assert_eq!(
        matches(&state, "zzlate arrival").await,
        Ok(vec![900]),
        "the write that waited is matched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn searches_and_reads_answer_while_a_resize_is_half_done() {
    let state = test_state(&queries());
    let (stopped, release) = stop_the_next_rebuild(&state);
    let rebuild_state = Arc::clone(&state);
    let rebuild = tokio::spawn(async move {
        let request = Request::builder()
            .method("POST")
            .uri("/_cluster/resize")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"num_shards":5}"#))
            .expect("request");
        send(&rebuild_state, request).await
    });
    wait_for(stopped).await;

    let failures = unanswered(&state).await;
    let old_answer = matches(&state, "1994 acme").await;
    let shards_meanwhile = state.cluster.num_shards();
    // A vocabulary change waits its turn behind the resize.
    let vocab_state = Arc::clone(&state);
    let mut vocab =
        tokio::spawn(async move { send(&vocab_state, req("PUT", "/_vocab", &pkg_vocab())).await });
    let vocab_waited = tokio::time::timeout(Duration::from_millis(200), &mut vocab)
        .await
        .is_err();
    let budgeted = budgeted_rebalance(&state).await;

    drop(release);
    let (resized, body) = tokio::time::timeout(Duration::from_secs(20), rebuild)
        .await
        .expect("the resize finished")
        .expect("resize task");
    let (vocab_status, vocab_body) = tokio::time::timeout(Duration::from_secs(20), vocab)
        .await
        .expect("the vocabulary change finished")
        .expect("vocab task");

    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(old_answer, Ok(vec![1]));
    assert_eq!(shards_meanwhile, 3, "the old layout serves until the swap");
    assert!(vocab_waited, "a vocabulary change ran beside a resize");
    assert_eq!(
        budgeted,
        Some((StatusCode::REQUEST_TIMEOUT, "rebalance_timeout".to_string())),
        "an operation with a time budget gives up within it"
    );
    assert_eq!(resized, StatusCode::OK, "{body}");
    assert_eq!(body["num_shards"], 5, "{body}");
    assert_eq!(vocab_status, StatusCode::OK, "{vocab_body}");
    assert_eq!(state.cluster.num_shards(), 5);
    assert_eq!(matches(&state, "1994 acme").await, Ok(vec![1]));
    assert_eq!(matches(&state, "widget pkg").await, Ok(vec![4]));
}

/// A remote resize holds the topology guard for its whole copy, with its write fence up. A
/// vocabulary change is refused while that lasts, and does not wait for the copy: neither
/// one that arrives during the copy, nor one that was already waiting when the fence went up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vocabulary_change_is_refused_while_a_remote_resize_copies() {
    for fenced_on_arrival in [true, false] {
        let state = test_state(&queries());
        // What a remote resize holds while it copies: the topology guard, alone.
        let holder_state = Arc::clone(&state);
        let (held_sender, held) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel::<()>(1);
        let holder = std::thread::spawn(move || {
            let _topology = holder_state.topology_guard.write();
            held_sender.send(()).expect("signal");
            let _ = released.recv();
        });
        held.recv().expect("topology held");
        if fenced_on_arrival {
            state.cluster.set_resize_write_fence_for_test(true);
        }
        let request_state = Arc::clone(&state);
        let mut change =
            tokio::spawn(
                async move { send(&request_state, req("PUT", "/_vocab", &pkg_vocab())).await },
            );
        let early = tokio::time::timeout(Duration::from_millis(200), &mut change).await;
        let answered_at_once = early.is_ok();
        // The fence goes up, as it does when the copy starts.
        state.cluster.set_resize_write_fence_for_test(true);
        let answer = match early {
            Ok(answer) => Some(answer),
            Err(_) => tokio::time::timeout(Duration::from_secs(5), &mut change)
                .await
                .ok(),
        };
        // Released before anything can fail.
        state.cluster.set_resize_write_fence_for_test(false);
        drop(release);
        holder.join().expect("holder");
        let (status, body) = answer
            .expect("the vocabulary change waited for the copy instead of being refused")
            .expect("request task");
        assert_eq!(
            answered_at_once, fenced_on_arrival,
            "a change that finds the fence up is refused at once; one that arrives before it \
             waits for the topology guard"
        );
        assert!(
            !status.is_success(),
            "the vocabulary change ran beside a remote resize: {status} {body}"
        );
        assert!(
            body.to_string().contains("remote resize"),
            "fenced on arrival = {fenced_on_arrival}: {status} {body}"
        );
        assert_eq!(
            matches(&state, "widget pkg").await,
            Ok(vec![]),
            "the refused change was applied"
        );
    }
}

/// The served path under load: searches whose titles route to most shards, in a pool of two
/// workers, while a thread resizes the cluster back and forth as fast as it can. Under the
/// cluster lock this stopped within seconds (ADR-207). Now nothing in the pool waits for a
/// rebuild: every search answers, with the same ids each time, and the rebuilds keep going.
#[test]
fn searches_and_rebuilds_keep_each_other_moving() {
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
    let requests = [
        (
            "/_search",
            serde_json::json!({ "documents": titles, "include_broad": true, "size": 5 }),
        ),
        (
            "/v2/_mpercolate",
            serde_json::json!({ "documents": titles[..40].to_vec(), "_source": false }),
        ),
        (
            "/v2/_mpercolate",
            serde_json::json!({ "documents": titles[40..60].to_vec() }),
        ),
    ];

    let stop = Arc::new(AtomicBool::new(false));
    let rebuilds = Arc::new(AtomicUsize::new(0));
    let rebuilder = {
        let (state, stop, rebuilds) =
            (Arc::clone(&state), Arc::clone(&stop), Arc::clone(&rebuilds));
        std::thread::spawn(move || -> Result<(), ShardError> {
            let mut shards = 9;
            while !stop.load(Ordering::SeqCst) {
                {
                    let _alone = state.admit_rebuild()?;
                    state.cluster.resize(shards)?;
                }
                shards = if shards == 9 { 8 } else { 9 };
                rebuilds.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let (run_state, run_rebuilds) = (Arc::clone(&state), Arc::clone(&rebuilds));
    let outcome = runtime.block_on(async move {
        // What each request answers, less the parts that say how long it took.
        let hits = |body: &serde_json::Value| -> String {
            fn strip(value: &mut serde_json::Value) {
                match value {
                    serde_json::Value::Object(map) => {
                        for key in ["took", "took_ms", "stats", "_shards", "timed_out"] {
                            map.remove(key);
                        }
                        map.values_mut().for_each(strip);
                    }
                    serde_json::Value::Array(items) => items.iter_mut().for_each(strip),
                    _ => {}
                }
            }
            let mut body = body.clone();
            strip(&mut body);
            body.to_string()
        };
        let mut expected: Vec<Option<String>> = vec![None; requests.len()];
        let started = Instant::now();
        let mut answered = 0usize;
        while started.elapsed() < Duration::from_secs(2) {
            let rebuilds_before = run_rebuilds.load(Ordering::SeqCst);
            let round: Vec<_> = (0..6)
                .map(|at| {
                    let which = at % requests.len();
                    let (path, body) = &requests[which];
                    let (state, path, body) = (Arc::clone(&run_state), *path, body.clone());
                    (
                        which,
                        tokio::spawn(async move { send(&state, req("POST", path, &body)).await }),
                    )
                })
                .collect();
            for (which, search) in round {
                match tokio::time::timeout(Duration::from_secs(10), search).await {
                    Ok(Ok((StatusCode::OK, body))) => {
                        let got = hits(&body);
                        match &expected[which] {
                            None => expected[which] = Some(got),
                            Some(first) if *first == got => {}
                            Some(first) => {
                                return Err(format!(
                                    "{} answered differently across a rebuild:\n{first}\n{got}",
                                    requests[which].0
                                ))
                            }
                        }
                        answered += 1;
                    }
                    Ok(other) => return Err(format!("a search answered {other:?}")),
                    Err(_) => return Err(format!("a search never answered ({answered} had)")),
                }
            }
            let turn_by = Instant::now() + Duration::from_secs(10);
            while run_rebuilds.load(Ordering::SeqCst) == rebuilds_before {
                if Instant::now() >= turn_by {
                    return Err(format!(
                        "no rebuild finished after a round ({rebuilds_before} so far)"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        Ok(answered)
    });
    stop.store(true, Ordering::SeqCst);
    // On failure the request threads may be stuck for good. Leave them: waiting for the
    // runtime would hang the test instead of failing it.
    runtime.shutdown_background();
    let answered = outcome.unwrap_or_else(|stuck| panic!("{stuck}"));
    rebuilder
        .join()
        .expect("rebuilder thread")
        .expect("every rebuild succeeds");
    assert!(answered > 0);
    assert!(rebuilds.load(Ordering::SeqCst) > 0);
}
