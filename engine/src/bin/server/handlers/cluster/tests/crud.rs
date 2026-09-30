use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_reports_cluster_mode() {
    let state = test_state(&seed());
    let (status, body) = send(&state, req_empty("GET", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "reverse-rusty");
    assert_eq!(body["cluster_name"], "reverse-rusty");
    assert_eq!(body["cluster_uuid"], "_na_");
    assert_eq!(body["version"]["distribution"], "reverse-rusty");
    assert_eq!(body["version"]["number"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["shards"], 3);

    let response = router(&state)
        .oneshot(req_empty("HEAD", "/"))
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HEAD body")
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_search_delete_round_trip() {
    let state = test_state(&seed());

    // Create.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1996 vertex"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 10);
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "created");
    assert!(body.get("error").is_none());

    // Search finds it (with per-request include_broad).
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 vertex premium"}, "include_broad": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(ids.contains(&10), "hits: {ids:?}");

    // Replace (upsert): old stops matching, new matches; 200 updated.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1997 metal"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "updated");
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 vertex premium"}}),
        ),
    )
    .await;
    let old_hits: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(!old_hits.contains(&10), "old version must stop matching");

    // GET returns the new source.
    let (status, body) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_source"]["query"], "1997 metal");

    // Delete; then 404.
    let (status, _) = send(&state, req_empty("DELETE", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_doc_matches_single_node_contract_and_rejects_controls_before_mutation() {
    let state = test_state(&seed());
    for (id, refresh) in [(80, "false"), (81, "true"), (82, "wait_for")] {
        let (status, _) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({"query":format!("zzdelete{id}"), "version": 7}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, body) = send(
            &state,
            req_empty("DELETE", &format!("/_doc/{id}?refresh={refresh}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_index"], "queries");
        assert_eq!(body["_id"], id);
        assert_eq!(body["result"], "deleted");
        assert_eq!(body["deleted_count"], 1);
        assert!(body.get("_version").is_none());
        assert!(body.get("_shards").is_none());

        let (status, _) = send(&state, req_empty("GET", &format!("/_doc/{id}"))).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "every refresh policy must publish before response"
        );
    }

    let (status, missing) = send(&state, req_empty("DELETE", "/_doc/80")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["_index"], "queries");
    assert_eq!(missing["result"], "not_found");
    assert!(missing.get("deleted_count").is_none());

    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/90",
            &serde_json::json!({"query":"espresso machine"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for suffix in [
        "refresh=immediate",
        "routing=custom",
        "version=1",
        "refresh=true&refresh=false",
    ] {
        let (status, body) = send(&state, req_empty("DELETE", &format!("/_doc/90?{suffix}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            send(&state, req_empty("GET", "/_doc/90")).await.0,
            StatusCode::OK,
            "invalid controls must not delete the live query"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_doc_create_only_and_query_parameter_contract_match_single_node() {
    let state = test_state(&seed());
    let first = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=wait_for",
            &serde_json::json!({"query":"wireless mouse","version":7}),
        ),
    );
    let second = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=true",
            &serde_json::json!({"query":"mechanical keyboard","version":8}),
        ),
    );
    let (a, b) = tokio::join!(first, second);
    let mut statuses = [a.0, b.0];
    statuses.sort_by_key(StatusCode::as_u16);
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    let (created, conflict) = if a.0 == StatusCode::CREATED {
        (a.1, b.1)
    } else {
        (b.1, a.1)
    };
    assert_eq!(created["_index"], "queries");
    assert!(
        created["_version"] == 7 || created["_version"] == 8,
        "the winning caller's display version is returned"
    );
    assert_eq!(
        conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, current) = send(&state, req_empty("GET", "/_doc/70")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        current["_source"]["query"] == "wireless mouse"
            || current["_source"]["query"] == "mechanical keyboard",
        "one complete create body wins"
    );
    assert_eq!(current["_version"], created["_version"]);

    let (status, malformed_conflict) = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create",
            &serde_json::json!({"query":"("}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        malformed_conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, invalid) = send(
        &state,
        req(
            "PUT",
            "/_doc/71?routing=custom",
            &serde_json::json!({"query":"espresso machine"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["type"], "illegal_argument_exception");
    let (status, _) = send(&state, req_empty("HEAD", "/_doc/71")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unsupported parameters reject before mutation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_doc_reads_back_post_freeze_tags_filters_and_head_status() {
    // The seed freezes an empty tag dictionary. These tags therefore use the
    // synthetic-id path internally; GET must read the canonical raw metadata
    // retained with the source, not attempt an impossible TagId reverse lookup.
    let state = test_state(&seed());
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/71",
            &serde_json::json!({
                "query": "acme chrome",
                "version": 9,
                "tags": {"tenant": "acme", "colors": ["red", "blue"]}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&state, req_empty("GET", "/_doc/71")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 9);
    assert_eq!(body["_source"]["query"], "acme chrome");
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");
    assert_eq!(
        body["_source"]["tags"]["colors"],
        serde_json::json!(["blue", "red"])
    );

    let (status, body) = send(
        &state,
        req_empty("GET", "/_doc/71?_source_includes=tags.tenant"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["_source"],
        serde_json::json!({"tags": {"tenant": "acme"}})
    );

    for (path, expected) in [
        ("/_doc/71", StatusCode::OK),
        ("/_doc/72", StatusCode::NOT_FOUND),
    ] {
        let (status, body) = send(&state, req_empty("HEAD", path)).await;
        assert_eq!(status, expected);
        assert!(body.is_null(), "HEAD must be bodyless");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejections_are_loud_not_silent() {
    let state = test_state(&seed());

    // Class-D upsert → 400 naming the boundary; the prior version (none) untouched.
    let (status, body) = send(
        &state,
        req("PUT", "/_doc/11", &serde_json::json!({"query": "-onlyneg"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");

    // explain → 400, never silently un-explained. (`rank` is SUPPORTED since ADR-075 —
    // covered by `ranked_search_orders_by_score`.)
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "x"}, "explain": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("explain"));

    // Compaction aliases + PUT /_settings → 501 with the alternative named.
    for uri in ["/_compact", "/_forcemerge"] {
        let (status, body) = send(&state, req_empty("POST", uri)).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("_checkpoint"));
    }
    let (status, _) = send(
        &state,
        req("PUT", "/_settings", &serde_json::json!({"max_segments": 4})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

/// A write waiting on `write_serial` must wait on a blocking thread, never on an async worker: on
/// a single-threaded runtime a parked worker stalls every other request (and, with remote shards,
/// the RPCs the serializer's holder needs to finish).
#[test]
fn a_queued_write_never_parks_the_async_runtime() {
    let state = test_state(&seed());
    let holder_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _writes = holder_state.write_serial.lock();
        locked_sender.send(()).expect("signal the held serializer");
        release_receiver.recv().expect("release the serializer");
    });
    locked_receiver.recv().expect("serializer held");

    let (read_sender, read_receiver) = std::sync::mpsc::channel();
    let server_state = Arc::clone(&state);
    let server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let put_state = Arc::clone(&server_state);
            let put = tokio::spawn(async move {
                send(
                    &put_state,
                    req(
                        "PUT",
                        "/_doc/20",
                        &serde_json::json!({"query": "1997 acme"}),
                    ),
                )
                .await
            });
            // Let the PUT start and queue behind the held serializer.
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            let (status, _) = send(&server_state, req_empty("GET", "/")).await;
            read_sender.send(status).expect("report the read");
            put.await.expect("put task")
        })
    });
    let read = read_receiver.recv_timeout(std::time::Duration::from_secs(10));
    release_sender.send(()).expect("release");
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
    let admitted = crate::state::MAX_QUEUED_CLUSTER_WRITES;
    let requested = admitted + 4;
    let state = test_state(&seed());
    let holder_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let holder = std::thread::spawn(move || {
        let _writes = holder_state.write_serial.lock();
        locked_sender.send(()).expect("signal the held serializer");
        // A closed channel also releases, so a failing test cannot strand the queued writers.
        let _ = release_receiver.recv();
    });
    locked_receiver.recv().expect("serializer held");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let run_state = Arc::clone(&state);
    let (held_after_cancel, applied) = runtime.block_on(async move {
        let writes: Vec<_> = (0..requested)
            .map(|index| {
                let put_state = Arc::clone(&run_state);
                tokio::spawn(async move {
                    let body = serde_json::json!({"query": "1997 acme"});
                    send(
                        &put_state,
                        req("PUT", &format!("/_doc/{}", 100 + index), &body),
                    )
                    .await
                })
            })
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
        drop(release_sender);
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

async fn wait_until(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !done() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}
