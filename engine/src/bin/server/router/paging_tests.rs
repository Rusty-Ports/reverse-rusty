//! What each delivery surface returns when a query is deleted between two pages.
//!
//! `/_search` cuts a page out of whatever the snapshot of that request matches, so a delete
//! between two offset requests shifts every later row and one is never delivered. A point
//! in time and an exhaustive job read one snapshot from start to finish and deliver every
//! row. The recall-first integration guide tells a consumer which to use; this pins why.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::build_router;
use crate::state::test_support::app_state;

const TITLE: &str = "zzpaging widget";

async fn call(
    router: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder().method(method).uri(path);
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .expect("request");
    let response = router.clone().oneshot(request).await.expect("router");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, bytes.to_vec())
}

async fn json_call(router: &Router, method: &str, path: &str, body: Option<Value>) -> Value {
    let (status, bytes) = call(router, method, path, body).await;
    assert!(status.is_success(), "{method} {path}: {status}");
    serde_json::from_slice(&bytes).expect("JSON body")
}

/// A server holding three queries that all match [`TITLE`]: ids 1, 2 and 3.
async fn three_matches() -> Router {
    let router = build_router(app_state(None), 256);
    for (id, query) in [(1u64, "zzpaging"), (2, "widget"), (3, "zzpaging widget")] {
        json_call(
            &router,
            "PUT",
            &format!("/_doc/{id}"),
            Some(json!({ "query": query })),
        )
        .await;
    }
    router
}

fn ids(hits: &Value) -> Vec<u64> {
    hits["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["_id"].as_u64().expect("id"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offset_page_skips_a_row_after_a_delete() {
    let router = three_matches().await;
    let page = |from: u64| json!({ "document": { "title": TITLE }, "from": from, "size": 1 });

    let first = json_call(&router, "POST", "/_search", Some(page(0))).await;
    assert_eq!(first["hits"]["total"], 3);
    assert_eq!(ids(&first), vec![1]);

    json_call(&router, "DELETE", "/_doc/1", None).await;

    // The second page of what is now a two-row result is its last row. Row 2 was on
    // neither page, and both requests answered 200.
    let second = json_call(&router, "POST", "/_search", Some(page(1))).await;
    assert_eq!(second["hits"]["total"], 2);
    assert_eq!(ids(&second), vec![3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_point_in_time_cursor_delivers_every_row_across_a_delete() {
    let router = three_matches().await;
    let opened = json_call(&router, "POST", "/v2/_pit?keep_alive=1m", Some(json!({}))).await;
    let pit = opened["pit_id"].as_str().expect("pit id").to_string();
    let page = |extra: Value| {
        let mut body = json!({
            "document": { "title": TITLE },
            "include_source": false,
            "size": 1,
        });
        for (key, value) in extra.as_object().expect("object") {
            body[key] = value.clone();
        }
        body
    };

    let first = json_call(
        &router,
        "POST",
        "/v2/_search",
        Some(page(json!({ "pit": { "id": pit } }))),
    )
    .await;
    let mut delivered = ids(&first);
    assert_eq!(delivered.len(), 1);
    json_call(&router, "DELETE", &format!("/_doc/{}", delivered[0]), None).await;

    let mut cursor = first["next_cursor"].as_str().map(str::to_string);
    while let Some(token) = cursor {
        let next = json_call(
            &router,
            "POST",
            "/v2/_search",
            Some(page(json!({ "cursor": token }))),
        )
        .await;
        delivered.extend(ids(&next));
        cursor = next["next_cursor"].as_str().map(str::to_string);
    }
    delivered.sort_unstable();
    assert_eq!(delivered, vec![1, 2, 3], "every row of the frozen snapshot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exhaustive_job_delivers_every_row_across_a_delete() {
    let router = three_matches().await;
    let created = json_call(
        &router,
        "POST",
        "/_percolate/jobs",
        Some(json!({ "document": { "title": TITLE } })),
    )
    .await;
    let job = created["job_id"].as_str().expect("job id").to_string();

    // The job reads the snapshot it was created on; a delete after that does not reach it.
    json_call(&router, "DELETE", "/_doc/1", None).await;

    let (status, stream) = call(
        &router,
        "GET",
        &format!("/_percolate/jobs/{job}/stream"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let frames: Vec<Value> = std::str::from_utf8(&stream)
        .expect("UTF-8 stream")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON frame"))
        .collect();
    let mut delivered: Vec<u64> = frames
        .iter()
        .filter(|frame| frame["type"] == "match_chunk")
        .flat_map(|frame| frame["members"].as_array().expect("members").clone())
        .map(|member| member["logical_id"].as_u64().expect("logical id"))
        .collect();
    delivered.sort_unstable();
    assert_eq!(delivered, vec![1, 2, 3]);
    let completion = frames.last().expect("completion frame");
    assert_eq!(completion["type"], "completion");
    assert_eq!(completion["exact_total"], 3);
}
