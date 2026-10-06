//! `--include-broad` sets the scope of every request that names none, on every surface
//! (ADR-201). It used to apply to the compatibility routes only: v2 search, v2 batch and
//! exhaustive jobs fell back to `standard`, so a consumer that relied on the server flag
//! and moved to one of them silently lost every class C and accepted class D match.

use super::*;
use reverse_rusty::QueryScope;

/// A corpus, a server with the given default, and a title whose match set is larger with
/// the broad lane on, with both expected id sets.
struct Fixture {
    state: Arc<AppState>,
    title: String,
    with_broad: Vec<u64>,
    without_broad: Vec<u64>,
}

fn fixture(include_broad: bool) -> Fixture {
    let (engine, titles) = corpus();
    let state = state_with(engine, include_broad);
    let title = titles
        .iter()
        .find(|t| expected_ids(&state, t, true).len() > expected_ids(&state, t, false).len())
        .expect("corpus(broad_frac=0.1) has a broad-affected title")
        .clone();
    let with_broad = expected_ids(&state, &title, true);
    let without_broad = expected_ids(&state, &title, false);
    assert!(with_broad.len() <= reverse_rusty::MAX_TOP_K);
    Fixture {
        state,
        title,
        with_broad,
        without_broad,
    }
}

/// The ids and the echoed scope of one v2 hit list.
fn ids_and_scope(response: &serde_json::Value) -> (Vec<u64>, String) {
    let mut ids: Vec<u64> = response["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["_id"].as_u64().expect("id"))
        .collect();
    ids.sort_unstable();
    let scope = response["query_scope"].as_str().expect("scope").to_string();
    (ids, scope)
}

async fn v2_search_of(fixture: &Fixture, scope: Option<&str>) -> (Vec<u64>, String) {
    let mut body = serde_json::json!({
        "document": {"title": fixture.title},
        "size": fixture.with_broad.len().max(1),
    });
    if let Some(scope) = scope {
        body["query_scope"] = scope.into();
    }
    let response = v2_search(State(Arc::clone(&fixture.state)), Json(v2_body(body)))
        .await
        .expect("v2 response");
    ids_and_scope(&serde_json::to_value(response.0).expect("json"))
}

async fn v2_batch_of(fixture: &Fixture, scope: Option<&str>) -> (Vec<u64>, String) {
    let mut body = serde_json::json!({
        "documents": [{"title": fixture.title}],
        "size": fixture.with_broad.len().max(1),
    });
    if let Some(scope) = scope {
        body["query_scope"] = scope.into();
    }
    let response = v2_mpercolate(State(Arc::clone(&fixture.state)), Json(v2_batch_body(body)))
        .await
        .expect("v2 batch response");
    let json = serde_json::to_value(response.0).expect("json");
    let (ids, _) = ids_and_scope(&serde_json::json!({
        "hits": json["responses"][0]["hits"],
        "query_scope": json["query_scope"],
    }));
    (
        ids,
        json["query_scope"].as_str().expect("scope").to_string(),
    )
}

fn job_scope_of(fixture: &Fixture, scope: Option<&str>) -> QueryScope {
    let mut body = serde_json::json!({"document": {"title": fixture.title}});
    if let Some(scope) = scope {
        body["query_scope"] = scope.into();
    }
    let created = crate::handlers::jobs::create_job_for_test(&fixture.state, body);
    let view = fixture
        .state
        .exhaustive_jobs
        .status(&created)
        .expect("retained");
    fixture.state.exhaustive_jobs.cancel(&created);
    view.query_scope
}

#[tokio::test]
async fn v2_search_without_a_scope_uses_the_server_default() {
    let on = fixture(true);
    let (ids, scope) = v2_search_of(&on, None).await;
    assert_eq!(scope, "with_broad");
    assert_eq!(ids, on.with_broad, "the server default includes broad");

    let off = fixture(false);
    let (ids, scope) = v2_search_of(&off, None).await;
    assert_eq!(scope, "standard");
    assert_eq!(ids, off.without_broad);
}

#[tokio::test]
async fn v2_batch_without_a_scope_uses_the_server_default() {
    let on = fixture(true);
    let (ids, scope) = v2_batch_of(&on, None).await;
    assert_eq!(scope, "with_broad");
    assert_eq!(ids, on.with_broad, "the server default includes broad");

    let off = fixture(false);
    let (ids, scope) = v2_batch_of(&off, None).await;
    assert_eq!(scope, "standard");
    assert_eq!(ids, off.without_broad);
}

#[tokio::test]
async fn an_exhaustive_job_without_a_scope_uses_the_server_default() {
    assert_eq!(job_scope_of(&fixture(true), None), QueryScope::WithBroad);
    assert_eq!(job_scope_of(&fixture(false), None), QueryScope::Standard);
}

/// The default is only a default: a request that names a scope gets it, in both directions.
#[tokio::test]
async fn a_request_that_names_a_scope_gets_that_scope_whatever_the_server_default() {
    let on = fixture(true);
    let (ids, scope) = v2_search_of(&on, Some("standard")).await;
    assert_eq!(
        (ids, scope.as_str()),
        (on.without_broad.clone(), "standard")
    );
    let (ids, scope) = v2_batch_of(&on, Some("standard")).await;
    assert_eq!(
        (ids, scope.as_str()),
        (on.without_broad.clone(), "standard")
    );
    assert_eq!(job_scope_of(&on, Some("standard")), QueryScope::Standard);

    let off = fixture(false);
    let (ids, scope) = v2_search_of(&off, Some("with_broad")).await;
    assert_eq!(
        (ids, scope.as_str()),
        (off.with_broad.clone(), "with_broad")
    );
    let (ids, scope) = v2_batch_of(&off, Some("with_broad")).await;
    assert_eq!(
        (ids, scope.as_str()),
        (off.with_broad.clone(), "with_broad")
    );
    assert_eq!(
        job_scope_of(&off, Some("with_broad")),
        QueryScope::WithBroad
    );
}

/// The trap this closes: with the server flag on, the compatibility route and the v2 route
/// answer the same title with the same candidates when neither request names a scope.
#[tokio::test]
async fn the_compatibility_and_v2_routes_agree_under_one_server_default() {
    for include_broad in [true, false] {
        let fixture = fixture(include_broad);
        let v1 = super::execution::search_ids(
            &fixture.state,
            serde_json::json!({
                "document": {"title": fixture.title},
                "size": fixture.with_broad.len().max(1),
            }),
        )
        .await
        .expect("compatibility search");
        let (v2, _) = v2_search_of(&fixture, None).await;
        assert_eq!(v1, v2, "include_broad={include_broad}");
    }
}
