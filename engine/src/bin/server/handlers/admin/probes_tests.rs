//! The probe routes in single-node mode. The coordinator's are tested with its health
//! route, where there is a shard and a control plane to fail.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    routing::any,
    Router,
};
use tower::ServiceExt;

use super::probes::{liveness, readiness, LIVENESS_PATH, READINESS_PATH};
use crate::state::AppState;

fn router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route(LIVENESS_PATH, any(liveness::<AppState>))
        .route(READINESS_PATH, any(readiness::<AppState>))
        .with_state(Arc::clone(state))
}

async fn send(state: &Arc<AppState>, method: Method, path: &str) -> (StatusCode, String, Vec<u8>) {
    let response = router(state)
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let cache = response
        .headers()
        .get(header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec();
    (status, cache, body)
}

#[tokio::test]
async fn the_probes_say_alive_and_ready_and_are_not_cached() {
    let state = crate::state::test_support::app_state(None);
    for (path, says) in [(LIVENESS_PATH, "alive"), (READINESS_PATH, "ready")] {
        let (status, cache, body) = send(&state, Method::GET, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(cache, "no-store", "{path}");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body, serde_json::json!({ "status": says }), "{path}");

        let (status, _, body) = send(&state, Method::HEAD, path).await;
        assert_eq!(status, StatusCode::OK, "HEAD {path}");
        assert!(body.is_empty(), "HEAD {path} has a body");

        let (status, _, _) = send(&state, Method::POST, path).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "POST {path}");
    }
}

/// A probe that waited for an admission slot would fail whenever the server is busy, and a
/// liveness probe that fails restarts the process. These take none: with every health
/// permit and the administrative slot held, they answer at once.
#[tokio::test]
async fn the_probes_take_no_admission() {
    let state = crate::state::test_support::app_state(None);
    let health = Arc::clone(&state.health_permits)
        .acquire_many_owned(state.health_permits.available_permits() as u32)
        .await
        .expect("health permits");
    let admin = Arc::clone(&state.stats_permits)
        .acquire_many_owned(state.stats_permits.available_permits() as u32)
        .await
        .expect("administrative slot");
    for path in [LIVENESS_PATH, READINESS_PATH] {
        let answer =
            tokio::time::timeout(Duration::from_millis(500), send(&state, Method::GET, path)).await;
        assert_eq!(
            answer.map(|(status, _, _)| status).ok(),
            Some(StatusCode::OK),
            "{path} waited for admission"
        );
    }
    drop((health, admin));
}
