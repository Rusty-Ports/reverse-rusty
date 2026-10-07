//! The two probe routes, for whatever supervises this process (ADR-211).
//!
//! - `GET`/`HEAD /_health/live`: is this process up, and is its runtime turning?
//! - `GET`/`HEAD /_health/ready`: has it assembled what it serves, and is it accepting
//!   connections?
//!
//! A liveness probe asks whether restarting the process would help, and a readiness probe
//! whether it should be sent traffic. Neither is the question `/_health` answers, which is
//! whether everything the cluster needs is well: `/_health` calls every shard and the control
//! plane and waits for an admission slot, on purpose, and is red when one of them does not
//! answer. Pointed at `/_health`, a liveness probe restarts a healthy coordinator because a
//! shard is down, and a readiness probe takes it out of service when it could still answer
//! for every other shard.
//!
//! So these two depend on nothing a restart cannot fix. They run on the main listener and
//! the async runtime, take no permit, and read nothing from the engine or the network. If
//! one does not answer, the listener is not accepting or the runtime is not turning.
//!
//! They answer the same way today. The listener binds only once the engine or the cluster
//! is assembled, and a graceful shutdown closes it first, so a process that answers at all
//! is both alive and ready. They are two routes because they are two contracts: readiness
//! may come to say more (a coordinator cut off from every shard, for one), and liveness
//! must not.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::RequestCtx;

pub(crate) const LIVENESS_PATH: &str = "/_health/live";
pub(crate) const READINESS_PATH: &str = "/_health/ready";

#[derive(Serialize)]
struct ProbeResponse {
    status: &'static str,
}

/// `GET`/`HEAD /_health/live`.
pub(crate) async fn liveness<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    answer(state.prom(), &method, "health_live", "alive")
}

/// `GET`/`HEAD /_health/ready`.
pub(crate) async fn readiness<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    answer(state.prom(), &method, "health_ready", "ready")
}

fn answer(
    prom: &PrometheusMetrics,
    method: &Method,
    endpoint: &'static str,
    status: &'static str,
) -> Response {
    let mut response = if method == Method::GET || method == Method::HEAD {
        Json(ProbeResponse { status }).into_response()
    } else {
        let mut refused = ApiError::response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "GET and HEAD are the only supported probe methods",
        )
        .into_response();
        refused
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        refused
    };
    prom.http_requests_total
        .with_label_values(&[endpoint, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    response
}
