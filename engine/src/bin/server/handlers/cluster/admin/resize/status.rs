//! `GET /_cluster/resize` and `GET /_cluster/resize/{operation_id}` (ADR-179): read-only
//! views of the bounded operation registry and the latest autoscaler observation. They read
//! neither the cluster nor the guards a rebuild holds, so they stay responsive while one runs.

use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::resize_ops::{AutoscaleStatus, ResizeOperation};
use crate::state::ClusterAppState;

use super::{cluster_resize_rejection, finish_cluster_resize_response, CLUSTER_RESIZE_ENDPOINT};

#[derive(Serialize)]
struct AutoscaleView {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_observation: Option<AutoscaleStatus>,
}

#[derive(Serialize)]
struct ResizeOperationsView {
    operations: Vec<ResizeOperation>,
    autoscaler: AutoscaleView,
}

fn reject_query(state: &ClusterAppState, request: &Request) -> Option<Response> {
    request.uri().query().filter(|q| !q.is_empty()).map(|_| {
        cluster_resize_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "resize status reads accept no query parameters",
        )
    })
}

pub(super) fn list_resize_operations(state: &ClusterAppState, request: &Request) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[CLUSTER_RESIZE_ENDPOINT])
        .start_timer();
    if let Some(rejection) = reject_query(state, request) {
        return rejection;
    }
    let ops = &state.resize_operations;
    finish_cluster_resize_response(
        &state.prom,
        Json(ResizeOperationsView {
            operations: ops.list(),
            autoscaler: AutoscaleView {
                enabled: ops.autoscale_enabled(),
                last_observation: ops.autoscale_status(),
            },
        })
        .into_response(),
    )
}

pub(super) fn get_resize_operation(
    state: &ClusterAppState,
    request: &Request,
    operation_id: &str,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[CLUSTER_RESIZE_ENDPOINT])
        .start_timer();
    if let Some(rejection) = reject_query(state, request) {
        return rejection;
    }
    match state.resize_operations.get(operation_id) {
        Some(record) => finish_cluster_resize_response(&state.prom, Json(record).into_response()),
        None => cluster_resize_rejection(
            &state.prom,
            StatusCode::NOT_FOUND,
            "resize_operation_not_found",
            "no retained resize operation has that operation_id",
        ),
    }
}
