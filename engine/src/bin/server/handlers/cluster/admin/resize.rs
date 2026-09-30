//! Strict native `/_cluster/resize`: the in-process blue/green rebuild (`POST`) and its
//! operation records (`GET`).
//!
//! Elasticsearch and OpenSearch resize one named index into a distinct target
//! index through `_split` or `_shrink`. Reverse Rusty instead replaces one
//! in-process reverse-query ring in place after rebuilding the complete live
//! corpus. Keep that semantic boundary explicit while adopting the manager
//! timeout spellings that map exactly to waiting for administrative admission
//! and exclusive topology access before the rebuild starts. Operation IDs,
//! placement-generation preconditions, and the status reads are ADR-179.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use reverse_rusty::cluster::ShardError;

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::resize_ops::{valid_operation_id, ResizeAdmission, ResizeOperation, ResizeOrigin};
use crate::state::{ClusterAppState, ClusterRebalanceTopology};

use super::super::shard_error_status;

mod execute;
mod status;
mod supervisor;

pub(crate) use execute::{run_resize, ResizeRun, ResizeRunOutcome};
use supervisor::ClusterResizeWorkerFailure;

pub(crate) const CLUSTER_RESIZE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_RESIZE_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_RESIZE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_RESIZE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
/// One ring entry carries 128 virtual nodes by default. Bound the public API so
/// a tiny JSON request cannot allocate an effectively unbounded ring/shard set.
const MAX_CLUSTER_RESIZE_SHARDS: usize = 1_024;
const CLUSTER_RESIZE_ENDPOINT: &str = "cluster_resize";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResizeParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterResizeParams {
    fn manager_timeout(self) -> Result<Duration, String> {
        if self.cluster_manager_timeout.is_some() && self.master_timeout.is_some() {
            return Err(
                "`cluster_manager_timeout` and `master_timeout` are aliases; specify exactly one"
                    .to_string(),
            );
        }
        let timeout = self
            .cluster_manager_timeout
            .or(self.master_timeout)
            .as_deref()
            .map(parse_cluster_resize_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_RESIZE_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_RESIZE_MANAGER_TIMEOUT {
            return Err("resize manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_resize_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResizeBody {
    num_shards: usize,
    #[serde(default, deserialize_with = "present")]
    operation_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    if_placement_generation: Option<u64>,
}

/// Optional body fields may be omitted but, like every resize field, never `null`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl ClusterResizeBody {
    fn validate(self) -> Result<ClusterResizeRequest, String> {
        if self.num_shards == 0 {
            return Err("`num_shards` must be at least 1".to_string());
        }
        if self.num_shards > MAX_CLUSTER_RESIZE_SHARDS {
            return Err(format!(
                "`num_shards` must not exceed {MAX_CLUSTER_RESIZE_SHARDS}"
            ));
        }
        if let Some(id) = self.operation_id.as_deref() {
            if !valid_operation_id(id) {
                return Err(format!(
                    "`operation_id` must be 1..={} characters of ASCII letters, digits, `-`, \
                     `_`, `.`, or `:`",
                    crate::resize_ops::MAX_RESIZE_OPERATION_ID_LEN
                ));
            }
        }
        Ok(ClusterResizeRequest {
            num_shards: self.num_shards,
            operation_id: self.operation_id,
            if_placement_generation: self.if_placement_generation,
        })
    }
}

/// A validated resize body.
struct ClusterResizeRequest {
    num_shards: usize,
    operation_id: Option<String>,
    if_placement_generation: Option<u64>,
}

pub(crate) struct ClusterResizeTransport {
    duration: HistogramTimer,
    manager_timeout: Duration,
    request: ClusterResizeRequest,
}

impl FromRequest<Arc<ClusterAppState>> for ClusterResizeTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_RESIZE_ENDPOINT])
            .start_timer();
        let Query(params) =
            Query::<ClusterResizeParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid resize query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_resize_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;
        if !is_json_content_type(request.headers()) {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST /_cluster/resize requires Content-Type: application/json",
            ));
        }

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_RESIZE_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let bytes = tokio::time::timeout(
            CLUSTER_RESIZE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_resize_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resize body did not complete within 250ms",
            )
        })?;
        // Tokio polls a ready body before its timeout timer. Enforce the
        // absolute boundary too when this task was starved past the deadline.
        if Instant::now() >= body_deadline {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resize body did not complete within 250ms",
            ));
        }
        let bytes = bytes.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_resize_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid resize body: {source}"),
            )
        })?;
        if bytes
            .iter()
            .find(|byte| !byte.is_ascii_whitespace())
            .copied()
            != Some(b'{')
        {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "the resize JSON body must be an object",
            ));
        }
        let resize = serde_json::from_slice::<ClusterResizeBody>(&bytes)
            .map_err(|source| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid resize JSON body: {source}"),
                )
            })?
            .validate()
            .map_err(|reason| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    reason,
                )
            })?;

        Ok(Self {
            duration,
            manager_timeout,
            request: resize,
        })
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
}

#[derive(Debug)]
pub(crate) struct ClusterResizeSuccess {
    pub(crate) old_num_shards: usize,
    pub(crate) num_shards: usize,
    pub(crate) rebuilt: usize,
    pub(crate) version: u64,
    pub(crate) placement_generation: u64,
}

enum ClusterResizeWorkerOutcome {
    NotStarted,
    PreconditionFailed { current: u64 },
    Finished(Result<ClusterResizeSuccess, ShardError>),
}

type ClusterResizeWorkerResult = Result<ClusterResizeWorkerOutcome, ClusterResizeWorkerFailure>;

#[derive(Serialize)]
struct ClusterResizeResponse {
    acknowledged: bool,
    shards_acknowledged: bool,
    version: u64,
    old_num_shards: usize,
    num_shards: usize,
    rebuilt: usize,
    placement_generation: u64,
    operation_id: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    replayed: bool,
}

/// `/_cluster/resize`: `GET` lists retained operations and autoscaler state; `POST`
/// rebuilds every live query under a fresh in-process ring and atomically swaps the
/// serving cluster. Admission and all blocking locks remain off Tokio.
#[instrument(skip_all)]
pub(crate) async fn cluster_resize(
    State(state): State<Arc<ClusterAppState>>,
    request: Request,
) -> Response {
    match *request.method() {
        Method::GET => status::list_resize_operations(&state, &request),
        Method::POST => match ClusterResizeTransport::from_request(request, &state).await {
            Ok(transport) => post_cluster_resize(state, transport).await,
            Err(rejection) => rejection,
        },
        _ => method_not_allowed(&state.prom, "GET, POST"),
    }
}

/// `GET /_cluster/resize/{operation_id}`: one retained operation record.
#[instrument(skip_all)]
pub(crate) async fn cluster_resize_operation(
    State(state): State<Arc<ClusterAppState>>,
    Path(operation_id): Path<String>,
    request: Request,
) -> Response {
    if request.method() != Method::GET {
        return method_not_allowed(&state.prom, "GET");
    }
    status::get_resize_operation(&state, &request, &operation_id)
}

fn method_not_allowed(prom: &PrometheusMetrics, allow: &'static str) -> Response {
    let mut response = cluster_resize_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("supported methods: {allow}"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static(allow));
    response
}

async fn post_cluster_resize(
    state: Arc<ClusterAppState>,
    transport: ClusterResizeTransport,
) -> Response {
    let ClusterResizeTransport {
        duration: _duration,
        manager_timeout,
        request,
    } = transport;
    if state.rebalance_topology != ClusterRebalanceTopology::InProcess {
        return cluster_resize_rejection(
            &state.prom,
            StatusCode::NOT_IMPLEMENTED,
            "not_supported_in_cluster_mode",
            "remote cluster resize is not implemented; build a separate cluster at the target \
             shard count, re-ingest and validate the corpus, then cut traffic over",
        );
    }
    if Instant::now().checked_add(manager_timeout).is_none() {
        return cluster_resize_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "resize manager timeout is too large for this platform",
        );
    }

    let operation_id = match state.resize_operations.admit(
        request.operation_id,
        ResizeOrigin::Api,
        request.num_shards,
        request.if_placement_generation,
    ) {
        ResizeAdmission::Execute(id) => id,
        ResizeAdmission::Replay(record) => return replay_response(&state.prom, &record),
        ResizeAdmission::InProgress(record) => {
            return cluster_resize_rejection(
                &state.prom,
                StatusCode::CONFLICT,
                "resize_in_progress",
                format!(
                    "resize operation {} is still {}; poll GET /_cluster/resize/{} for its \
                     terminal state",
                    record.operation_id,
                    serde_json::to_string(&record.state).unwrap_or_default(),
                    record.operation_id
                ),
            );
        }
        ResizeAdmission::Conflict(record) => {
            return cluster_resize_rejection(
                &state.prom,
                StatusCode::CONFLICT,
                "operation_id_conflict",
                format!(
                    "operation_id {} already names a different retained resize request",
                    record.operation_id
                ),
            );
        }
        ResizeAdmission::Full => {
            return cluster_resize_rejection(
                &state.prom,
                StatusCode::TOO_MANY_REQUESTS,
                "resize_registry_full",
                "every retained resize operation is still active",
            );
        }
    };

    let outcome = run_resize(
        &state,
        ResizeRun {
            operation_id: operation_id.clone(),
            num_shards: request.num_shards,
            if_placement_generation: request.if_placement_generation,
            manager_timeout,
        },
    )
    .await;
    finish_cluster_resize_run(&state.prom, operation_id, outcome)
}

fn replay_response(prom: &PrometheusMetrics, record: &ResizeOperation) -> Response {
    let Some(outcome) = record.outcome else {
        return cluster_resize_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "resize_unavailable",
            "a succeeded resize record has no recorded outcome",
        );
    };
    finish_cluster_resize_response(
        prom,
        Json(ClusterResizeResponse {
            acknowledged: true,
            shards_acknowledged: true,
            version: outcome.version,
            old_num_shards: outcome.old_num_shards,
            num_shards: outcome.num_shards,
            rebuilt: outcome.rebuilt,
            placement_generation: outcome.placement_generation,
            operation_id: record.operation_id.clone(),
            replayed: true,
        })
        .into_response(),
    )
}

fn finish_cluster_resize_run(
    prom: &PrometheusMetrics,
    operation_id: String,
    outcome: ResizeRunOutcome,
) -> Response {
    let rejected = |status: StatusCode, error_type: &str, reason: String| {
        cluster_resize_operation_rejection(prom, status, error_type, &reason, &operation_id)
    };
    match outcome {
        ResizeRunOutcome::Succeeded(success) => finish_cluster_resize_response(
            prom,
            Json(ClusterResizeResponse {
                acknowledged: true,
                shards_acknowledged: true,
                version: success.version,
                old_num_shards: success.old_num_shards,
                num_shards: success.num_shards,
                rebuilt: success.rebuilt,
                placement_generation: success.placement_generation,
                operation_id,
                replayed: false,
            })
            .into_response(),
        ),
        ResizeRunOutcome::NotStarted => rejected(
            StatusCode::REQUEST_TIMEOUT,
            "resize_timeout",
            NOT_STARTED_REASON.to_string(),
        ),
        ResizeRunOutcome::Unavailable(reason) => rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            "resize_unavailable",
            reason.to_string(),
        ),
        ResizeRunOutcome::PreconditionFailed { current } => rejected(
            StatusCode::CONFLICT,
            "placement_generation_mismatch",
            format!(
                "the serving placement generation is {current}; the resize precondition was not \
                 met and no resize was started"
            ),
        ),
        ResizeRunOutcome::Failed(source) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let failure = execute::started_failure(&source);
            rejected(status, &failure.error_type, failure.reason)
        }
        ResizeRunOutcome::WorkerFailed => rejected(
            StatusCode::INTERNAL_SERVER_ERROR,
            "resize_unavailable",
            "resize worker failed".to_string(),
        ),
    }
}

/// A structured error that also names the admitted operation, so a caller that omitted
/// `operation_id` can still inspect and retry that exact operation (ADR-179).
fn cluster_resize_operation_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &str,
    reason: &str,
    operation_id: &str,
) -> Response {
    finish_cluster_resize_response(
        prom,
        (
            status,
            Json(serde_json::json!({
                "error": { "type": error_type, "reason": reason },
                "status": status.as_u16(),
                "operation_id": operation_id,
            })),
        )
            .into_response(),
    )
}

const NOT_STARTED_REASON: &str =
    "timed out waiting for resize admission or exclusive cluster access; no resize was started";

fn cluster_resize_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_resize_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_resize_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_RESIZE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
