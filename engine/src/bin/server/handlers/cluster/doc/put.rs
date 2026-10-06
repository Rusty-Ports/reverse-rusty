use super::{
    error, extract_ranked_ingest, info, instrument, partial_write_guidance, shard_error_response,
    shard_error_status, upsert_status, warn, ApiError, Arc, ClusterAppState, Instant, IntoResponse,
    Json, Path, PutDocBody, PutDocParams, PutDocResponse, Query, QueryRejection, Response,
    ShardError, State, StatusCode, QUERY_INDEX,
};

/// PUT /_doc/{id} — cluster-atomic index/create operation (ADR-117). The default
/// upsert replaces by id under ONE coordinator log frame; `op_type=create` uses
/// the insert-only `Add` funnel and conflicts without logging when the id is live.
/// A write that not every shard took (remote clusters only) answers 503 `partial`
/// (ADR-194): the caller retries it, or `POST /_cluster/resync` converges it while
/// this coordinator stays up.
#[instrument(skip(state, params, body), fields(query_id = id))]
pub(crate) async fn cluster_put_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
    params: Result<Query<PutDocParams>, QueryRejection>,
    Json(body): Json<PutDocBody>,
) -> Response {
    let start = Instant::now();
    let params = match params {
        Ok(Query(params)) => params,
        Err(e) => {
            warn!(query_id = id, error = %e, "invalid index-document query parameters");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            state
                .prom
                .http_request_duration
                .with_label_values(&["put_doc"])
                .observe(start.elapsed().as_secs_f64());
            return ApiError::response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("invalid index-document query parameters: {e}"),
            )
            .into_response();
        }
    };
    params.acknowledge_refresh_policy();
    // A malformed tag value is a caller error: 400 before any coordinator work
    // (ADR-073 — never silently drop a tag the caller asked for).
    let tags = match extract_ranked_ingest(&body.rest) {
        Ok((tags, _rank)) => tags,
        Err((error_type, msg)) => {
            warn!(query_id = id, error = %msg, "invalid tag value");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            // Keep the latency histogram complete (mirrors the single-node
            // handler — every other exit records a duration).
            state
                .prom
                .http_request_duration
                .with_label_values(&["put_doc"])
                .observe(start.elapsed().as_secs_f64());
            if error_type == "invalid_tag_value" {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _index: QUERY_INDEX,
                        _id: id,
                        _version: None,
                        result: "error",
                        error: Some(msg),
                    }),
                )
                    .into_response();
            }
            return ApiError::response(StatusCode::BAD_REQUEST, error_type, msg).into_response();
        }
    };
    let create_only = params.create_only();
    let (query, version) = (body.query.clone(), body.version);
    let result = super::super::run_cluster_write(&state, move |cluster| {
        if create_only {
            cluster
                .create_query_with_tags(id, &query, version, &tags)
                .map(|outcome| (0, outcome))
        } else {
            cluster.upsert_query_with_tags(id, &query, version, &tags)
        }
    })
    .await;
    let response = match result {
        Ok((removed, outcome)) => {
            let (status, result, error) = upsert_status(removed, &outcome);
            match status {
                StatusCode::CREATED => info!(query_id = id, "query registered"),
                StatusCode::OK => info!(query_id = id, removed, "query replaced"),
                _ => warn!(query_id = id, result, "query rejected"),
            }
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", status.as_str()])
                .inc();
            (
                status,
                Json(PutDocResponse {
                    _index: QUERY_INDEX,
                    _id: id,
                    _version: status.is_success().then_some(body.version),
                    result,
                    error,
                }),
            )
                .into_response()
        }
        Err(ShardError::PartiallyApplied {
            ref applied,
            ref failed,
            ..
        }) => {
            warn!(
                query_id = id,
                ?applied,
                ?failed,
                "document write did not reach every shard; queued for repair"
            );
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "503"])
                .inc();
            partial_put_response(id, create_only, applied, failed)
        }
        Err(ShardError::DuplicateLogicalId(_)) if params.create_only() => {
            warn!(
                query_id = id,
                "create-only write conflicts with a live document"
            );
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "409"])
                .inc();
            ApiError::response(
                StatusCode::CONFLICT,
                "version_conflict_engine_exception",
                format!("document {id} already exists; op_type=create requires a missing id"),
            )
            .into_response()
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster document write failed");
            let status = shard_error_status(&e);
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", status.as_str()])
                .inc();
            shard_error_response("document write rejected", &e)
        }
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// The answer to a PUT that not every shard took: a retryable failure with no `_version`,
/// since no version of the document is stored cluster-wide.
fn partial_put_response(
    id: u64,
    create_only: bool,
    applied: &[usize],
    failed: &[usize],
) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(PutDocResponse {
            _index: QUERY_INDEX,
            _id: id,
            _version: None,
            result: "partial",
            error: Some(partial_write_guidance(create_only, applied, failed)),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("response JSON")
    }

    #[tokio::test]
    async fn a_partial_put_is_an_explicit_retryable_failure() {
        let response = partial_put_response(7, false, &[0], &[1]);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_of(response).await;
        assert_eq!(body["_id"], 7);
        assert_eq!(body["result"], "partial");
        assert!(
            body.get("_version").is_none(),
            "no version is stored on every shard"
        );
        let guidance = body["error"].as_str().expect("guidance");
        assert!(guidance.contains("applied on shards [0], pending on [1]"));
        assert!(guidance.contains("retry this idempotent index operation"));
        assert!(guidance.contains("/_cluster/resync"));
        for claim in ["durably logged", "reopen", "double-log"] {
            assert!(!guidance.contains(claim), "{guidance}");
        }
    }

    /// A create that is retried as a create answers 409 on a coordinator that restarted in
    /// between, so the guidance has to name the operation that converges anywhere.
    #[tokio::test]
    async fn a_partial_create_is_told_to_retry_as_an_index_operation() {
        let response = partial_put_response(7, true, &[], &[2]);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_of(response).await;
        assert_eq!(body["result"], "partial");
        let guidance = body["error"].as_str().expect("guidance");
        assert!(guidance.contains("applied on shards [], pending on [2]"));
        assert!(guidance.contains("as an index operation (without op_type=create)"));
        assert!(!guidance.contains("retry this idempotent index operation"));
    }
}
