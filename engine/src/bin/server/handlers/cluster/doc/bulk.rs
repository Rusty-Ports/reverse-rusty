use super::{
    bulk_body_rejection, bulk_query_rejection, bulk_rejection, error_item, fail_item, info,
    instrument, item_inner_mut, parse_bulk_request, pending_item, shard_error_status, succeed_item,
    Arc, BulkActionKind, BulkItem, BulkParams, BulkResponse, Bytes, BytesRejection,
    ClusterAppState, HeaderMap, Instant, IntoResponse, Json, ParsedBulkItem, Query, QueryRejection,
    Response, ShardError, State, StatusCode,
};

/// Strict coordinator HTTP boundary for `POST /_bulk`.
#[instrument(skip_all)]
pub(crate) async fn cluster_bulk_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<BulkParams>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["bulk"])
        .start_timer();
    let Query(params) = match params {
        Ok(params) => params,
        Err(error) => return bulk_query_rejection(&state.prom, &error),
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => return bulk_body_rejection(&state.prom, &error),
    };
    let items = match parse_bulk_request(&headers, &body, params) {
        Ok(items) => items,
        Err(error) => {
            return bulk_rejection(&state.prom, error.status, error.error_type, error.reason);
        }
    };
    // The batch holds `write_admission` and makes remote write RPCs, so it runs on a blocking thread,
    // never on an async worker (see `run_cluster_write`).
    let permit = match super::super::admit_cluster_write(&state).await {
        Ok(permit) => permit,
        Err(error) => {
            return bulk_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster_write_unavailable",
                error.to_string(),
            );
        }
    };
    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        cluster_bulk_inner(&worker_state, items)
    });
    match worker.await {
        Ok(response) => response,
        Err(error) => bulk_rejection(
            &state.prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "cluster_write_failed",
            format!("cluster bulk worker failed: {error}"),
        ),
    }
}

fn cluster_bulk_inner(state: &Arc<ClusterAppState>, items: Vec<ParsedBulkItem>) -> Response {
    let start = Instant::now();
    let mut responses: Vec<BulkItem> = Vec::with_capacity(items.len());
    let mut accepted = 0usize;

    // The batch is an ordinary write: it shares admission with other writes for as long as
    // it runs. Two batches that run at once interleave their items; each item is applied
    // whole, and the cluster orders writes to one id by its log (ADR-177).
    let _write = state.write_admission.read();
    let cluster = &state.cluster;
    for item in items {
        let source = match item.source {
            Ok(source) => source,
            Err(error) => {
                responses.push(error_item(
                    item.action,
                    item.id,
                    StatusCode::BAD_REQUEST,
                    error.error_type,
                    error.reason,
                ));
                continue;
            }
        };
        let mut response = pending_item(item.action, item.id);
        let result = match item.action {
            BulkActionKind::Index => {
                cluster.upsert_query_with_tags(item.id, &source.query, source.version, &source.tags)
            }
            BulkActionKind::Create => cluster
                .create_query_with_tags(item.id, &source.query, source.version, &source.tags)
                .map(|outcome| (0, outcome)),
        };
        match result {
            Ok((removed, outcome)) => {
                let (status, result, error) = super::upsert_status(removed, &outcome);
                if let Some(class) = outcome.class().filter(|_| status.is_success()) {
                    accepted += 1;
                    succeed_item(&mut response, status, source.version, result, class);
                } else {
                    fail_item(
                        &mut response,
                        status,
                        if matches!(
                            outcome,
                            reverse_rusty::cluster::AddOutcome::RejectedParse(_)
                        ) {
                            "parse_exception"
                        } else {
                            "illegal_argument_exception"
                        },
                        error.unwrap_or_else(|| "bulk item was rejected".to_string()),
                    );
                }
            }
            Err(ShardError::DuplicateLogicalId(_)) if item.action == BulkActionKind::Create => {
                fail_item(
                    &mut response,
                    StatusCode::CONFLICT,
                    "version_conflict_engine_exception",
                    format!(
                        "document {} already exists; `create` requires a missing id",
                        item.id
                    ),
                );
            }
            Err(ShardError::PartiallyApplied {
                applied, failed, ..
            }) => fail_partial_item(&mut response, item.action, &applied, &failed),
            Err(error) => {
                let status = shard_error_status(&error);
                fail_item(
                    &mut response,
                    status,
                    "cluster_write_error",
                    format!("write rejected: {error}"),
                );
            }
        }
        responses.push(response);
    }

    let errors = responses.iter_mut().any(|item| {
        let inner = item_inner_mut(item);
        inner.error.is_some()
    });
    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    info!(
        accepted,
        items = responses.len(),
        errors,
        "cluster bulk complete"
    );
    state
        .prom
        .http_requests_total
        .with_label_values(&["bulk", "200"])
        .inc();
    Json(BulkResponse {
        took: took_ms.floor() as u64,
        took_ms,
        errors,
        items: responses,
    })
    .into_response()
}

/// An item that not every shard took is a failed item (ADR-194): it is not counted as
/// accepted, carries no version, and tells the caller how to retry it.
fn fail_partial_item(
    response: &mut BulkItem,
    action: BulkActionKind,
    applied: &[usize],
    failed: &[usize],
) {
    fail_item(
        response,
        StatusCode::SERVICE_UNAVAILABLE,
        "partial_write",
        super::partial_write_guidance(action == BulkActionKind::Create, applied, failed),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partial_item_is_a_failed_item_with_retry_guidance() {
        for (action, retry) in [
            (
                BulkActionKind::Index,
                "retry this idempotent index operation",
            ),
            (
                BulkActionKind::Create,
                "as an index operation (without op_type=create)",
            ),
        ] {
            let mut item = pending_item(action, 7);
            fail_partial_item(&mut item, action, &[0], &[1]);
            let inner = item_inner_mut(&mut item);
            assert_eq!(inner.status, 503);
            assert!(inner.version.is_none() && inner.result.is_none());
            let error = inner.error.as_ref().expect("the item failed");
            assert_eq!(error.error_type, "partial_write");
            assert!(error.reason.contains(retry), "{}", error.reason);
            assert!(error.reason.contains("/_cluster/resync"));
            assert!(!error.reason.contains("durably logged"), "{}", error.reason);
        }
    }
}
