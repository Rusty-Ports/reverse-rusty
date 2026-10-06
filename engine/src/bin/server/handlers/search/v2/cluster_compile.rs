//! The coordinator's ranked-search compile step.
//!
//! One brief read of the cluster engine: compile the rank program, pre-check the PIT and
//! compute the request fingerprint. It runs on a blocking thread under read admission
//! (ADR-191), because the cluster lock is not available while a vocabulary rebuild or a
//! resize holds or waits for it, and a request waiting for it must not park an async worker.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use reverse_rusty::cluster::ClusterRankedError;
use reverse_rusty::{CompiledRankProgram, PitId, QueryScope, RankProgramSpec, TopKOptions};

use super::delivery::{failure_response, DeliveryFailure};
use super::{page, rank_program_error, record_outcome, ApiError, ClusterAppState, Reject};

/// Run a compile step inside the request's deadline. The step waits for read admission, a
/// blocking thread and the cluster lock, and all of that counts against the request's
/// timeout, as the wait for a search permit does (ADR-099). `None` when the deadline passed
/// first; the admitted worker still finishes and frees its permit on its own.
///
/// A timeout too large to represent has no deadline. It is reported after the compile, as
/// it always was, so the step runs unbounded here.
pub(in crate::handlers::search) async fn within_deadline<T>(
    deadline: Option<Instant>,
    step: impl Future<Output = T>,
) -> Option<T> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), step)
            .await
            .ok(),
        None => Some(step.await),
    }
}

/// The 408 a ranked request answers when its compile step did not finish in time: the same
/// response, metrics and log line as a delivery that timed out.
pub(in crate::handlers::search) fn timed_out(
    state: &ClusterAppState,
    started: Instant,
    options: TopKOptions,
    timeout: Duration,
    label: &'static str,
) -> Reject {
    failure_response::<ClusterAppState, ClusterRankedError>(
        state,
        started,
        options,
        timeout,
        label,
        DeliveryFailure::Elapsed,
    )
}

pub(super) struct CompileRequest {
    pub(super) rank: RankProgramSpec,
    /// The pinned view of a paged request, already verified as a token.
    pub(super) pit: Option<PitId>,
    /// The fingerprint a cursor was minted for; a different request is a client mismatch.
    pub(super) expected_fingerprint: Option<[u8; 32]>,
    pub(super) title: String,
    pub(super) filter: Vec<(String, Vec<String>)>,
    pub(super) scope: QueryScope,
}

/// Compile under the cluster lock. The stale gate runs BEFORE the fingerprint so a rebuilt
/// normalizer cannot mis-classify a dead cursor as a client mismatch; the kernel re-gates
/// inside its own blocking closure, so the gap between here and there stays fail-closed.
///
/// The worker only classifies a failure; the outcome metric is recorded here, by the request
/// that receives the result. A request that already answered 408 has dropped this future, so
/// a worker that finishes afterwards cannot count the same request a second time.
pub(super) async fn compile(
    state: &Arc<ClusterAppState>,
    request: CompileRequest,
) -> Result<(CompiledRankProgram, Option<page::MintCtx>), Reject> {
    let scope = request.scope;
    let profiles = Arc::clone(&state.rank_profiles);
    let compiled = crate::handlers::cluster::read_cluster(state, move |cluster| {
        let CompileRequest {
            rank,
            pit,
            expected_fingerprint,
            title,
            filter,
            scope,
        } = request;
        let program = cluster
            .compile_rank_program_with_profiles(&rank, &profiles)
            .map_err(|error| ("validation", rank_program_error(&error)))?;
        let Some(pit) = pit else {
            return Ok((program, None));
        };
        if let Err(error) = cluster.check_pit(pit, Instant::now()) {
            let (status, kind, outcome) = error.v2_http_class();
            return Err((
                outcome,
                ApiError::response(
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    kind,
                    error.to_string(),
                ),
            ));
        }
        let fingerprint = crate::pit::request_fingerprint(
            cluster.normalizer(),
            cluster.dict(),
            &title,
            scope,
            &rank,
            &filter,
        );
        if expected_fingerprint.is_some_and(|expected| expected != fingerprint) {
            return Err(("cursor_mismatch", crate::pit::cursor_mismatch_response()));
        }
        Ok((program, Some(page::MintCtx { pit, fingerprint })))
    })
    .await;
    match compiled {
        Ok(Ok(compiled)) => Ok(compiled),
        Ok(Err((outcome, reject))) => {
            record_outcome(&state.prom, outcome, scope);
            Err(reject)
        }
        Err(error) => {
            record_outcome(&state.prom, "error", scope);
            Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                format!("ranked search could not read the cluster: {error}"),
            ))
        }
    }
}
