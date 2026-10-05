//! The coordinator's ranked-search compile step.
//!
//! One brief read of the cluster engine: compile the rank program, pre-check the PIT and
//! compute the request fingerprint. It runs on a blocking thread under read admission
//! (ADR-191), because the cluster lock is not available while a vocabulary rebuild or a
//! resize holds or waits for it, and a request waiting for it must not park an async worker.

use std::sync::Arc;
use std::time::Instant;

use axum::http::StatusCode;
use reverse_rusty::{CompiledRankProgram, PitId, QueryScope, RankProgramSpec};

use super::{page, rank_program_error, record_outcome, ApiError, ClusterAppState, Reject};

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
pub(super) async fn compile(
    state: &Arc<ClusterAppState>,
    request: CompileRequest,
) -> Result<(CompiledRankProgram, Option<page::MintCtx>), Reject> {
    let scope = request.scope;
    let worker_state = Arc::clone(state);
    let compiled = crate::handlers::cluster::read_cluster(state, move |cluster| {
        let state = worker_state;
        let CompileRequest {
            rank,
            pit,
            expected_fingerprint,
            title,
            filter,
            scope,
        } = request;
        let program = match cluster.compile_rank_program_with_profiles(&rank, &state.rank_profiles)
        {
            Ok(program) => program,
            Err(error) => {
                record_outcome(&state.prom, "validation", scope);
                return Err(rank_program_error(&error));
            }
        };
        let Some(pit) = pit else {
            return Ok((program, None));
        };
        if let Err(error) = cluster.check_pit(pit, Instant::now()) {
            let (status, kind, outcome) = error.v2_http_class();
            record_outcome(&state.prom, outcome, scope);
            return Err(ApiError::response(
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                kind,
                error.to_string(),
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
            record_outcome(&state.prom, "cursor_mismatch", scope);
            return Err(crate::pit::cursor_mismatch_response());
        }
        Ok((program, Some(page::MintCtx { pit, fingerprint })))
    })
    .await;
    match compiled {
        Ok(compiled) => compiled,
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
