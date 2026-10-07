//! The coordinator's ranked-search compile step.
//!
//! One brief read of the cluster engine: compile the rank program, pre-check the PIT and
//! compute the request fingerprint. It runs on a blocking thread under read admission
//! (ADR-191): the PIT check can call a remote shard, and a request waiting for that must not
//! park an async worker.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use reverse_rusty::cluster::ClusterRankedError;
use reverse_rusty::{CompiledRankProgram, PitId, QueryScope, RankProgramSpec, TopKOptions};

use super::delivery::{failure_response, DeliveryFailure};
use super::{page, rank_program_error, record_outcome, ApiError, ClusterAppState, Reject};

/// Run a compile step inside the request's deadline. The step waits for read admission and
/// a blocking thread, and all of that counts against the request's
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

/// Check a cursor's point in time and compute the request's fingerprint, in that order.
///
/// The stale gate runs first, so that a rebuilt normalizer cannot make a dead cursor look
/// like a client's mistake. A rebuild can also swap its layout in between the gate and the
/// fingerprint: the fingerprint is then computed under a normalizer the cursor was not minted
/// under, and it does not match. That cursor is stale too, so the gate is asked again before
/// the request is blamed.
pub(super) fn cursor_fingerprint(
    cluster: &reverse_rusty::cluster::ClusterEngine,
    pit: PitId,
    expected: Option<[u8; 32]>,
    fingerprint: impl FnOnce() -> [u8; 32],
) -> Result<[u8; 32], (&'static str, Reject)> {
    let stale_gate = || {
        cluster.check_pit(pit, Instant::now()).map_err(|error| {
            let (status, kind, outcome) = error.v2_http_class();
            (
                outcome,
                ApiError::response(
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    kind,
                    error.to_string(),
                ),
            )
        })
    };
    stale_gate()?;
    let fingerprint = fingerprint();
    if expected.is_some_and(|expected| expected != fingerprint) {
        stale_gate()?;
        return Err(("cursor_mismatch", crate::pit::cursor_mismatch_response()));
    }
    Ok(fingerprint)
}

/// Compile against the cluster. The stale gate runs BEFORE the fingerprint so a rebuilt
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
        let fingerprint = cursor_fingerprint(cluster, pit, expected_fingerprint, || {
            // The normalizer and the dictionary of one layout.
            cluster.read_on_one_layout(|| {
                crate::pit::request_fingerprint(
                    &cluster.normalizer(),
                    &cluster.dict(),
                    &title,
                    scope,
                    &rank,
                    &filter,
                )
            })
        })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
    use reverse_rusty::{Normalizer, PitConfig};

    fn cluster() -> ClusterEngine {
        let config = ClusterConfig {
            num_shards: 3,
            include_broad: true,
            ..Default::default()
        };
        let queries: Vec<(u64, String)> = (1..=20u64)
            .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 5)))
            .collect();
        ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &queries,
        )
        .expect("cluster")
    }

    /// A rebuild that swaps its layout in between the stale gate and the fingerprint leaves a
    /// fingerprint that does not match. The cursor is stale; the request is not at fault.
    #[test]
    fn a_cursor_that_a_rebuild_overtook_is_stale_not_mismatched() {
        let cluster = cluster();
        let pit = cluster
            .open_pit(None, &PitConfig::default(), Instant::now())
            .expect("open");
        let minted_under_the_old_layout = [1u8; 32];
        let (outcome, (status, _)) =
            cursor_fingerprint(&cluster, pit, Some(minted_under_the_old_layout), || {
                // The swap lands here: after the gate passed, before the fingerprint.
                cluster.resize(4).expect("resize");
                [2u8; 32]
            })
            .expect_err("the cursor is dead");
        assert_ne!(outcome, "cursor_mismatch");
        assert_eq!(status, StatusCode::CONFLICT);
    }

    /// With no rebuild, a fingerprint that does not match is the request's doing, and one that
    /// matches is returned.
    #[test]
    fn a_live_cursor_is_judged_by_its_fingerprint() {
        let cluster = cluster();
        let pit = cluster
            .open_pit(None, &PitConfig::default(), Instant::now())
            .expect("open");
        let (outcome, (status, _)) =
            cursor_fingerprint(&cluster, pit, Some([1u8; 32]), || [2u8; 32])
                .expect_err("another request's cursor");
        assert_eq!(outcome, "cursor_mismatch");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            cursor_fingerprint(&cluster, pit, Some([3u8; 32]), || [3u8; 32]).ok(),
            Some([3u8; 32])
        );
        assert_eq!(
            cursor_fingerprint(&cluster, pit, None, || [4u8; 32]).ok(),
            Some([4u8; 32])
        );
    }
}
