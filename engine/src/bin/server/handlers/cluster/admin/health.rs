//! Coordinator implementation of the strict native health contract.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::{ClusterEngine, ShardError};

use crate::handlers::admin::{
    finish_health_response, validate_health_request, wait_delay, HealthParams, HealthStatus,
    HealthTransport,
};
use crate::state::ClusterAppState;

#[derive(Clone)]
struct ClusterHealth {
    status: HealthStatus,
    deadline_expired: bool,
    shards: usize,
    pending_repairs: usize,
    out_of_sync_replicas: usize,
    reason: Option<&'static str>,
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    status: &'static str,
    mode: &'static str,
    timed_out: bool,
    shards: usize,
    pending_repairs: usize,
    /// Replicas reads cannot fail over to (ADR-195). 0 without replicas.
    out_of_sync_replicas: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

/// `GET`/`HEAD /_health` — fail-loud serving and control-plane readiness.
///
/// The required control-state and per-position probes may cross the network.
/// They therefore share stats admission and execute on a blocking worker.
#[instrument(skip_all)]
pub(crate) async fn cluster_health(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<HealthParams>, QueryRejection>,
    transport: HealthTransport,
) -> Response {
    let (_permit, _duration, head, body) = transport.into_parts();
    let request = match validate_health_request(&state.prom, params, body, head) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let waits_for_status = request.waits_for_status();
    let deadline = match request.deadline() {
        Ok(deadline) => deadline,
        Err(reason) => {
            return crate::handlers::admin::health_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
                head,
            )
        }
    };

    let mut last_observation = None;
    loop {
        if let Some(last) = last_observation.as_ref() {
            if Instant::now() >= deadline {
                return finish_health_response(
                    &state.prom,
                    cluster_response(last, true, waits_for_status),
                    head,
                );
            }
        }
        let current = collect_once(&state, deadline).await;
        // Tokio's timeout polls the worker before its timer. A worker result
        // that became ready while this task was starved can therefore arrive
        // after the deadline without `deadline_expired`; the wall clock wins.
        if current.deadline_expired || Instant::now() >= deadline {
            let reported = timeout_observation(&current, last_observation.as_ref());
            return finish_health_response(
                &state.prom,
                cluster_response(reported, true, waits_for_status),
                head,
            );
        }
        if request.satisfied_by(current.status) {
            return finish_health_response(
                &state.prom,
                cluster_response(&current, false, waits_for_status),
                head,
            );
        }
        let Some(delay) = wait_delay(deadline) else {
            return finish_health_response(
                &state.prom,
                cluster_response(&current, true, waits_for_status),
                head,
            );
        };
        last_observation = Some(current);
        tokio::time::sleep(delay).await;
    }
}

fn timeout_observation<'a>(
    current: &'a ClusterHealth,
    last: Option<&'a ClusterHealth>,
) -> &'a ClusterHealth {
    last.unwrap_or(current)
}

async fn collect_once(state: &Arc<ClusterAppState>, deadline: Instant) -> ClusterHealth {
    let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
        return unavailable_fallback(state, "health deadline elapsed before admission", true);
    };
    let permit = match tokio::time::timeout(
        admission_budget,
        Arc::clone(&state.stats_permits).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return unavailable_fallback(state, "health admission is closed", false),
        Err(_) => {
            return unavailable_fallback(state, "health admission deadline elapsed", true);
        }
    };
    let worker_state = Arc::clone(state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let cluster = &worker_state.cluster;
        let shards = cluster.num_shards();
        let degraded = (cluster.pending_repairs(), cluster.out_of_sync_replicas());
        collect_cluster_health(cluster).map_err(|source| (source, shards, degraded))
    });
    let Some(probe_budget) = deadline.checked_duration_since(Instant::now()) else {
        return unavailable_fallback(
            state,
            "health deadline elapsed before dependency probe",
            true,
        );
    };
    match tokio::time::timeout(probe_budget, worker).await {
        Err(_) => unavailable_fallback(state, "health dependency probe deadline elapsed", true),
        Ok(Ok(Ok(health))) => health,
        Ok(Ok(Err((source, shards, (pending_repairs, out_of_sync_replicas))))) => {
            warn!(error = %source, "cluster health dependency probe failed");
            ClusterHealth {
                status: HealthStatus::Red,
                deadline_expired: false,
                shards,
                pending_repairs,
                out_of_sync_replicas,
                reason: Some("required shard or control-plane probe failed"),
            }
        }
        Ok(Err(join_error)) => {
            error!(error = %join_error, "cluster health worker failed");
            unavailable_fallback(state, "health worker failed", false)
        }
    }
}

/// The health of the cluster. The committed topology is compared with the serving shards,
/// and a vocabulary change or a resize replaces the one after the other, so the comparison
/// counts only when no such change overlapped it. While one runs, the cluster is reported as
/// what it is: serving, and rebuilding.
fn collect_cluster_health(cluster: &ClusterEngine) -> Result<ClusterHealth, ShardError> {
    match cluster.read_between_layout_changes(|| compare_topology(cluster)) {
        Some(health) => health,
        None => rebuilding_health(cluster),
    }
}

/// Yellow while a rebuild runs: searches answer from the layout it replaces, and writes wait.
/// Every serving shard still has to answer the count.
fn rebuilding_health(cluster: &ClusterEngine) -> Result<ClusterHealth, ShardError> {
    let counts = cluster.shard_query_counts()?;
    Ok(ClusterHealth {
        status: HealthStatus::Yellow,
        deadline_expired: false,
        shards: counts.len(),
        pending_repairs: cluster.pending_repairs(),
        out_of_sync_replicas: cluster.out_of_sync_replicas(),
        reason: Some(
            "a vocabulary change or a resize is rebuilding the cluster; searches answer from \
             the layout it replaces and writes wait until it finishes",
        ),
    })
}

fn compare_topology(cluster: &ClusterEngine) -> Result<ClusterHealth, ShardError> {
    let control = cluster.control_state()?;
    let counts = cluster.shard_query_counts()?;
    if control.num_shards as usize != counts.len() {
        return Err(ShardError::ControlPlane(format!(
            "committed shard count {} does not match the serving ring count {}",
            control.num_shards,
            counts.len()
        )));
    }

    let mut positions = BTreeSet::new();
    for assignment in &control.assignments {
        let position = assignment.position as usize;
        if position >= counts.len() {
            return Err(ShardError::ControlPlane(format!(
                "committed assignment names out-of-range shard position {position}"
            )));
        }
        if !positions.insert(position) {
            return Err(ShardError::ControlPlane(format!(
                "committed topology contains duplicate shard position {position}"
            )));
        }
    }
    if positions.len() != counts.len() {
        let missing = (0..counts.len())
            .find(|position| !positions.contains(position))
            .unwrap_or(positions.len());
        return Err(ShardError::ControlPlane(format!(
            "no committed node assignment for shard position {missing}"
        )));
    }

    let pending_repairs = cluster.pending_repairs();
    let out_of_sync_replicas = cluster.out_of_sync_replicas();
    let (status, reason) = serving_status(pending_repairs, out_of_sync_replicas);
    Ok(ClusterHealth {
        status,
        deadline_expired: false,
        shards: counts.len(),
        pending_repairs,
        out_of_sync_replicas,
        reason,
    })
}

/// The status of a cluster whose every probe answered. It is yellow while it serves with
/// something owed: a write some shard has not taken, or a replica that reads cannot fail
/// over to (ADR-195), which leaves its position with less redundancy than configured.
fn serving_status(
    pending_repairs: usize,
    out_of_sync_replicas: usize,
) -> (HealthStatus, Option<&'static str>) {
    if pending_repairs > 0 {
        (
            HealthStatus::Yellow,
            Some("partial applies are queued; POST /_cluster/resync converges them"),
        )
    } else if out_of_sync_replicas > 0 {
        (
            HealthStatus::Yellow,
            Some(
                "a replica is outside the in-sync set, so reads cannot fail over to it; \
                 recover it from its primary",
            ),
        )
    } else {
        (HealthStatus::Green, None)
    }
}

fn unavailable_fallback(
    state: &ClusterAppState,
    log_message: &'static str,
    deadline_expired: bool,
) -> ClusterHealth {
    warn!(message = log_message, "cluster health unavailable");
    // Three reads that take no lock a rebuild holds, so this answers on an async worker.
    let cluster = &state.cluster;
    let (shards, pending_repairs, out_of_sync_replicas) = (
        cluster.num_shards(),
        cluster.pending_repairs(),
        cluster.out_of_sync_replicas(),
    );
    ClusterHealth {
        status: HealthStatus::Red,
        deadline_expired,
        shards,
        pending_repairs,
        out_of_sync_replicas,
        reason: Some("required shard or control-plane probe failed"),
    }
}

fn cluster_response(current: &ClusterHealth, timed_out: bool, waits_for_status: bool) -> Response {
    let status = if timed_out {
        StatusCode::REQUEST_TIMEOUT
    } else if current.status == HealthStatus::Red {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(ClusterHealthResponse {
            status: current.status.as_str(),
            mode: "cluster",
            timed_out,
            shards: current.shards,
            pending_repairs: current.pending_repairs,
            out_of_sync_replicas: current.out_of_sync_replicas,
            reason: if timed_out {
                Some(if waits_for_status {
                    "requested health status was not reached before timeout"
                } else {
                    "health dependency probe did not complete before timeout"
                })
            } else {
                current.reason
            },
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reverse_rusty::cluster::ClusterConfig;
    use reverse_rusty::Normalizer;

    #[test]
    fn healthy_in_process_topology_is_green() {
        let config = ClusterConfig {
            num_shards: 3,
            ..Default::default()
        };
        let cluster =
            ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), &config, &[])
                .expect("cluster");
        let health = collect_cluster_health(&cluster).expect("health");
        assert_eq!(health.status, HealthStatus::Green);
        assert_eq!(health.shards, 3);
    }

    #[test]
    fn expired_probe_preserves_the_last_completed_observation() {
        let last = ClusterHealth {
            status: HealthStatus::Yellow,
            deadline_expired: false,
            shards: 3,
            pending_repairs: 1,
            out_of_sync_replicas: 0,
            reason: Some("repair pending"),
        };
        let expired = ClusterHealth {
            status: HealthStatus::Red,
            deadline_expired: true,
            shards: 0,
            pending_repairs: 0,
            out_of_sync_replicas: 0,
            reason: Some("probe deadline elapsed"),
        };
        let reported = timeout_observation(&expired, Some(&last));
        assert_eq!(reported.status, HealthStatus::Yellow);
        assert_eq!(reported.shards, 3);
        assert_eq!(reported.pending_repairs, 1);
    }

    #[test]
    fn a_replica_reads_cannot_fail_over_to_makes_the_cluster_yellow() {
        assert_eq!(serving_status(0, 0), (HealthStatus::Green, None));
        let (status, reason) = serving_status(0, 2);
        assert_eq!(status, HealthStatus::Yellow);
        assert!(reason.expect("a reason").contains("in-sync set"));
        // A queued repair is named first: it is the one a title can already miss on.
        let (status, reason) = serving_status(3, 2);
        assert_eq!(status, HealthStatus::Yellow);
        assert!(reason.expect("a reason").contains("/_cluster/resync"));
    }
}
