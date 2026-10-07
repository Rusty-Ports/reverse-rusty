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

use reverse_rusty::cluster::{ClusterEngine, ClusterState, ShardError};

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
    rebuild_in_progress: bool,
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
    /// A vocabulary change or a resize is rebuilding the cluster, or is about to. Searches
    /// answer meanwhile and writes wait. It does not change `status` (ADR-210).
    rebuild_in_progress: bool,
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
        collect_cluster_health(cluster).unwrap_or_else(|source| {
            warn!(error = %source, "cluster health dependency probe failed");
            counts_beside_a_failure(cluster)
        })
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
        Ok(Ok(health)) => health,
        Ok(Err(join_error)) => {
            error!(error = %join_error, "cluster health worker failed");
            unavailable_fallback(state, "health worker failed", false)
        }
    }
}

/// The health of the cluster: the serving shards of one pinned layout, compared with the
/// committed topology.
///
/// A rebuild publishes its layout and then commits the control state, so for a moment the
/// two differ. A difference is a fault only if it is still there with no rebuild running:
/// while one runs it is passed over, and if one may have finished between the two reads,
/// both are read again.
///
/// The colour says what is served and how redundantly. A rebuild does not change it, because
/// searches answer exactly throughout; it is reported beside the colour.
fn collect_cluster_health(cluster: &ClusterEngine) -> Result<ClusterHealth, ShardError> {
    const LOOKS: usize = 3;
    let mut difference = None;
    for _ in 0..LOOKS {
        let layout = cluster.published();
        // Every serving shard has to answer.
        let shards = layout.shard_query_counts()?.len();
        let control = cluster.control_state()?;
        // Asked last: a rebuild that began or ended while the two were read is seen here,
        // or leaves them agreeing on the next look.
        let rebuild_in_progress = cluster.layout_change_in_progress();
        match compare_topology(&control, shards) {
            Err(found) if !rebuild_in_progress => difference = Some(found),
            _ => {
                let pending_repairs = cluster.pending_repairs();
                let out_of_sync_replicas = layout.out_of_sync_replicas();
                let (status, reason) = serving_status(pending_repairs, out_of_sync_replicas);
                return Ok(ClusterHealth {
                    status,
                    deadline_expired: false,
                    shards,
                    pending_repairs,
                    out_of_sync_replicas,
                    rebuild_in_progress,
                    reason,
                });
            }
        }
    }
    Err(difference.expect("looked at least once"))
}

/// Whether the committed topology names exactly the serving shard positions.
fn compare_topology(control: &ClusterState, shards: usize) -> Result<(), ShardError> {
    if control.num_shards as usize != shards {
        return Err(ShardError::ControlPlane(format!(
            "committed shard count {} does not match the serving ring count {shards}",
            control.num_shards
        )));
    }

    let mut positions = BTreeSet::new();
    for assignment in &control.assignments {
        let position = assignment.position as usize;
        if position >= shards {
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
    if positions.len() != shards {
        let missing = (0..shards)
            .find(|position| !positions.contains(position))
            .unwrap_or(positions.len());
        return Err(ShardError::ControlPlane(format!(
            "no committed node assignment for shard position {missing}"
        )));
    }
    Ok(())
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
    ClusterHealth {
        deadline_expired,
        ..counts_beside_a_failure(&state.cluster)
    }
}

/// A red answer, with the counts that can be had without a probe. They take no lock a
/// rebuild holds and call no shard, so this also answers on an async worker.
fn counts_beside_a_failure(cluster: &ClusterEngine) -> ClusterHealth {
    let layout = cluster.published();
    ClusterHealth {
        status: HealthStatus::Red,
        deadline_expired: false,
        shards: layout.num_shards(),
        pending_repairs: cluster.pending_repairs(),
        out_of_sync_replicas: layout.out_of_sync_replicas(),
        rebuild_in_progress: cluster.layout_change_in_progress(),
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
            rebuild_in_progress: current.rebuild_in_progress,
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
        assert!(!health.rebuild_in_progress);
    }

    /// A cluster whose committed topology names four shards while it serves three: what a
    /// resize leaves between its swap and its control commit, and what a failed commit
    /// leaves behind.
    fn cluster_with_a_topology_difference() -> ClusterEngine {
        let config = ClusterConfig {
            num_shards: 3,
            include_broad: true,
            ..Default::default()
        };
        let queries: Vec<(u64, String)> = (1..=20u64)
            .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 5)))
            .collect();
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &queries,
        )
        .expect("cluster");
        let committed = cluster.control_state().expect("control state");
        let four = reverse_rusty::cluster::InMemoryControlPlane::single_node(
            4,
            committed.vnodes,
            committed.dict_fingerprint,
        );
        cluster.with_control_plane(Box::new(four))
    }

    /// A difference between the committed topology and the serving shards is a fault when
    /// nothing is replacing them.
    #[test]
    fn a_topology_difference_that_stays_is_a_fault() {
        let cluster = cluster_with_a_topology_difference();
        let fault = collect_cluster_health(&cluster)
            .err()
            .expect("the difference is reported");
        assert!(
            fault.to_string().contains("does not match"),
            "unexpected fault: {fault}"
        );
    }

    /// While a rebuild runs the same difference is what a rebuild makes on its way, so it is
    /// passed over. The colour stays what the serving shards say, and the rebuild is
    /// reported beside it.
    #[test]
    fn a_topology_difference_is_passed_over_while_a_rebuild_runs() {
        let cluster = Arc::new(cluster_with_a_topology_difference());
        let (stopped_sender, stopped) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel::<()>(1);
        let released = std::sync::Mutex::new(released);
        cluster.set_rebuild_hook_for_test(Some(Arc::new(move || {
            let _ = stopped_sender.try_send(());
            let _ = released.lock().expect("release").recv();
        })));
        let rebuilding = Arc::clone(&cluster);
        let rebuild = std::thread::spawn(move || {
            let vocab = reverse_rusty::vocab::Vocab::default();
            // Its own outcome does not matter here: the control state it commits to is
            // already wrong.
            let _ = rebuilding.set_vocab(vocab);
        });
        stopped
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the rebuild reached its stopping point");
        let during = collect_cluster_health(&cluster);
        // Released before anything can fail.
        drop(release);
        rebuild.join().expect("rebuild thread");
        let during = during.expect("a rebuild in progress is not a fault");
        assert_eq!(during.status, HealthStatus::Green);
        assert!(during.rebuild_in_progress);
        assert_eq!(during.shards, 3);
    }

    #[test]
    fn expired_probe_preserves_the_last_completed_observation() {
        let last = ClusterHealth {
            status: HealthStatus::Yellow,
            deadline_expired: false,
            shards: 3,
            pending_repairs: 1,
            out_of_sync_replicas: 0,
            rebuild_in_progress: false,
            reason: Some("repair pending"),
        };
        let expired = ClusterHealth {
            status: HealthStatus::Red,
            deadline_expired: true,
            shards: 0,
            pending_repairs: 0,
            out_of_sync_replicas: 0,
            rebuild_in_progress: false,
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
