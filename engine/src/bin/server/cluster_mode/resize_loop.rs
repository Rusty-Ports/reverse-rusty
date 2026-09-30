//! The opt-in governed resize loop (ADR-179): periodically observe the serving layout and
//! per-shard selective corpus, feed the pure [`ResizeGovernor`], and execute an accepted,
//! bounded resize through the same admission and worker path as `POST /_cluster/resize`.
//!
//! It lives at the server layer so `ClusterEngine` stays clock-free and thread-free. The
//! governor owns hysteresis, cooldown, bounded steps, and the futility hold; every accepted
//! operation carries its observed placement generation as a precondition, so an operator resize
//! or vocabulary rebuild that lands between observation and execution makes the automatic
//! operation fail its precondition instead of stacking a second layout change.
//!
//! Lifecycle: spawned before serving and aborted at the start of shutdown. An in-flight resize
//! runs on its own supervised worker and retains the shared administration permit, which
//! shutdown acquires before its final checkpoint.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use reverse_rusty::cluster::{
    AutoscaleConfig, ResizeGovernor, ResizeGovernorConfig, ResizeObservation, ResizeVerdict,
};

use crate::handlers::{run_resize, ResizeRun, ResizeRunOutcome};
use crate::resize_ops::{unix_ms_now, AutoscaleStatus, ResizeAdmission, ResizeOrigin};
use crate::state::ClusterAppState;

/// The manager timeout for an automatic operation: the same bound the REST API allows.
const AUTOSCALE_RESIZE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);

/// Validated loop configuration.
#[derive(Clone, Debug)]
pub(crate) struct AutoscaleResizeConfig {
    pub(crate) interval: Duration,
    pub(crate) split_corpus_threshold: usize,
    pub(crate) governor: ResizeGovernorConfig,
}

/// Spawn the loop, returning its handle for shutdown.
pub(crate) fn spawn_resize_loop(
    state: Arc<ClusterAppState>,
    config: AutoscaleResizeConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            interval_secs = config.interval.as_secs(),
            split_corpus_threshold = config.split_corpus_threshold,
            required_observations = config.governor.required_observations,
            cooldown_secs = config.governor.cooldown.as_secs(),
            max_step = config.governor.max_step,
            max_shards = config.governor.max_shards,
            min_relief_percent = config.governor.min_relief_percent,
            "governed resize loop started (ADR-179)"
        );
        let autoscale = AutoscaleConfig {
            enabled: true,
            split_corpus_threshold: config.split_corpus_threshold,
            ..AutoscaleConfig::default()
        };
        let mut governor = ResizeGovernor::new();
        let mut pending: Option<GovernedOperation> = None;
        loop {
            tokio::time::sleep(config.interval).await;
            // An accepted operation that swapped the serving layout but failed to commit it
            // keeps durable writes paused (ADR-178). Retry that exact operation until it
            // commits, before and independently of any new growth decision.
            if let Some(operation) = pending.take() {
                match run_operation(&state, &operation, true).await {
                    OperationResult::Produced(generation) => {
                        governor.record_outcome(Instant::now(), Some(generation));
                    }
                    OperationResult::NeedsCommit => pending = Some(operation),
                    OperationResult::NoChange => governor.record_outcome(Instant::now(), None),
                }
                continue;
            }
            let Some(observation) = observe(&state, &autoscale).await else {
                continue;
            };
            let verdict = governor.observe(Instant::now(), observation, &config.governor);
            state.resize_operations.record_autoscale(AutoscaleStatus {
                observed_at_ms: unix_ms_now(),
                num_shards: observation.num_shards,
                placement_generation: observation.placement_generation,
                recommended: observation.recommended,
                max_selective_corpus: observation.max_selective_corpus,
                verdict: verdict.clone(),
            });
            if let ResizeVerdict::Accept {
                from,
                to,
                if_placement_generation,
            } = verdict
            {
                let Some(operation) = admit(&state, from, to, if_placement_generation) else {
                    governor.record_outcome(Instant::now(), None);
                    continue;
                };
                match run_operation(&state, &operation, false).await {
                    OperationResult::Produced(generation) => {
                        governor.record_outcome(Instant::now(), Some(generation));
                    }
                    OperationResult::NeedsCommit => pending = Some(operation),
                    OperationResult::NoChange => governor.record_outcome(Instant::now(), None),
                }
            }
        }
    })
}

/// One admitted automatic operation, retained so a failed commit can be retried under the
/// same operation ID.
struct GovernedOperation {
    id: String,
    to: usize,
    if_placement_generation: u64,
}

enum OperationResult {
    /// The operation committed this placement generation.
    Produced(u64),
    /// The layout did not change.
    NoChange,
    /// The serving layout changed but is not durably committed; retry the same operation.
    NeedsCommit,
}

/// Collect one observation off the async runtime: the cluster read lock can wait behind an
/// exclusive rebuild.
async fn observe(
    state: &Arc<ClusterAppState>,
    autoscale: &AutoscaleConfig,
) -> Option<ResizeObservation> {
    let st = Arc::clone(state);
    let cfg = autoscale.clone();
    match tokio::task::spawn_blocking(move || st.cluster.read().resize_observation(&cfg)).await {
        Ok(Ok(observation)) => Some(observation),
        Ok(Err(error)) => {
            warn!(error = %error, "governed resize: load observation failed; retrying next interval");
            None
        }
        Err(error) => {
            warn!(error = %error, "governed resize: observation task failed; retrying next interval");
            None
        }
    }
}

/// Admit one accepted operation under a generated autoscaler ID.
fn admit(
    state: &Arc<ClusterAppState>,
    from: usize,
    to: usize,
    if_placement_generation: u64,
) -> Option<GovernedOperation> {
    match state.resize_operations.admit(
        None,
        ResizeOrigin::Autoscaler,
        to,
        Some(if_placement_generation),
    ) {
        ResizeAdmission::Execute(id) => {
            info!(
                operation_id = %id,
                from,
                to,
                if_placement_generation,
                "governed resize accepted"
            );
            Some(GovernedOperation {
                id,
                to,
                if_placement_generation,
            })
        }
        other => {
            warn!(
                ?other,
                "governed resize: operation registry refused the operation"
            );
            None
        }
    }
}

/// Run one governed operation through the ordinary resize path. A `retry` first re-admits the
/// same failed record; the registry keeps its uncommitted generation, which lets the worker's
/// precondition accept the operation's own swap.
async fn run_operation(
    state: &Arc<ClusterAppState>,
    operation: &GovernedOperation,
    retry: bool,
) -> OperationResult {
    if retry
        && !matches!(
            state.resize_operations.admit(
                Some(operation.id.clone()),
                ResizeOrigin::Autoscaler,
                operation.to,
                Some(operation.if_placement_generation),
            ),
            ResizeAdmission::Execute(_)
        )
    {
        warn!(operation_id = %operation.id, "governed resize: retained operation is not retryable");
        return OperationResult::NoChange;
    }
    let outcome = run_resize(
        state,
        ResizeRun {
            operation_id: operation.id.clone(),
            num_shards: operation.to,
            if_placement_generation: Some(operation.if_placement_generation),
            manager_timeout: AUTOSCALE_RESIZE_MANAGER_TIMEOUT,
        },
    )
    .await;
    let id = &operation.id;
    match outcome {
        ResizeRunOutcome::Succeeded(success) => {
            info!(
                operation_id = %id,
                old_num_shards = success.old_num_shards,
                num_shards = success.num_shards,
                rebuilt = success.rebuilt,
                placement_generation = success.placement_generation,
                "governed resize completed"
            );
            OperationResult::Produced(success.placement_generation)
        }
        ResizeRunOutcome::NotStarted => {
            info!(operation_id = %id, "governed resize not started before its deadline");
            uncommitted_or(state, id, OperationResult::NoChange)
        }
        ResizeRunOutcome::PreconditionFailed { current } => {
            info!(
                operation_id = %id,
                current,
                if_placement_generation = operation.if_placement_generation,
                "governed resize skipped: the layout changed since observation"
            );
            OperationResult::NoChange
        }
        ResizeRunOutcome::Unavailable(reason) => {
            warn!(operation_id = %id, reason, "governed resize unavailable");
            uncommitted_or(state, id, OperationResult::NoChange)
        }
        ResizeRunOutcome::Failed(error) => {
            warn!(operation_id = %id, error = %error, "governed resize failed");
            uncommitted_or(state, id, OperationResult::NoChange)
        }
        ResizeRunOutcome::WorkerFailed => {
            warn!(operation_id = %id, "governed resize worker failed");
            uncommitted_or(state, id, OperationResult::NoChange)
        }
    }
}

/// `NeedsCommit` when the operation's record holds an uncommitted swapped generation (from
/// this attempt or an earlier one), otherwise `fallback`.
fn uncommitted_or(
    state: &Arc<ClusterAppState>,
    id: &str,
    fallback: OperationResult,
) -> OperationResult {
    let uncommitted = state
        .resize_operations
        .get(id)
        .and_then(|record| record.uncommitted_generation);
    if uncommitted.is_some() {
        warn!(
            operation_id = %id,
            "governed resize swapped the serving layout without committing it; retrying next interval"
        );
        OperationResult::NeedsCommit
    } else {
        fallback
    }
}
