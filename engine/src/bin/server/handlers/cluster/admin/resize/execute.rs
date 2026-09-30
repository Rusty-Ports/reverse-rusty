//! Shared resize execution for the REST endpoint and the autoscale loop (ADR-167/179).
//!
//! One admitted operation runs on a dedicated, supervised OS thread that acquires
//! the exclusive topology, REST-write, and cluster guards, checks the optional
//! placement-generation precondition, and only then starts the rebuild. The worker
//! records its own progress in the operation registry, so a record reaches a
//! terminal state even after the caller disconnects or the loop is aborted.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::TryAcquireError;
use tracing::{error, warn};

use reverse_rusty::cluster::ShardError;

use crate::resize_ops::{ResizeFailure, ResizeOperations, ResizeOutcome};
use crate::state::ClusterAppState;

use super::supervisor::supervise_cluster_resize_worker;
use super::{ClusterResizeSuccess, ClusterResizeWorkerOutcome};

/// One admitted resize request.
pub(crate) struct ResizeRun {
    pub(crate) operation_id: String,
    pub(crate) num_shards: usize,
    pub(crate) if_placement_generation: Option<u64>,
    pub(crate) manager_timeout: Duration,
    /// Fresh target nodes for a remote resize (ADR-180); empty for the in-process rebuild.
    pub(crate) targets: Vec<reverse_rusty::cluster::NodeDescriptor>,
}

/// The terminal result of [`run_resize`] as seen by its caller.
pub(crate) enum ResizeRunOutcome {
    Succeeded(ClusterResizeSuccess),
    /// Admission or an exclusive guard was not obtained before the deadline; no rebuild
    /// started and none can start later.
    NotStarted,
    /// Admission is closed or the worker could not be dispatched.
    Unavailable(&'static str),
    /// The serving placement generation differed from the caller's precondition.
    PreconditionFailed {
        current: u64,
    },
    /// The rebuild started and failed.
    Failed(ShardError),
    /// The dedicated worker panicked or its completion channel failed.
    WorkerFailed,
}

#[derive(Clone, Copy)]
pub(super) enum ResizeStart {
    Queued,
    Started,
    Cancelled,
}

pub(super) fn begin_cluster_resize(
    gate: &Mutex<ResizeStart>,
    deadline: Instant,
    no_wait: bool,
) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ResizeStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ResizeStart::Cancelled;
        return false;
    }
    *start = ResizeStart::Started;
    true
}

fn cancel_queued_cluster_resize(gate: &Mutex<ResizeStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ResizeStart::Queued | ResizeStart::Cancelled => {
            *start = ResizeStart::Cancelled;
            true
        }
        ResizeStart::Started => false,
    }
}

struct CancelQueuedClusterResize(Arc<Mutex<ResizeStart>>);

impl Drop for CancelQueuedClusterResize {
    fn drop(&mut self) {
        let _cancelled = cancel_queued_cluster_resize(&self.0);
    }
}

pub(super) fn not_started_failure() -> ResizeFailure {
    ResizeFailure {
        error_type: "resize_timeout".into(),
        reason: "admission or exclusive cluster access was not obtained before the manager \
                 deadline; no resize was started"
            .into(),
    }
}

/// Marks a still-queued record not-started when the caller drops the operation before its
/// worker is dispatched (a disconnect or loop abort while waiting for admission).
struct QueuedRecordGuard {
    ops: Arc<ResizeOperations>,
    id: String,
    armed: bool,
}

impl Drop for QueuedRecordGuard {
    fn drop(&mut self) {
        if self.armed {
            self.ops.mark_not_started(&self.id, not_started_failure());
        }
    }
}

/// Marks a record failed if the worker unwinds before reporting a terminal state.
pub(super) struct WorkerRecordGuard {
    pub(super) ops: Arc<ResizeOperations>,
    pub(super) id: String,
    finished: bool,
}

impl Drop for WorkerRecordGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.ops.mark_failed(
                &self.id,
                ResizeFailure {
                    error_type: "resize_unavailable".into(),
                    reason: "resize worker failed".into(),
                },
            );
        }
    }
}

/// Sanitized failure recorded for a started rebuild that did not reach an attested state.
pub(crate) fn started_failure(source: &ShardError) -> ResizeFailure {
    let (_, error_type) = source.write_http_class();
    ResizeFailure {
        error_type: error_type.to_string(),
        reason: "resize did not produce an attested terminal cluster state; inspect /_health \
                 and /_cluster/state before retrying"
            .into(),
    }
}

/// Admit, dispatch, and await one resize. The manager timeout bounds admission and every
/// exclusive-lock wait until the rebuild atomically starts; a started rebuild is always
/// awaited to its exact terminal result.
pub(crate) async fn run_resize(state: &Arc<ClusterAppState>, run: ResizeRun) -> ResizeRunOutcome {
    let ops = Arc::clone(&state.resize_operations);
    let mut queued = QueuedRecordGuard {
        ops: Arc::clone(&ops),
        id: run.operation_id.clone(),
        armed: true,
    };
    let no_wait = run.manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(run.manager_timeout) else {
        return ResizeRunOutcome::NotStarted;
    };
    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return ResizeRunOutcome::NotStarted,
            Err(TryAcquireError::Closed) => {
                return unavailable(&mut queued, "resize admission is closed");
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return ResizeRunOutcome::NotStarted;
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return ResizeRunOutcome::NotStarted,
            Ok(Err(_)) => return unavailable(&mut queued, "resize admission is closed"),
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return ResizeRunOutcome::NotStarted;
    }

    let worker_state = Arc::clone(state);
    let gate = Arc::new(Mutex::new(ResizeStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterResize(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let worker_ops = Arc::clone(&ops);
    let worker_id = run.operation_id.clone();
    let num_shards = run.num_shards;
    let if_generation = run.if_placement_generation;
    let targets = run.targets;
    let completion = match supervise_cluster_resize_worker(move || {
        let _permit = permit;
        let mut record = WorkerRecordGuard {
            ops: worker_ops,
            id: worker_id,
            finished: false,
        };
        let outcome = resize_worker(
            &worker_state,
            &worker_gate,
            started_sender,
            &record,
            deadline,
            no_wait,
            num_shards,
            if_generation,
            targets,
        );
        record.finished = true;
        outcome
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated resize worker");
            return unavailable(&mut queued, "resize worker could not be started");
        }
    };
    // The worker now owns terminal record keeping.
    queued.armed = false;
    let mut completion = completion;

    let finished = if no_wait {
        completion.await
    } else {
        let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(sleep);
        tokio::select! {
            outcome = &mut completion => outcome,
            started = &mut started_receiver => {
                if started.is_err() {
                    warn!("resize worker ended without sending its start signal");
                }
                completion.await
            },
            () = &mut sleep => {
                if cancel_queued_cluster_resize(&gate) {
                    return ResizeRunOutcome::NotStarted;
                }
                // The worker acquired every exclusive guard and started before the manager
                // deadline. A blue/green swap cannot be cancelled safely at an arbitrary
                // deadline, so await its outcome.
                completion.await
            }
        }
    };
    match finished {
        Ok(Ok(ClusterResizeWorkerOutcome::NotStarted)) => ResizeRunOutcome::NotStarted,
        Ok(Ok(ClusterResizeWorkerOutcome::PreconditionFailed { current })) => {
            ResizeRunOutcome::PreconditionFailed { current }
        }
        Ok(Ok(ClusterResizeWorkerOutcome::Finished(Ok(success)))) => {
            ResizeRunOutcome::Succeeded(success)
        }
        Ok(Ok(ClusterResizeWorkerOutcome::Finished(Err(source)))) => {
            ResizeRunOutcome::Failed(source)
        }
        Ok(Err(_)) => ResizeRunOutcome::WorkerFailed,
        Err(source) => {
            error!(error = %source, "resize completion supervisor failed");
            ResizeRunOutcome::WorkerFailed
        }
    }
}

fn unavailable(queued: &mut QueuedRecordGuard, reason: &'static str) -> ResizeRunOutcome {
    queued.armed = false;
    queued.ops.mark_failed(
        &queued.id,
        ResizeFailure {
            error_type: "resize_unavailable".into(),
            reason: reason.into(),
        },
    );
    ResizeRunOutcome::Unavailable(reason)
}

#[allow(clippy::too_many_arguments)]
fn resize_worker(
    state: &ClusterAppState,
    gate: &Mutex<ResizeStart>,
    started_sender: tokio::sync::oneshot::Sender<()>,
    record: &WorkerRecordGuard,
    deadline: Instant,
    no_wait: bool,
    num_shards: usize,
    if_generation: Option<u64>,
    targets: Vec<reverse_rusty::cluster::NodeDescriptor>,
) -> ClusterResizeWorkerOutcome {
    let not_started = || {
        record
            .ops
            .mark_not_started(&record.id, not_started_failure());
        ClusterResizeWorkerOutcome::NotStarted
    };
    let topology = if no_wait {
        state.topology_guard.try_write()
    } else {
        deadline
            .checked_duration_since(Instant::now())
            .and_then(|budget| state.topology_guard.try_write_for(budget))
    };
    let Some(_topology) = topology else {
        return not_started();
    };
    let writes = if no_wait {
        state.write_serial.try_lock()
    } else {
        deadline
            .checked_duration_since(Instant::now())
            .and_then(|budget| state.write_serial.try_lock_for(budget))
    };
    let Some(writes) = writes else {
        return not_started();
    };
    #[cfg(feature = "distributed")]
    if !targets.is_empty() {
        return super::remote::remote_resize_worker(
            state,
            writes,
            gate,
            started_sender,
            record,
            deadline,
            no_wait,
            num_shards,
            if_generation,
            targets,
        );
    }
    #[cfg(not(feature = "distributed"))]
    drop(targets);
    let _writes = writes;
    let cluster = if no_wait {
        state.cluster.try_write()
    } else {
        deadline
            .checked_duration_since(Instant::now())
            .and_then(|budget| state.cluster.try_write_for(budget))
    };
    let Some(mut cluster) = cluster else {
        return not_started();
    };
    if !begin_cluster_resize(gate, deadline, no_wait) {
        return not_started();
    }
    let _sent = started_sender.send(());
    let current = cluster.placement_generation().0;
    // A retry of an operation whose earlier attempt swapped the serving layout but failed to
    // commit it may proceed at exactly that generation, so it can finish its own commit
    // (ADR-178/179). Any other layout change still fails the precondition.
    let heals_own_swap = record
        .ops
        .get(&record.id)
        .and_then(|r| r.uncommitted_generation)
        .is_some_and(|generation| generation == current && cluster.num_shards() == num_shards);
    if if_generation.is_some_and(|expected| expected != current) && !heals_own_swap {
        record.ops.mark_failed(
            &record.id,
            ResizeFailure {
                error_type: "placement_generation_mismatch".into(),
                reason: format!(
                    "the serving placement generation is {current}; the resize precondition \
                     was not met and no resize was started"
                ),
            },
        );
        return ClusterResizeWorkerOutcome::PreconditionFailed { current };
    }
    record.ops.mark_running(&record.id);
    let old_num_shards = cluster.num_shards();
    let result = cluster.resize(num_shards).and_then(|rebuilt| {
        let control = cluster.control_state()?;
        let placement_generation = cluster.placement_generation().0;
        if control.num_shards as usize != num_shards
            || control.placement_generation != placement_generation
        {
            return Err(ShardError::ControlPlane(format!(
                "resize terminal attestation failed: serving state is generation \
                 {placement_generation}/{num_shards} shards but committed control state is \
                 generation {}/{} shards",
                control.placement_generation, control.num_shards
            )));
        }
        Ok(ClusterResizeSuccess {
            old_num_shards,
            num_shards,
            rebuilt,
            version: control.epoch,
            placement_generation,
        })
    });
    match &result {
        Ok(success) => record.ops.mark_succeeded(
            &record.id,
            ResizeOutcome {
                old_num_shards: success.old_num_shards,
                num_shards: success.num_shards,
                rebuilt: success.rebuilt,
                version: success.version,
                placement_generation: success.placement_generation,
            },
        ),
        Err(source) => {
            let serving = cluster.placement_generation().0;
            if serving == current {
                record.ops.mark_failed(&record.id, started_failure(source));
            } else {
                record
                    .ops
                    .mark_failed_uncommitted(&record.id, started_failure(source), serving);
            }
        }
    }
    ClusterResizeWorkerOutcome::Finished(result)
}
