//! Remote blue/green resize worker (ADR-180): prepare under the shared cluster lock so reads keep
//! serving the old layout, install under a brief exclusive lock, then retire the old slots. The
//! REST write serializer is released as soon as the engine's write fence is up, so writers are
//! refused by the fence instead of blocking runtime workers for the whole copy.

use std::time::Instant;

use parking_lot::{Mutex, MutexGuard};

use reverse_rusty::cluster::{NodeDescriptor, RemoteResizeRequest};

use crate::resize_ops::{ResizeFailure, ResizeOutcome};
use crate::state::ClusterAppState;

use super::execute::{
    begin_cluster_resize, not_started_failure, started_failure, ResizeStart, WorkerRecordGuard,
};
use super::{ClusterResizeSuccess, ClusterResizeWorkerOutcome};

/// The control-plane intent key for a registry operation ID: FNV-1a over its bytes, never zero.
pub(super) fn intent_operation_id(operation_id: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in operation_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash.max(1)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn remote_resize_worker(
    _running: tokio::sync::OwnedSemaphorePermit,
    state: &ClusterAppState,
    writes: MutexGuard<'_, ()>,
    gate: &Mutex<ResizeStart>,
    started_sender: tokio::sync::oneshot::Sender<()>,
    record: &WorkerRecordGuard,
    deadline: Instant,
    no_wait: bool,
    num_shards: usize,
    if_generation: Option<u64>,
    targets: Vec<NodeDescriptor>,
) -> ClusterResizeWorkerOutcome {
    let cluster = if no_wait {
        state.cluster.try_read()
    } else {
        deadline
            .checked_duration_since(Instant::now())
            .and_then(|budget| state.cluster.try_read_for(budget))
    };
    let Some(cluster) = cluster else {
        record
            .ops
            .mark_not_started(&record.id, not_started_failure());
        return ClusterResizeWorkerOutcome::NotStarted;
    };
    if !begin_cluster_resize(gate, deadline, no_wait) {
        record
            .ops
            .mark_not_started(&record.id, not_started_failure());
        return ClusterResizeWorkerOutcome::NotStarted;
    }
    let _sent = started_sender.send(());
    let current = cluster.placement_generation().0;
    if if_generation.is_some_and(|expected| expected != current) {
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
    let request = RemoteResizeRequest {
        operation_id: intent_operation_id(&record.id),
        num_shards,
        targets,
    };
    // Reads continue on the old layout while the new one is built and committed. Holding the
    // write serializer until the fence is up keeps a vocabulary rebuild from queueing for the
    // exclusive lock behind this copy, which would stall every read; vocabulary handlers check
    // the fence before asking for that lock.
    let prepared = cluster.prepare_remote_resize_then(&request, move || drop(writes));
    drop(cluster);
    let result = prepared.and_then(|prepared| {
        // The new layout is committed; the swap itself is brief and cannot be skipped.
        let retired = state.cluster.write().install_remote_resize(prepared)?;
        let cluster = state.cluster.read();
        let report = cluster.finish_remote_resize(retired)?;
        let version = cluster.control_state()?.epoch;
        Ok(ClusterResizeSuccess {
            old_num_shards: report.old_num_shards,
            num_shards: report.num_shards,
            rebuilt: report.exported as usize,
            version,
            placement_generation: report.placement_generation,
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
        Err(source) => record.ops.mark_failed(&record.id, started_failure(source)),
    }
    ClusterResizeWorkerOutcome::Finished(result)
}

#[cfg(test)]
mod tests {
    use super::intent_operation_id;

    #[test]
    fn intent_ids_are_stable_distinct_and_non_zero() {
        assert_eq!(intent_operation_id("grow-8"), intent_operation_id("grow-8"));
        assert_ne!(intent_operation_id("grow-8"), intent_operation_id("grow-9"));
        assert_ne!(intent_operation_id(""), 0);
    }
}
