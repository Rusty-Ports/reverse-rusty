//! Remote blue/green resize worker (ADR-180): prepare (which retires the old nodes before it
//! commits) while reads keep serving the old layout during the copy, install in a brief layout
//! change, then finish. Write admission is released as soon as the engine's write fence is up,
//! so writers are refused by the fence instead of blocking runtime workers for the whole copy.

use std::time::Instant;

use parking_lot::{Mutex, RwLockWriteGuard};

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
    writes: RwLockWriteGuard<'_, ()>,
    gate: &Mutex<ResizeStart>,
    started_sender: tokio::sync::oneshot::Sender<()>,
    record: &WorkerRecordGuard,
    deadline: Instant,
    no_wait: bool,
    num_shards: usize,
    if_generation: Option<u64>,
    targets: Vec<NodeDescriptor>,
) -> ClusterResizeWorkerOutcome {
    let cluster = &state.cluster;
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
    // Reads continue on the old layout while the new one is built and committed. Write
    // admission is released once the fence is up: a write that arrives during the copy is
    // refused by the fence, and a vocabulary change is refused where it is admitted
    // (`ClusterAppState::admit_rebuild`) and again by the engine.
    let prepared = cluster.prepare_remote_resize_then(&request, move || drop(writes));
    let result = prepared.and_then(|prepared| {
        // The new layout is committed; the swap itself is brief and cannot be skipped. Like
        // every layout change it holds write admission alone (ADR-206). Admission was
        // released when the write fence went up; a write that arrives now is refused by the
        // fence and gives its share straight back.
        let admission = state.write_admission.write();
        let retired = cluster.install_remote_resize(prepared)?;
        drop(admission);
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
