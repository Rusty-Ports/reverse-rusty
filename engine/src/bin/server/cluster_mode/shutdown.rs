//! Shutdown quiescence for cluster work that outlives its HTTP request.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::error;

use crate::state::{
    ClusterAppState, MAX_CONCURRENT_CLUSTER_HANDOFFS, MAX_CONCURRENT_CLUSTER_REASSIGNS,
    MAX_CONCURRENT_CLUSTER_REBALANCES, MAX_CONCURRENT_CLUSTER_RECONCILES, MAX_CONCURRENT_STATS,
    MAX_QUEUED_CLUSTER_WRITES,
};

/// Acquire and retain every admission boundary for work that may outlive its HTTP request,
/// before durability cleanup.
///
/// Rebalance, reconcile/GC, raw handoff, move-and-commit reassignment, corpus-administration, and
/// remote resize workers own their permit for their complete synchronous workflow, and every
/// cluster write owns a write permit until it finishes (ADR-183), including after an HTTP
/// disconnect. Taking each boundary's full capacity therefore joins every detached worker before
/// the shutdown flush and checkpoint, and retaining the permits stops a late draining request from
/// starting more work. In particular, a detached resize or write may be waiting for
/// `write_serial`; quiescing only that lock would let the shutdown checkpoint win it first and the
/// work land after cleanup.
pub(crate) async fn quiesce_detached_work(state: &ClusterAppState) -> Vec<OwnedSemaphorePermit> {
    // A remote resize returns its corpus-administration slot for health probes once it holds the
    // topology guard, and releases `write_serial` once its write fence is up; its own permit
    // covers the copy and cutover through their terminal result.
    let boundaries: [(&str, &Arc<Semaphore>, usize); 7] = [
        (
            "rebalance",
            &state.rebalance_permits,
            MAX_CONCURRENT_CLUSTER_REBALANCES,
        ),
        (
            "reconcile",
            &state.reconcile_permits,
            MAX_CONCURRENT_CLUSTER_RECONCILES,
        ),
        (
            "handoff",
            &state.handoff_permits,
            MAX_CONCURRENT_CLUSTER_HANDOFFS,
        ),
        (
            "reassign",
            &state.reassign_permits,
            MAX_CONCURRENT_CLUSTER_REASSIGNS,
        ),
        (
            "corpus-administration",
            &state.stats_permits,
            MAX_CONCURRENT_STATS,
        ),
        ("remote resize", &state.remote_resize_permits, 1),
        (
            "cluster write",
            &state.write_permits,
            MAX_QUEUED_CLUSTER_WRITES,
        ),
    ];
    let mut guards = Vec::with_capacity(boundaries.len());
    for (work, permits, capacity) in boundaries {
        match quiesce_admission(permits, capacity).await {
            Ok(guard) => guards.push(guard),
            Err(source) => {
                error!(error = %source, "{work} admission closed during cluster shutdown");
            }
        }
    }
    guards
}

async fn quiesce_admission(
    permits: &Arc<Semaphore>,
    capacity: usize,
) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
    let capacity = u32::try_from(capacity).unwrap_or(u32::MAX);
    Arc::clone(permits).acquire_many_owned(capacity).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_quiescence_waits_for_and_then_retains_worker_admission() {
        let permits = Arc::new(Semaphore::new(2));
        let active = Arc::clone(&permits)
            .acquire_owned()
            .await
            .expect("active worker permit");
        let wait_permits = Arc::clone(&permits);
        let mut shutdown = Box::pin(quiesce_admission(&wait_permits, 2));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut shutdown)
                .await
                .is_err(),
            "shutdown must wait while a detached worker owns admission"
        );

        drop(active);
        let guard = tokio::time::timeout(std::time::Duration::from_secs(1), &mut shutdown)
            .await
            .expect("shutdown quiescence completed")
            .expect("worker admission remained open");
        assert_eq!(
            permits.available_permits(),
            0,
            "shutdown must retain admission through durability cleanup"
        );
        drop(guard);
        assert_eq!(permits.available_permits(), 2);
    }
}
