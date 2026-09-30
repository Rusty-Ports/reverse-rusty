//! The dedicated runtime every cluster RPC runs on.
//!
//! Request handlers and administrative workers wait on the coordinator's synchronous locks
//! (`write_serial`, the cluster `RwLock`), and some of them do so on the HTTP runtime's worker
//! threads. If the RPCs a lock holder is waiting on were also driven by those workers (their
//! connections' I/O and timers), enough blocked handlers would stall the runtime and the holder
//! would never finish: a deadlock that needs no bug beyond ordinary concurrent writes. Cluster
//! RPC channels are therefore created and driven on this separate runtime, whose workers never
//! take coordinator locks, so every lock holder makes progress or times out.

use std::sync::OnceLock;

use tokio::runtime::{Builder, Handle, Runtime};

/// Kept for the life of the process: a runtime must never be dropped from async context.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// The handle every cluster connection, control-plane client, and data-moving operation is given.
pub(crate) fn cluster_rpc_handle() -> Handle {
    RUNTIME
        .get_or_init(|| {
            let workers = std::thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 8));
            Builder::new_multi_thread()
                .worker_threads(workers)
                .thread_name("rr-cluster-rpc")
                .enable_all()
                .build()
                .expect("failed to build the cluster RPC runtime")
        })
        .handle()
        .clone()
}

#[cfg(test)]
mod tests {
    use super::cluster_rpc_handle;

    #[tokio::test]
    async fn cluster_rpcs_run_on_their_own_runtime() {
        // Even from inside another runtime, work spawned on the cluster handle runs on the
        // dedicated workers, never on the caller's.
        let name = cluster_rpc_handle()
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .expect("task");
        assert_eq!(name.as_deref(), Some("rr-cluster-rpc"));
    }
}
