# ADR-183 — Isolate cluster RPCs from the HTTP runtime

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A remote coordinator could deadlock under ordinary concurrent writes.

- The PUT, DELETE, bulk, and flush handlers waited on the coordinator's synchronous locks
  (`write_serial`, the cluster `RwLock`, flush admission) directly on HTTP runtime worker threads.
- The lock holder then made remote shard RPCs through `block_in_place` and a `Handle::block_on` of
  that same runtime. The tonic channels, their connection tasks, and the I/O and timer drivers all
  lived on the HTTP runtime.
- Once every worker was parked on a lock, nothing drove the holder's RPC. Its own deadline could
  not fire either, because the timer driver was parked too, so the lock was never released.

No resize, handoff, or other administrative operation was needed: enough concurrent writes against
a remote cluster were sufficient. Long lock holders (an exhaustive job holding `write_serial`, a
data-moving administrative worker holding the cluster read lock while a vocabulary change queues
for the write lock) made the window larger.

## Decision

1. **Cluster RPCs run on a dedicated runtime.** The coordinator builds one process-lifetime
   multi-thread runtime (`rr-cluster-rpc`, 2–8 workers) and gives its handle to cluster assembly,
   the control-plane client, and every data-moving administrative operation (handoff, reassign,
   rebalance, reconcile, orphan GC, and the reconcile loop). Its workers never take a coordinator
   lock, so every lock holder's RPCs make progress or time out regardless of what the HTTP workers
   are doing. The runtime is never dropped, since a runtime must not be dropped from async context.
2. **Write handlers wait off the async workers.** PUT, DELETE, bulk, and flush acquire their locks
   and run their remote writes on a blocking thread (`run_cluster_write`, and the equivalent wrap
   for bulk and flush), so a queued write never parks an HTTP worker and other requests keep being
   served.
3. **Write admission outlives the request.** A blocking worker cannot be cancelled, so a write
   whose client disconnects keeps running after its handler future is dropped, and the
   request-concurrency slot that future held is freed. Each write therefore first awaits a permit
   from a dedicated semaphore (`MAX_QUEUED_CLUSTER_WRITES`, 32) and moves it into the worker, which
   releases it only when the write finishes. At most 32 writers can hold blocking threads, however
   many clients disconnect. A request cancelled while waiting for a permit never starts its write.

## Alternatives

- **Switch to `tokio::sync` locks.** Rejected: every synchronous holder (Rayon workers, dedicated
  administrative threads) would need `blocking_*` calls, and the bounded `try_*_for` waits the
  administrative paths rely on have no direct equivalent.
- **Add HTTP worker threads.** Rejected: it only raises the number of concurrent writes needed to
  deadlock.
- **Only move handler lock waits to blocking threads.** Rejected as the sole fix: other
  worker-side waits remain (plain reads queue behind a waiting writer on the fair `RwLock`), so RPC
  progress must not depend on HTTP workers at all.

## Consequences

Cluster RPC progress is independent of HTTP load. Writes queued behind a long lock holder no
longer degrade unrelated requests. The coordinator runs a second, small runtime. Beyond 32 queued
writes, further writes wait asynchronously for admission; a disconnected client's admitted write
still completes, exactly as it would have if the client had stayed.

## Proof

- A unit test shows work spawned on the cluster handle runs on the dedicated `rr-cluster-rpc`
  threads, even when spawned from another runtime.
- A handler test holds `write_serial`, queues a PUT on a single-threaded runtime, and requires a
  concurrent read to be served. It fails (within its timeout) when the PUT waits on the async
  worker, as it did before this change.
- A handler test queues more writes than there are permits behind a held `write_serial`, cancels
  every request, and requires the permits to stay held until the admitted workers finish, and
  exactly the admitted writes to apply. It fails if the permit is dropped with the handler future,
  released before the worker's write completes, or not taken at all.

**See also:** ADR-029, ADR-047, ADR-085, ADR-180.
