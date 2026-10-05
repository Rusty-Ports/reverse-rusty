# ADR-191 — Data-plane handlers never wait for a blocking lock on a runtime worker

> [Engine quality & operations decisions](areas/engine-quality-and-operations.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The server's shared state sits behind synchronous locks: the standalone engine mutex and flush
mutex, and the coordinator's write serializer and cluster `RwLock`. A thread waiting for one of
them does not yield; it stays parked. ADR-183 moved the coordinator's write handlers off the
async workers. These still waited on a worker:

- **Standalone `PUT /_doc`, `DELETE /_doc`, `POST /_bulk` and `/_flush`.** Each took the engine
  mutex inside its `async fn` and did the work there: the WAL append, the NDJSON parse of a body
  of up to 100 MB, a bulk segment commit, and the memtable flush plus auto-compaction that any
  write can trigger. A waiting flush also blocked on the flush mutex.
- **Coordinator `GET`/`HEAD /_doc`, `GET /`, the `/v2/_search` and `/v2/_mpercolate` compile
  step, and job creation.** Each took `cluster.read()` inline. That is brief on its own, but it
  waits whenever the exclusive lock is held or queued for.

The long holders already run on blocking threads: compaction, backup and vocabulary or alias
replacement hold the engine mutex for as long as they take; a vocabulary rebuild or an in-process
resize holds the exclusive cluster lock for an `O(corpus)` rebuild. While one runs, each arriving
request of the kinds above parked one worker. The runtime has one worker per CPU, or per CPU of
the container. Once that many requests were waiting, nothing was left to poll anything: searches
that only need a lock-free snapshot, `/_health`, connection accepts. The Helm coordinator probes
`/_health` for liveness.

This is an availability defect. Matching correctness and durability were never affected.

## Decision

1. **The rule.** A data-plane handler never waits for one of these locks, and never does engine
   or cluster work, on an async worker. It awaits an admission permit and then runs on a blocking
   thread.
2. **Standalone writes** go through `run_engine_write` (PUT, DELETE) or the equivalent wrap
   (bulk, which also parses its body there, and flush, which also waits for the flush mutex
   there). Admission is a semaphore of `MAX_QUEUED_WRITES` (32) permits. The permit is awaited,
   so a request cancelled in the queue starts nothing, and it is moved into the worker, so a
   write whose client disconnects still holds its slot until it finishes. Queued writers are
   futures; at most 32 hold a thread.
3. **The worker publishes.** The read snapshot is published by the blocking worker, under the
   same engine lock as the write. A request dropped after admission still publishes what it
   wrote, and the write and its read view are one commit.
4. **Shutdown** takes every write permit before its final flush, so a detached write lands
   before it and none is admitted after it.
5. **`/_flush?wait_if_ongoing=false`** checks for a flush in progress before awaiting admission,
   so it still answers `409 flush_in_progress_exception` when that flush and queued writes hold
   every permit. The worker re-checks under the lock.
6. **Coordinator reads** go through `read_cluster`, the same shape with its own semaphore
   (`MAX_QUEUED_CLUSTER_READS`, 64): the brief read of the cluster engine runs on a blocking
   thread, and the threads parked behind one exclusive holder are bounded.
7. **A write that cannot report a result** (its worker panicked, or admission is closed at
   shutdown) answers `500 write_worker_failed`. Whether it was applied is not known to the
   server in that case, and the response says so rather than guessing.

## Alternatives considered

- **`block_in_place` around the lock sections.** A smaller diff, but each waiter still costs a
  worker hand-off, it panics on the current-thread runtime the tests use, and it puts no bound on
  the waiters.
- **`tokio::sync` locks.** Every synchronous holder (Rayon workers, the supervised maintenance
  threads) would need `blocking_*` calls, and the bounded `try_*_for` waits the administrative
  paths rely on have no equivalent. ADR-183 rejected it for the same reasons.
- **A bounded wait that answers 429.** It protects latency but changes what a client sees under
  load. Queued writes wait, as they did before; only where they wait changed.
- **A type-level token for lock access.** It would make the rule a compile error instead of a
  test failure, at the cost of touching every one of some seventy lock sites. The table-driven
  tests below cover each route and are where a new route is added.

## Consequences

- A compaction, backup, vocabulary rebuild or resize no longer degrades unrelated requests,
  however many writes or reads queue behind it. The statements in ADR-138 and the backup and
  resize guides that maintenance pauses writes and not reads now hold under write load.
- One thread hop per write and per coordinator point read. It is small next to a WAL append.
- Beyond 32 queued standalone writes, or 64 queued coordinator reads, further requests wait
  asynchronously for admission.
- Not covered here: the shard server's gRPC handlers, which take the per-shard engine mutex
  inline (RR-049), and the exhaustive job's wait on its output channel while it holds the write
  serializer (RR-062).

## Proven

- `handlers/doc/tests/write_admission.rs`: with the engine mutex held, a queued PUT, DELETE or
  bulk request on a single-threaded runtime does not stop a lock-free read from being served;
  cancelled writes keep their permits until their workers finish, and exactly the admitted ones
  apply; a write whose request is dropped after admission is still published; shutdown
  quiescence waits for an admitted write and then holds every permit.
- `handlers/admin/flush_tests.rs`: a flush queued behind the engine mutex or behind another
  flush does not stop a timer on a single-threaded runtime; a non-waiting flush answers 409
  while a flush and queued writers hold every permit.
- `handlers/cluster/tests/read_admission.rs`: with the exclusive cluster lock held, each of
  `GET /_doc`, `HEAD /_doc`, `GET /`, `POST /v2/_search`, `POST /v2/_mpercolate` and
  `POST /_percolate/jobs` waits without stopping a timer on a single-threaded runtime;
  cancelled reads keep their permits until their workers finish.

Each case fails, within its timeout, when its handler takes the lock on the async worker.

**See also:** ADR-183 (coordinator write handlers and the dedicated RPC runtime), ADR-099
(bounded search admission), ADR-137 (flush contract), ADR-138 (compaction contract).
