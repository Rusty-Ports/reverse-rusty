# ADR-206 — Coordinator writes share admission

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

In coordinator mode the server held one mutex, `write_serial`, for three different purposes:

- **To order writes.** Every `PUT`, `DELETE` and bulk batch held it across its log append and
  its whole shard fan-out (ADR-070).
- **To give a search a stable view.** A search that returns sources or an explanation took it
  before taking the cluster's mutation-frozen view. `/v2/_search` and `/v2/_mpercolate` return
  sources by default.
- **To keep whole-cluster operations apart from writes.** Flush, checkpoint, backup, a
  vocabulary change, resync, resize and an exhaustive job each held it for their whole run.

Only the third needs it. The cluster orders writes itself: a write holds the mutation barrier
shared and a lock on its logical id from before its log append until its fan-out is complete
(ADR-177). And the mutation-frozen view excludes every mutation by itself, and since ADR-197
every checkpoint, flush and backup.

What the one mutex cost:

- **Served writes ran one at a time.** The per-id locks of ADR-177 never saw two served writers;
  the gain that decision measured was available to a library embedder only.
- **One slow write held up every write.** A write to a remote position that does not answer
  waits for the write deadline (30 seconds by default), and every other write, to any id, queued
  behind it.
- **A default v2 search waited for things it does not depend on:** a whole bulk batch, a
  resize, and an exhaustive job, which holds the mutex until its stream has been read.
- The comments on the lock, and ADR-070, said that reads never take it.

## Decision

1. **`write_admission` is a reader-writer lock.**
2. **An ordinary write takes it shared**: `PUT /_doc/{id}`, `DELETE /_doc/{id}` and a `_bulk`
   batch, for as long as it runs. Writes run beside each other. Writes to one id are applied in
   the order the coordinator logged them, as before, because the id's lock is held across the
   append and the fan-out.
3. **An operation that needs every write finished and none started takes it alone**: flush,
   checkpoint, backup, the four vocabulary changes, resync, resize, an exhaustive job, and the
   shutdown sequence. The lock prefers a waiting exclusive holder, so a stream of writes cannot
   starve one.
4. **A read does not take it.** A read that returns sources or an explanation takes the
   cluster's mutation-frozen view, which is what gives it one coherent state.
5. **The order is fixed:** write admission, then the cluster lock, then the mutation barrier,
   then the id lock.
6. **At most 32 writes run at once**, the admission bound ADR-183 already set for how many
   writes may hold a blocking thread.
7. **A search that returns sources stays out of the search pool.** It holds the cluster lock
   for its whole run. The pool's workers take that lock for each title they match, and they
   wait when a writer is queued for it. Had such a search waited for a worker while holding
   the lock, a vocabulary rebuild queued behind it would have completed a cycle: the workers
   wait for the rebuild, the rebuild for the search, the search for a worker. The old mutex
   prevented this by accident, because a rebuild held it too. Now the search runs on the
   blocking thread it already has. Such searches run one at a time, so this is one thread; its
   fan-out to the shards uses the process's general pool. The rule is stated where the pool is
   declared: never wait for the search pool while holding the cluster lock.

## What changes for a caller

- **Two bulk batches that run at the same time interleave their items.** Before, the second
  batch waited for the first. Each item is still applied whole, writes to one id are applied
  in logged order, and a reopen replays the same order. A client that sends two batches at
  once for the same ids gets the last one logged for each id, as it would from Elasticsearch
  or OpenSearch. A client that needs one batch applied before another sends them one after
  the other.
- **A write to one id no longer waits for a slow write to another.**
- **A search that returns sources no longer waits for a bulk batch to finish, for a resize or
  for an exhaustive job.** It still waits for the writes in flight at that moment and for a
  checkpoint, flush or backup in progress, and it still runs alone among such searches,
  because the view it takes is exclusive.

## Alternatives considered

- **Take the mutex off reads and leave writes as they were.** It fixes the search and leaves
  served writes one at a time, with every write behind the slowest.
- **A second lock that keeps bulk batches from interleaving.** It keeps an ordering no client
  can observe across two requests it sent at once, and a long batch would again hold up every
  other batch.
- **No server-level exclusion at all**, relying on the library's barrier for checkpoint, flush
  and backup (ADR-197). Resize, resync and a vocabulary change still need writes held out for
  their whole run, and an exhaustive job needs the corpus still until its stream is read.

## Consequences

- The comments in `state.rs` and `handlers/cluster.rs` describe the lock as it is. ADR-070's
  concurrency model and its "reads are never blocked by writes" are superseded here; ADR-169
  and ADR-177 carry dated notes.
- The cycle in item 7 was already possible in one place before this change: a remote resize
  takes the cluster's write lock for its cutover after it has released write admission. Keeping
  searches that hold the lock out of the pool closes that as well.
- **Not changed, and still limits of the coordinator:**
  - One write still visits its shards one after another.
  - A remote position that does not answer still costs every write that touches it the write
    deadline. Since ADR-185 an upsert touches only the shards of its placement, unless the
    placement moves; a delete visits every shard.
  - Searches that return sources exclude each other and wait for writes in flight.
  - An exhaustive job holds write admission alone until its stream is read, so writes wait
    for it.

## Proven

- `handlers/cluster/tests/write_admission.rs`: with a write in flight (its shared admission
  held), a `PUT`, a `DELETE` and a bulk batch for other ids are applied; with admission held
  alone, a write waits and is applied afterwards; with admission held alone, `/v2/_search`,
  `/v2/_mpercolate` and `/_search` with `_source` answer; flush, checkpoint, replacing the
  vocabulary, learning and applying a vocabulary, importing aliases and learning aliases each
  wait for a write in flight and then run.
- `handlers/cluster/tests/search_pool.rs`: with every worker of the search pool occupied, a
  search that returns ids only waits, and `/v2/_search`, `/v2/_mpercolate` and `/_search` with
  sources answer.
- `handlers/cluster/tests/write_concurrency.rs`, on a durable three-shard cluster: eight
  clients each post five bulk batches over the same thirty-two ids, in different orders. Every
  id ends on a body one of them wrote for it, the index matches the stored source, and a
  reopen replays the log to the same bodies. Eight clients create, replace and delete distinct
  ids at once; every id ends as its own client left it, live and after a reopen.
- `handlers/cluster/tests/checkpoint.rs`, `backup.rs` and `resync.rs`: a checkpoint, a backup
  and a resync wait for a write in flight. `handlers/jobs/tests.rs`: an exhaustive job waiting
  for admission can be cancelled. `tests/resize/operations.rs`: a resize holds admission alone.
- The library's own tests of concurrent writers are unchanged
  (`coordinator/tests/write_concurrency`, ADR-177), as are the tests that the mutation-frozen
  view excludes a write (`consistent_read_view_*`).

**See also:** ADR-070 (the cluster REST surface), ADR-177 (per-id write locks), ADR-183 (write
admission on blocking threads), ADR-185 (the upsert funnel), ADR-197 (checkpoint, flush and
backup exclude mutations).
