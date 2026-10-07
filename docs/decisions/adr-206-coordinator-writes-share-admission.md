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

The first does not need it. The cluster orders writes itself: a write holds the mutation
barrier shared and a lock on its logical id from before its log append until its fan-out is
complete (ADR-177). The second does not need it for what it was meant for: the mutation-frozen
view excludes every mutation by itself. It did one more thing for searches that nobody had
written down, which the decision below keeps (item 4).

What the one mutex cost:

- **Served writes ran one at a time.** The per-id locks of ADR-177 never saw two served writers;
  the gain that decision measured was available to a library embedder only.
- **One slow write held up every write.** A write to a remote position that does not answer
  waits for the write deadline (30 seconds by default), and every other write, to any id, queued
  behind it.
- **A default v2 search waited for a whole bulk batch**, where it needs only the item in
  flight to finish.
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
4. **A search that returns sources or an explanation takes it shared, like a write.** It is
   kept apart from writes by the cluster's mutation-frozen view, not by this lock. It shares
   admission for another reason. Such a search holds the cluster's read lock while it waits
   for its view and then for a worker of the search pool. The pool's workers take the same
   lock for each title they match, and they wait when a writer is queued for it. If a
   vocabulary rebuild could queue for the write lock behind the search, the workers would wait
   for the rebuild, the rebuild for the search, and the search for a worker. Whoever takes the
   cluster's write lock holds write admission alone, so a search that shares admission cannot
   have one queue behind it. The old mutex did this by accident; the shared side does it on
   purpose. One function runs every such search (`run_with_stable_view`), and the rule is
   stated where the pool is declared.
5. **A search that returns ids only takes nothing**, as before.
6. **The order is fixed:** write admission, then the cluster lock, then the mutation barrier,
   then the id lock.
7. **At most 32 writes run at once**, the admission bound ADR-183 already set for how many
   writes may hold a blocking thread.

## What changes for a caller

- **Two bulk batches that run at the same time interleave their items.** Before, the second
  batch waited for the first. Each item is still applied whole, writes to one id are applied
  in logged order, and a reopen replays the same order. A client that sends two batches at
  once for the same ids gets the last one logged for each id, as it would from Elasticsearch
  or OpenSearch. A client that needs one batch applied before another sends them one after
  the other.
- **A write to one id no longer waits for a slow write to another.**
- **A search that returns sources no longer waits for a bulk batch to finish**, or for a
  write to another id. It still waits for the writes in flight at that moment, and for every
  operation that holds admission alone (a checkpoint, flush or backup, a vocabulary change, a
  resync, a resize, an exhaustive job), and it still runs alone among such searches, because
  the view it takes is exclusive.

## Alternatives considered

- **Take the lock off enriched searches altogether.** This was the first version. It opened
  the cycle described in item 4. Two ways of closing it without a lock were tried and set
  aside: keeping such searches out of the search pool (their fan-out then ignores the
  configured thread budget), and taking a pool worker before the cluster lock (a long wait for
  the view then parks a pool worker, and a worker that holds the lock can pick up another
  search while it waits on its own fan-out). Taking the lock off would also have gained
  little: an exhaustive job and a checkpoint hold the mutation barrier, so the search waits
  for them at its view in any case.
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
- Two cycles of the same kind exist on `main` and are not closed here: a remote resize takes
  the cluster's write lock for its cutover after it has released write admission, and a pool
  worker that holds the cluster's read lock for one title can pick up another title's job
  while it waits on its own fan-out, which blocks if a writer has queued meanwhile. They are
  recorded as a separate issue.
  **2026-10-06:** both are closed by
  [ADR-207](adr-207-cluster-writers-wait-outside-the-search-pool.md). A writer of the cluster
  lock now waits at the search pool's gate, the remote cutover holds write admission alone,
  and `write_cluster` checks the rule of item 4 where the lock is taken.
- **Not changed, and still limits of the coordinator:**
  - One write still visits its shards one after another.
  - A remote position that does not answer still costs every write that touches it the write
    deadline. Since ADR-185 an upsert touches only the shards of its placement, unless the
    placement moves; a delete visits every shard.
  - Searches that return sources exclude each other and wait for writes in flight.
  - An exhaustive job holds write admission alone until its stream is read, so writes and
    searches that return sources wait for it.

## Proven

- `handlers/cluster/tests/write_admission.rs`: with a write in flight (its shared admission
  held), a `PUT`, a `DELETE` and a bulk batch for other ids are applied, and `/v2/_search`,
  `/v2/_mpercolate` and `/_search` with `_source` answer; with admission held alone, a write
  and a search that returns sources each wait and run afterwards; flush, checkpoint, replacing
  the vocabulary, learning and applying a vocabulary, importing aliases and learning aliases
  each wait for a write in flight and then run.
- `handlers/cluster/tests/search_pool.rs`: with every worker of the search pool occupied,
  each of the three enriched surfaces shares admission and waits for a worker; a vocabulary
  change that arrives then waits at admission and does not queue for the cluster's write
  lock, which stays free to read; once a worker is free the search answers, and then the
  vocabulary change runs.
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
