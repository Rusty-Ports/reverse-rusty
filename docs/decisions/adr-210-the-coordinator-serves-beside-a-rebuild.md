# ADR-210 — The coordinator serves beside a rebuild

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted · **Supersedes:**
> [ADR-207](adr-207-cluster-writers-wait-outside-the-search-pool.md)

## Problem

A vocabulary change or an in-process resize rebuilds every shard, which takes as long as the
corpus is large. In coordinator mode nothing answered while it ran, for two reasons.

1. **The server's own lock.** The server held the engine inside a reader-writer lock and took
   the write side for a rebuild. Every search, every read and every health probe took the read
   side, so they waited for the rebuild and its checkpoint. That lock is also why the search
   pool needed a gate (ADR-207): a pool worker that asked for the read side behind a queued
   rebuild could stop the pool for good.
2. **The administrative slot.** One permit bounds expensive administrative work. A rebuild
   held it for its whole run, and `/_health`, `/_metrics`, `/_stats`, `/_cat/shards`,
   `/_settings`, `GET /_vocab`, the alias registry and `/_cluster/state` wait for the same
   permit. With the lock gone they would still have queued behind the rebuild: a health probe
   answering red at its deadline, a metrics scrape hanging, for as long as the rebuild took.

Since ADR-209 the engine keeps a rebuild apart from everything but a search by itself, and
its rebuild methods take shared access. The server's lock and the slot were what was left.

## Decision

1. **The server holds the engine with no lock.** `ClusterLock`, the search pool's gate,
   `write_cluster` and `ClusterWrite` are deleted. The search pool is a thread budget and
   nothing else.
2. **A rebuild holds the topology guard and write admission alone for its whole run**
   (`ClusterAppState::admit_rebuild`; the resize handler takes the same two locks in the same
   order with its time budget). A vocabulary change did not take the topology guard before.
   It does now, so that an operation with a time budget (a rebalance, a reassignment, a
   handoff, a reconcile, a GC, a node registration) still gives up at that guard within its
   budget. Without it, such an operation would start, enter the engine, and wait there for
   the rebuild with no way to give up.
3. **A search takes nothing.** One that returns sources or an explanation takes the engine's
   mutation-frozen view and no longer shares write admission. A rebuild holds admission for
   its whole run, and this search must wait only for the moment the rebuild swaps its layout
   in, which the view does (ADR-209 publishes under the same barrier). This amends item 4 of
   ADR-206, whose reason was the cluster lock.
4. **Administrative changes have their own admission slot** (`admin_change_permits`), apart
   from the slot for administrative reads. So the reads are never queued behind a rebuild, or
   behind a change that is itself waiting for one. The operations on the new slot are the
   four vocabulary changes and an in-process resize, which are rebuilds, and a node
   registration, a deregistration and a resync, which wait for a rebuild at the topology
   guard or at write admission. One is admitted at a time and the rest wait as futures, as
   they did on the old slot.
5. **A vocabulary change is refused, not parked, while a remote resize copies.** A remote
   resize holds the topology guard for its whole copy, with its write fence up. A change that
   waited for the guard would wait for the copy. `admit_rebuild` looks at the fence when the
   change arrives and again while it waits for the guard; the engine checks it once more when
   the change begins.
6. **What a request reports about the layout, it reads from one pinned layout.** Each
   accessor of the engine reads the layout that is published when it is called, and the
   server's lock used to keep two such reads of one request on one layout. Without it a
   rebuild can swap in between them. Review found three: `/_stats` put eight shard counts
   beside a shard total of nine; a health probe read the committed topology before a resize's
   swap and the shard counts after it, took the difference for a fault, and answered red; a
   cursor request passed the stale gate on the old layout and computed its fingerprint under
   the new normalizer, and answered "mismatch" for a cursor that was stale.

   The engine has `ClusterEngine::published`, which pins the published layout and returns a
   `PublishedLayout` with the same accessors. The pin stays what it was across a swap, so
   everything read through it belongs together. `/_stats` reads its counts from one pin. A
   cursor request pins before the stale gate and computes its fingerprint from the pin, so a
   cursor that passes the gate is fingerprinted under the normalizer it was minted under; the
   gate is also asked a second time before a fingerprint is called a mismatch.

   A test reads the server's source (`handlers/cluster/tests/one_layout.rs`) and fails a
   function that reads the layout from the engine more than once, where it should pin. The
   exceptions are named in it with their reasons.
7. **A difference between the layout and the control state is a fault only when no rebuild
   is making it.** A rebuild publishes its layout and then commits the control state, and no
   pin can make those two one. `/_health` and `/_cat/shards` compare a pinned layout with the
   control state, and ask `ClusterEngine::layout_change_in_progress` before they call a
   difference a fault: while a rebuild runs the difference is passed over, and when one may
   just have finished both are read again. A difference that is still there with nothing
   running is reported, as before.
8. **A rebuild does not change the colour of `/_health`.** The colour says what is served
   and how redundantly, and searches answer exactly throughout a rebuild. The response
   carries `rebuild_in_progress` beside the colour instead.
9. **Nothing that runs in the search pool takes a lock that a rebuild holds or waits for.**
   A worker that waited behind a rebuild would hold up the workers that are waiting for it,
   which is the deadlock ADR-207 describes. The types no longer enforce this (there is no
   lock to hide). A stress test of real rebuilds against a two-worker pool does.

## What changes for a caller

- **Searches and reads answer during a vocabulary change or a resize.** Until the swap they
  answer from the layout being replaced; after it, from the new one. This covers every search
  route with and without sources, `GET` and `HEAD /_doc/{id}`, `/`, `/_health`, `/_metrics`,
  `/_stats`, `/_cat/shards`, `/_settings`, `GET /_vocab`, `GET /_vocab/aliases` and
  `/_cluster/state`.
- **A source-free `/_search` batch can see the swap between two of its titles.** Each title
  is matched whole on one layout, but the titles before the swap are matched under the old
  vocabulary and the ones after it under the new. ADR-207 had made a batch wait out the
  change as a side effect of its gate; that is gone. A search that returns sources, and a
  `/v2/_mpercolate` batch, still see one layout throughout.
- **`/_health` has a new field, `rebuild_in_progress`.** It is true while a vocabulary change
  or a resize is rebuilding the cluster, or is waiting for the operations in flight before it
  starts. The colour does not change for a rebuild. To wait for a rebuild, wait for the
  request that started it: a vocabulary change answers when it is done, and a resize has its
  operation record.
- **Writes wait for a rebuild**, at write admission, as before.
- **An operation with a time budget answers "not started" within it** during a rebuild, as
  before.
- **Expensive administrative reads can run beside a rebuild** (a stats scan, a vocabulary
  snapshot, read-only learning). Before, the shared slot kept them apart. A rebuild already
  holds two layouts in memory, so size headroom for a rebuild and one such read together.

## Alternatives considered

- **Keep a server lock, for requests that are not searches.** It would give a request with a
  time budget somewhere to give up. The topology guard and write admission already are that
  place, once a vocabulary change takes the topology guard as a resize does. A third lock
  would say the same thing.
- **Run a read again if a swap landed inside it**, in place of a pin. This was built first
  (a closure that the engine re-ran when the published layout had changed under it, and a
  second form that refused the read while a rebuild ran). It works, but it is the pattern
  for data that has no single pointer to pin: the read can see a mixture before it is
  thrown away, so it has to be free of side effects and cheap to repeat, and a stats scan
  that calls every shard is neither. See Prior art.
- **Answer `/_health` yellow while a rebuild runs.** Also built first, so that
  `wait_for_status=green` would wait for a rebuild. But yellow means reduced redundancy or
  work owed, operators alert on it when it lasts, and a rebuild lasts minutes with nothing
  wrong. A flag beside the colour says what is happening without saying something is wrong.
  A `wait_for_no_rebuild` parameter in the manner of Elasticsearch's
  `wait_for_no_relocating_shards` was left out: the health parameters are shared with
  single-node mode, and the request that starts a rebuild already answers when it is done.
- **Let operations with a time budget wait inside the engine.** Simpler, and they would lose
  their "not started" answer: a rebalance sent during a rebuild would hold its thread and its
  permit until the rebuild finished.
- **Leave rebuilds on the administrative slot and give `/_health` a slot of its own.** Health
  would answer, and a metrics scrape and every other read would still hang for the rebuild.
- **Load one layout for a whole source-free batch**, to keep a batch on one vocabulary. It
  needs a batch form of the compatibility route in the engine. The titles of such a batch are
  independent requests in one envelope, and each is exact for the layout it ran on, so this
  was left out.

## Consequences

- The lock order in the server is the topology guard, then write admission; inside the engine,
  the layout lock, then the mutation barrier.
- ADR-191 runs brief reads of the cluster on blocking threads. Its reason is narrower now: a
  brief read no longer waits for a rebuild at a lock, but it can still wait for a remote
  shard, and one that is not a search waits inside the engine.
- `layout_change_in_progress` is also true for a rebuild that is waiting for the operations
  in flight before it starts, so `rebuild_in_progress` is true from the moment a rebuild is
  asked for. Writes are already waiting by then.
- The per-call accessors stay on the engine, for a caller that reads one thing. Nothing but
  the source test stops a server function from calling two of them.
- `/_health`'s shard probe still shares one admission slot with stats scans and vocabulary
  reads, so a slow one of those can make a probe answer at its deadline. That is older than
  this change and is tracked separately.
- The engine has two hidden test seams (`set_rebuild_hook_for_test`,
  `set_resize_write_fence_for_test`). The server's tests use them to stop a real rebuild
  half-way and to stand in for a remote copy.
- While a rebuild runs and searches answer, both layouts are in memory (ADR-209).
- Still open, on the roadmap: writes wait for the whole rebuild, and a rebuild's time and peak
  memory at scale are not yet measured.

## Proven

`handlers/cluster/tests/rebuild_availability.rs`:

- A real `PUT /_vocab`, stopped once it holds the engine alone: sixteen requests (every
  search route with and without sources, the document reads, and the administrative reads
  listed above) answer `200`, each within two seconds, while a resync with a 30-second budget
  is waiting for the rebuild; `/_health` is green with `rebuild_in_progress` true, and false
  afterwards; a search answers under the old vocabulary; a write waits; a
  rebalance with a 25 ms budget answers `408 rebalance_timeout`. When the rebuild goes on, it,
  the write and the resync succeed, a search answers under the new vocabulary, and the
  written query is matched.
- The same with a real `POST /_cluster/resize`: the old shard count serves until the swap, a
  vocabulary change waits its turn, and afterwards both have taken effect.
- A vocabulary change is refused while a remote resize copies, both when the fence is up on
  arrival (refused at once) and when it goes up while the change waits for the topology guard.
- Under load: six searches at a time over 200 twelve-word titles on eight shards, in a pool
  of two workers, while a thread resizes the cluster back and forth as fast as it can. Every
  search answers, each request answers the same ids every time, and the rebuilds keep going.
  Under the cluster lock this arrangement stopped within seconds.

`write_admission.rs`: the three surfaces that return sources answer while write admission is
held shared and while it is held alone. `search_pool.rs`: a rebuild that arrives behind a
search waiting for a pool worker stops nothing; both finish once a worker is free. The
tests of vocabulary, alias, resize and membership handlers that used to hold the cluster's
write lock now stop a real rebuild, or hold the guard or the slot the handler waits for.

`coordinator/tests/layout_change.rs`: a pinned layout answers with what it had when it was
pinned after a resize has swapped another in, and a new pin is of the new layout; a layout
change in progress can be seen without waiting for it, and an operation in flight is not
taken for one. `admin/health.rs`: a topology difference that stays is a fault, and the same
difference is passed over, with the colour unchanged and `rebuild_in_progress` set, while a
real rebuild runs. `cluster_compile.rs`: a cursor that a rebuild overtook between the stale
gate and the fingerprint is reported stale (409), and a live cursor is judged by its
fingerprint. `one_layout.rs` is the source test of decision 6.

Seventeen mutations were run and each fails a test. In the server's admission: a search with
sources that shares write admission again; a vocabulary change that does not take the
topology guard; a vocabulary change, a resize and a resync on the slot for administrative
reads; a pool worker that takes write admission for each title (the pool stops, as ADR-207
describes); a rebuild admitted without looking at the remote fence; and the fence looked at
only on arrival. In what a request reads: `/_stats` reading two counts from the engine and
not from its pin, a new function that reads the layout twice from the engine, and a cursor
fingerprint computed from the engine and not from the pin (the source test fails all three);
health taking a difference for a fault while a rebuild runs, passing over every difference,
and not asking whether a rebuild is running; the shard listing not asking; a rebuild in
progress never reported; and a mismatched fingerprint not checked against the stale gate
again.

## Prior art

Looked up after the first version of decisions 6 to 8 was written, and the reason they
changed.

- **Pin one snapshot for an operation.** The `arc-swap` documentation shows this exact
  mistake, a value loaded once for each phase of an operation, under "WARNING!! This is
  broken, because in between phase_1 and phase_2, the other thread could have replaced the
  config", and gives the fix: load once and pass it down
  ([patterns](https://docs.rs/arc-swap/latest/arc_swap/docs/patterns/index.html)). The Linux
  kernel says the same of an RCU pointer: copy it to a local variable and use that, because
  repeated dereferences "do not guarantee that the same pointer will be returned"
  ([What is RCU?](https://docs.kernel.org/RCU/whatisRCU.html)). Elasticsearch hands each
  master action one immutable `ClusterState` and computes cluster health from it; Lucene has
  a request acquire one searcher and use it throughout; RocksDB has a read reference one
  `SuperVersion`. The single-node engine here already serves from one loaded snapshot.
- **Retry is for data with no pointer to pin.** The kernel's sequence locks are for small
  data that is "rarely written to (e.g. system time)", and the mechanism "cannot be used if
  the protected data contains pointers"
  ([seqlock](https://docs.kernel.org/locking/seqlock.html)). Our layout is a pointer, so the
  first version's re-run was the wrong tool.
- **Two stores that change one after the other.** Systems either bundle them into one
  published object or stamp both and treat a difference as a transition. Elasticsearch's
  `_cat/shards` joins the cluster state with a separate stats response and tolerates the
  skew. We keep the comparison strict, because a real difference here is a failed commit,
  and ask whether a rebuild is running before calling one a fault.
- **Health colour is about the data, and planned work is reported beside it.** In
  Elasticsearch a relocating shard is still active (`ShardRouting.active()` is
  `started() || relocating()`), so a relocation leaves the cluster green; it is counted in
  `relocating_shards` and waited for with `wait_for_no_relocating_shards`. Health can be
  called under a global block ("we want users to be able to call this even when there are
  global blocks"), and a block does not change the colour. Solr reports a shard split, and
  Vespa a reindexing, as a state of its own and not as worse health.
- **Long work does not share a pool with monitoring.** Elasticsearch runs health and stats
  on a `management` pool that is "deliberately small in order to throttle the rate at which
  such tasks are executed", and gives snapshots and force-merges pools of their own. That
  is decision 4. Its source also warns that work on the management pool must be cancellable
  so that one client cannot delay the rest, which is the remaining coupling noted under
  Consequences.

**See also:** ADR-046 (the vocabulary rebuild), ADR-180 (the remote resize), ADR-191 (reads
off the runtime), ADR-206 (write admission), ADR-207 (the gate this retires), ADR-208 (the
published layout), ADR-209 (only a search runs beside a layout change).
