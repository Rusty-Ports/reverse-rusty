# ADR-207 — A cluster writer waits outside the search pool

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

In coordinator mode a search runs in the server's search pool, and the pool's workers take
the cluster's read lock for each title they match. The lock makes a new reader wait while a
writer is queued, so that a stream of readers cannot starve a writer. The write lock is taken
by the four vocabulary changes, by an in-process resize, and by the cutover of a remote
resize.

A title that routes to more than one shard fans out inside the pool. The worker that holds
the read lock for that title waits for the fan-out, and while it waits it and the workers
running the fan-out pick up other jobs of the pool: another title of the same batch, or a
title of another request. That job asks for the read lock too. If a writer has queued
meanwhile, three things wait for each other:

- the new job waits for the writer, because a reader does not pass a queued writer;
- the writer waits for the first title's guard;
- the first title waits for its fan-out, which is under or behind the job that is now
  waiting.

Nothing recovers. Every worker ends up parked, every search waits for the pool, the
vocabulary change never returns, and writes wait for the admission it holds.

It needs only a batch whose titles route to several shards and a vocabulary change or a
resize that arrives while the batch runs. It was reproduced on `main`: a two-thread pool,
eight shards, six `/_search` requests of 200 titles at a time, and one thread that takes and
releases the write lock. The coordinator stopped within seconds. A thread sample showed both
pool workers waiting for the read lock under a `join`, and the writer waiting for the write
lock.

ADR-206 closed one case of this, a search that returns sources and waits for a worker while
it holds the lock. It recorded two more as open: this one, and the cutover of a remote
resize, which took the write lock after it had released write admission.

## Decision

1. **The search pool has a gate**, a reader-writer lock of its own.
2. **A request holds the gate shared for as long as its work is in the pool.** It takes it on
   its own thread, before the work is installed in the pool and before it takes the cluster
   lock.
3. **Whatever takes the cluster's write lock holds the gate alone**, from before it asks for
   the lock until after it has released it. So a writer is queued for the cluster lock, or
   holds it, only while the pool is empty. A worker of the pool never finds a writer queued,
   and requests that arrive meanwhile wait on their own threads, behind the writer.
4. **The types keep both rules.** The pool's workers are private to `SearchPool`, and work
   reaches them only through `SearchPool::enter`. The write side of the cluster lock is
   private to `ClusterLock`, and is reachable only through `ClusterAppState::write_cluster`
   and its timed form. A handler cannot put work in the pool without the gate, and cannot
   take the write lock without closing it.
5. **The write lock is taken under write admission, and the caller shows it.**
   `write_cluster` takes the caller's exclusive admission guard, checks that it belongs to
   this coordinator, and keeps a borrow of it for as long as the write lock is held. This is
   ADR-206's rule, enforced where the lock is taken. The cutover of a remote resize, which
   did not follow it, now does.
6. **The gate is taken once for a request.** It prefers a waiting writer, as the cluster
   lock does, so a second shared hold under the first would be the same cycle one level up.
   Work that is already in the pool is under the hold of the request that brought it in, so
   it enters again without taking the gate.
7. **The order is fixed:** write admission, then the gate, then the cluster lock, then the
   mutation barrier, then the id lock.

## Prior art

The hazard is documented where the pieces come from. A lock that prefers a waiting writer
must not be read again under itself: `parking_lot` says so of its `RwLock`, and Go says so of
`sync.RWMutex`. A work-stealing pool does that re-reading for you: a worker that waits on a
parallel call runs other queued work, which Rayon records as "using rayon under a Mutex can
lead to deadlocks" with no better advice than not to hold a lock across a parallel call.

Systems that serve reads beside rare whole-structure changes use one of two designs:

- **Operation permits.** Every operation holds a permit, and a transition takes all of them
  and waits for the operations in flight to drain. Elasticsearch does this for a shard
  (`IndexShardOperationPermits`), and Seastar calls the primitive a gate. This decision is
  that design.
- **Published snapshots.** A request pins one immutable version for its whole run, and a
  change is built beside it and swapped in, so neither waits for the other. Lucene's
  `ReferenceManager` does this for searchers, and this server's single-node mode does it for
  the whole engine. It is the better design for the coordinator too, because it also removes
  the wait for the rebuild itself, and it would make this gate unnecessary. It means
  splitting the cluster's serving layout from its coordination state, which is a larger
  change than a deadlock should wait for. The roadmap carries it as "In-process rebuilds that
  keep serving".

Two things those systems do that this decision does not: Elasticsearch's transition waits
for its permits with a timeout, and it queues the operations that arrive meanwhile instead
of parking their threads. Here a resize waits with its budget, a vocabulary change waits
without one, and a search that arrives parks a blocking thread, as it did before at the
cluster lock.

## What changes for a caller

- **A vocabulary change or a resize waits for the search requests in flight**, where it
  used to wait for the titles in flight and could run between two titles of one batch. The
  wait is bounded by the search timeout. Searches that arrive while it waits queue behind it,
  as they did at the cluster lock.
- **A source-free `/_search` batch is matched under one vocabulary and one shard layout** from
  its first title to its last.
- **The coordinator no longer stops** when a vocabulary change or a resize meets a search
  batch.
- A resize with `timeout=0` still reports that it could not start when a search is running.

## Alternatives considered

- **Let a thread that already holds the read lock take it again without waiting.** This was
  the first version: a per-thread count of guards, and a read that does not wait for a queued
  writer when the count is above zero. It fixes the case where the second read is on the same
  thread as the first, and a test of exactly that case passed. The served test still stopped.
  The worker that waits is not always one that holds a guard: it can be running the fan-out
  of a worker that does, and nothing on its own thread says so.
- **Let every read inside the pool pass a queued writer.** A steady stream of searches would
  then starve a vocabulary change, so the writer's turn would have to come from somewhere
  else, which is the gate. With the gate in place a read inside the pool never meets a queued
  writer, so nothing is gained.
- **One read guard for each request, taken on the request's thread, with the pool's work
  borrowing the cluster from it.** No worker would touch the lock, and a writer would wait
  for whole requests exactly as it does now, with no second lock. It is a rule about what the
  pool's work may call, and the types cannot keep it: a later handler that reads the lock
  inside the pool would reopen the cycle without a test failing. The gate is kept by two
  functions that every path goes through.
- **A larger pool, or a fan-out that does not use the pool.** Neither removes the cycle. A
  fan-out outside the pool also ignores the configured thread budget, which ADR-206 already
  set aside.

## Consequences

- ADR-206's item 4 no longer carries the safety of the pool alone. A search that returns
  sources holds the gate like every other request. It still shares write admission.
- The two cycles ADR-206 recorded as open are closed.
- An exhaustive job runs in its own pool and holds write admission alone for its whole run,
  so no writer of the cluster lock can queue beside it. Nothing changes there.
- Single-node mode is not involved. Its searches read a published snapshot and take no lock.
- A test can still take the bare write lock (`ClusterLock::write`, compiled for tests only)
  to make a request wait. No production code can.

## Proven

`handlers/cluster/tests/pool_gate.rs`:

- A worker of the pool holds the read lock for a title in flight. A writer arrives. It waits
  at the gate, the cluster lock stays free to read, and sixteen more reads run on the pool's
  workers. A request that arrives after the writer waits behind it. Once the first request
  leaves the pool the writer runs, and then the later request. With the gate taken after the
  lock instead of before it, this test stops as `main` did, and fails.
- With a writer holding the lock, a request's work does not reach the pool. It runs once the
  writer is done.
- Work in the pool enters the pool again, on eight workers' worth of jobs, while a writer
  waits at the gate. It does not wait for the writer, and the writer runs once the request
  has left.
- The timed form gives up at the gate when work is in the pool, without waiting when it has
  no deadline and after its budget when it has one. It does not hold or queue for the cluster
  lock while it waits at the gate. Within its budget it waits for a request to leave the pool
  and for a reader outside the pool to release the lock. When it gives up at the lock it
  reopens the gate. A deadline already passed is not a try without waiting.
- `write_cluster` refuses a guard that is not this coordinator's write admission.
- Through the served path: `/_search` and `/v2/_mpercolate` without sources, six at a time
  over 200 twelve-word titles on eight shards, while a thread takes and releases the write
  lock the way a vocabulary change does. Every search answers and the writer keeps taking its
  turn. The requests return ids only on purpose: a search that returns sources shares write
  admission, which keeps the writer away from it by itself.

Eleven mutations of the gate were run against these tests. Ten fail a test: the lock asked
for before the gate, in the plain and the timed form; work installed without the gate; a
request passing a waiting writer; work in the pool taking the gate again; every entry
skipping the gate; the timed form not waiting at the gate, not waiting at the lock, or
ignoring its deadline; no admission check. One changes nothing a test can see: a search that
returns sources entering the pool without the gate, because write admission already keeps
every writer away from it.

`handlers/cluster/tests/search_pool.rs`, `write_admission.rs`, `vocab.rs` and
`tests/resize/` are unchanged and pass: a vocabulary change and a resize still wait for
admission, and a search that returns sources still waits for a worker inside the pool.

**See also:** ADR-046 (the vocabulary rebuild), ADR-099 (the search deadline), ADR-180 (the
remote resize), ADR-206 (write admission).
