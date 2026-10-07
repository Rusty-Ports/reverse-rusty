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
6. **A read that combines the layout with the control state is told when a rebuild
   overlapped it.** The lock used to hold such a read together. A rebuild replaces the layout
   and then commits the control state, so a health probe that read the committed topology
   before a resize's swap and the shard counts after it took the difference for a fault and
   answered red. The engine has `read_between_layout_changes`: it runs a lock-free read and
   returns it only if no layout change was running at any point while it ran. `/_health`
   compares the topology under it, and answers yellow with a reason while a rebuild runs.
   `/_cat/shards` reads again until its two halves agree. A cursor request asks the stale
   gate a second time before it calls a fingerprint a mismatch, because a rebuild that swaps
   in between the gate and the fingerprint leaves a fingerprint computed under a normalizer
   the cursor was not minted under.
7. **Nothing that runs in the search pool takes a lock that a rebuild holds or waits for.**
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
- **`/_health` is yellow while a rebuild runs**, with a reason that says so, and HTTP 200.
  `wait_for_status=green` waits for the rebuild to finish.
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
- **Report health green during a rebuild.** Searches are exact throughout, but writes are
  waiting, and an automation that waits for green should not move on while a rebuild is half
  done. Yellow is what the status means elsewhere: serving, with something owed.
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
- `read_between_layout_changes` treats a change that is waiting for the operations in flight
  as running, so `/_health` is yellow from the moment a rebuild is asked for. Writes are
  already waiting by then.
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
  is waiting for the rebuild; `/_health` is yellow and says a rebuild is running, and green
  again afterwards; a search answers under the old vocabulary; a write waits; a
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

`coordinator/tests/layout_change.rs`: a lock-free read is passed with no change about, is
refused while one runs, is refused when one was running as it began even if that one has
finished by the time it ends, is run again when one came and went inside it, and is refused
when one began inside it. `cluster_compile.rs`: a cursor that a rebuild overtook between the
stale gate and the fingerprint is reported stale (409), and a live cursor is judged by its
fingerprint.

Thirteen mutations were run and each fails a test: the three checks of
`read_between_layout_changes` removed one at a time; a health probe that compares the topology
whatever is running; a mismatched fingerprint not checked against the stale gate again; a
search with sources that shares write
admission again; a vocabulary change that does not take the topology guard; a vocabulary
change, a resize, and a resync, on the slot for administrative reads; a pool worker that takes write admission
for each title (the pool stops, as ADR-207 describes); a rebuild admitted without looking at
the remote fence; and the fence looked at only on arrival.

**See also:** ADR-046 (the vocabulary rebuild), ADR-180 (the remote resize), ADR-191 (reads
off the runtime), ADR-206 (write admission), ADR-207 (the gate this retires), ADR-208 (the
published layout), ADR-209 (only a search runs beside a layout change).
