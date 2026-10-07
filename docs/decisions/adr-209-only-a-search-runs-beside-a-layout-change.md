# ADR-209 — Only a search runs beside a layout change

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

ADR-208 made the coordinator's serving state one published layout, but a vocabulary change and
an in-process resize still took the engine exclusively (`&mut self`). That exclusivity kept
everything away from a rebuild: writes, maintenance, statistics, recoveries, and searches.
Only the last is what makes a rebuild an outage, and a search no longer needs it: it runs on
the layout it loaded.

This decision lets a search run beside a layout change, in the library. The server still
takes its own lock around a rebuild; removing that is the next step.

## Decision

1. **The engine has a layout lock.** A layout change holds it alone for its whole run. Every
   other operation holds it shared, taken once at its entry and before any other lock. A
   search does not take it. An operation with a deadline, or one that can be cancelled (an
   exhaustive read, a deadline-bound topology move), keeps that while it waits for the lock.
2. **What runs beside a layout change is an allow-list.** A search is an operation that only
   reads shard data, through the one layout it loaded: the match routes, the ranked and batch
   routes and their source fetch, a read of one stored document, and the accessors that say
   what the published layout is. Everything else is excluded, as it was when a change had the
   engine to itself: writes, repairs, bulk load, checkpoint, flush, backup, replica recovery,
   point-in-time open and close, exhaustive reads, load snapshots, topology moves. A path
   nobody thought about is excluded by default.
3. **The allow-list is checked.** `coordinator/tests/layout_discipline.rs` names the searches.
   Any other function that loads the layout without the lock fails the test, as does one that
   loads twice, one that calls another that loads, and a helper that loads or calls one that
   does. The last three matter more now: a second shared hold of the layout lock, on the same
   thread or on a worker the first is waiting for, would wait behind a change that is waiting
   for the first.
4. **A layout change takes shared access to the engine.** `resize`, `set_vocab`, the learning
   and alias import paths, `resize_to_recommended`, `install_remote_resize` and
   `resize_remote` are `&self`. Deciding and changing are one change: an alias import and the
   learning paths read the vocabulary inside the change that replaces it, and
   `resize_to_recommended` measures the load inside the change that resizes.
5. **A layout is published on the exclusive side of the mutation barrier**, together with the
   logical-id directory, the repair queue and the release of the old layout's point-in-time
   pins. A search that returns sources holds that side for its frozen view. It may be open
   while a change builds, and because the swap waits for it, it sees one layout from its
   first step to its last.
6. **The files of a replaced layout stay until no search runs on it.** A search that loaded
   the old layout finishes on it and may still open one of its files (a source sidecar is
   mapped lazily). The engine remembers each replaced layout until its last holder lets go,
   and the post-commit cleanup (superseded segments, superseded source sidecars, the
   directories of positions a shrink removed) runs only when none is held. A rebuild waits up
   to two seconds for that before its checkpoint; whatever is left is never selected and goes
   at the next checkpoint, which now also restores the shard-directory set.
7. **A shard that is being replaced refuses to write.** The new layout is built in the old
   shards' directories and shares their segment numbers, log and checkpoint sidecar. From the
   moment the build starts the old shards' storage is frozen at the shards themselves: a
   write, a flush, a seal or a recovery through one is refused, whoever asks. A change that
   fails thaws them; one that publishes leaves them frozen for good. The layout lock keeps
   every caller away; the freeze is what holds if one gets past it.
8. **A layout change does not queue behind a remote resize.** That resize holds the layout
   lock shared for its whole copy. A change waiting behind it would hold every other
   operation back until the copy was done, so it is refused at once while the resize has
   writes fenced. Checking for the fence and asking for the lock are one step, and so are
   taking the lock for a copy and raising its fence (a small admission lock covers both), so
   a copy cannot start between a change's check and its request. The cutover of the remote
   resize is itself a layout change.

## What changes for a caller

- **Library:** the rebuild methods take `&self`. A search answers during a rebuild and
  returns what the layout it loaded returns: the old result or the new one, never a mix.
  Every other operation waits for the rebuild, where the borrow checker used to make the
  overlap impossible.
- **Server:** nothing yet. It still holds its cluster lock and the search pool's gate around
  a rebuild (ADR-207), so searches still wait.

## Designs that were tried first

- **Exclude what must be excluded.** The first version replaced exclusive access with a list:
  a write fence for writes, a maintenance lock for checkpoint, flush and backup, and an order
  inside a write's admission. Two rounds of review found five more things the list had
  missed: a replica recovery that sealed its primary into a directory being rebuilt; a repair
  queue emptied while exhaustive reads still ran on the unrepaired shards; an exhaustive read
  that loaded its layout before it took the barrier; a recommended resize measured on one
  layout and applied to another; and a load snapshot that mixed the new layout with the old
  control state. Each could be fixed. The list could not be shown to be complete: some 200
  methods had relied on the exclusivity without saying so. Listing what is allowed instead
  makes all five impossible and needs no fence.
- **Hold the mutation barrier exclusively for the whole rebuild.** It would hold out every
  search that returns sources, which is the default on the ranked routes.
- **Let writes through and carry them into the new layout.** It keeps writes available
  during a rebuild and is a later improvement, not a precondition for serving searches.
- **Delete a replaced layout's files at the swap and let an old search fail.** The failure
  would be loud on the ranked routes and silent on the compatibility route, where a missing
  source is not an error.

## Consequences

- While a search still holds the old layout, both layouts' shards are in memory and both
  sets of files are on disk.
- A rebuild's swap waits for a frozen read view in flight. The rebuild itself waits for every
  operation in flight that is not a search, and they wait for it.
- A search must never take the layout lock, directly or through something it calls. A work
  pool whose workers did could deadlock the way ADR-207 describes.
- `ClusterEngine` has no method that needs exclusive access except assembly.

## Proven

`tests/cluster_oracle/concurrent_rebuild.rs` (through the public API):

- three readers match 4,000 titles in a loop across eight resizes; every result is the exact
  one, and searches did run while a resize was in progress;
- across an alias import, a title that only the new vocabulary matches returns the old result
  or the new one, and a reader that has seen the new one never sees the old one again;
- three writers add queries across eight resizes; every acknowledged write is matched
  afterwards and the seeded corpus is intact.

`coordinator/tests/layout_change.rs` and `tests/write_concurrency/layout_change.rs`, with
hooks that stop a write or a rebuild half-way:

- during a layout change a write waits while a match, a frozen read view and a count answer;
  a checkpoint, a second layout change, a load snapshot and a replica recovery wait too; and
  a layout change waits for an operation in flight;
- a layout change waits for a mutation that has been admitted, and for a write that is on
  its way to a shard, and the write is in the layout the change builds;
- a layout is not published while a frozen read view is open;
- with a repair queued and a resize stopped while it reads the corpus, the queue is still
  there and an exhaustive read waits; afterwards the queue is empty and the read returns the
  id once;
- a recommended resize that starts while another resize is running is decided on the layout
  that resize built, and changes nothing;
- on a durable cluster the segment files of a replaced layout exist while something holds
  that layout, a read through it works, and the next checkpoint after its release removes
  them;
- a replaced shard refuses a seal, a flush and a delete, still answers reads, writes no
  segment, and the cluster reopens on what the new layout committed; a layout change that
  does not publish thaws the shards it froze;
- a layout change is refused at once while a remote resize has writes fenced, and one that
  arrives while a copy is about to start waits where it holds nothing back and is then
  refused;
- an exhaustive read gives up at its deadline while a layout change holds it back;
- an in-memory engine forgets the layouts it has replaced once they are released.

Eighteen mutations of the design were run against these tests and each fails one: a mutation
that loads the layout before it holds its locks; an operation that does not keep the layout
lock; a load snapshot, and an exhaustive read, taken without it (both also fail the rule
test); a layout published outside the barrier; a replaced layout that is not remembered, and
one that is never forgotten; cleanup that does not wait for replaced layouts; a logical-id
directory that is not replaced with the layout; a repair queue that is not emptied at the
swap; replaced shards that are not frozen, a failed change that does not thaw, a published
change that thaws, and a frozen shard that still writes; a recommended resize measured before
its layout change; a layout change that queues behind a remote resize, and one that checks
for it without the admission; and an exhaustive read that ignores its deadline at the lock.
One survived at first and showed that nothing tested a change waiting for an operation in
flight that is not a write; that test is in.

**See also:** ADR-177 (the mutation barrier and id locks), ADR-180 (the remote resize and its
write fence), ADR-197 (checkpoint excludes mutations), ADR-207 (the gate this work retires),
ADR-208 (the published layout).
