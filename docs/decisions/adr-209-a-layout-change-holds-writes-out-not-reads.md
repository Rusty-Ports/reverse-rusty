# ADR-209 — A layout change holds writes out, not reads

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

ADR-208 made the coordinator's serving state one published layout, but a vocabulary change and
an in-process resize still took the engine exclusively (`&mut self`). Exclusive access did
three jobs at once:

- it kept **writes** away while the rebuild read the corpus and built the new layout;
- it kept **other maintenance** (a checkpoint, a flush, a backup, another rebuild) out of the
  directories the new layout was being built in;
- it kept **reads** away, which nothing needed any more: a read runs on the layout it loaded.

Only the third is what makes a rebuild an outage. This decision does the first two with
narrower means, in the library. The server still takes its own lock around a rebuild; removing
that is the next step.

## Decision

1. **A layout change takes shared access.** `resize`, `set_vocab`, the learning and alias
   import paths, `install_remote_resize` and `resize_remote` are `&self`.
2. **One layout change, checkpoint, flush or backup at a time.** They share a maintenance
   lock. A layout change holds it for its whole run.
3. **A layout change refuses writes with the write fence** (the one a remote resize already
   uses, ADR-180). It raises the fence and then takes the mutation barrier exclusively once,
   so every write that passed the fence check has applied before the corpus is read. Writes
   that arrive later are refused with a message that says to retry. It does not hold the
   barrier while it builds: a search that returns sources takes the same side of that barrier
   for its frozen view, and would wait for the whole rebuild.
4. **A mutation takes the barrier before it loads the layout** (`admit_mutation`), and holds
   it until it has applied. Loaded the other way round, a write could load the layout, lose
   the processor while a rebuild ran from fence to publish, and then apply to shards nothing
   reads any more. A checkpoint, a flush and a backup take the maintenance lock, then the
   barrier, and load after both.
5. **A layout is published on the exclusive side of the mutation barrier**, together with the
   logical-id directory and the release of the old layout's point-in-time pins. A frozen read
   view, a point-in-time open and an exhaustive read hold that side for as long as they run,
   so each sees one layout, one directory and its own pins from its first step to its last.
   Writes need this too. A mutation checks the fence after it has parsed and placed its
   query, with the barrier held. Because the swap waits for the barrier, a mutation that was
   admitted while the change was building reaches its check before the change can finish and
   lower the fence, and is refused. Without it the mutation would find the fence down and
   apply to the layout it loaded, which has been replaced.
6. **Reading the vocabulary and replacing it happen inside one layout change.** An alias
   import and the learning paths begin the change, read the current vocabulary, and replace
   it, so two of them cannot both start from the same vocabulary.
7. **The files of a replaced layout stay until nothing runs on it.** An operation that loaded
   the old layout finishes on it and may still open one of its files (a source sidecar is
   mapped lazily). The engine remembers each replaced layout until its last holder lets go,
   and the post-commit cleanup (superseded segments, superseded source sidecars, the
   directories of positions a shrink removed) runs only when none is held. A rebuild waits up
   to two seconds for that before its checkpoint; whatever is left is never selected and goes
   at the next checkpoint, which now also restores the shard-directory set.
8. **The rule test covers the new shape.** A function that runs inside a layout change never
   loads the layout from outside it, and a mutation's `admit_mutation` counts as its one load.

## What changes for a caller

- **Library:** the rebuild methods take `&self`. A write during a rebuild is refused with
  "writes are paused while the cluster's layout is rebuilt" where the borrow checker used to
  make it impossible. Reads run throughout and return what the layout they loaded returns:
  the old result or the new one, never a mix.
- **Server:** nothing yet. It still holds its cluster lock and the search pool's gate around
  a rebuild (ADR-207), so searches still wait. Served writes wait at write admission, as
  before, and never see the fence.

## Alternatives considered

- **Hold the mutation barrier exclusively for the whole rebuild.** One lock instead of a
  fence and a brief drain. It would hold out every search that returns sources, which is the
  default on the ranked routes.
- **Let writes through and carry them into the new layout** (a catch-up drain or a dual
  write). It keeps writes available during a rebuild. It needs the rebuild to know which
  writes it has not seen and a final fenced step anyway; it is a later improvement, not a
  precondition for serving reads.
- **Delete a replaced layout's files at the swap and let an old read fail.** The failure
  would be loud on the ranked routes and silent on the compatibility route, where a missing
  source is not an error. Keeping the files costs disk for the length of the longest read.
- **Retry a write whose layout was replaced under it**, instead of ordering the barrier
  before the load. It needs every write path to be restartable from the top and an error that
  means "again". The order is one function.

## Consequences

- While an operation still holds the old layout, both layouts' shards are in memory and both
  sets of files are on disk.
- A rebuild's swap waits for a frozen read view, a point-in-time open or an exhaustive read
  in flight, and a checkpoint waits for a rebuild.
- The write fence's messages no longer name a remote resize only.
- `ClusterEngine` has no method that needs exclusive access except assembly.

## Proven

`tests/cluster_oracle/concurrent_rebuild.rs` (through the public API):

- three readers match 4,000 titles in a loop across eight resizes; every result is the exact
  one, and reads did run while a resize was in progress;
- across an alias import, a title that only the new vocabulary matches returns the old result
  or the new one, and a reader that has seen the new one never sees the old one again;
- three writers add queries across eight resizes; every acknowledged write is matched
  afterwards, every other was refused with the retry message, and the seeded corpus is intact.

`coordinator/tests/layout_change.rs` and `tests/write_concurrency/layout_change.rs`:

- a mutation stopped between the two steps of its admission while a resize runs is refused
  or kept, and the resize waits for it;
- a write that has passed the fence check and is paused on its way to a shard: the resize
  waits, and the write is in the new layout;
- during a layout change an add, an upsert and a remove are refused, a read and a frozen read
  view answer, and afterwards nothing the refused writes asked for has happened;
- a checkpoint and a second layout change wait for the one in progress;
- a layout is not published while a frozen read view is open;
- on a durable cluster, the segment files of a replaced layout exist for as long as a holder
  of that layout does and a read through it works; the next checkpoint after its release
  removes them.

Eight mutations of the protocol were run against these tests, and each fails one: a mutation
that loads the layout before it holds the barrier; a layout change that raises no fence; one
that does not wait for writes in flight; a layout published outside the barrier; a replaced
layout that is not remembered; cleanup that does not wait for replaced layouts; a checkpoint
that does not take the maintenance lock; and a logical-id directory that is not replaced with
the layout.

**See also:** ADR-177 (the mutation barrier and id locks), ADR-180 (the write fence),
ADR-197 (checkpoint excludes mutations), ADR-207 (the gate this work retires), ADR-208 (the
published layout).
