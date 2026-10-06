# ADR-197 — A cluster checkpoint, flush or backup excludes mutations

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

`ClusterEngine` is `Send + Sync` and is shared behind an `Arc`. Its public `checkpoint` seals
each shard, commits a manifest that says "every write up to log position P is in these
segments", truncates the log through P and sweeps segment files the manifest does not list.
It took no lock that a write takes, so a write could be half-way between its log append and
its shard while the checkpoint ran:

- **A lost acknowledged write.** The write's frame is at or before P, but its row reaches the
  shard after that shard was sealed. The row is only in a memtable and its frame is truncated.
  A crash before the next checkpoint loses the add (or keeps the old version of an upsert, or
  brings back a removed row).
- **A cluster that does not reopen.** A create is appended after P was read and lands in a
  shard that has not been sealed yet. It is in a committed segment and in the retained log
  tail. Reopen seeds the id from the segment, replays the frame, and fails with a duplicate
  logical id.
- **Two checkpoints at once** compute the same next epoch, and one's sweep can delete the
  other's segments. A public `flush` between a checkpoint's segment list and its sweep has
  the file it wrote deleted.

The shipped server was not exposed: every write, checkpoint, flush and backup it issues holds
its own write serializer. A library embedder was, and nothing told it so. `backup_to`'s
documentation asked the caller to hold "the cluster write-serialization lock", which is the
server's lock and not something the library offers.

## Decision

1. **`checkpoint` takes the exclusive side of the mutation barrier** (the lock ADR-113
   introduced: every write holds it shared from before its log append until its shard fan-out
   is complete). In-flight writes finish first and new ones wait until the manifest is
   committed, the log truncated and the sweep done.
2. **`flush` and `backup_to` take it too.** A backup holds it across its checkpoint and its
   copy, so it is a consistent snapshot with no lock of the caller's.
3. **`checkpoint_quiesced` is the body, for callers that already exclude mutations.** A bulk
   load checkpoints while it holds the barrier shared together with the bulk id guard, and
   taking the barrier again would wait for itself. Vocabulary rebuilds and resize have
   `&mut self`; `open` has not shared the engine yet. All of them call the internal variant.
4. The lock order is unchanged: the barrier, then a logical-id lock, then a shard.

## Alternatives considered

- **Hold the barrier only while the log position is read and the shards are sealed,** and
  write the manifest outside it. Writes would stall for less time. It needs its own proof
  for the source sidecars, the alias-import manifest identity and the sweep; it can follow
  once that is written down.
- **A separate coordinator mutex for checkpoint, flush and backup.** It would serialize those
  three with each other and do nothing about writes, which are the problem.
- **Document that the caller must serialize.** It leaves a way to lose acknowledged writes on
  a public, thread-safe API.

## Consequences

- Library writes wait for the whole of a checkpoint, including its source rewrite. The server
  already imposed that through its write serializer, so its behaviour does not change.
- PIT opens and exhaustive delivery take the same exclusive side, so they and a checkpoint
  wait for each other. Ordinary reads never touch the barrier.
- A caller that holds the barrier shared and calls the public `checkpoint` or `flush` on the
  same thread would deadlock. Nothing in the crate does; the internal variant exists for
  the one path that needs it.

## Proven

`cluster/coordinator/tests/write_concurrency/checkpoint.rs`, on a durable cluster with a shard
wrapper that pauses one call:

- a checkpoint started while a logged write has not reached its shard does not finish until
  the write does, and the write is there after a reopen (it was lost before);
- a create issued while a checkpoint is sealing does not complete until the checkpoint does,
  and the cluster reopens with it (reopen failed with a duplicate id before);
- two checkpoints commit two epochs; a flush waits for a checkpoint; a backup waits for an
  in-flight write and contains it;
- a bulk load into a durable cluster still checkpoints and returns.

The copy inside `backup_to` runs under the same hold as its checkpoint; no test pauses between
the two, because there is no seam there.

**See also:** ADR-113 (the barrier), ADR-177 (per-id write locks), ADR-031/032 (the manifest and
log a checkpoint commits), ADR-079 (backup).
