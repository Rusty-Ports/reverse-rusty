# ADR-190 — No commit leaves an in-memory base segment behind

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

ADR-051 made a flush whose segment file cannot be written fail closed: the rows are served from
an in-memory base segment, the manifest is not committed, and the WAL is left intact so a restart
replays them. A vocabulary rebuild whose write fails does the same with the whole corpus. That
decision covered the failing operation itself and stopped there.

The process keeps running. The next commit that succeeds (a later flush, a bulk batch, a
compaction) writes a manifest that lists only on-disk segments, records a WAL watermark meaning
"every mutation up to here is in these segments", and then checkpoints and resets the WAL. For
the rows in the in-memory segment that statement is false, and the reset discards their only
durable copy. They are served until the process exits and are gone after the restart:

- after a failed flush, the acknowledged writes of that flush;
- after a failed vocabulary rebuild, every query in the store except the ones written since.

One transient I/O error (a full disk is the usual one) followed by ordinary traffic is enough,
because the default auto-flush threshold triggers the second flush on its own.

Cluster shards were not affected. Their commit point is the coordinator's checkpoint, which
already aborts while the shard is unhealthy and refuses a segment list that contains an
in-memory segment.

## Decision

1. **Every single-node manifest commit first writes any in-memory base segment to disk.**
   `write_manifest_capturing`, the one function all such commits go through, calls
   `persist_fallback_segments` before it builds the segment list. Flush, bulk ingest, compaction,
   reseal, vocabulary commits and the first-mask seal therefore all carry a stranded segment to
   disk with them, without each needing to know about it.
2. **If one still cannot be written, the commit does not happen.** The function returns the same
   failure a failed manifest write does. Callers already treat that as "nothing was retired":
   flush leaves the WAL as it is, and bulk ingest and compaction roll back.
3. **The segment is replaced in place.** The file holds the same rows at the same local ids with
   the liveness they have now, and it takes the segment's position and generation. Addresses
   handed out earlier stay valid, a delete already applied to the segment is part of the file,
   and the order the manifest records is the order readers already see.
4. **A flush with an empty memtable still commits a stranded segment.** Otherwise an idle engine
   would keep the rows in memory until its next write.
5. **The first-mask seal (ADR-188) commits a stranded segment instead of refusing the batch.**
   It refused because it could not make those rows durable. Now the commit does; the batch is
   refused only while the segment still cannot be written.
6. **A positional memtable tombstone is refused while a flush is uncommitted.**
   `Engine::tombstone(local_id)` logs a row by its position in the memtable, and replay
   rebuilds one memtable from the whole WAL tail. While a segment sealed from an earlier memtable
   is not in the committed manifest (its write or its commit failed), the tail also holds that
   segment's rows, ahead of the current memtable's, so the logged position names a different row
   after a restart: a query nobody deleted disappears and the deleted one returns. The call now
   fails in that state, as ADR-122 already does for base-segment positions that the manifest does
   not list. Deletes and upserts by logical id carry no position and are unaffected; they are
   what the server and the cluster use.

## Alternatives considered

- **Keep the WAL while an in-memory segment exists** (make the reset conditional). Tried while
  fixing ADR-188 and rejected: the manifest would still record a watermark that covers the
  stranded rows, and replay skips a delete at or below the watermark as already applied, so a
  deleted row would come back. The watermark has to be true; keeping extra log does not make it so.
- **Make flush all-or-nothing**, leaving the memtable in place when the write fails. It removes
  the stranded segment for flush but not for the vocabulary rebuild, which must install the
  rebuilt corpus in memory because the normalizer has already changed. It also turns every write
  past the flush threshold into another full flush attempt while the disk is failing.
- **Refuse every commit until restart.** Safe, and what the cluster does at a checkpoint, but on
  a single node it lets the WAL grow without bound after one transient error.

## Consequences

- An acknowledged write survives a failed flush followed by any number of later commits and a
  restart. So does a corpus whose vocabulary rebuild could not be written.
- After the storage recovers, the next commit (or an explicit flush) puts the stranded rows on
  disk. `persistence_healthy` stays false until the engine is reopened, as ADR-051 decided, so
  `/_flush` keeps reporting the degraded state.
- While the storage is still failing, nothing is committed and the WAL keeps growing. That was
  already true for the rows of the failed flush; it now holds for later rows too, which is what
  keeps them recoverable.
- A stranded segment written late gets a higher-numbered file than segments that follow it in
  the manifest. File numbers were never ordered by position (compaction already breaks that).
- A library caller that uses `Engine::tombstone` gets an error between a failed flush and the
  next commit and should delete by logical id there. The server never calls it.

## Proven

- `tests/persistence/fallback_commit.rs`: a failed flush followed by a successful flush, an
  empty flush or a bulk batch leaves every row on disk and present after a restart; two stranded
  segments are both written; a delete of a stranded row is not undone; while the segment still
  cannot be written nothing is committed and a restart recovers the rows from the WAL; a failed
  vocabulary rebuild followed by a flush keeps the corpus and its vocabulary, and the engine
  keeps committing afterwards; a positional memtable tombstone is refused after a failed flush
  and after a failed rebuild (where the segment count alone would not show it), a logical
  delete in the same state replays correctly, and positional tombstones work again once every
  segment is committed.
- `tests/persistence/mask_stability.rs`: the first batch is refused while a stranded segment
  cannot be written, and otherwise commits it before the mask is assigned, so its rows stay
  default-visible across a restart.

**See also:** ADR-051 (fail-closed flush, compaction and rebuild), ADR-017 (the manifest as
commit point), ADR-066 (tombstones at the commit point), ADR-122 (fail-closed positional
tombstones), ADR-188 (first-mask seal).
