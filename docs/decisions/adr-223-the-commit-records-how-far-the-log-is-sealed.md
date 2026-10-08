# ADR-223 — The commit records how far the log is sealed

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A single-node manifest recorded one position in the write-ahead log: the watermark, the last
record appended when the manifest was committed (ADR-066). A flush commits after it has
sealed the memtable into a segment, so for a flush "at or below the watermark" means "in a
segment". A merge and a bulk load commit without sealing the memtable. Their watermark covers
records whose rows only the memtable holds.

Recovery therefore could not skip everything at or below the watermark. It decided record by
record: an insert or an upsert at or below the watermark was skipped when a segment or the
memtable already held a row of that id and source generation, and replayed otherwise.

That test is exact only while every row a commit captured is still in a segment. A merge
drops the rows that were replaced or deleted. After a flush that could not write its segment
(the memtable is sealed in memory and the log is kept, ADR-051) and the merge that follows,
the log still holds every record and the merged segment holds only the live rows. The crash
matrix of ADR-221 found what follows from that:

1. **A replaced query came back.** The insert an upsert had replaced was replayed, because
   no segment held its row, and the upsert was skipped, because a segment held its row.
2. **A delete by memtable position named another row.** The replayed rows of dropped records
   sat ahead of the rows written since, so every position after them was shifted. A
   delete by position written after the merge then deleted a live query at replay and left
   the deleted one alive. `Engine::tombstone` is a library call; the server and the cluster
   delete by id.
3. Fixing the first by taking the replayed row away again left its source text behind in a
   further sequence, which review found.

Each is a case the row-by-row test gets wrong, and each fix made another.

## Decision

**The manifest records how far the log is sealed into its segments.** The memtable holds
exactly the rows of the records appended since it was last replaced by an empty one. So one
sequence number says which records are in segments and which are in the memtable: the last
record appended when the memtable was last sealed.

1. **Manifest v9 appends `wal_sealed_through`.** Every commit writes the position the engine
   holds. It is independent of the watermark, which stays for what it always meant: the
   effects on segments (the tombstone bitmaps) are in the commit through it.
2. **Only a seal moves the position.** A flush, the seal before a first bulk load, and a
   vocabulary rebuild replace the memtable, and all three do it through one function,
   `Engine::take_memtable`, which sets the position to the last record appended. A merge and
   a bulk load do not call it, and their commits repeat the position they found. A seal that
   is undone (its commit failed and the memtable was put back) puts the position back too.
   A commit names every sealed segment, writing one that is still in memory to disk first
   (ADR-190), so the position a commit records is always true of the segments it names.
3. **Recovery divides the log at the two positions.**
   - A record at or below the sealed position is skipped whole. Its rows, and what it did
     to other rows, are in the segments.
   - Every record above it is replayed in order, so the memtable comes back row for row.
     A position in it is the position it was.
   - Among those, a record at or below the watermark has its effect on segments in the
     commit already, so only its effect on the memtable is replayed. That is the rule of
     ADR-066 and ADR-067, unchanged.
4. **A manifest that does not record the position is recovered by the rule it was written
   under,** the row-by-row test, with the two repairs above. The engine then does not know
   how far its log is sealed, writes the earlier layout for commits that do not seal, and
   records the position at its first seal.
5. **The next record is numbered above both positions** when a log is reopened, so it can
   never be skipped as sealed.

## What changes for a deployment

- A single-node directory is committed as manifest v9 from the first flush by this binary.
  **An older binary refuses to open it** ("unsupported manifest version 9"). Roll back before
  the first flush, or restore a backup taken by the older binary.
- A directory written by an older binary opens as before. If it was left in the state above
  (a flush that failed, then a merge, with no flush since) it is recovered once by the old
  rule, with the repairs for a replaced query and its source text. A delete by memtable
  position in that state is still replayed against the old positions that one time.
- Nothing changes for a cluster shard, which has no manifest of its own and recovers from
  its translog.

## Alternatives considered

- **Keep deciding row by row and repair each case.** Two repairs were made (the delete half
  of a skipped upsert, then its source text) and the third case, positions, has no repair:
  a position is only right when every record before it is replayed or none is.
- **Reset the log whenever a commit has captured everything it holds.** It shortens the time
  a stale prefix stays and does not remove the crash between the commit and the reset, which
  leaves the same state.
- **A marker record in the log at each seal,** valid when a later commit's watermark is at
  or above it. It needs no change to the manifest and it puts the commit's meaning in two
  files. The systems below keep the replay position in the commit record.
- **Seal the memtable at every commit,** so the watermark alone is exact. A merge would then
  write a small segment each time it ran, and a bulk load would flush a memtable it has
  nothing to do with.
- **Flush at the first open of an earlier directory** to give it a position at once. It adds
  a commit, and a way to fail, to an open. The first flush comes soon enough by itself.

## Consequences

- The row-by-row test stays, for manifests that do not record the position. It is used at
  most once per directory.
- A refusal remains (ADR-190): a delete by memtable position is refused while a flush is not
  committed, because the manifest on disk does not yet say that flush's rows are sealed.
- A stale prefix can stay in the log after a merge has captured a sealed-in-memory segment,
  until the next flush resets the log. Recovery skips it by position.

## Proven

- `segment/lifecycle/recovery/sealed_log_tests.rs`: the delete by memtable position names
  the same row after a restart (it fails without the change: an acknowledged-live query is
  gone); records at or below the sealed position are skipped whole after a crash between a
  flush's commit and its log reset; a merge and a bulk load leave the position, the
  watermark moves on, and the memtable is rebuilt row for row with a delete by position
  before the commit and one after; a vocabulary rebuild is a seal; a seal that is undone
  puts the position back; a manifest without the position is recovered as before, keeps
  that layout for a commit that does not seal, and gains the position at its first seal; a
  deleted query has no source text after a restart in either layout; the next record is
  numbered above the sealed position when the watermark is below it.
- `segment/lifecycle/recovery/replayed_upsert_tests.rs`: a replaced query stays replaced in
  both layouts.
- `storage::manifest::tests`: v9 is the v8 bytes and one position; a manifest without it is
  v8 byte for byte; a short v9 and an unknown version are refused.
- The single-node crash matrix (ADR-221) deletes by memtable position in its seed and after
  every failed step, and passes.

## Prior art

Sources read 2026-10-08.

| System | Where the replay position is kept | What moves it | Recovery |
|---|---|---|---|
| LevelDB | the log number in the MANIFEST | a memtable flush: `edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed`. A compaction's edit does not set it | replays every log file whose number is at or above it |
| RocksDB | the log number in the MANIFEST, for each column family | a flush | "A WAL is deleted (or archived if archival is enabled) when all column families have flushed beyond the largest sequence number contained in the WAL" |
| PostgreSQL | the checkpoint's position in `pg_control` | a checkpoint | "it performs the REDO operation by scanning forward from the WAL location indicated in the checkpoint record" |

Each keeps one position in its commit record, moves it only when the in-memory data has
been written out, and replays everything after it. None asks, record by record, whether the
data files already hold the change. LevelDB and RocksDB start a new log file with each
memtable, so their position is a file number; this engine has one log, so it is a sequence
number.

**See also:** ADR-051 (a failed flush seals in memory and keeps the log), ADR-066 (the
watermark and the tombstone bitmaps), ADR-067 (an upsert's two halves at replay), ADR-190 (a
commit leaves no segment in memory), ADR-221 (the matrix that found this).
