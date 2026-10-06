# ADR-198 — The WAL is replaced, never truncated in place

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

After every committed flush, and after a committed vocabulary rebuild, the single-node engine
resets its write-ahead log: everything in it is now in segments the manifest lists. The reset
truncated `wal.log` to zero bytes and then wrote the eight-byte header. Creating the log on a
node's first start did the same.

A process killed between the truncate and the header write, or a power cut before the sync
that follows, left `wal.log` with fewer than eight bytes. No data was missing: the manifest and
segments had been committed before the reset. But the next start read the log first, found it
"too small", and refused to start. The node stayed down until someone deleted the file by
hand, which no runbook mentioned. The window opens once per flush.

ADR-182 had already closed the other half of this: a reset that fails after truncating no
longer lets later writes be acknowledged into a file the next start cannot read, because the
old append handle is disabled before the file is touched.

## Decision

1. **Reset replaces the log.** An empty log (header only) is written and synced beside
   `wal.log`, then renamed over it, and the directory is synced. A crash at any point leaves
   either the old log, whose records the manifest already covers, or the new one. Both open.
2. **Creation does the same,** so a first start cannot leave a partial header either.
3. **The old handle is disabled at the rename, not before.** Until the rename the log and its
   handle are untouched: a reset that cannot build its replacement leaves the WAL exactly as
   it was, still taking writes. From the rename on the old handle addresses an unlinked file,
   so if anything after it fails, appends are refused until a reopen.
4. **A log whose header was interrupted opens as an empty log.** A file shorter than the
   header, all of whose bytes are the bytes a header has there, is what the old truncate
   window (or an interrupted first start) left. It never held a record, so `Wal::open` and
   `Wal::recover` treat it as empty and the open publishes a whole header. Any other short
   file, and any file with a complete but wrong header, is still refused and left untouched.
5. The handle a reset or creation leaves is opened for append, like the one an ordinary open
   uses.

## Alternatives considered

- **Truncate to the header instead of to zero** (`set_len(8)` on the open handle). One system
  call and no second file. It keeps whatever header the file was created with, and it reuses
  the handle that ADR-182 disables on any I/O doubt, so the two mechanisms would have to be
  reconciled. Replacement gives the same guarantee and also covers creation.
- **Only tolerate the short file on open.** It makes the node start, and leaves in place a
  reset that destroys the log before it has built the next one.
- **Mark the WAL unhealthy on every reset failure.** A reset that failed before the rename
  changed nothing; refusing writes for it would turn a benign failure into an outage.

## Consequences

- One extra file create, rename and directory sync per flush. A flush already writes a
  segment and a manifest.
- A crash can leave `wal.log.tmp` behind. It is never read as the log, and the next reset
  writes over it.
- Nodes that an older binary left with a cut header now start without manual repair.
- The `WalReset` durability event keeps its meaning (the log still holds records that are
  re-applied idempotently) for a failure before the rename. After the rename a failure also
  stops appends until a reopen, which the append path reports.

## Proven

- `wal/tests.rs`: a reset publishes a new file (a different inode) that holds exactly the
  header, and appends follow the header, also after a second reset; a file cut at each of the
  eight header offsets opens empty, is repaired and takes writes; a short file that is not a
  header prefix, and a full wrong header, are refused and not rewritten; a reset that cannot
  build its replacement leaves every record in place and the log in use.
- `tests/persistence/wal_reset.rs`: an engine whose committed data directory has its log cut
  at 0, 3 or 7 bytes starts, serves everything, and takes a write that survives another
  restart (it refused to start before); a leftover replacement file is ignored and
  overwritten.

**See also:** ADR-013 (the write-ahead log), ADR-182 (validated recovery and the disabled
handle), ADR-051 (fail-closed flush).
