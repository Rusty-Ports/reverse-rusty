# ADR-182 — Validate and repair logs before reopening for append

> [Storage decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The standalone WAL, coordinator log, shard translog, and Raft record log stopped their forward
scans at damaged suffixes but reopened the original file for append. A process could acknowledge a
new mutation behind the old torn bytes, then lose it on the next restart because replay stopped at
the same bytes again. The coordinator scanner also advanced past a frame header before checking
its body, undercounting a header-only torn write by eight bytes.

Complete unknown opcodes, malformed mutation fields, or invalid placement metadata were often
classified as torn tails. A subsequent checkpoint could erase such records. The WAL ignored its
header version and trailing unknown extensions, violating the fail-loud incompatibility contract.
Separate length, CRC, and body writes also allowed an I/O failure to leave a partial frame that a
later successful append could follow.

## Decision

Share a cold-path framing implementation while retaining each format's header and payload decoder.
The match core, signature cover, placement decisions, and on-disk record layouts are unchanged.

1. Validate the header, then decode every complete length/CRC frame. Commit the scan boundary only
   after its payload passes validation. Complete CRC failures, unknown operations, malformed fields,
   invalid UTF-8, invalid placement, and unsupported extensions return `InvalidData`; recovery
   modifies no bytes on these failures.
2. An incomplete final header/body or all-zero final padding is repairable. Before returning an
   append handle, truncate to the last fully decoded boundary and `sync_all` the cut, even when
   normal appends use the page-cache durability policy. A read-only replay reports the suffix but
   does not modify it.
3. A damaged length prefix must not discard an acknowledged payload or conceal later frames.
   Before repairing an incomplete body, an incremental CRC pass checks whether any available
   payload prefix matches the stored CRC, including a complete payload followed by padding or
   another incomplete write. Also look for a later complete CRC-valid frame. Finding either refuses
   repair. CRC work has a linear budget in suffix size; budget exhaustion also refuses repair as
   ambiguous. This is conservative detection, never a resynchronization or salvage path.
4. Encode a contiguous frame and submit it through one `write_all`. Any write, flush, or sync error
   disables further appends on that handle. Recovery can reopen a validated prefix; replacement
   paths disable the old handle before rename so a later error cannot acknowledge an append to an
   unlinked inode. Assigned WAL/coordinator positions remain monotonic across ambiguous failures.
5. Preserve the exact removed-byte count until startup replay or observer delivery. Standalone and
   coordinator recovery emit `WalTornTail`; shard-local restart queues the same event and delivers
   it when a sink attaches, outside the engine and sink mutexes.
6. Fallible standalone open propagates initialization and append-handle failures on both the
   manifest and pre-first-manifest paths. It cannot inherit an infallible constructor's degraded
   in-memory fallback after failed log validation, repair, or sync.

## Compatibility

No format bump: this hardens readers of the existing WAL v7, coordinator/translog v4, and supported
Raft headers. Known WAL v1–v7 shapes remain readable, including mixed files created by historical
writers that did not update the header. A validated old WAL header is upgraded through a separate
non-append write handle before current appends; unknown/newer headers fail without repair.
WAL tag counts and field lengths are checked before encoding so a successful writer cannot create
an undecodable record by narrowing a length. The live engine validates these bounds before the WAL
path and returns a client error, preserving storage health and the prior version on a rejected
upsert. Source generations and optional-priority extensions
must have their exact known shape.

This supersedes the permissive corruption/unknown-frame behavior in ADR-013/066 and the unfenced
unflushed-WAL boundary recorded in ADR-068. It cannot teach an old binary to reject headers it
historically ignored. Rollback to those binaries requires a pre-upgrade backup or the clean-flush
precautions in the [upgrade runbook](../operations/rolling-upgrade.md#2-the-compatibility-fence-contract).
A refused log is preserved for diagnosis or restore, not automatically overwritten or salvaged.

## Alternatives and prior art

- **Keep a point-in-time prefix on any error:** refuses to distinguish incomplete writes from
  complete corruption or future schema. It can discard acknowledged records, so it conflicts with
  this project's recovery contract. [RocksDB's recovery modes](https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes)
  make these availability/consistency tradeoffs explicit rather than declaring every bad frame safe.
- **Repair only unexpected EOF:** the closest fit.
  [etcd's WAL repair implementation](https://raw.githubusercontent.com/etcd-io/etcd/main/server/storage/wal/repair.go)
  retains the last decoded offset, repairs an unexpected EOF with truncate and fsync, and rejects
  other validation errors. We also tolerate final zero padding and conservatively check for complete
  payloads or later CRC-valid records behind a damaged length.
- **Automatically skip/resynchronize past corruption:** rejected. A skipped delete or upsert would
  reorder mutation semantics and can produce false negatives or resurrect data.
- **Back up every repaired full log:** not required for an unequivocally incomplete suffix and
  would copy the entire retained log on startup. Complete corrupt/incompatible logs remain intact;
  ambiguous repair is refused. Operator-controlled salvage can be designed separately.
- **Rollback a failed append in place:** a sync failure can be ambiguous, and truncation can itself
  fail. A sticky failure guard is simpler and prevents any later acknowledgement until recovery.

## Validation

Regression tests cover incomplete frame lengths including the exact eight-byte header boundary,
repair → acknowledged append → second reopen, complete CRC/unknown/malformed record refusal with
byte preservation, header fences and legacy upgrades, malformed tag/placement/priority fields,
zero padding, damaged lengths hiding complete payloads or later frames, pre-first-manifest I/O
failure propagation, oversized-input rejection without health degradation, partial-write and sync faults,
and one-time diagnostics across both standalone startup paths, coordinator reopen, and shard-local
restart. A shard observer test acquires the engine mutex from its callback to prove delivery runs
outside that lock. The WAL process test kills a writer twice in the same directory without an
intervening checkpoint, then verifies the union of acknowledged writes against the independent
oracle. Existing persistence and distributed durability suites remain the end-to-end contract checks.
