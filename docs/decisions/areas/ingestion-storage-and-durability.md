# Ingestion, storage & durability decisions

> [Architecture decision hub](../../DECISIONS.md)

Write paths, segments, WAL and source persistence, compaction, recovery, and durable mutation semantics.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [004](../adr-004-lsm-write-path.md) | LSM write path | Uses memtables, immutable segments, tombstones, and epoch publication instead of full rebuilds. | Accepted |
| [009](../adr-009-score-based-compaction.md) | Score-based compaction | Chooses compactions that reduce time-integrated segment probes rather than enforcing fixed levels. | Accepted |
| [012](../adr-012-mmap-segment-format.md) | mmap segment format | Stores frozen indexes in a custom CRC-checked format for zero-copy reads. | Accepted |
| [013](../adr-013-write-ahead-log.md) | Write-ahead log | Makes writes WAL-first with CRC-framed recovery and configurable fsync policy. | Accepted |
| [014](../adr-014-query-source-store.md) | Query source store | Persists original query documents outside segments so source text never enters the match path. | Accepted |
| [016](../adr-016-snapshot-read-path-arcswap.md) | Snapshot reads with ArcSwap | Gives readers lock-free immutable snapshots while writers publish structural deltas. | Accepted |
| [017](../adr-017-durable-bulk-ingest.md) | Durable bulk ingest | Treats a segment as the artifact and the manifest update as the atomic commit. | Accepted |
| [018](../adr-018-bulk-ingest-per-item-outcomes.md) | Per-item bulk outcomes | Reports every accepted or rejected bulk item instead of only aggregate counts. | Accepted |
| [020](../adr-020-resident-memory-reduction.md) | Resident-memory reduction | Moves source and logical-index data into lazy or flat representations to cut bytes per query. | Accepted |
| [051](../adr-051-fail-closed-flush-compaction.md) | Fail-closed replacement operations | Builds durable replacements before deleting the state they supersede. | Accepted |
| [056](../adr-056-compaction-reanchoring.md) | Compaction re-anchoring | Optionally recalculates drifted covers during merge without demoting visible queries into broad-only scope. | Accepted |
| [057](../adr-057-frozen-dict-format-versioning.md) | Frozen-space format versioning | Adds versioned headers and strict decoding to feature and tag dictionaries. | Accepted |
| [066](../adr-066-tombstone-durability-at-commit.md) | Tombstone durability | Persists dead-local bitmaps and a WAL watermark so deleted base rows cannot reappear after reopen. | Accepted |
| [121](../adr-121-atomic-source-sidecar-commit.md) | Atomic source-sidecar commits | Lets the manifest atomically select the source generation paired with its segment registry. | Accepted |
| [122](../adr-122-fail-closed-positional-tombstones.md) | Fail-closed positional tombstones | Uses generation-bearing addresses so stale or dead positional deletes reject before WAL append. | Accepted |
| [136](../adr-136-bulk-api-contract.md) | Bulk REST API contract | Makes NDJSON strict, aligns index/create semantics, and preserves the fresh-corpus segment fast path. | Accepted |
| [137](../adr-137-flush-api-contract.md) | Flush REST API contract | Adds strict familiar controls and shard results while surfacing local shard durability failures. | Accepted |
| [138](../adr-138-compaction-api-contract.md) | Compaction REST API contract | Makes native force-all truthful and adds a strict force-merge compatibility subset. | Accepted |
| [139](../adr-139-backup-api-contract.md) | Backup REST API contract | Makes native backup strict, async-runtime-safe, and no-clobber through verified promotion. | Accepted |
| [161](../adr-161-checkpoint-api-contract.md) | Checkpoint REST API contract | Makes cluster durability commits strict, supervised, and explicit about nondurable coordinator modes. | Accepted |
| [182](../adr-182-validated-log-recovery.md) | Validated log recovery | Repairs only incomplete final writes before append, refuses complete corruption/incompatibility, and fences failed append handles. | Accepted |
| [184](../adr-184-recorded-feature-model.md) | Recorded feature model | Records the vocabulary and a normalizer fingerprint in every manifest commit, restores it on reopen, and refuses a corpus under any other normalizer. | Accepted |
| [188](../adr-188-mask-assigned-once.md) | Mask assigned once | Makes the top-64 mask assignment a one-time event enforced in the dictionary, seals the memtable before it, and stops the server re-running the initial load on a populated store. | Accepted |
| [198](../adr-198-wal-is-replaced-not-truncated.md) | The WAL is replaced, not truncated | Resets and creates the write-ahead log by renaming a complete empty log into place, and opens a log whose header an older truncate left incomplete as empty, so a crash during a reset can no longer leave a node that refuses to start. | Accepted |
| [190](../adr-190-no-commit-around-in-memory-segments.md) | No commit around in-memory segments | Writes a segment a failed flush or rebuild left in memory to disk before any manifest commit, or refuses the commit, so a later flush cannot retire WAL frames of rows no manifest lists. | Accepted |
| [200](../adr-200-keep-an-exact-source-sidecar.md) | Keep an exact source sidecar | Gives the source store a content version so a commit that changed no document selects the sidecar it has, instead of writing the corpus again; reports each full write as an event and metrics. | Accepted |
| [212](../adr-212-a-log-is-created-whole.md) | A log is created whole | Creates the cluster log, the shard translog and the raft log by renaming a complete header-only file into place, and lets the owner of a log that an older release left shorter than its header finish it only when nothing on disk says the log was ever whole. | Accepted |
| [213](../adr-213-a-lost-log-is-refused.md) | A lost log is refused, not recreated | Refuses to open a store whose commit record says a log existed and whose log is gone, changing nothing on disk; ends a cluster build with the checkpoint whose manifest says the log exists; makes a translog reset and a log checkpoint leave the old log in place until the new one is whole. | Accepted |
| [214](../adr-214-a-shard-keeps-what-it-replaced.md) | A shard keeps what it replaced until its owner commits | Stops a shard from unlinking a segment file as soon as it has replaced it: the file stays while a record names it, and the owner of that record removes it (the coordinator after committing its manifest; a shard node under its installation barrier, once the shard's checkpoint file on disk no longer names it). A kill after a flush, or a failed checkpoint, no longer leaves a cluster that cannot reopen. | Accepted |
| [215](../adr-215-a-build-does-not-build-over-what-it-finds.md) | A build does not build over what it finds | A durable cluster build marks its directory before it creates shard state. A build that finds the mark and no manifest removes what the unfinished one left and starts again; shard state with no manifest and no mark, or a directory that holds a cluster, is refused. A first start killed during its load no longer leaves a doubled corpus. | Accepted |
| [216](../adr-216-a-lost-log-is-accepted-by-name.md) | A lost log is accepted by name, and on record first | Gives the single-node engine and the in-process cluster a way to start without a lost log and without a backup: the refusal names a token for that one loss, a start with the token records the loss in the data directory before it puts an empty log in place, and later starts report from that record (two gauges, an alert on a value). A token left in place accepts nothing else. | Accepted |
| [221](../adr-221-every-step-of-a-durable-operation-has-a-name.md) | Every step of a durable operation has a name, and a test fails each one | Names the filesystem steps of durable operations where they already funnel, lets a test record and fail them by directory, and adds a matrix that fails every step of every in-process cluster operation three ways; its first run found that a crash during a checkpoint left the reopened cluster refusing creates, which is fixed. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
