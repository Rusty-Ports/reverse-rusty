# ADR-222 — A manifest that was renamed into place is in effect

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A single-node commit writes its new segment and source sidecar and then publishes
`manifest.bin`: write a temporary file, sync it, rename it over the old manifest, sync the
directory. The publication returned one error for every failure, and every caller treated an
error as "the commit did not happen": it put its in-memory state back and deleted the new
segment and sidecar.

When the rename had succeeded and the directory sync after it failed, that was wrong. The
manifest on disk was the new one, and it named the files that were being deleted. If the
process stopped before another commit succeeded, the next start read that manifest, could not
load the segment, skipped it with a warning, and served without its queries. Acknowledged
queries were gone and nothing refused.

The crash matrix of ADR-221 found it on its first run over the single-node engine: every
commit path (a flush, a compaction, a bulk load) lost data when the step `sync_dir
manifest.bin` failed and the process then stopped.

## Decision

The rename is the publication. Once it has been issued, and it or the sync after it reports an
error, what a power loss will leave is not known: the new manifest, or the one it replaced.
What a restart without a power loss will read is known: the new one.

1. **The publication says which failure it was.** `publish_by_rename` returns an error only
   when the rename did not happen, and `Published::NotSynced` when it did and the directory
   sync failed. A rename that reported an error and took effect anyway (a network filesystem
   that loses the reply) is told by its source being gone.
2. **A renamed manifest is a commit.** The engine adopts the new state in memory, as it does
   for any commit, because that is what a restart reads. Nothing is rolled back.
3. **Nothing that either manifest may name is removed, and nothing that assumes one of them is
   done,** until a later commit is renamed and synced. While the engine is in that state no
   segment or sidecar is removed, no checkpoint marker is written to the write-ahead log, and
   the log is not reset: the older manifest needs every record in it. The four functions
   every such path goes through hold this, not the ten callers.
4. **The state ends with the next commit that is synced, or with a restart.** The failed
   directory sync is never retried by itself. A new commit writes a new manifest, renames it
   and syncs the directory, and that sync covers the directory as it then is.
5. **It is reported, and a bulk load is not acknowledged.** `persistence_healthy` goes false
   (health `red`) and a `manifest_write` durability failure is emitted, saying that the commit
   is in effect and may not survive a power loss. A flush or a compaction acknowledges
   nothing, and a live write is in the log. A bulk load is in neither, so it returns an error
   that says the batch is in effect and not known to be on disk.

A manifest that was not renamed is not in effect. That commit is rolled back and what it
wrote is removed, as before.

## What changes for a deployment

- After an I/O error on the directory sync that follows a manifest rename, the node keeps its
  data across a restart. Before, a restart could lose every query in the segment that commit
  wrote.
- In that state the node reports `red`, keeps the files the previous manifest named (they are
  unreferenced files afterwards, and stay until they are removed by hand), and does not reset
  its write-ahead log, which therefore grows until a commit is synced. Restart the node on
  healthy storage; a restart reads the manifest on disk and starts clean.
- A bulk load that hits it answers with an error while its rows are served. Read them back
  before repeating it.

## Alternatives considered

- **Undo the publication** by writing the old manifest again and renaming it back, then
  delete the new files. It needs a second complete publication on a device that has just
  failed one, and when that fails too the engine is where this decision already puts it.
  Lucene can undo because its commit creates a new file name and leaves the previous commit
  file in place; this manifest is replaced in place.
- **Stop the process**, as PostgreSQL does for a failed sync. Correct, and a library should
  not end its host. The server can still choose to restart on `red`.
- **Keep the old state in memory** and refuse writes, as LevelDB and RocksDB do. Either state
  is safe once nothing is deleted. The new one is what a restart reads, so serving it means a
  restart changes nothing.
- **Retry the directory sync.** A later sync of the same failed state can report success
  falsely.

## Consequences

- Files replaced by a commit that was not synced are never removed by the engine. Single-node
  data directories already do not reclaim unreferenced files; that is on the roadmap.
- Recovery still skips a segment it cannot load and serves without it. That is what made
  this loss silent and it is a separate, open decision.
- The cluster coordinator's manifest has the same publication and a different owner of the
  files: a shard removes nothing until the coordinator's sweep, which runs only after a
  manifest that was written without error. The matrix of ADR-221 covers that path.

## Proven

- `segment/persistence/published_manifest_tests.rs`: a compaction, a flush and a bulk load
  whose manifest was renamed and not synced keep every file the manifest on disk names, keep
  the previous manifest's files and the log, serve what was acknowledged, and reopen with it;
  the next synced commit resets the log again; a manifest that was not renamed is rolled back
  as before. The first two fail on the code before this change ("the manifest names the
  segment seg_000003.seg, which is gone").
- `storage::tests`: the three outcomes of a publication, and a rename judged by what is on
  disk.
- The single-node crash matrix (ADR-221, next stage) is what found it and passes with it.

## Prior art

Sources read 2026-10-08.

| System | After the publication step, when a later sync fails | Deletes what the new metadata names | Then |
|---|---|---|---|
| LevelDB | unknown: "After a background error, we don't know whether a new version may or may not have been committed, so we cannot safely garbage collect." | no | writes return the sticky error until the database is reopened |
| RocksDB | unknown; new files are quarantined "to avoid prematurely deleting files that ended up getting recorded in Manifest as live files", and the old manifest is kept "in case new manifest file's CURRENT file wasn't created successfully" | no | background error; resuming writes a brand-new manifest |
| SQLite | committed, and the error is returned | no (the journal is gone) | the pager discards its cache and reloads from disk |
| PostgreSQL | unknown: "we wouldn't know whether this slot would persist after an OS crash or not - so, force a restart" | no | PANIC and recovery; a failed sync is not retried because a later attempt "might falsely report success" |
| Lucene | failed, made true by deleting the renamed commit file, which works because the previous commit file is a different name and still there | only the commit file | commit again under a new generation |

RocksDB's source describes this defect: "Should we delete the new MANIFEST successfully, a
subsequent recovery attempt will likely see the CURRENT pointing to the new MANIFEST, thus
fail. We will not be able to open the DB again."

What is taken from them: only a failure before the rename is known not to have committed;
after it, keep the files of both versions, do nothing that assumes either, and leave the state
only by a fresh publication that succeeds end to end or by a restart.

**See also:** ADR-017 (the commit point), ADR-051 (fail closed around the log), ADR-066 (the
manifest's log watermark), ADR-190 (no commit leaves a segment in memory), ADR-221 (the
matrix that found this).
