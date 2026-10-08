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
error, the commit is neither failed nor finished. What a restart will read is known: the new
manifest. What a power loss will leave is not: the new one, or the one it replaced.

1. **The publication says which failure it was.** `publish_by_rename` returns an error only
   when the rename did not happen, and `Published::NotSynced` when it did and the directory
   sync failed. A rename that reported an error and took effect anyway (a network filesystem
   that loses the reply) is told by its source being gone.
2. **From then on the engine makes no durable transition that depends on which manifest is
   on disk,** for the rest of the process:
   - **No file is removed.** The commit is reported to its caller as failed, and the caller
     rolls its in-memory state back as for any failed commit, but the two functions every
     removal goes through do nothing: the manifest on disk names what that commit wrote.
   - **Nothing more is committed.** The one function that writes the manifest refuses. A
     commit on top of either manifest would have to be right for both, and with no commit
     there is no checkpoint marker and no reset of the write-ahead log, which the older
     manifest needs whole.
   - **No write names a row by its position** (`tombstone`, `tombstone_in`): a position is a
     place in one manifest's layout, and the log may be replayed over either.
3. **Reads go on, and so do writes that name a row by its id.** They are in the log, and the
   log replays over either manifest.
4. **A restart ends it.** The engine reads what is on disk. An open now syncs the data
   directory and the segment directory before it builds on what they hold, and fails when it
   cannot, so the manifest it starts from is known to be the one a power loss leaves.
5. **It is reported.** `persistence_healthy` goes false (health `red`) and a `manifest_write`
   durability failure says what happened and that a restart is needed. A bulk load, which is
   not in the log, is refused with an error that says so; the one whose manifest was renamed
   and not synced returns an error too, is not served, and is served after a restart.

A manifest that was not renamed is not in effect. That commit is rolled back and what it
wrote is removed, as before, and the engine goes on committing.

## What changes for a deployment

- After an I/O error on the directory sync that follows a manifest rename, the node keeps its
  data across a restart. Before, a restart could lose every query in the segment that commit
  wrote.
- In that state the node reports `red`, serves reads, takes writes by id into its log, and
  commits nothing: no flush, compaction, bulk load or vocabulary change takes effect, and the
  log and the memtable grow. **Restart it on healthy storage.** It then starts from the
  manifest on disk, with every acknowledged write.
- Files the commit wrote and files the previous manifest named both stay. The ones the
  manifest on disk does not name are unreferenced afterwards and are removed by hand.
- A node whose data directory cannot be synced does not start.
- `POST /_bulk` publishes the engine's state to readers after a batch whatever its outcome,
  so a node's health change after a failed batch is visible at once.

## Alternatives considered

- **Adopt the new manifest and carry on,** keeping both sets of files until a later commit is
  synced. This was built first. Two rounds of review each found paths that had to know about
  the state: positional deletes, an empty flush that no longer retried, replay over a log kept
  across later commits (a replaced query came back), a restart that forgot the state,
  commits that have no log to fall back on. A state that every later path must respect is
  the wrong place to hold an invariant. Stopping commits holds it in the one function they
  all go through, and it is what LevelDB and PostgreSQL do.
- **Undo the publication** by writing the old manifest again and renaming it back, then
  delete the new files. It needs a second complete publication on a device that has just
  failed one. Lucene can undo because its commit creates a new file name and leaves the
  previous commit file in place; this manifest is replaced in place.
- **Stop the process**, as PostgreSQL does for a failed sync. A library should not end its
  host, and reads and logged writes are safe. The server can choose to restart on `red`.
- **Retry the directory sync.** A later sync of the same failed state can report success
  falsely.
- **Write the manifest again at every open** to prove it durable. It costs a manifest's size
  at each start. Syncing the directories is what PostgreSQL does at the start of recovery.

## Consequences

- Availability is traded for certainty: after this error the node needs a restart before it
  commits again.
- Files left unreferenced are never removed by the engine. Single-node data directories
  already do not reclaim unreferenced files; that is on the roadmap.
- Recovery still skips a segment it cannot load and serves without it. That is what made
  this loss silent and it is a separate, open decision.
- The cluster coordinator's manifest has the same publication and a different owner of the
  files: a shard removes nothing until the coordinator's sweep, which runs only after a
  manifest that was written without error. The matrix of ADR-221 covers that path.

## Proven

- `segment/persistence/published_manifest_tests.rs`: after a compaction, a flush or a bulk
  load whose manifest was renamed and not synced, every file the manifest on disk names and
  every file the replaced manifest named is there, nothing was removed, and the log was
  neither checkpointed nor reset; nothing more is committed (no manifest step is taken by a
  flush, a compaction or a bulk load) while inserts, upserts and deletes by id are taken and
  all of them are there after a restart; a delete by position is refused; a restart commits
  and resets the log again; an open syncs its directories first and fails when it cannot; a
  manifest that was not renamed is rolled back as before. The first two fail on the code
  before this change ("the manifest names the segment seg_000003.seg, which is gone").
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
after it, keep the files of both versions and do nothing that assumes either. LevelDB's way
out is a reopen and PostgreSQL's is a restart, and that is the way out here.

**See also:** ADR-017 (the commit point), ADR-051 (fail closed around the log), ADR-066 (the
manifest's log watermark), ADR-190 (no commit leaves a segment in memory), ADR-221 (the
matrix that found this).
