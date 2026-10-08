# ADR-222 — Read-only after a manifest that was renamed and not synced

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
2. **From then on the engine is read-only and its data directory does not change,** for the
   rest of the process:
   - **The log is closed, so every write is refused.** Each mutation of the engine (an
     insert, an upsert, a delete by id or by position) appends to the log before it is
     applied, and a closed log refuses the append, as it does after a failed one.
   - **No file of a commit is written.** The three functions that write one (a segment, a
     source sidecar, the manifest) refuse before they write, so a flush, a compaction or a
     bulk load that is tried again writes nothing.
   - **No file is removed.** The commit is reported to its caller as failed, and the caller
     rolls its in-memory state back as for any failed commit, but the two functions every
     removal goes through do nothing: the manifest on disk names what that commit wrote.
3. **Reads go on,** from the state the engine held when the sync failed.
4. **A restart ends it.** The engine reads what is on disk. Either manifest, with the log as
   it was at the failed sync, is a state that a crash at that point of a commit leaves (the
   new manifest with the log not yet reset, or the old one with the log whole), and recovery
   is written for both.
5. **An open publishes the manifest it read again before it builds on it:** the same bytes,
   a new file, a new rename, a new directory sync. It fails when it cannot. A restart of the
   process does not clear what the kernel holds about a failed sync, and a sync issued again
   over the same state can report success without having written it. A publication issued
   again does not depend on that: when it returns, the manifest a power loss leaves is the
   one that was read.
6. **It is reported.** `persistence_healthy` goes false (health `red`) and a `manifest_write`
   durability failure says what happened and that a restart is needed. Each refused write
   returns an error that says the node is read-only until a restart. A bulk load whose own
   manifest was the one returns an error that says a restart serves the batch.

A manifest that was not renamed is not in effect. That commit is rolled back and what it
wrote is removed, as before, and the engine goes on committing.

### Why every write, and not only commits

Two things have to hold for a record that is added to the log in this state: it must mean
the same replayed over either manifest, and what it was checked against when it was admitted
must be true of both. They hold when the two manifests hold the same rows (a flush, a
compaction). They do not after a bulk load: the batch is in its manifest and not in the log,
so it is absent from the engine and present in what a restart reads. A create of one of its
ids is admitted, because the id is absent, and after the restart the id has two rows. A
sidecar written by a later commit takes the name of the one the renamed manifest selects,
because the next name counts from the selected sidecar and a rolled-back commit does not
advance it.

Each of these can be fenced. There is no end to them that can be shown, and one closed log
stops them all.

## What changes for a deployment

- After an I/O error on the directory sync that follows a manifest rename, the node keeps its
  data across a restart. Before, a restart could lose every query in the segment that commit
  wrote.
- In that state the node reports `red`, serves reads, and **refuses every write** (`503
  persistence_unavailable`) until it is restarted. **Restart it on healthy storage.** It then
  starts from the manifest on disk, with every acknowledged write.
- A bulk load whose manifest was the one returns an error and is not served; after the
  restart it is. A client that retries it then finds its `create` items in conflict.
- Files the commit wrote and files the previous manifest named both stay. The ones the
  manifest on disk does not name are unreferenced afterwards and are removed by hand.
- Every start writes `manifest.bin` again with the bytes it has, before it loads what the
  manifest names. A node that cannot do that does not start.
- `POST /_bulk` publishes the engine's state to readers after a batch whatever its outcome,
  so a node's health change after a failed batch is visible at once.

## Alternatives considered

- **Adopt the new manifest and carry on,** keeping both sets of files until a later commit is
  synced. This was built first. Two rounds of review each found paths that had to know about
  the state: positional deletes, an empty flush that no longer retried, replay over a log kept
  across later commits (a replaced query came back), a restart that forgot the state,
  commits that have no log to fall back on.
- **Stop commits and keep taking writes by id into the log.** This was built second. A
  refused commit still wrote its segment and its sidecar before it reached the manifest, and
  the sidecar replaced the one the renamed manifest selects; and review found the create
  that duplicates after an uncertain bulk load. A state that every later path must respect
  is the wrong place to hold an invariant. LevelDB and RocksDB refuse writes in this state,
  and the log already had a closed state, which every write goes through.
- **Let the next commit that is synced end the state.** Between the two the process holds
  one state and the disk may hold another, and every name and counter the two commits share
  would have to be shown safe. The sidecar's name is one that is not.
- **Undo the publication** by writing the old manifest again and renaming it back, then
  delete the new files. It needs a second complete publication on a device that has just
  failed one. Lucene can undo because its commit creates a new file name and leaves the
  previous commit file in place; this manifest is replaced in place.
- **Stop the process**, as PostgreSQL does for a failed sync. A library should not end its
  host, and reads are safe. The server can choose to restart on `red`.
- **Retry the directory sync.** A later sync of the same failed state can report success
  falsely.
- **Only sync the directories at an open.** This was the first form of that step. A later
  sync that may report success falsely is the reason for not retrying inside the process,
  and a restart of the process changes nothing the kernel holds. PostgreSQL syncs its data
  directory at the start of crash recovery and then replays its log, which writes the data
  again. LevelDB writes a new manifest and a new `CURRENT` at every open. The publication
  issued again is that step here, at the cost of one manifest's size per start.

## Consequences

- Availability is traded for certainty: after this error the node takes no write until it
  is restarted.
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
  neither checkpointed nor reset. From then on an insert, an upsert, a delete by id, a
  delete by position and a bulk load are each refused, a flush and a compaction take no
  step, the writer of the manifest refuses by itself, and every file in the directory keeps
  its name and its bytes, the log included. A restart has what was acknowledged, takes
  writes and commits again. With the replaced manifest put back, as a power loss may, an
  open has every acknowledged write too. A create of an id that an uncertain bulk load
  holds is refused, and after the restart the id has one row. An open publishes its
  manifest again before anything else, with the bytes it read, and fails when any of the
  four steps of that fails. A manifest that was not renamed is rolled back as before. The
  compaction and flush tests fail on the code before this change ("the manifest names the
  segment seg_000003.seg, which is gone").
- `storage::tests`: the three outcomes of a publication, and a rename judged by what is on
  disk.
- The server: a bulk batch whose commit failed is seen in the node's health at once.
- The single-node crash matrix (ADR-221, next stage) is what found it and passes with it.

## Prior art

Sources read 2026-10-08.

| System | After the publication step, when a later sync fails | Deletes what the new metadata names | Then |
|---|---|---|---|
| LevelDB | unknown: "After a background error, we don't know whether a new version may or may not have been committed, so we cannot safely garbage collect." | no | writes return the sticky error until the database is reopened; an open writes a new manifest and a new `CURRENT` |
| RocksDB | unknown; new files are quarantined "to avoid prematurely deleting files that ended up getting recorded in Manifest as live files", and the old manifest is kept "in case new manifest file's CURRENT file wasn't created successfully" | no | "may cause the database instance to go into read-only mode and further user writes may not be accepted"; for a fatal error "the only way to recover is to close the DB" |
| SQLite | committed, and the error is returned | no (the journal is gone) | the pager discards its cache and reloads from disk |
| PostgreSQL | unknown: "we wouldn't know whether this slot would persist after an OS crash or not - so, force a restart" | no | PANIC and recovery; a failed sync is not retried because a later attempt "might falsely report success" |
| Lucene | failed, made true by deleting the renamed commit file, which works because the previous commit file is a different name and still there | only the commit file | commit again under a new generation |

RocksDB's source describes this defect: "Should we delete the new MANIFEST successfully, a
subsequent recovery attempt will likely see the CURRENT pointing to the new MANIFEST, thus
fail. We will not be able to open the DB again."

LevelDB's open does not build on the manifest it found. `VersionSet::Recover` asks "See if
we can reuse the existing MANIFEST file", `ReuseManifest` answers no unless the experimental
`reuse_logs` option is set, and `DB::Open` then writes a snapshot to a new manifest file:
"If we just created a new descriptor file, install it by writing a new CURRENT file that
points to it."

What is taken from them: only a failure before the rename is known not to have committed;
after it, keep the files of both versions, take no write, and do nothing that assumes either
version. LevelDB's way out is a reopen, RocksDB's is to close the database and PostgreSQL's
is a restart, and that is the way out here. The reopen issues its publication again, as
LevelDB's does.

**See also:** ADR-017 (the commit point), ADR-051 (fail closed around the log), ADR-066 (the
manifest's log watermark), ADR-190 (no commit leaves a segment in memory), ADR-221 (the
matrix that found this).
