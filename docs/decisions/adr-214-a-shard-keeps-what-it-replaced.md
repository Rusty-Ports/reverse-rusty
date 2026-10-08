# ADR-214 — A shard keeps what it replaced until its owner commits

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A shard's engine replaces segment files: a compaction merges several into one, and a seal
rewrites a segment that holds deletions. A standalone engine then commits a manifest that
names the replacement, and removes the old files after that commit.

A shard's engine owns no manifest. The record that names its files is its owner's:

- for a shard of a durable in-process cluster, the coordinator's `cluster_manifest.bin`,
  written at the next checkpoint;
- for a shard on a shard node, that shard's checkpoint file (`shard.ckpt`), written at the
  next seal.

The shard's engine went through the same code as a standalone one. Its own commit is a no-op
that succeeds, and right after it the engine unlinked the files it had just replaced. Until
the owner's next commit, the owner's record named files that were gone.

Two ways to lose a cluster that way, both reproduced:

1. **No failed write at all.** Remove some stored queries, flush, kill the process before the
   next checkpoint. The removals put a segment over its holes threshold, the flush compacts,
   the compaction replaces the segment the manifest names and removes it. The reopen fails
   with `attaching shard segments: No such file or directory`. The server flushes at shutdown
   before its checkpoint, and `POST /_flush` flushes at any time, so a kill during shutdown,
   or any crash after a flush, is enough.
2. **A checkpoint whose manifest write fails.** The checkpoint rewrites every segment that
   holds a deletion and then writes the manifest. If the write fails, the old manifest stays
   committed, and it names the segments from before the rewrite.

The same order was in a shard node's seal: rewrite, then the checkpoint file. A kill in
between left a node that could not restart.

Nothing was lost in the sense of bytes: the rewritten segments held the rows. But no record
named them, and the store did not open.

The vocabulary recompile already had the right rule, with the reason in a comment (ADR-118):
"A cluster shard does NOT own its registry … Leave the old files … for that owner to remove
after its commit". Compaction and the rewrite did not follow it.

## Decision

1. **Who names a shard's files decides what it does with one it has replaced.** There are
   three cases (`ReplacedFiles`), and one function tells them apart: `cleanup_segment_files`,
   through which compaction, the rewrite and the vocabulary recompile all retire a file.
   - *A coordinator's manifest names them* (a primary of an in-process cluster). The shard
     leaves the file where it is. It never unlinks it.
   - *The shard's own checkpoint file names them* (a shard on a shard node). The shard reads
     the checkpoint file from disk at that moment. A replaced file the record does not name
     is removed at once: nothing could reopen from it. One it names is kept.
   - *Nothing names them* (a replica of an in-process cluster: it is in no manifest and is
     rebuilt from its primary on reopen). There is no record a removal could contradict, so
     the file is removed at once, as it is by an engine that owns its manifest.
2. **The owner of the record removes a kept file once its record no longer names it.**
   - *In-process cluster.* After the coordinator has committed its manifest, its orphan
     sweep removes whatever that manifest does not name from each primary's directory. It
     did that already, for crash leftovers; now it is also how a replaced file goes. The
     sweep works from the directory and the committed record, so it does not depend on a
     shard object surviving until the checkpoint (a rebuild replaces them).
   - *Shard node.* The node removes the files a shard has kept, in a worker that holds the
     node's installation barrier on the slot's installed shard: the seal worker and the
     staged-load worker. The removal reads the checkpoint file from disk again and takes
     only what it no longer names.
3. **On a node, a file is removed later by name only under the installation barrier.** A
   recovery writes received files into the directory of the shard it will replace, and a
   received file can carry the name of a file that shard has kept. The barrier is the lock
   every installation into a slot already holds for its whole length (a recovery, a drop,
   an added shard, an adopted dictionary) and every seal worker holds until its disk writes
   are done. So a deferred removal cannot run while a recovery is writing, and it cannot
   reach a shard that has been replaced. A shard is also sealed outside the barrier, to
   serve as a recovery source; that seal writes the checkpoint file and removes nothing
   kept.
4. **The record on disk is the authority, each time.** Neither decision rests on what an
   earlier write of the checkpoint file is remembered to have said. A record that cannot be
   read is taken to name every file. So the order of a removal and a checkpoint-file write
   does not matter, and a failed write removes nothing: that includes a commit whose outcome
   is not known (the manifest's rename happened and the directory sync after it failed).
   The files stay, and whichever record is on disk can be opened.
5. **A shard is told which case it is.** It cannot tell from its files: a shard of an
   in-process cluster writes a checkpoint file too, and nothing reopens from it. The default
   is the first case, where the worst a mistake does is keep files.
   - A shard node tells each shard it takes into a slot that its checkpoint file is its
     commit record (`LocalShard::own_the_commit_record`), through the one constructor of a
     slot's state (`ServerState::new`), which a source test keeps the only one.
   - A replicated shard tells each replica it takes in that nothing names its files
     (`Shard::no_record_names_your_segment_files`), through the one constructor of a replica
     slot (`ReplicaSlot::new`). A remote replica is a shard on a node and ignores it.
6. **Crash leftovers go the same way.** A file that was replaced, or written and never
   committed, before a crash is named by no record, and the coordinator's sweep removes it
   from a primary's directory after the next checkpoint.

## What changes for a deployment

- A durable cluster that is killed after a flush, or whose checkpoint fails after a deletion,
  reopens. So does a shard node killed between a rewrite and its checkpoint file.
- A shard's directory can hold replaced files beside their replacements. Allow disk for it.
  - *In-process cluster:* the segments compacted since the last checkpoint. A checkpoint
    removes them.
  - *Shard node:* at most the segments its checkpoint file names, until the next `Seal` or
    staged load. Files the checkpoint file never named are removed as they are replaced.
- No format change, no new setting.

## Alternatives considered

- **Fix the two call sites** (guard each removal on "owns a manifest", as the recompile did).
  It leaves the next caller free to get it wrong, and it leaves the replaced files with nobody
  to remove them on a shard node and on a replica.
- **Keep a list on every shard, and have the coordinator tell its shards to release after
  its commit.** This was the first form. Review found three ways for a list to go unreleased
  (a staged load commits through a second function; a replica that is out of sync is skipped
  by the write fan-out; a rebuild replaces the shard objects, and their lists with them,
  before its checkpoint), and a mutation check showed the release itself was redundant for a
  primary, whose directory the sweep already cleans. A list in memory is only as good as
  every path that should drain it. So a primary lists nothing and is swept, and a replica has
  nothing to wait for.
- **On a node: list every replaced file, release the list after each checkpoint-file write,
  and switch removal off on a shard while a recovery replaces it.** This was the second form.
  It tied safety to three things held in memory: that every write of the checkpoint file
  went through the one function that released, that the list was accurate about what that
  write had said, and that the switch was thrown before the first received file and thrown
  back if the recovery failed. Review found the first received-file race, and then that a
  failed recovery left the switch off for good. It also kept every replaced file until the
  next checkpoint-file write, and a replica on a node is never sealed by its coordinator, so
  there it kept them for ever. Reading the record at the moment of the decision, and
  deferring only under the barrier that installations already hold, needs none of the three.
- **Sweep a shard node's directory after each seal** for files its checkpoint file does not
  name. Safe only under the installation barrier, like the removal above, and then it would
  also take crash leftovers. It is the larger change and it removes files this engine never
  wrote, so a mistake costs more; it stays on the roadmap with the leftovers.
- **Make the shard's rewrite part of the coordinator's commit** (rewrite to a staging name,
  rename at commit). Several shards, one manifest: the manifest is already the single commit
  point, and the files are harmless until it names them.

## Consequences

- Disk use between a replacement and the removal is higher, as above.
- A shard that is told nothing and has no coordinator sweeping its directory keeps what it
  replaces. Today every shard is a coordinator's primary, a replica, or a node's.
- A replica on a shard node is not sealed by its coordinator, so the files its checkpoint
  file names stay after it has replaced them (a bounded set), and its checkpoint file does
  not advance. That predates this change; on the
  [roadmap](../roadmap.md#a-replica-on-a-shard-node-is-never-sealed).
- **What this leaves:** on a shard node, files left by a crash (replaced before it, or
  written and never committed) are not swept; they cost disk and nothing else. On the
  [roadmap](../roadmap.md#orphan-segment-files-on-a-shard-node).
- The underlying hazard is not closed here: a recovery writes into the directory of a shard
  that is still installed, under names that shard can be using. This change makes sure a
  deferred removal is not one more way for that to go wrong. On the
  [roadmap](../roadmap.md#a-recovery-writes-into-the-directory-of-the-shard-it-replaces).
- The checkpoint contract in the clustering design ("only then permits old artifacts … to be
  reclaimed") is now what the code does.

## Proven

- `cluster/coordinator/tests/replaced_segments.rs`: removals, a flush that compacts, and a
  kill: every file the manifest names is still on disk, the cluster reopens, the removals
  hold from the log and a write from before the kill is there (the reopen failed before). A
  checkpoint whose manifest cannot be written leaves every named file on disk and a cluster
  that reopens; the next checkpoint goes through and each shard's directory then holds
  exactly what the manifest names. Replaced files are on disk beside their replacements
  until the checkpoint and gone after it. In a cluster with replicas, a replica holds no
  replaced file at any point, while its primary keeps one until the checkpoint.
- `cluster/shard/tests/recovery.rs`, on a shard that owns its commit record: a seal that
  rewrites a segment and then cannot write its checkpoint file leaves the file the old
  checkpoint file names, a removal asked for at that point takes nothing, and the shard
  restarts with the deletion applied (it could not restart before). A seal alone removes no
  kept file, also when another file has arrived under its name; the node's removal then
  takes it. A replaced file that no checkpoint file ever named is removed at once. With a
  checkpoint file that cannot be read, nothing is removed. The three cases side by side.
- `cluster/server/tests/own_commit_record.rs`: a node's shard owns its commit record, for an
  in-memory and a durable node; no slot state is built outside the constructor that tells
  the shard. A `Seal` on a node leaves exactly the files its checkpoint file names. Serving
  as a recovery source rewrites a segment and removes nothing kept; the next `Seal` does. A
  recovery that fails does not stop the next seal removing what the shard replaced. No code
  in the node removes a replaced file outside a worker that holds the installation barrier,
  and the two workers that do are the only callers.
- `cluster/server/tests/stage_ingest.rs`: a staged load that compacts leaves, on a shard
  node, only the files its checkpoint file names.
- Mutation checks, each after an unmutated baseline: a node's shard removing a replaced file
  at once although its checkpoint file names it; keeping every replaced file; never reading
  its checkpoint file; reading one that names nothing; a record that cannot be read taken to
  name nothing; the removal leaving files, and not reading the record; the seal worker and
  the staged-load worker not removing; a seal removing kept files itself; serving as a
  recovery source removing them; a node not telling its shards; a group not telling its
  replicas; a shard removing at once by default, when opened and when new.

## Prior art

Two questions, each looked up in the sources (read 2026-10-08).

**When may a replaced immutable file be deleted, and who deletes it?**

- **Lucene.** `IndexFileDeleter`: "When all the commits referencing a certain file have been
  deleted, the refcount for that file becomes zero, and the file is deleted." The deleter
  runs only after `pending_segments_N` has been renamed to `segments_N`
  (`IndexWriter.finishCommit`), and at the next writer open it removes "abandoned files eg
  due to crash of IndexWriter".
- **RocksDB.** A compaction never deletes its inputs. A sweeper does
  (`FindObsoleteFiles` / `PurgeObsoleteFiles`) once no live version references them, and
  "never deletes any file that has number bigger than any of the file number in
  pending_outputs_". When the MANIFEST write fails, files whose state is ambiguous are
  quarantined: "file deletion refrain from deleting them".
- **LevelDB.** "After a background error, we don't know whether a new version may or may
  not have been committed, so we cannot safely garbage collect."
- **PostgreSQL.** "a deletion request is NOT executed immediately, but is just entered in
  the list. When and if the transaction commits, we can delete the physical file."
  (`catalog/storage.c`)
- **Elasticsearch's snapshot repository**, the closest match, because the writers are not
  the committer: data nodes write shard blobs while "metadata outside of the shard directory
  has not been updated", the master then writes the repository's metadata, and obsolete
  blobs are collected "so that it can be deleted at the end", after that write.
- **Iceberg** on an unknown commit outcome: "At this time no files will be deleted".

The pattern they share: the commit record is the only authority; the writer of new files
does not unlink old ones; deletion follows the commit, and an unknown outcome deletes
nothing. Decisions 1, 2 and 4 are that pattern.

**How is a deferred deletion kept apart from someone else writing files into the same
directory?** (Asked after review found the received-file race.)

- **Lucene.** Only the holder of the directory's `write.lock` deletes: "you must hold the
  write.lock before instantiating this class" (`IndexFileDeleter`). And a name is not
  written twice: at open the writer moves its counters past every name in the directory, "to
  avoid double-write in cases where the previous IndexWriter did not gracefully
  close/rollback" (`inflateGens`).
- **Elasticsearch peer recovery.** The receiving shard's engine is closed while files
  arrive; they arrive under temporary names (`recovery.<uuid>.`); and the step that renames
  them and deletes what the source does not have takes the store's metadata write lock and
  Lucene's `write.lock`. `Store.cleanupAndVerify`: "This method deletes every file in this
  store that is not contained in the given source meta data".
- **RocksDB.** A file being ingested is protected by a file number that is reserved first
  and never reused: "To protect the external file, we have to make sure the file number
  will never being reused." Its switch for turning deletions off (`DisableFileDeletions`)
  is a counter, "to allow different callers to independently disable file deletion", and is
  documented for backups.
- **PostgreSQL.** A dropped relation's first file is truncated and left until the next
  checkpoint: "Leaving the empty file in place prevents that relfilenumber from being
  reused."
- **SQLite.** Takes the exclusive lock, then looks again: "If the journal does not exist,
  it usually means that some other connection managed to get in and roll it back before
  this connection obtained the exclusive lock above."

Two protections recur. Deletion is done by the holder of the directory's one writer lock,
looking at the current state after taking it: that is decision 3, with the node's
installation barrier as the lock. And a file name is never used twice, which makes a
deferred deletion by name safe whatever else happens: this engine does not have that
property across a recovery, which is the hazard the roadmap item is about. Nothing surveyed
switches deletion off for the length of a recovery; where a switch exists it is a counter
with a matching release, for readers of files.

**See also:** ADR-032 (segments as the cluster's durable base), ADR-039 (the shard's
checkpoint file), ADR-051 (build durable, then destroy), ADR-118 (the recompile's rule),
ADR-213 (whose review found this).
