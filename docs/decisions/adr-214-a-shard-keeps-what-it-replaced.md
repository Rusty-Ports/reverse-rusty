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
   - *The shard's own checkpoint file names them* (a shard on a shard node). The shard keeps
     the file and lists it.
   - *Nothing names them* (a replica of an in-process cluster: it is in no manifest and is
     rebuilt from its primary on reopen). There is no record a removal could contradict, so
     the file is removed at once, as it is by an engine that owns its manifest.
2. **The owner of the record removes the file once its commit is durable.**
   - *In-process cluster.* After the coordinator has committed its manifest, its orphan
     sweep removes whatever that manifest does not name from each primary's directory. It
     did that already, for crash leftovers; now it is also how a replaced file goes. The
     sweep works from the directory and the committed record, so it does not depend on a
     shard object surviving until the checkpoint (a rebuild replaces them).
   - *Shard node.* A shard releases what it listed right after it has written a checkpoint
     file that no longer names it. Every write of that file by a running shard goes through
     one function (`LocalShard::write_checkpoint_file`): a seal, a recovery's commit, and a
     bulk or staged load, which commits by that file and is not followed by a seal. A node
     lists instead of sweeping because a recovery writes received files into the shard's
     directory before the shard that will hold them exists.
3. **A failed commit removes nothing.** That includes a commit whose outcome is not known
   (the manifest's rename happened and the directory sync after it failed): the files stay,
   and whichever manifest is on disk can be opened.
4. **A shard is told which case it is.** It cannot tell from its files: a shard of an
   in-process cluster writes a checkpoint file too, and nothing reopens from it. The default
   is the first case, where the worst a mistake does is keep files.
   - A shard node tells each shard it takes into a slot that its checkpoint file is its
     commit record (`LocalShard::own_the_commit_record`), through the one constructor of a
     slot's state (`ServerState::new`), which a source test keeps the only one.
   - A replicated shard tells each replica it takes in that nothing names its files
     (`Shard::no_record_names_your_segment_files`), through the one constructor of a replica
     slot (`ReplicaSlot::new`). A remote replica is a shard on a node and ignores it.
5. **A shard that a recovery is replacing removes nothing.** A recovery writes the segment
   files it receives into the directory of the shard it will replace, and a received file
   can carry the name of a file that shard has replaced and listed. That shard can still be
   sealed while the files arrive (it may be asked to serve as a recovery source). So the
   recovery tells it first, before it writes anything, and from then on the shard forgets
   its list and leaves every file where it is. What it had listed stays on disk, named by
   no record.
6. **Crash leftovers go the same way.** A file that was replaced, or written and never
   committed, before a crash is named by no record, and the coordinator's sweep removes it
   from a primary's directory after the next checkpoint.

## What changes for a deployment

- A durable cluster that is killed after a flush, or whose checkpoint fails after a deletion,
  reopens.
- Between a compaction and the next checkpoint (or, on a shard node, the next seal), a shard's
  directory holds the replaced files as well as the replacement. Allow disk for it: up to the
  size of the segments compacted since the last checkpoint. A checkpoint releases them.
- No format change, no new setting.

## Alternatives considered

- **Fix the two call sites** (guard each removal on "owns a manifest", as the recompile did).
  It leaves the next caller free to get it wrong, and it leaves the replaced files with nobody
  to remove them on a shard node and on a replica.
- **Keep and list on every shard, and have the coordinator tell its shards to release after
  its commit.** This was the first form. Review found three ways for a list to go unreleased
  (a staged load commits through a second function; a replica that is out of sync is skipped
  by the write fan-out; a rebuild replaces the shard objects, and their lists with them,
  before its checkpoint), and a mutation check showed the release itself was redundant for a
  primary, whose directory the sweep already cleans. A list in memory is only as good as
  every path that should drain it. So a primary lists nothing and is swept, a replica has
  nothing to wait for, and only a shard node, which cannot be swept, keeps a list.
- **Sweep a shard node's directory after each seal** for files its checkpoint file does not
  name. A recovery writes received segment files into the same directory before the shard
  that will hold them exists; a sweep would remove them. A list of what this engine itself
  replaced cannot touch a file it did not write.
- **Make the shard's rewrite part of the coordinator's commit** (rewrite to a staging name,
  rename at commit). Several shards, one manifest: the manifest is already the single commit
  point, and the files are harmless until it names them.

## Consequences

- Disk use between a compaction and the next commit is higher, as above.
- A shard that is told nothing and has no coordinator sweeping its directory keeps what it
  replaces. Today every shard is a coordinator's primary, a replica, or a node's.
- **What this leaves:** on a shard node, files left by a crash (replaced before it, or
  written and never committed), and files a shard had listed when a recovery replaced it,
  are not swept; they cost disk and nothing else. On the
  [roadmap](../roadmap.md#orphan-segment-files-on-a-shard-node).
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
- `cluster/shard/tests/recovery.rs`: on a shard that owns its commit record, a seal that
  rewrites a segment and then cannot write its checkpoint file leaves the file the old
  checkpoint file names, and the shard restarts with the deletion applied (it could not
  restart before); a seal that does write the checkpoint file removes the replaced file. The
  three cases side by side: named by its own checkpoint file, the replaced file is gone
  after the seal; named by a coordinator's manifest, it is still there; named by nothing, it
  is gone. A shard that has been told a recovery is replacing it is sealed after a file has
  arrived under the name of one it had listed, and the file is untouched.
- `cluster/server/tests/stage_ingest.rs`: a staged load that compacts leaves, on a shard
  node, only the files its checkpoint file names.
- `cluster/server/tests/own_commit_record.rs`: a node's shard owns its commit record, for an
  in-memory and a durable node; no slot state is built outside the constructor that tells
  the shard; the recovery handler tells the shard it replaces before it receives its first
  file.
- Mutation checks, each after an unmutated baseline: a shard removing a replaced file at
  once whoever names it; a node's shard removing at once; a node's shard not releasing after
  its checkpoint file, and releasing before it; a node not telling its shards; a group not
  telling its replicas; a release that removes nothing; a bulk or staged load writing the
  checkpoint file without releasing; a shard removing at once by default, when opened and
  when new; a shard being replaced still releasing its list; a recovery not telling the
  shard it replaces.

## Prior art

The question is when a replaced immutable file may be deleted, and who deletes it (sources
read 2026-10-08).

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
nothing. Decisions 1 to 3 are that pattern. RocksDB's pending set is why a shard node lists
what it replaced instead of sweeping its directory.

**See also:** ADR-032 (segments as the cluster's durable base), ADR-039 (the shard's
checkpoint file), ADR-051 (build durable, then destroy), ADR-118 (the recompile's rule),
ADR-213 (whose review found this).
