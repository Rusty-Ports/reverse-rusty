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

1. **The writer of a new file never unlinks the old one.** An engine that owns no manifest
   keeps the files it has replaced and lists them (`Engine::retired_segment_files`). This is
   one function, `cleanup_segment_files`, and compaction, the rewrite and the vocabulary
   recompile all retire through it. An engine that owns its manifest is unchanged: it removes
   them after its own commit.
2. **The owner releases them once its commit is durable.**
   - *In-process cluster.* After the coordinator has committed its manifest, it tells every
     shard to release (`Shard::release_retired_segment_files`), every replica included, in
     sync or not: a replica's files are in no manifest, and no sweep looks at its directory.
   - *Shard node.* A shard releases by itself, right after it has written a checkpoint file
     that no longer names the files. Every write of that file by a running shard goes through
     one function (`LocalShard::write_checkpoint_file`): a seal, a recovery's commit, and a
     bulk or staged load, which commits by that file and is not followed by a seal.
3. **A failed commit releases nothing.** That includes a commit whose outcome is not known
   (the manifest's rename happened and the directory sync after it failed): the files stay,
   and whichever manifest is on disk can be opened.
4. **A shard is told which case it is.** A shard cannot tell from its files whether its own
   checkpoint file is its commit record: a shard of an in-process cluster writes one too, and
   nothing reopens from it. So a shard node tells each shard it takes into a slot
   (`LocalShard::own_the_commit_record`), through the one constructor of a slot's state
   (`ServerState::new`), which a source test keeps the only one. The default is "does not
   own": the worst a wrong default does is keep files longer.
5. **Crash leftovers are the sweep's.** A file that was replaced, or written and never
   committed, before a crash is named by no record. The coordinator's orphan sweep, which
   already follows every checkpoint, removes those in the primaries' directories.

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
- **Let the coordinator's orphan sweep do all of it,** with shards simply never deleting. The
  sweep covers primaries only, and it runs only when no retired layout is still being read.
  A replica and a shard node would keep every replaced file for ever.
- **Sweep a shard node's directory after each seal** for files its checkpoint file does not
  name. A recovery writes received segment files into the same directory before the shard
  that will hold them exists; a sweep would remove them. A list of what this engine itself
  replaced cannot touch a file it did not write.
- **Make the shard's rewrite part of the coordinator's commit** (rewrite to a staging name,
  rename at commit). Several shards, one manifest: the manifest is already the single commit
  point, and the files are harmless until it names them.

## Consequences

- Disk use between a compaction and the next commit is higher, as above.
- A shard that is not told it owns its commit record, and has no coordinator to release it,
  keeps what it replaces until it is dropped. Today every shard has one or the other.
- **What this leaves:** on a shard node, files left by a crash (replaced before it, or
  written and never committed) are not swept; they cost disk and nothing else. On the
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
  until the checkpoint and gone after it. In a cluster with replicas, the replicas' replaced
  files are released with the commit.
- `cluster/shard/tests/recovery.rs`: on a shard that owns its commit record, a seal that
  rewrites a segment and then cannot write its checkpoint file leaves the file the old
  checkpoint file names, and the shard restarts with the deletion applied (it could not
  restart before); a seal that does write the checkpoint file removes the replaced file. A
  shard that does not own its record keeps the replaced file through its own seal, until it
  is released. A replica that is out of sync is released with its group.
- `cluster/server/tests/stage_ingest.rs`: a staged load that compacts leaves, on a shard
  node, only the files its checkpoint file names.
- `cluster/server/tests/own_commit_record.rs`: a node's shard owns its commit record, for an
  in-memory and a durable node; no slot state is built outside the constructor that tells
  the shard.
- Mutation checks, each after an unmutated baseline: a shard removing a replaced file at
  once; the coordinator not releasing; a shard node not releasing after its checkpoint file;
  every shard taking its own checkpoint file for its commit record; a node not telling its
  shards; a replica not released; only in-sync replicas released; a bulk or staged load
  writing the checkpoint file without releasing; a release that removes nothing.

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
