# ADR-212 — A log is created whole

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted · **Extends:**
> [ADR-198](adr-198-wal-is-replaced-not-truncated.md)

## Problem

Two durable logs were created by making the file at its own path and then writing its
eight-byte header: the coordinator's `cluster.log` (a shard's `translog.clog` is the same
code), and a control node's `raft-log.bin`. A process that stopped between the two steps left
a file shorter than its header. A crash does that, and so does a power loss before the sync,
and so does a full disk, where the create succeeds and the write fails.

Every later start refused the file:

- **A control node** answered `raft store open: raft log: header is truncated`.
- **A coordinator** answered `opening cluster log: clog too small`. `build` writes the cluster
  manifest before it creates the log, so the next start finds a manifest, takes the reopen
  path and reads the short log.

No acknowledged data was missing, because the header is written and synced before the first
append. But the node stayed down until someone deleted the file by hand, which no runbook
mentioned. On Kubernetes that is a crash loop that does not end when its cause does. It was
found that way: a three-node control plane was started on a full disk, and none of the three
started again once there was space.

A shard's translog shares the code and was not stuck by it. A durable shard writes its
checkpoint file after its translog exists, and a start that finds no checkpoint file removes
the translog and makes a new one.

Creation also never synced the directory. A file's sync does not promise that its directory
entry is durable (`fsync(2)`), so that a log existed at all after a power loss rested on what
the filesystem happened to do.

ADR-198 fixed the same weakness in the single-node write-ahead log. These two were left.

## Decision

1. **A log is created whole.** A complete header-only log is written and synced beside the
   path (`<name>.tmp`), renamed into place, and the directory is synced. A crash or a full
   disk leaves no file at the log's path, or a whole one. This is one function,
   `storage::framed_log::publish_empty_log`, and the write-ahead log, the cluster log, the
   shard translog and the raft log all create through it.
2. **Opening a log never decides that a short file is an interrupted creation.** A file
   shorter than its header is either a log whose creation was interrupted, which never held a
   record, or a log that has lost its content. The file cannot say which. `FileClusterLog::open`
   refuses both, as before, and leaves the file as it found it.
3. **The owner decides, from what else is on disk, and only an owner with nothing that says
   the log was ever whole has it finished:**
   - **Coordinator.** `build` writes its manifest (epoch 0, position 0) and then creates the
     log, so that manifest is no evidence that the log was whole. On reopen under it, a short
     log is replaced with an empty one. Every later manifest is written by a checkpoint, which
     needs the log open and replaces it through a rename, and which bumps the epoch even when
     it records position 0 (a checkpoint before the first write). Under any manifest but the
     first, a short log has lost its content and the cluster is refused.
   - **Control node.** A short raft log is removed, and the node starts as the fresh node it
     is, only when it has no vote, no committed index, no purge point and no snapshot. A node
     with any of those once had a log. It is refused with an error that names the file it
     found, because a node that has voted or acknowledged entries must not come back with an
     empty log.
   - **Data node.** A restarting shard has a checkpoint file, written after its translog
     existed whole. Its short translog is never finished and the shard is refused, whatever
     position the checkpoint records.
4. **Only the start of a supported header counts.** A short file is an interrupted creation
   when every byte it holds is the byte a supported header has there: the magic, and any
   format version this reader supports, because the version changed between releases. A
   short file with other content is left alone and refused. This is the test ADR-198 uses,
   and the write-ahead log now calls the same function.

## What changes for a deployment

- A control node or a coordinator that an earlier release left with a short log starts after
  the upgrade. Nothing has to be deleted.
- Nothing changes for a node whose logs are whole. No format changes; a downgrade reads what
  this release writes.
- A `<log>.tmp` file can be left beside a log by a creation that was interrupted. It is never
  read as the log, and the next creation writes over it.

## Alternatives considered

- **Only accept the short file on open.** It lets a stuck node start, and leaves a creation
  that can still produce the short file and that never syncs the directory.
- **Recognise the short file inside `open`, for every caller.** This is what ADR-198 does for
  the write-ahead log, where the manifest covers every record a reset log held. It was the
  first form of this change, keyed on "no checkpoint behind the log". A restarting data node
  showed it was wrong: its checkpoint file can record position zero and still proves the
  translog was once whole. The evidence is the owner's, so the decision is too.
- **Create the log before the manifest in `build`,** so that a manifest always means a whole
  log. It would make the coordinator's case as strict as the data node's, and it would refuse
  a cluster that an older release left with a manifest and a short log, which is the cluster
  this decision exists to start. It is the right order for the stricter rule in the roadmap
  (below).
- **Remove a short raft log on any node.** A node that has voted and then starts with an
  empty log can vote again in the same term or accept a shorter history. Raft's safety rests
  on that state surviving a restart.

## Consequences

- One extra file, rename and directory sync when a log is first created. Appends are
  unchanged.
- The directory entry of a new log is durable before the first append is acknowledged.
- A coordinator's short log under the manifest `build` wrote is taken for an interrupted
  creation. A cluster that has taken writes and never checkpointed has that manifest too, so
  if its log were then cut below the header, the reopen would not tell. For that a filesystem
  has to shorten a file below bytes that were synced, which is the judgement ADR-198 makes
  for the write-ahead log. The roadmap item below removes the case: once `build` creates the
  log before the manifest, a manifest always means a whole log.
- **Not decided here:** a log that is missing altogether is still created empty on reopen,
  also where a checkpoint, a checkpoint file or a vote says it once existed. That is a log
  that was lost, not one that was interrupted, and it should be refused the same way. It is
  on the [roadmap](../roadmap.md#a-lost-log-is-refused-not-recreated).

## Proven

- `storage/framed_log/tests.rs`: an empty log is published whole, with no replacement file
  left behind; a leftover replacement file from an interrupted creation is written over; an
  interrupted header is a strict prefix of a supported one, and nothing else is.
- `cluster/clog/tests/recovery.rs`: a file cut at each of the eight header offsets is
  finished into an empty log, takes a write and reads it back, and so does the cut header of
  an earlier format; `open` refuses every one of those files, with and without a checkpoint
  position, and does not change them; a short file that is not a header, a whole log and a
  missing log are left as they are; a creation that cannot write its replacement leaves
  nothing at the log's path.
- `cluster/coordinator/tests/log_creation.rs`: a built durable cluster whose log is cut to 0,
  3 or 7 bytes reopens, serves what was built, takes a write, and reopens again with it (it
  was refused before); the same cut after a checkpoint is refused with `clog too small` and
  the file is not changed, also when the checkpoint ran before the first write and so records
  position zero.
- `cluster/shard/tests/recovery.rs`: a restarting shard whose translog is cut to 0, 4 or 7
  bytes is refused and the file is not changed; a shard with a short translog and no
  checkpoint file starts fresh.
- `cluster/control_raft/log_store.rs`: a raft store whose log is cut to 0, 1, 4 or 6 bytes
  opens, holds a whole header, and opens again; beside a vote, a committed index or a snapshot
  the same file is refused and not changed; a short file that is no header is refused; a
  creation that cannot write its replacement leaves nothing at the log's path.
- By hand: a `controlserver` data directory holding a 0-byte `raft-log.bin` (what the full
  disk left) starts and bootstraps. Before, it answered `header is truncated`.
- Mutation checks, each after an unmutated baseline: the coordinator not finishing a
  creation; finishing one after a checkpoint; telling a checkpoint by its position alone; any
  short file taken for an interrupted
  creation; the raft guard removed; the raft repair not called; a log created at its own
  path, header second; only the current format's header recognised; `open` finishing a
  creation itself; a short file finished whatever it holds, for the cluster log and for the
  raft log.

## Prior art

Creating a file that must be whole by writing it elsewhere and renaming it into place is the
established way, and so is refusing to guess about a log that might have held data.

- **etcd** builds a new write-ahead log in a temporary directory and renames the directory
  into place ("keep temporary wal directory so WAL initialization appears atomic"), then
  syncs the parent directory to persist the rename (`server/storage/wal/wal.go`, `Create`).
- **LevelDB** points `CURRENT` at a new manifest by writing a temporary file with
  `WriteStringToFileSync` and renaming it (`db/filename.cc`, `SetCurrentFile`).
- **PostgreSQL** `durable_rename` syncs the source, renames it, and syncs the directory, so
  that either the old or the new file exists after a crash
  (`src/backend/storage/file/fd.c`).
- **Raft.** The current term, the vote and the log are state a server writes to stable
  storage before it answers (Ongaro and Ousterhout, "In Search of an Understandable Consensus
  Algorithm", figure 2). Ongaro's thesis is explicit about losing it: "If a server loses any
  of its persistent state, it cannot safely rejoin the cluster with its prior identity."
  openraft, which the control plane runs on, says of its storage that if "the raft logs are
  lost … the resulting behavior is undefined", and it does not detect the case. So the check
  has to be in the store, and that is why the control node's repair looks at the rest of its
  state first.

What is not taken: these systems have always created their logs this way, so none of them
needs to recognise a short file. Items 2 to 4 exist only for nodes that an earlier release of
this project left stuck.

**See also:** ADR-198 (the same change for the write-ahead log), ADR-182 (validated log
recovery), ADR-041 (the durable raft store), ADR-032 and ADR-039 (the cluster log and the
shard translog).
