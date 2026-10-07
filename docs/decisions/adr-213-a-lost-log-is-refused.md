# ADR-213 — A lost log is refused, not recreated

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted · **Extends:**
> [ADR-212](adr-212-a-log-is-created-whole.md)

## Problem

Four stores keep a commit record and, beside it, a log of the writes acknowledged since that
record:

| Store | Commit record | Log |
|---|---|---|
| Single-node engine | `manifest.bin` | `wal.log` |
| In-process durable cluster | `cluster_manifest.bin` | `cluster.log` |
| Durable shard node | the shard's checkpoint file | `translog.clog` |
| Control node | vote, committed index, purge point, snapshot | `raft-log.bin` |

Each opened its log the same way: if the file is there, replay it; if it is not, create an
empty one. None asked whether its own commit record meant that a log had to be there.

So a log that was deleted, left out of a restore, or lost by the filesystem became an empty
log at the next start. Every acknowledged write that was only in it was gone, and the store
said nothing: no error, no event, no change in health. For this system that is its worst
failure, a stored query that silently stops matching. A control node came back with an empty
log beside its vote, which Raft does not allow: it could vote again in a term it had voted
in, or accept a shorter history.

One of the ways a log goes missing was ours. Until ADR-212, deleting the log by hand was the
only way to restart a node whose first start had been interrupted.

All four were reproduced on the release before this one: a store with one flushed write and
one write only in the log, the log removed, opened without complaint and without the second
write.

## Decision

1. **The commit record says whether its log exists.**
   - *Single node:* a manifest was written after the log was opened, always.
   - *Shard node:* a checkpoint file was written after the translog existed, always.
   - *Control node:* a vote, committed index, purge point or snapshot was written after the
     log was created, always.
   - *Cluster:* `build` writes its manifest at epoch 0, creates the cluster log, and now ends
     with a checkpoint, which writes the manifest again at epoch 1. A checkpoint has always
     needed the log open and bumped the epoch. So a manifest at epoch 1 or later was written
     with its log in place (`ClusterManifest::written_with_its_log`), and epoch 0 means only
     "the build has not finished".
2. **A store whose commit record says the log exists refuses to open without it.** The error
   names the file, says what proves it existed, says that the acknowledged writes that were
   only in it are lost, and says what to do.
3. **A refusal changes nothing.** No log is created in the lost one's place. The coordinator
   checks its log before it attaches a single shard, because attaching resets a shard's
   translog, and when the cluster log is gone those translogs are the only place its writes
   still exist. With the file put back, the same directory opens with every write.
4. **A reopen that finds epoch 0 finishes the build.** It is looking at a build that stopped
   part-way, or at a cluster from a release before this one that never checkpointed. It
   creates the log if there is none, finishes one that is shorter than its header (ADR-212),
   and then makes the checkpoint the build did not. Such a cluster is lenient about its log
   for that one start and not after it.
5. **`FileClusterLog::open` takes the caller's answer.** Its new argument, `IfMissing`, is
   `Create` or `Refuse`, and every caller has to pass one. The default that lost data cannot
   be reached by leaving something out.
6. **Nothing of ours leaves a commit record without its log.** A shard's translog reset (at
   attach, and when a recovery target installs what it received) removed the old file and
   then created a new one. A recovery target does that while its old checkpoint file is
   still in place, so a crash between the two steps would have left exactly the state
   decision 2 refuses: a node that could not restart by itself. The reset now writes the new
   translog beside the old one and renames it over it.
7. **A backup of a store whose log is gone is refused,** and so is verifying one. Such a
   backup would restore to a store that refuses to open, and the running store is
   acknowledging writes into a file no restart will read.
8. **A checkpoint builds the log's replacement before it touches the log.** The cluster
   log's checkpoint disabled its append handle and then wrote the replacement, so a
   checkpoint that could not write it left a log that refused every later write until a
   restart, and reported the failure as harmless. The handle is now disabled at the rename
   and not before, which is the rule ADR-198 gave the write-ahead log. It matters here
   because `build` and an epoch-0 reopen now end with a checkpoint: without this they could
   return a cluster that takes no writes.

## What changes for a deployment

- **A store whose log is missing no longer starts.** It did, without the writes that were in
  the log.
  - *Single-node server, in-process coordinator:* restore the data directory from a backup.
  - *Shard node:* recover it into an empty data directory from a replica or from the
    coordinator, or restore its volume from a snapshot.
  - *Control node:* leave it down. The other control nodes keep their majority, and full
    strength comes back through the recovery of the control plane as a whole. Do not empty
    its directory and do not give it an older copy: either rolls its vote and its log back.
- **Do not delete a log to get a node started.** It was never safe; now it also does not
  work.
- **There is not yet a supported way to start without the lost writes** when there is no
  backup. One is on the [roadmap](../roadmap.md#starting-without-a-lost-log); see
  Alternatives for why it is not part of this decision.
- A cluster built by this release is at epoch 1 when `build` returns (it was 0), and its
  next checkpoint is epoch 2. A cluster from an earlier release that is still at epoch 0
  takes one checkpoint the first time this release opens it. The epoch is reported by
  `ClusterEngine::epoch` and in the shutdown log line; nothing reads it as a count.
- No format change. An older release reads an epoch-1 manifest as it reads any other.

## Alternatives considered

- **An override in the same change** (`--accept-lost-log`: start from the last flush or
  checkpoint with an empty log, and report the loss as a durability event). It was built and
  reviewed with this change, and taken out again. Five review rounds found a different hole
  in it each time, and all of one kind: the evidence that a loss was accepted lived in memory
  and in event delivery. A shard node delivered events to nobody; an alert on the increase
  of a counter that is already 1 at the first scrape never fires; and a start that accepted
  the loss and then failed at a later step left an empty log behind, so that the next start,
  without the flag, opened in silence. The systems that have this feature write the evidence
  down where a restart finds it (Elasticsearch stamps a new history UUID; PostgreSQL's
  `pg_resetwal` rewrites the control file). That is a design of its own, and the refusal does
  not need it to be correct.
- **Create the log before the manifest,** so that a manifest by itself means a log. This was
  the first form of the change and review found what it breaks. A build that fails to create
  the log (a full disk) then leaves shard state and no manifest; the next start builds again
  over that state, each shard restores the rows its own checkpoint file lists, and the corpus
  is ingested a second time on top of them. With the manifest first, the same failure leaves
  an epoch-0 manifest and the next start opens it and finishes. (A build that stops before
  its *first* manifest has that problem in any order; it is older than this decision and is
  on the [roadmap](../roadmap.md#an-interrupted-first-build).)
- **A new manifest field** that says "written with the log". The epoch already says it: only
  `build` ever wrote 0, and a checkpoint always moves past it.
- **Refuse an epoch-0 manifest with no log as well.** It cannot be told from a build that
  stopped part-way, which has never served and is harmless to finish.
- **Give the log an identity** (a UUID in the log's header, repeated in the commit record),
  as Elasticsearch ties a commit to its translog. It would also catch a log from another
  store, or from an older backup, put in place of the lost one. It needs a new header in
  three logs and a new field in three commit records, with the compatibility fences that go
  with them; it is on the [roadmap](../roadmap.md#a-log-has-an-identity).
- **Start degraded and report it,** as the engine does for a corrupt segment
  (`segment_recovery`). A missing segment is visible in health; missing writes are visible
  nowhere, and the caller was told they were durable.

## Consequences

- A lost log is an outage until someone restores the store, where it used to be silent data
  loss.
- **What this leaves:**
  - No supported way on without a backup (above).
  - A cluster from a release before this one that has taken writes and never completed a
    checkpoint has an epoch-0 manifest. If its log is lost before its first start on this
    release, that start still recreates it; the start then moves the cluster to epoch 1 and
    the case is closed. A graceful stop makes a checkpoint, so this is a cluster that has
    only ever been killed.
  - A log that is deleted while the store is running goes unnoticed until the next start.
    The store keeps acknowledging writes into the unlinked file. The next start now refuses
    instead of hiding it, but those writes are already gone.
    ([Roadmap](../roadmap.md#a-log-deleted-under-a-running-store).)
  - A log replaced by a different, well-formed log is accepted. (The identity above.)
- A refused coordinator leaves its shards' translogs as they were, so the lost writes of an
  in-process cluster can still be read from them by hand. Nothing does that automatically.
- `tests/cluster_durability_oracle/attach.rs` used "delete the log and reopen" to prove that
  a checkpoint captures everything in segments. It now empties the log instead (a header-only
  log is what a checkpoint leaves), and asserts that deleting it is refused.

## Proven

- `tests/persistence/lost_log.rs`: a single-node directory with a flushed write, a write only
  in the log, and the log removed is refused, twice, and no log is created; with the log put
  back it opens with both writes. A directory with no manifest and no log starts.
- `cluster/coordinator/tests/log_creation.rs`: the same for a durable cluster, built only and
  after a checkpoint; a built cluster is at epoch 1 and a reopen makes no further checkpoint;
  a manifest at epoch 0 with no log, or with a log cut short, opens, is moved to epoch 1 by
  that open, and refuses a lost log from then on; a log cut short under a manifest at epoch
  1 or 2 is refused; a build that cannot create its log fails with an epoch-0 manifest, and
  the next start finishes it and holds the same rows as a build that was never disturbed; a
  build whose final checkpoint cannot rewrite the log still returns a cluster that takes
  writes; a refused open, for a log that is gone and for one cut short, leaves every shard's translog
  byte for byte as it was.
- `cluster/shard/tests/recovery.rs`: the same for a restarting shard and its translog.
- `cluster/translog.rs`: a translog reset that cannot finish leaves the old translog as it
  was; before, the old file was already gone.
- `cluster/control_raft/log_store.rs`: a node with a vote, a committed index, a purge point
  or a snapshot and no log is refused, and no log is created; a node with no state starts.
- `cluster/clog/tests`: `open` with `IfMissing::Refuse` refuses a missing file and creates
  nothing; with `IfMissing::Create` it creates one. A checkpoint that cannot build its
  replacement fails, and the log still takes writes and holds what it held.
- `storage/backup/tests.rs`: a store without its log is not backed up, with a refusal that
  names the source's own log, and a backup directory without its log does not verify; a
  cluster at epoch 0 is copied as before.
- Mutation checks, each after an unmutated baseline, are listed in the pull request.

## Prior art

What established systems do when the log is gone and the commit record is still there
(sources read 2026-10-07).

- **RocksDB.** By default nothing proves a write-ahead log should exist: "If a WAL is missing
  or corrupted, RocksDB has no way of knowing this, and will silently recover with corrupted
  content" (wiki, *Track WAL in MANIFEST*). The option `track_and_verify_wals_in_manifest`
  was added for that: the MANIFEST records each synced WAL, and `DB::Open` fails with
  `Corruption: Missing WAL with log number: N` (`db/wal_edit.cc`, `WalSet::CheckWals`).
- **Elasticsearch.** A Lucene commit carries its translog's UUID "to ensure a strong
  association between the lucene index and the transaction log file". A shard whose translog
  is missing fails with `TranslogCorruptedException`; "it fails that shard copy and refuses
  to use it". An empty translog is made only by the explicit tool
  `elasticsearch-shard remove-corrupted-data` ("YOU MAY LOSE DATA"), which also stamps a new
  history UUID and needs `accept_data_loss: true` to allocate the shard.
- **PostgreSQL.** `pg_control` records where the checkpoint record is, and startup fails with
  `could not locate a valid checkpoint record` when the segment holding it is gone.
  `pg_resetwal` is the way on: "It should be used only as a last resort".
- **Raft.** Ongaro's thesis: "If a server loses any of its persistent state, it cannot safely
  rejoin the cluster with its prior identity. Such a server can usually be added back into
  the cluster with a new identity." openraft, which the control plane runs on, says that if
  "the raft logs are lost … the resulting behavior is undefined" and that it "cannot detect
  this"; its start-up check refuses an inverted log but has no check for a vote beside an
  empty log. TiKV validates at open ("log at recorded commit index … doesn't exist, may lose
  data").
- **Counter-examples.** etcd with its whole WAL directory missing takes its no-WAL bootstrap
  path and, joining an existing cluster, creates a fresh WAL. SQLite and Kafka have no record
  that a log should exist. These lose data without an error, or with a warning.

Four patterns recur. This decision takes the first two, and names the others as what the
way on has to be built from.

1. The commit record says which log it expects (decision 1; an identity would say it more
   strongly).
2. Refuse by default (decisions 2 and 3).
3. Go on only through an explicit, loudly worded step. Not taken here.
4. A forced recovery leaves durable evidence, so the loss shows downstream and at the next
   start. Not taken here; it is what the override got wrong.

**See also:** ADR-212 (a log is created whole; a short log), ADR-198 (the write-ahead log is
replaced, not truncated), ADR-182 (validated log recovery), ADR-041 (the durable raft store),
ADR-051 (fail-closed flush).
