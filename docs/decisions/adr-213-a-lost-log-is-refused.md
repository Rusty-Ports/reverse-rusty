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
log at the next start. Every write acknowledged since the last flush or checkpoint was gone,
and the store said nothing: no error, no event, no change in health. For this system that is
its worst failure, a stored query that silently stops matching. A control node came back with
an empty log beside its vote, which Raft does not allow: it could vote again in a term it had
voted in, or accept a shorter history.

One of the ways a log goes missing was ours. Until ADR-212, deleting the log by hand was the
only way to restart a node whose first start had been interrupted.

The single-node engine and the three cluster stores were each reproduced on the release
before this one: a store with one flushed write and one write only in the log, the log
removed, opened without complaint and without the second write.

## Decision

1. **The log exists before anything says it does.** `ClusterEngine::build` creates the
   cluster log and then writes the manifest. It wrote the manifest first. A crash between the
   two now leaves a log and no manifest, which is not a cluster, and the build runs again.
   The other three stores already created their log before their first commit record.
2. **The commit record says whether its log exists.**
   - *Single node:* a manifest was written after the log was opened, always.
   - *Shard node:* a checkpoint file was written after the translog existed, always.
   - *Control node:* a vote, committed index, purge point or snapshot was written after the
     log was created, always.
   - *Cluster:* `build` writes its manifest at **epoch 1** (it wrote 0), and a checkpoint has
     always needed the log open and bumped the epoch. So a manifest at epoch 1 or later was
     written with its log in place, and epoch 0 means a manifest that a release before this
     one wrote before it created the log. `ClusterManifest::written_with_its_log` is that
     test, and the reopen and the backup both ask it.
3. **A store whose commit record says the log exists refuses to open without it.** Nothing is
   created in the log's place. The error names the file, says what proves it existed, says
   that the writes acknowledged since then are lost, and says what to do.
4. **One explicit way to go on: `accept_lost_log`** (`--accept-lost-log` on `server` and
   `shardserver`). With it, the store opens from its last flush or checkpoint with an empty
   log and reports a `log_lost` durability event that names the position after which writes
   were lost. It changes nothing when the log is there, and it does not make a damaged log
   acceptable: a log shorter than its header is still refused (ADR-212). It is meant for one
   start.
5. **A control node has no such flag.** A node that has lost its log must not rejoin with
   an empty one beside its vote. Its data directory is restored from a snapshot; until then
   it stays down, and the other control nodes keep their majority. Replacing one control node
   with an empty one is the textbook answer, and it needs a control plane that can add a
   member, which this one cannot yet do.
6. **`FileClusterLog::open` takes the caller's answer.** Its new argument, `IfMissing`, is
   `Create` or `Refuse`, and every caller has to pass one. The default that lost data cannot
   be reached by leaving something out.
7. **A backup of a store whose log is gone is refused,** and so is verifying one. Such a
   backup would restore to a store that refuses to open, and the running store is
   acknowledging writes into a file no restart will read.
8. **Nothing of ours leaves a commit record without its log.** A shard's translog reset
   (at attach, and when a recovery target installs what it received) removed the old file
   and then created a new one. A recovery target does that while its old checkpoint file is
   still in place, so a crash between the two steps would have left exactly the state
   decision 3 refuses, and a node that could not restart by itself. The reset now writes the
   new translog beside the old one and renames it over it: a crash leaves the old translog
   or the new one, and both restart.

## What changes for a deployment

- **A store whose log is missing no longer starts.** It did, without the writes that were in
  the log. Restore from a backup, or pass `--accept-lost-log` for one start to go on without
  them. For a shard node, recovering it into an empty data directory from a replica or the
  coordinator is the better way. A control node takes no flag: restore its data directory
  from a snapshot.
- **Do not delete a log to get a node started.** It was never safe; now it also does not
  work.
- A cluster built by this release starts at epoch 1, and its first checkpoint is epoch 2.
  The epoch is reported by `ClusterEngine::epoch` and in the shutdown log line; nothing reads
  it as a count.
- No format change. An older release reads an epoch-1 manifest as it reads any other.
- `GET /_settings` shows `accept_lost_log`. It is a startup setting.
- `durability_failures_total` has a new `op` value, `log_lost`. The existing zero-tolerance
  alert covers it.

## Alternatives considered

- **Give the log an identity** (a UUID in the log's header, repeated in the commit record),
  as Elasticsearch ties a commit to its translog. It would also catch a log from another
  store, or from an older backup, put in place of the lost one. It needs a new header in
  three logs and a new field in three commit records, with the compatibility fences that go
  with them. This decision needs no format change and closes the silent loss; the identity
  is on the [roadmap](../roadmap.md#a-log-has-an-identity).
- **Keep the manifest first and end `build` with a checkpoint,** so that a built cluster is
  at epoch 1 by the usual route. It writes the manifest twice and still leaves a window in
  which a manifest exists and a log does not.
- **A new manifest field** that says "written with the log". The epoch already says it: only
  `build` ever wrote 0.
- **Refuse an epoch-0 manifest with no log as well.** It cannot be told from a build that an
  older release left unfinished, which has never served and is harmless to start.
- **An offline tool instead of a start-up flag,** as PostgreSQL's `pg_resetwal` and
  Elasticsearch's `elasticsearch-shard` are. It cannot be left switched on. Here it would
  mean running a second binary against a volume that a crash-looping pod holds; the house
  precedent for an explicit recovery is a flag for one start
  (`--recover-divergent-replicas`, ADR-195). The flag acts only when a log is actually
  missing and reports each time it does.
- **Start degraded and report it,** as the engine does for a corrupt segment
  (`segment_recovery`). A missing segment is visible in health; missing writes are visible
  nowhere, and the caller was told they were durable.

## Consequences

- A lost log is an outage until someone decides, where it used to be silent data loss.
- **What this leaves:**
  - A cluster from a release before this one that has taken writes and has never completed
    a checkpoint still has an epoch-0 manifest, so a lost log there is still recreated. A
    graceful stop makes a checkpoint, so this is a cluster that has only ever been killed.
  - A log that is deleted while the store is running goes unnoticed until the next start.
    The store keeps acknowledging writes into the unlinked file. The next start now refuses
    instead of hiding it, but those writes are already gone.
    ([Roadmap](../roadmap.md#a-log-deleted-under-a-running-store).)
  - A log replaced by a different, well-formed log is accepted. (The identity above.)
  - `accept_lost_log` left set would accept a later loss too. It reports each time, and the
    startup banner should not be the only place an operator sees it; the event and the
    metric are.
- After an accepted loss on a shard node, replicas of that shard that had the lost writes
  hold more than their primary. The coordinator finds them unequal when it connects and
  keeps them out of the in-sync set (ADR-195); recovering them from the primary discards
  those writes there too. That is the meaning of accepting the loss on the primary.
- `tests/cluster_durability_oracle/attach.rs` used "delete the log and reopen" to prove that
  a checkpoint captures everything in segments. It still proves that, by accepting the loss
  explicitly, and now also asserts the refusal.

## Proven

- `tests/persistence/lost_log.rs`: a single-node directory with a flushed write, a write only
  in the log, and the log removed is refused with an error that names the way out, and no
  log is created; with `accept_lost_log` it opens with the flushed write, without the other,
  and reports `log_lost` once; it then reopens with or without the setting and reports
  nothing. A directory with no manifest and no log starts.
- `cluster/coordinator/tests/log_creation.rs`: the same for a durable cluster, built only and
  after a checkpoint; the setting changes nothing when the log is there; a manifest at
  epoch 0 with no log, or with a log cut short, opens as before; a log cut short under a
  manifest at epoch 1 or 2 is refused with or without the setting; a build that cannot
  create its log leaves no manifest, and the directory is not a cluster.
- `cluster/shard/tests/recovery.rs`: the same for a restarting shard and its translog.
- `cluster/translog.rs`: a translog reset that cannot finish leaves the old translog as it
  was; before, the old file was already gone.
- `cluster/control_raft/log_store.rs`: a node with a vote, a committed index, a purge point
  or a snapshot and no log is refused, and no log is created; a node with no state starts.
- `cluster/clog/tests`: `open` with `IfMissing::Refuse` refuses a missing file and creates
  nothing; with `IfMissing::Create` it creates one.
- `tests/persistence/backup.rs` and `tests/cluster_durability_oracle`: a backup of a store
  whose log is gone is refused, and a backup directory without its log does not verify.
- `bin/server/cli.rs`, `bin/shardserver/args.rs`: the flag reaches the engine and the cluster
  configuration, and is off unless given.
- Mutation checks, each after an unmutated baseline, are listed in the pull request.

## Prior art

What established systems do when the log is gone and the commit record is still there
(sources read 2026-10-07).

- **RocksDB.** By default nothing proves a write-ahead log should exist: "If a WAL is missing
  or corrupted, RocksDB has no way of knowing this, and will silently recover with corrupted
  content" (wiki, *Track WAL in MANIFEST*). The option `track_and_verify_wals_in_manifest`
  was added for that: the MANIFEST records each synced WAL, and `DB::Open` fails with
  `Corruption: Missing WAL with log number: N` (`db/wal_edit.cc`, `WalSet::CheckWals`).
  RocksDB also creates the WAL before the MANIFEST edit that refers to it.
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

Four patterns recur, and this decision takes the first three:

1. The commit record says which log it expects (decision 2; the identity would say it more
   strongly).
2. The log is durable before anything says it exists (decision 1, and ADR-212's directory
   sync).
3. Refuse by default, and go on only through an explicit, loudly worded step (decisions 3
   and 4).
4. A forced recovery changes identity, so the loss shows downstream. Not taken as such: a
   control node cannot be given a new identity until the control plane can add a member, so
   it is refused outright (decision 5); for a shard node the coordinator's replica comparison
   makes the loss show.

**See also:** ADR-212 (a log is created whole; a short log), ADR-198 (the write-ahead log is
replaced, not truncated), ADR-182 (validated log recovery), ADR-195 (replicas proven at
connect), ADR-041 (the durable raft store), ADR-051 (fail-closed flush).
