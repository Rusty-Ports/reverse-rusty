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

1. **Nothing says the log exists before it does.** `ClusterEngine::build` writes its manifest
   at epoch 0, creates the cluster log, and then ends with a checkpoint, which writes the
   manifest again at epoch 1. A checkpoint has always needed the log open and bumped the
   epoch. So a manifest at epoch 1 or later was written with its log in place, and epoch 0
   means only "the build has not finished". The other three stores already created their log
   before their first commit record.
2. **The commit record says whether its log exists.**
   - *Single node:* a manifest was written after the log was opened, always.
   - *Shard node:* a checkpoint file was written after the translog existed, always.
   - *Control node:* a vote, committed index, purge point or snapshot was written after the
     log was created, always.
   - *Cluster:* `ClusterManifest::written_with_its_log` (epoch 1 or later), which the reopen
     and the backup both ask. A reopen that finds epoch 0 is looking at a build that stopped
     part-way, or at a cluster from a release before this one that never checkpointed. It
     creates the log if there is none, finishes one that is shorter than its header
     (ADR-212), and then makes the checkpoint the build did not. Such a cluster is lenient
     about its log for that one start and not after it.
3. **A store whose commit record says the log exists refuses to open without it,** and a
   refusal touches nothing. No log is created in the lost one's place, and the coordinator
   checks its log before it attaches a single shard: attaching resets a shard's translog,
   and when the cluster log is gone those translogs are the only place its writes still
   exist. The error names the file, says what proves it existed, says that the acknowledged
   writes that were only in it are lost, and says what to do.
4. **One explicit way to go on: `accept_lost_log`** (`--accept-lost-log` on `server` and
   `shardserver`). With it, the store opens from its last flush or checkpoint with an empty
   log and reports a `log_lost` durability event that says what was lost: for a cluster and
   for a shard, every write after the checkpoint's log position; for a single node, every
   acknowledged write that had not been flushed to a segment. (A single-node manifest's log
   watermark is not that boundary: a bulk ingest or a compaction advances it without flushing
   the memtable, so an unflushed write can be older. The report says so and names no
   position to replay from.) The flag changes nothing when the log is there, and it does not
   make a damaged log acceptable: a log shorter than its header is still refused (ADR-212).
   It is meant for one start, and both binaries say at every start that it is set.
5. **A control node has no such flag, and no repair of its own.** A node that has lost its
   log must not rejoin with an empty one beside its vote, and an older copy of its directory
   is no better: it rolls the vote and the log back, and a node that has forgotten an entry
   it acknowledged can help elect a leader that lacks it. The node stays down, the other
   control nodes keep their majority, and full strength comes back through the recovery of
   the control plane as a whole (operations: disaster recovery, control-plane loss).
   Replacing one node under a new identity is the textbook answer, and it needs a control
   plane that can add a member, which this one cannot yet do.
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
9. **A shard node reports what its shards report.** A shard node gave none of its shards an
   event sink, so nothing they said about durability reached a log or a metric, and an
   accepted loss there would have been said to nobody. Every shard a node hosts is now wired
   to one node-level channel where its state is built (`ServerState::new`, the only way to
   build one; a source test keeps it so). The node counts durability failures by operation
   in `reverse_rusty_shard_durability_failures_total{op}`, with `log_lost` listed at zero
   from the start, and the binary prints each event on standard error.

## What changes for a deployment

- **A store whose log is missing no longer starts.** It did, without the writes that were in
  the log. Restore from a backup, or pass `--accept-lost-log` for one start to go on without
  them. For a shard node, recovering it into an empty data directory from a replica or the
  coordinator is the better way. A control node takes no flag and is not repaired alone:
  leave it down and recover the control plane as a whole.
- **Do not delete a log to get a node started.** It was never safe; now it also does not
  work.
- A cluster built by this release is at epoch 1 when `build` returns (it was 0), and its
  next checkpoint is epoch 2. A cluster from an earlier release that is still at epoch 0
  takes one checkpoint the first time this release opens it. The epoch is reported by
  `ClusterEngine::epoch` and in the shutdown log line; nothing reads it as a count.
- No format change. An older release reads an epoch-1 manifest as it reads any other.
- `GET /_settings` shows `accept_lost_log`. It is a startup setting.
- `reverse_rusty_durability_failures_total` has a new `op` value, `log_lost`, and a shard
  node has a new counter, `reverse_rusty_shard_durability_failures_total{op}`, with a
  `DURABILITY <op>: …` line on standard error for each event its shards report (including
  `wal_torn_tail`, the repair of a torn translog tail at start-up, which was reported to
  nobody before).
- Three alert rules ship in `deploy/prometheus-alerts.yml`: `RRLogLost` and `RRShardLogLost`
  fire on the counter's **value**, because the loss happens at start-up, before the first
  scrape, and a rule on its increase would never see it; `RRShardDurabilityFailure` fires on
  an increase of any other shard-node event except `wal_torn_tail`.

## Alternatives considered

- **Give the log an identity** (a UUID in the log's header, repeated in the commit record),
  as Elasticsearch ties a commit to its translog. It would also catch a log from another
  store, or from an older backup, put in place of the lost one. It needs a new header in
  three logs and a new field in three commit records, with the compatibility fences that go
  with them. This decision needs no format change and closes the silent loss; the identity
  is on the [roadmap](../roadmap.md#a-log-has-an-identity).
- **Create the log before the manifest,** so that a manifest by itself means a log. This
  was the first form of the change and review found what it breaks. A build that fails to
  create the log (a full disk) then leaves shard state and no manifest; the next start
  builds again over that state, each shard restores the rows its own checkpoint file lists,
  and the corpus is ingested a second time on top of them. With the manifest first, the same
  failure leaves an epoch-0 manifest and the next start opens it and finishes. (A build that
  stops before its *first* manifest has that problem in any order; it is older than this
  decision and is on the [roadmap](../roadmap.md#an-interrupted-first-build).)
- **A new manifest field** that says "written with the log". The epoch already says it: only
  `build` ever wrote 0, and a checkpoint always moves past it.
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
  - `accept_lost_log` left set would accept a later loss too. It reports each time, and the
    startup banner should not be the only place an operator sees it; the event and the
    metric are.
- A refused coordinator leaves its shards' translogs as they were, so the lost writes of an
  in-process cluster can still be read from them by hand. Nothing does that automatically,
  and `accept_lost_log` discards them: the shards are attached and their translogs reset.
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
  after a checkpoint; the setting changes nothing when the log is there; a built cluster is
  at epoch 1; a manifest at epoch 0 with no log, or with a log cut short, opens, is moved to
  epoch 1 by that open, and refuses a lost log from then on; a log cut short under a
  manifest at epoch 1 or 2 is refused with or without the setting; a build that cannot
  create its log fails with an epoch-0 manifest, and the next start opens it, finishes the
  build, and holds the same rows as a build that was never disturbed.
- `cluster/shard/tests/recovery.rs`: the same for a restarting shard and its translog.
- `cluster/translog.rs`: a translog reset that cannot finish leaves the old translog as it
  was; before, the old file was already gone.
- `cluster/coordinator/tests/log_creation.rs`: a refused open, for a log that is gone and
  for one cut short, leaves every shard's translog byte for byte as it was.
- `tests/persistence/lost_log.rs`: after a bulk ingest that advanced the manifest's watermark
  past a write that was still only in the log, the accepted loss loses that write, and the
  report names no sequence number to replay from.
- `cluster/server/tests/node_events.rs`: a shard node started over a slot whose translog is
  gone is refused; with the setting it starts, counts one `log_lost` in its metrics before
  any sink exists, and hands the event to the sink once; the count is listed at zero on a
  serving and on a pending node; no shard state is built outside the constructor that
  wires it.
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
