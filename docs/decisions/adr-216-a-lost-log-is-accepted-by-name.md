# ADR-216 — A lost log is accepted by name, and on record first

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A store whose commit record says a log existed refuses to open when the log is gone
([ADR-213](adr-213-a-lost-log-is-refused.md)): the writes acknowledged since its last flush or
checkpoint were in it, and opening with an empty log would drop them without a word. The only
way on was to restore the store. An operator with no backup had a store that held everything
up to its last flush or checkpoint and could not be started at all.

A first way on was built with ADR-213 and taken out before it merged. It was a start-up
switch (`--accept-lost-log`) that opened the store with an empty log and reported the loss as
a durability event. Review found a different hole each round, and all of one kind: the
evidence that a loss had been accepted lived in the memory of the start that accepted it, and
in the delivery of an event.

- A start that accepted the loss, put an empty log in place and then failed at a later step
  left that empty log behind. The next start, without the switch, found a log, opened, and
  reported nothing.
- An alert on the *increase* of a counter that is already 1 at the first scrape never fires:
  the loss is accepted at start-up.
- The single-node report named the manifest's log watermark as the boundary of the loss. A
  bulk load or a compaction advances the watermark without flushing, so an unflushed write
  can be older than it.
- A switch that is left in the start-up arguments accepts the next loss too, before anybody
  has decided to.

## Decision

1. **A loss is accepted by name.** The refusal names a token for the loss it is refusing: the
   log's file name, what the commit record says, and one more than the number of losses this
   directory has already accepted and carried out (`wal.log:segment-7-seq-42:1`). The store
   accepts the loss when it is opened with exactly that token
   (`EngineConfig::accept_lost_log`, `ClusterConfig::accept_lost_log`, the server's
   `--accept-lost-log <TOKEN>`), and with nothing else. There is no form that accepts
   "whatever is lost".
2. **So the token is spent by its own use.** Once the loss is carried out the count has
   moved, and the next loss, under the same commit record or another, has a different token.
   A token that is left in the start-up arguments accepts nothing more, and does nothing
   when the log is present.
3. **The loss is on record before anything is changed.** Accepting appends an entry to
   `log_loss.accepted` in the data directory (the log, the commit record, the time), marked
   *pending*, and that file is on disk before the empty log is created. Then the log is put
   in place, and the entry is marked *applied*. For the cluster this happens before any
   shard is attached, as the refusal does.
4. **Every open settles the record against what it finds.** An open that finds its log, or
   has just created it, marks a pending entry applied. So a start that accepted the loss,
   replaced the log and then stopped is completed by the next start, which is given no token
   and finds nothing missing; the loss is on record all the same. A start that stopped
   before the log was replaced left a pending entry and no log: the store is still refused,
   the refusal names the same token, and that token finishes the acceptance without
   recording it twice.
5. **What a later start reports is read from the record,** not from an event. The server
   reads the file at every start and exports two gauges, registered at zero for a store that
   has accepted nothing: `log_losses_accepted` (how many) and
   `log_loss_last_accepted_timestamp_seconds` (when the latest was accepted). They are
   values, so an alert on them needs no sample from before the restart. It logs a warning at
   every start of a store whose record is not empty. The start that accepts a loss also
   raises a `log_lost` durability event (data at risk).
6. **The record is evidence.** A record that cannot be read fails the open, with the log
   present or not; it is not treated as empty. A backup carries it: a store restored from
   the copy holds no more than the one it was taken from, and says so. Nothing clears it.
   The time gauge lets an alert stop firing by itself; the count stays.
7. **What is lost is said in words, not as a position.** Single node: "the writes that had
   not been flushed to a segment". Cluster: "the writes acknowledged since its last
   checkpoint". The commit record's numbers are in the token to identify the state the store
   starts from, and are not presented as a boundary.
8. **Which stores.** The single-node engine (`wal.log`) and the in-process cluster
   coordinator (`cluster.log`). Not a shard node yet, and not a control node: see
   Consequences.

## What changes for a deployment

- A single-node server or an in-process cluster whose log is gone can be started without a
  backup: read the token from the refusal, start once with `--accept-lost-log <TOKEN>`. The
  store opens with what it had flushed or checkpointed. **The writes that were only in the
  lost log are gone,** and every client that was told they were stored was told wrong:
  replay the window from the upstream system of record if there is one.
- Restoring a backup is still the better way on when there is one, and putting the file back
  is better than both.
- One new file in a data directory that has accepted a loss (`log_loss.accepted`), two new
  gauges, one new alert (`RRLogLossAccepted`), one new durability operation (`log_lost`).
  No format change. An earlier release ignores the file.

## Alternatives considered

- **A start-up switch** (the first form). Everything under Problem. A switch is a standing
  setting: of the systems looked at, every one that accepts loss through a flag or a setting
  accepts the next loss the same way.
- **An offline tool** (`pg_resetwal`, `elasticsearch-shard`). The most common shape, and it
  cannot be left switched on. It needs the store's volume mounted where the tool runs, which
  on Kubernetes means scaling the workload down and running a one-off pod with the claim
  attached, for an operation that is one argument at start. Naming the loss gives the flag
  the property the tool has by construction. A tool can be added on the same record.
- **A file the operator places and the start consumes** (Consul's `peers.json`). Also
  one-shot, with the same access problem as a tool, and a file that is deleted on success
  leaves no evidence; the record here is kept.
- **Finish a pending acceptance at the next start without the token.** A pending entry with
  no log could also be a second loss under an unchanged commit record, after an acceptance
  that was carried out and never settled. That cannot happen as decided (every open
  settles), but the token costs nothing to ask for again and under a supervisor it is still
  in the arguments.
- **A way to clear the record,** so that a count-based alert can be resolved. Clearing would
  reset the count that makes a token unique. The time gauge resolves the alert instead.
- **Rebuild the lost cluster log from the shards' translogs.** An in-process shard's
  translog holds the mutations applied since the last seal, which overlap what the cluster
  log held. That is recovery, not acceptance, and worth its own look: today those translogs
  are reset when the shards are attached.

## Consequences

- A store can now be started in a state that is knowingly short of what it acknowledged. The
  step is explicit, bound to one loss, recorded, exported, and alerted on; it is still a
  decision to lose data.
- **A shard node gets no such step yet.** Its translog can be lost the same way
  (`translog.clog`), but its writes may still exist on a replica or in what the coordinator
  resends, its record would live per slot, and a shard node has no place to report from
  that reaches an operator. On the [roadmap](../roadmap.md#starting-a-shard-node-without-a-lost-translog).
- **A control node gets none.** A node that has voted must not come back with an empty log;
  it needs a new identity, and so a control plane that can add a member.
- After an accepted loss the store's clients hold acknowledgements for writes it does not
  have. Nothing in the store can repair that.
- The record can say more than happened, never less. A start that recorded the loss and
  stopped before it replaced the log leaves a pending entry; if the original log is then
  found and put back, the next open finds a log and marks the entry applied, although
  nothing was lost. An open cannot tell an original log from an empty replacement.
- The token is not part of `GET /_settings`: it is an instruction to one open, not a
  property of the running store.

## Proven

- `tests/persistence/accepted_loss.rs` (single node): the refusal names a token; opened with
  it the store serves what was flushed and not what was only in the log, raises `log_lost`,
  records one applied entry and takes writes; later starts need no token and the old token
  accepts nothing; a second loss is refused under the first token, names a second one, and is
  recorded as a second entry. A token for another loss accepts nothing and leaves no log and
  no record. A start that cannot create the empty log leaves a pending entry and no log, the
  store is still refused with the same token, and the token then finishes it with one entry.
  A pending entry beside a log is marked applied by a start that is given no token. A record
  that cannot be read fails the open. A backup carries the record.
- `cluster/coordinator/tests/accepted_loss.rs`: the same for the cluster log, and a refusal,
  or a start that stops after recording, leaves every other file in the directory as it was.
- `bin/server`: the flag reaches the engine configuration and the cluster configuration and
  has no bare form; both gauges are exported at zero for a store that has accepted nothing,
  and are read from the record in the data directory.
- Mutation checks, each after an unmutated baseline: listed in the pull request.

## Prior art

How other systems let an operator accept a loss, on four questions: the shape of the step,
whether it is spent by use, what durable evidence it leaves and when, and how it is reported
afterwards (sources read 2026-10-08).

- **Elasticsearch `elasticsearch-shard remove-corrupted-data`.** An offline tool ("Stop
  Elasticsearch before running `elasticsearch-shard`"), per shard, with an interactive
  confirmation. It writes an empty translog and then a new history UUID and allocation id,
  in that order: the evidence follows the destructive step. The shard then stays unassigned
  until a reroute with `accept_data_loss`: "To ensure that these implications are
  well-understood, this command requires the flag `accept_data_loss` to be explicitly set to
  `true`."
- **PostgreSQL `pg_resetwal`.** An offline tool that "will refuse to start up if it finds a
  server lock file", with `-f` per run. It leaves no evidence: it sets the control file's
  state to cleanly shut down. The documentation carries the rest: "You should immediately
  dump your data, run `initdb`, and restore."
- **CockroachDB `debug recover`** (loss of quorum). A plan file, bound to its cluster and
  carrying an id, applied offline or staged and applied at the next restart; "Regardless of
  application success or failure, staged plan would be removed." The evidence is written in
  the same batch as the change ("replica recovery evidence record"), kept in the store until
  it has been delivered ("Events are removed from the store unless callback returns false or
  error"), and reported as a structured event, a row in `system.rangelog`, the
  `range.recoveries` metric, and by `debug recover verify`.
- **Consul `raft/peers.json`.** A file the operator places, read at start: "This file will
  be deleted after Consul starts and ingests this file." It leaves two log lines.
- **Standing settings.** etcd `--force-new-cluster` is read at every start. MySQL
  `innodb_force_recovery`, Kafka `unclean.leader.election.enable`, Cassandra's
  `ignorereplayerrors` and RocksDB's recovery options are settings; MySQL makes its own
  tolerable by refusing writes while it is set.

What is taken from them. None of them accepts loss safely through a start-up flag: the flags
are all standing. The ones that are spent by use are bound to something (a shard and a
prompt, a plan id, a file that is consumed), so the flag here is bound to the loss it names.
Evidence before the destructive step is not the common practice, and Elasticsearch has the
gap that review found in the first form of this feature; CockroachDB is the model for the
record: written with the change, kept until it has been reported, and reported by more than a
log line.

**See also:** ADR-021 (durability failures are observable), ADR-051 (fail closed),
ADR-198 (a log is replaced by a rename), ADR-212 (a log is created whole), ADR-213 (the
refusal this decision gives a way past).
