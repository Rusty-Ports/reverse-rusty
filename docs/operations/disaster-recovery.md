# Disaster recovery

This runbook explains what you can lose, what each loss costs (RPO/RTO), and the
recovery flow for the failures the per-component tables don't cover — volume loss, control-quorum
majority loss, and whole-cluster loss. Routine single-component failures (a pod restart, a rolling
restart, a replica failover) are **not** re-documented here — they live in
[`cluster-deployment.md` §6](cluster-deployment.md) (Compose) and
[`kubernetes-deployment.md` §5](kubernetes-deployment.md) (Helm); this page is for the events that
need a backup.

> TL;DR — every acked write on a durable instance survives a process crash (WAL/translog replay);
> what a **volume loss** costs is everything since your last backup or snapshot. Take backups
> ([`backup-restore.md`](backup-restore.md)), rehearse the restore (the [Rehearsal
> section](backup-restore.md#rehearsal--prove-you-can-restore)), and prefer restoring control-plane
> volumes over re-bootstrapping a quorum.

## 1. The RPO/RTO model

RPO (how much acked data a failure may cost) and RTO (how long until serving again) by mode and
failure class. "Crash" = the process dies (OOM-kill, SIGKILL, node reboot with the volume intact);
"volume loss" = the durable directory is gone or corrupt.

| Mode | Failure | RPO | RTO |
|---|---|---|---|
| Single-node (`--data-dir`) | crash | **0 acked writes** (WAL replay; ADR-013/088) — see the power-loss caveat below | restart + reopen (seconds; grows with corpus) |
| Single-node | volume loss | since the last backup | restore ([`backup-restore.md`](backup-restore.md)) + restart |
| In-process cluster (`--cluster --data-dir`) | crash | **0** (coordinator log + manifest + per-shard segments; ADR-031/032) | restart + reopen |
| In-process cluster | volume loss | since the last backup | restore + restart |
| Remote (Compose/Helm), RF=1 | shard pod crash | **0** (per-shard translog + committed segments, including successfully recovered targets; ADR-039/181) | pod restart; reads routing to it `502` meanwhile (fail-loud, ADR-072) |
| Remote, RF=1 | **shard volume loss** | since the last snapshot **of that shard** | §3.1 below |
| Remote, RF≥2 | one node lost | **0 for reads when the position has an in-sync replica** (failover, ADR-035); a replica that missed a write, or was not proven equal to its primary when the coordinator connected (ADR-195), is never served, so those reads fail loud (502) instead. Writes need the primary | automatic for reads; `/_health` is yellow and reports `out_of_sync_replicas` while redundancy is reduced; replica recovery per [runbook §6](cluster-deployment.md) |
| Remote | coordinator crash or restart | **0 acked writes** (it holds no data). A write answered 503 `partial` was never acked: until its writer repeats it, it stays missing from the shards that refused it, because the repair queue is the coordinator's memory (ADR-194) | restart; writers repeat their 503s |
| Remote | control-plane **minority** loss | 0 (quorum holds; durable Raft, ADR-041) | restart the node; it rejoins |
| Remote | control-plane **majority** loss | cluster-state document: to the last control-volume snapshot (query data is unaffected — it lives on the shards) | §3.2 below |
| Any | whole-cluster loss | since the last **consistent backup set** | §3.3 below |

**The power-loss caveat** (deployment-modes [§4](deployment-modes.md)): `wal_sync_on_write`
defaults **false** — an acked write survives a process crash, not necessarily a power cut on the
same host (the OS page cache is the window). Turn on fsync-per-write where that RPO matters, on
the process that holds the data (ADR-192):

- single-node and in-process cluster: `server --wal-sync-on-write`;
- remote (Compose/Helm): `shardserver --wal-sync-on-write true` on **every shard node** (Helm
  `shard.walSyncOnWrite=true`, Compose `RR_SHARD_WAL_SYNC_ON_WRITE=true`). The coordinator holds
  no shard data and refuses the flag. Confirm it per shard with
  `reverse_rusty_shard_translog_sync_on_write == 1` on the shard's `/_metrics`.

On Kubernetes/cloud volumes a "node loss" normally detaches the volume rather than losing the
page cache silently, but the honest statement is: default RPO 0 is against process death, not
power loss.

**RTO evidence, not promises:** restore time is dominated by copying the backup and reopening
(mmap + log tail replay). Measure yours in the backup rehearsal and record it — that number is
your real RTO.

## 2. Scenario → procedure map

| Scenario | Go to |
|---|---|
| Shard pod crashed / restarting | [runbook §6 row 1](cluster-deployment.md) — self-restores from its volume |
| Need to restart everything one by one | [runbook §6 rolling restart](cluster-deployment.md); upgrades → [`rolling-upgrade.md`](rolling-upgrade.md) |
| Coordinator down/restarted | [runbook §6](cluster-deployment.md) — stateless, reconnects and re-derives; before a planned restart, drain its repair queue (`POST /_cluster/resync` until `still_pending` is 0) |
| One control node down | [runbook §6](cluster-deployment.md) — quorum holds, restart it |
| Replica failover / replacement (RF>1) | [runbook §6 last two rows](cluster-deployment.md) — fresh-volume replicas need explicit peer recovery |
| Shard **volume** lost (RF=1) | **§3.1 below** |
| Control quorum majority lost | **§3.2 below** |
| Everything lost (site/namespace deletion) | **§3.3 below** |
| A node refuses to start: `… is missing, but …` (its log is gone) | **§3.4 below** |
| Backup taking / verifying / restoring mechanics | [`backup-restore.md`](backup-restore.md) |

## 3. The flows this page owns

### 3.1 Shard volume loss at RF=1

The failure that actually costs data. The shard's segments + translog are gone; no replica holds a
copy.

1. **Stop routing damage:** reads fanning to the dead shard already fail `502` (fail-loud — no
   silent short results, ADR-072). Leave it down; do not start a fresh empty shard on a new volume
   — an empty serving shard turns loud errors into **silent false negatives**.
2. **Restore the shard's last snapshot** into a fresh volume (the per-shard volume-snapshot
   procedure in [`backup-restore.md` §zero-write-stall](backup-restore.md)); start the shard on it.
   It self-restores from the restored segments + translog like any durable restart.
3. **Reconcile the gap:** everything ingested into that shard after the snapshot is lost. If the
   upstream system of record can replay recent writes (the percolator-workload pattern — stored
   queries come from a source database), re-ingest the window since the snapshot timestamp;
   coordinator upserts are idempotent per id (ADR-067/070).
4. **Verify** (§4) before declaring recovery.

At RF≥2 this scenario is a non-event (peer recovery rebuilds the member from the group,
ADR-035/036) — which is the argument for RF≥2 on any deployment where a volume loss is plausible.

### 3.2 Control-plane majority loss

Query data is untouched (it lives on the shards); what is at risk is the **cluster-state
document** — membership + the committed shard→node assignments (ADR-086/090).

**Preferred: restore, don't re-bootstrap.** Restore the control nodes' `--data-dir` volumes from
snapshots and start them; the quorum re-forms from the durable Raft state (ADR-041) and the
committed document — including any data-moving reassignments — is preserved. Control state is tiny
and changes rarely, so even a stale snapshot is usually exactly right.

**Fallback: re-bootstrap a fresh quorum.** Only when no control snapshot exists:

1. Start fresh control nodes with the **same node ids, same `--shards K`**, correct
   `--advertise-url`s (a wildcard bind refuses to start — ADR-082), and `--bootstrap` on the
   first.
2. Restart the coordinator. Against a genesis (empty) document, `--route-by-assignments` seeds the
   committed map **position-preservingly from its `--shard-endpoint` order** (ADR-086) — for a
   cluster that never ran a data-moving reassignment, that reproduces the placement exactly.
3. **If the cluster HAD been reassigned/reconciled away from the CLI order**, the seeded map no
   longer matches where the data actually sits. This stays fail-loud, not silent: a probe to a
   node that doesn't host the position returns `not_found` → `502` (slots are shard-id-keyed,
   ADR-093). Converge with one `POST /_cluster/reconcile` (data-moving, idempotent, ADR-092) —
   it moves each position to its desired node and commits the map. Budget O(moved corpus) for it.
4. **Verify** (§4).

### 3.3 Whole-cluster loss

Rebuild from the last **consistent backup set** (all shard volumes + control volumes snapshotted
around the same quiesce — see the point-in-time warning below).

1. Restore every `shardN-data` volume and every `controlN-data` volume from the same backup set.
2. Bring up in the bootstrap order ([runbook §3](cluster-deployment.md)): **control quorum →
   shards → coordinator**. Each layer self-restores from its restored volume; the coordinator
   reconnects, re-ships the dict, and routes by the restored committed assignments.
3. Reconcile the ingest gap since the backup set from the upstream system of record (§3.1 step 3).
4. **Verify** (§4).

**The point-in-time warning** (the named cross-shard-barrier constraint, deployment-modes
[§4](deployment-modes.md) / ADR-079): a remote cluster's backups are **per-shard consistent**, not
cross-shard consistent. Snapshots taken at different times can disagree — a query acked between
shard A's snapshot and shard B's exists in one restored shard and not the other (matching is
per-query, so the effect is "that query is missing", not corruption). For a consistent **set**,
quiesce writes (pause the ingest pipeline), snapshot every shard + control volume, then resume —
the [runbook §7 procedure](cluster-deployment.md) (a stateless coordinator's
`POST /_checkpoint` seals primaries independently and `POST /_backup` returns 400; each node's volume
is crash-consistent on its own). If you must restore from a non-quiesced set, treat the window
between the oldest and newest snapshot as lost and replay it from upstream.

### 3.4 A store refuses to start because its log is gone

The start-up error reads `<file> is missing, but <what proves it existed>. The writes
acknowledged since then were in it and are lost.` (ADR-213). The store still has everything up
to its last flush or checkpoint. What it has lost is every write it acknowledged after that.
Earlier releases started in this state without those writes and said nothing; this is the same
loss, reported.

First find out why the file is gone (someone removed it, a restore left it out, the volume lost
it) and delete nothing else. **Never remove a log to get a node started.** Then, by role:

| Role | File | Ways on |
|---|---|---|
| Single-node server | `wal.log` | Restore the data directory from a backup ([`backup-restore.md`](backup-restore.md)). Or start **once** with `--accept-lost-log`: it serves from its last flush. |
| In-process cluster coordinator | `cluster.log` | The same; it serves from its last checkpoint. |
| Shard node of a remote cluster | `translog.clog` | Treat it as a lost volume: §3.1 at RF=1, the replica-replacement row of [runbook §6](cluster-deployment.md) at RF≥2. Or start it **once** with `--accept-lost-log`: it serves from its last checkpoint, and replicas that still hold the lost writes are found unequal at the coordinator's next connect and kept out of the in-sync set (ADR-195). |
| Control node | `raft-log.bin` | **No flag.** A node that has voted must not come back with an empty log. Restore the node's data directory from a snapshot (§3.2). Until then leave it down: the other control nodes keep their majority. Do not start it on a partly emptied directory. |

After an accepted loss:

1. The server logs the loss and `durability_failures_total{op="log_lost"}` counts it once. A
   shard node prints a `DURABILITY log_lost: …` line on standard error and counts it in
   `reverse_rusty_shard_durability_failures_total{op="log_lost"}`. For a cluster or a shard
   the event names the log position after which writes were lost. For a single-node server it
   says that every unflushed write is lost and that these can be older than the manifest's
   log watermark, so replay from before the last flush, not from that number.
2. Replay the window since the last flush or checkpoint from the upstream system of record
   (§3.1 step 3); upserts are idempotent per id.
3. Remove `--accept-lost-log`. Left in place it would accept the next loss as well.
4. Take a fresh backup, then **verify** (§4).

A backup taken from a store in this state is refused, and a backup directory that lacks its log
does not verify, so a restore cannot bring the problem back.

## 4. Post-recovery verification checklist

- [ ] `/_health` green; every shard `reverse_rusty_shard_ready 1` on its `/_metrics` (ADR-091).
- [ ] `GET /_stats` matches the recorded pre-loss snapshot (or known-lost window): compare
      `live_queries` for standalone, or the cluster's physical `total_queries` +
      `shard_queries[]` under the same placement. Do not treat the cluster physical total as a
      distinct logical-document count (ADR-140).
- [ ] **Golden-titles probe:** percolate a kept file of representative titles and diff the matched
      ids against the recorded expected output (the same probe set the backup rehearsal uses —
      [`backup-restore.md`](backup-restore.md)). Fan-out sanity: `GET /_cat/shards` shows every
      position serving.
- [ ] A test **write** round-trips: `PUT /_doc/<sentinel>` → `POST /_search` with a matching title
      → `DELETE /_doc/<sentinel>`.
- [ ] Alerts quiet ([`alerting.md`](alerting.md)): no `durability_failures_total` increase, no
      transport errors, control plane reports one leader.
- [ ] Record what the recovery actually took (wall-clock, data window lost) — that is your
      measured RTO/RPO for the next revision of this page.
