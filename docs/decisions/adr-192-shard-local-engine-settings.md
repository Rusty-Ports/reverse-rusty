# ADR-192 — A shard node sets its own durability and storage settings

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

In the remote topology (Compose, Helm) every shard slot is built from the configuration its
`shardserver` process holds. That configuration was `EngineConfig::default()` with two
overrides, the hot-anchor threshold and tag-segment skipping. Four settings that belong to the
process holding a shard's disk had no flag on that process and no field on the wire:

- `wal_sync_on_write`: fsync the shard translog on every write, so an acknowledged write
  survives a power loss and not only a process crash;
- `retain_source`, `max_segments`, `memtable_flush_threshold`: memory and compaction cost.

The coordinator accepts flags of the same names. Against remote shards it used them for
nothing: it holds no shard and its own log is in memory. It still reported them from
`GET /_settings` as the per-shard configuration. So a coordinator started with
`--wal-sync-on-write` logged no warning, answered `per_shard.wal_sync_on_write: true`, and every
shard synced at flush checkpoints only. The disaster-recovery guide told operators to "flip the
knob" for power-loss durability directly under the remote rows, and in that topology the knob
could not be set.

A process crash was never affected (the translog replays). What was missing was the opt-in
hardening against power loss, and the cost settings.

## Decision

1. **The settings are node-local, like the hot-anchor threshold and tag-segment skipping.**
   `shardserver` takes `--wal-sync-on-write <true|false>`, `--retain-source <true|false>`,
   `--max-segments N` and `--memtable-flush-threshold N`. The value is explicit so manifests can
   template it. The assembled configuration is validated like the server's, and a flag that
   cannot be read is an error, not a silent default.
2. **Every slot on the node uses them**, however it comes to exist: adopted, added beside
   another, restored from disk at startup, or filled by a peer recovery. The settings are not
   stored with the data, so a restart under a different flag changes them.
3. **A remote coordinator refuses `--wal-sync-on-write`.** It is a durability promise that
   process cannot keep, and accepting it is how the misconfiguration looked correct. The error
   names `shardserver --wal-sync-on-write true`. The three cost flags only warn, as the
   threshold and tag-skipping flags already do, and name the shard flag.
4. **`GET /_settings` says whose values it shows.** On a remote coordinator the response
   carries `shard_local`, the list of `per_shard` keys each shard node sets for itself. Those
   values are the coordinator's and say nothing about the shards. In-process the field is
   absent: there `per_shard` is every shard's configuration.
5. **A shard node states its policy.** The startup banner says whether the translog is synced
   on every write, and `/_metrics` exports
   `reverse_rusty_shard_translog_sync_on_write{shard}`: 1 when every write is fsynced, 0 when
   the shard syncs at flush checkpoints or has no data directory.
6. **The manifests expose it.** Helm: `shard.walSyncOnWrite`, `shard.retainSource`,
   `shard.maxSegments`, `shard.memtableFlushThreshold`. Compose:
   `RR_SHARD_WAL_SYNC_ON_WRITE`.

## Alternatives considered

- **Ship the configuration from the coordinator in `AdoptDict`.** One source of truth, but a
  shard restores itself from disk before any coordinator connects (ADR-072), so the shipped
  values would have to be persisted beside the data; it is a wire change; and several
  coordinators could disagree. Worth revisiting if the control plane ever owns a configuration
  document.
- **Have each shard attest its settings in the handshake**, so the coordinator can report the
  real values and warn when replicas disagree. It is the better end state for `/_settings` and
  a natural follow-up; the metric and the `shard_local` marker make the current state honest
  without a wire change.
- **Warn on `--wal-sync-on-write` instead of refusing.** A warning is what the threshold flag
  gets, because a mismatch there costs only speed. Here the flag claims durability.

## Consequences

- An operator of the remote topology can have power-loss durability: set the flag on every
  shard node. A mixed set gives each copy the durability of its own node; the metric shows
  which.
- A coordinator command line that passed `--wal-sync-on-write` in remote mode now fails at
  startup. It was not doing what it said.
- `retain_source = false` and the compaction thresholds can be set on remote shards.
- `per_shard` on a remote coordinator still shows the coordinator's values for the listed keys.
  Read them from each shard node's flags or metric.

## Proven

- `shardserver` argument tests: the four flags reach the engine configuration; none set means
  the defaults; an unreadable value or one the engine rejects is an error; the banner states
  what survives.
- `cluster/server/tests/engine_config.rs`: an adopted slot, a slot added beside it and a slot
  restored at startup all report the node's sync policy, a restart under the other flag
  changes it, and a node without a data directory reports 0.
- `tests/cluster_grpc_oracle/engine_config.rs`: over real gRPC, a slot filled by peer recovery
  from a node with the default policy keeps the target node's fsync-per-write.
- Coordinator flag tests: defaults need nothing, `--wal-sync-on-write` is refused and names
  the shard flag, the cost flags warn and name theirs; every key in `shard_local` is a
  `per_shard` key.
- Settings tests: `shard_local` is absent in-process and lists the keys on a remote cluster.
- The Helm chart CI renders the four values into the shard StatefulSet as `shardserver`
  parses them.

**See also:** ADR-013 (write-ahead log), ADR-039 (per-shard translog), ADR-072 (shard
self-restore), ADR-105 and ADR-174 (the two settings that were already node-local), ADR-091
(shard metrics).
