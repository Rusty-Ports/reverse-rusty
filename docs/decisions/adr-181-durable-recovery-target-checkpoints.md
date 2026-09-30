# ADR-181 — Durable recovery target checkpoints

> [Clustering — replication & control plane](areas/clustering-replication-and-control-plane.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted (2026-09-30)

## Context

`RecoverFrom` synced and attached a peer's files and reset the target's translog, but retained its
previous `shard.ckpt`. A successful recovery followed by restart therefore selected an empty or old
base. An old nonzero checkpoint could also skip the new log's first mutations. The existing oracles
checked live recovery, without restarting its target. Remote `seal_for_checkpoint` only flushed and
returned a zero sentinel, and a stateless coordinator's checkpoint never sealed its data nodes.

Peer systems tie durable base selection to log replay: [RocksDB checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints)
copy the manifest/current selectors alongside data, while [Elasticsearch flush](https://www.elastic.co/docs/reference/elasticsearch/index-settings/translog)
commits index state before starting a new translog generation. Reverse Rusty already has an atomic
sidecar and retention-aware seal; reuse them without introducing another format or dependency.

## Decision

After receiving and syncing the advertised files and segment directory, attach the new shard
privately. Commit its segment list, segment-ID cursor, dictionary/compiler stamps, source selector,
and **local** replay floor through the existing CRC + temporary file + rename + directory-sync
sidecar writer. Attachment starts the local translog at zero. The source's watermark is only the
remote catch-up cursor. A sidecar error fails recovery before publication and acknowledgement.

The recovery-only helper requires a fresh empty local translog. It writes only the selector:
calling the full seal here would unnecessarily rewrite the entire source corpus immediately after
copying it. Generic in-process attachment remains governed by the coordinator manifest and does not
gain an implicit data-node checkpoint.

Add a per-slot `Seal` RPC that calls the existing seal: flush/reseal, persist sources, commit the
sidecar, then trim to the active retention floor. Its reply contains the real local watermark and
attests the placement generation and shard count. `RemoteShard` rejects mismatched replies and has
no flush/sentinel fallback. An old peer fails `UNIMPLEMENTED`; roll data nodes before coordinators.
Mesh authentication and coordinator ownership apply through the existing RPC middleware.
The seal runs on a blocking worker and completes its commit after RPC cancellation, leaving the
gRPC runtime available for reads and health checks.

A remote `ClusterEngine::checkpoint` seals each current primary. The REST response keeps
`durable: false` and epoch zero because the coordinator has no manifest, but reports sealed primary
positions in `shards_checkpointed`. An error fails the sweep; some earlier primaries may already be
sealed, which is safe to retry. Replicas can receive the same RPC independently. No automatic seal
timer or globally consistent remote snapshot is introduced; volume-set backups still quiesce writes.
No durable format or compiler-semantic stamp changes.

## Alternatives and limits

- **Full seal after attachment:** correct but rewrites just-copied sources; choose a selector-only
  commit on the unpublished target and keep full seals for maintenance.
- **Implicit checkpoint in generic attachment:** changes the in-process manifest authority; reject.
- **Keep the zero sentinel:** cannot attest persistence or bound remote replay; reject.
- **Atomic staging-directory replacement:** remains separate work. Recovery targets must be kept
  out of serving until recovery/catch-up succeeds. A failed or interrupted recovery is retried;
  this decision guarantees the acknowledged recovery base, not preservation of a replaced target's
  previous contents during an unacknowledged copy.

## Validation and outcome

The gRPC oracle restarts fresh and previously checkpointed recovery targets without an intervening
seal, checks the recovered base plus add/remove tail, and injects sidecar failure to prove recovery
cannot acknowledge or publish without its commit. A remote checkpoint test checks translog shrink
and restart replay of subsequent writes. The broad corpus peer-recovery and concurrent handoff
oracles also restart their targets. The container harness repeats recall checks after target restart.

ADR-039's data-node restart guarantee now covers successful recovery targets. ADR-040's log bound
requires seals to run; lease release permits reclamation at the next seal, not immediate autonomous
GC. The full engine gate and local Codex review are required before merging this change.
