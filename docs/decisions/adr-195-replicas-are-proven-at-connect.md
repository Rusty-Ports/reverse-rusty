# ADR-195 — A remote replica is proven against its primary before reads may fail over to it

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

On the remote replicated topology a read that cannot reach a position's primary fails over to a
replica, but only to one the coordinator has marked in sync (ADR-035). A replica leaves the
in-sync set when a replicated write to it fails, so a copy that may be missing a write is never
served.

That mark is one flag in the coordinator's memory, and every coordinator that connects set it
for every replica without looking. Two ordinary sequences therefore ended in a silent miss:

- **A replica that missed writes.** A replica is down while writes land on its primary. It
  comes back on its own volume. The coordinator restarts, for any reason, and the new process
  trusts the replica again. When the primary later becomes unreachable, reads are answered from
  the replica: `Ok`, without the queries written while it was down.
- **A replica on an empty volume.** A replacement replica listed in the endpoint group was
  assembled as in sync without recovery. The runbook warned about it; nothing prevented it.

Nothing durable records which replicas are in sync, `/_health` did not show it, and the
disaster-recovery table promised "RPO 0 for reads" at RF≥2 without the condition it depends on.
The in-process replicated cluster is not affected: it rebuilds its replicas from the primary on
every open.

## Decision

1. **A coordinator proves each remote replica when it connects.** `connect_replicated` compares
   every replica's content fingerprint and live count (ADR-097) with its primary's, after all
   copies are connected and before the cluster is assembled. The copies are quiescent then:
   this coordinator is not serving, and its exclusive lease has fenced the previous one. A
   replica is in sync only on an exact match.
2. **Any doubt is a refusal.** A fingerprint that differs, a copy that cannot attest its content
   (an old peer, a node without retained sources, a transport error), or a primary that cannot
   attest its own: the replica starts outside the in-sync set. Its data is left alone. Reads
   never fail over to it and writes are not fanned to it, the same as a replica that failed a
   write. Each such replica is reported as a `ReplicaDesync` event naming the copy and the
   reason.
3. **Recovery is the operator's decision.** With `--recover-divergent-replicas`
   (`ClusterConfig::recover_divergent_replicas`), a replica that fails the proof is re-recovered
   from its primary at connect (the existing `RecoverFrom` copy and tail drain) and proved
   again. Recovery discards what the replica held. It is off by default because the coordinator
   cannot tell which copy is right: when the *primary* is the one that lost its volume,
   recovering the replica from it would erase the surviving copy.
4. **The state is visible.** `ClusterEngine::out_of_sync_replicas` counts replicas outside the
   in-sync set. `/_health` reports it and is yellow with a reason while it is non-zero;
   `/_stats` reports it too.

## Alternatives considered

- **Recover every divergent replica automatically.** It would restore redundancy without an
  operator, and destroy the only good copy when the primary is the damaged one. Without primary
  terms or an in-sync set that survives the coordinator, the coordinator has no way to know.
- **A durable in-sync set** in the control plane, with primary terms, as Elasticsearch keeps
  allocation ids. This is the complete answer and the follow-up: it also covers a coordinator
  that crashes between a missed write and the next connect, which the proof handles only because
  it re-derives everything from content. It needs the control plane to be mandatory, and
  primary promotion; neither exists yet.
- **Compare only the live id sets,** which the connect already enumerates. A mismatch proves
  divergence, but a missed upsert keeps the same id, so equal sets prove nothing.
- **An online route that recovers one replica** without restarting the coordinator. It needs a
  write quiesce on the position while the recovered copy is swapped in. The connect-time flag
  gives operators a path today; the route is follow-up work.

## Consequences

- A stale or empty replica is no longer served. A read that cannot reach its primary and has no
  proven replica fails loudly (502), which is what an unreplicated position does.
- Redundancy can be lower than configured after a connect, and stays lower until the replica is
  recovered: restart the coordinator with `--recover-divergent-replicas` once the primaries are
  known good. `/_health` is yellow in the meantime, so a deploy that waits for green waits.
- Connecting costs one content fingerprint per copy of every replicated position. The
  fingerprint reads every live source on the node. Unreplicated clusters pay nothing.
- With `retain_source=false` a node cannot produce the fingerprint, so its replicas are never
  proven and failover is unavailable. That is the fail-loud side of the trade.
- A replica that lost a write under the *current* coordinator is handled as before (dropped
  from the in-sync set when the write fails). Nothing puts it back at runtime.

## Proven

- `tests/cluster_grpc_oracle/replica_proof.rs`, over real gRPC with nodes stopped and restarted
  on their own volumes: a replica that was down during a write is not trusted by the next
  coordinator, and a read after the primary is lost fails instead of answering without the
  missed query (it answered `Ok` with no match before); with recovery requested the replica is
  rebuilt and serves both queries on failover; equal replicas stay trusted across a coordinator
  restart; an empty replica beside a populated primary is not trusted.
- `cluster/replica/tests/proofs.rs`: reads never fail over to a replica without a proof, also
  when it is first in the failover order; writes are not fanned to it; it is reported once the
  observer is installed.
- `cluster/coordinator/distributed/replica_sync.rs`: only an exact fingerprint and count match
  is a proof.
- Server: `/_health` is yellow with a reason while a replica is out of sync; the flag reaches
  the cluster configuration and is off by default.

**See also:** ADR-035 (replication and failover), ADR-097 (content fingerprint), ADR-036/040
(peer recovery and retention leases), ADR-094 (the boot-time presumption this replaces).
