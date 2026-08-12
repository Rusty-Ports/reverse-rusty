# `POST /_cluster/reassign` — Move and commit one assignment

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Move one global logical position to a registered data-node membership ID. The coordinator attests
the current live primary, persists a versioned move intent, peer-recovers the target when needed,
records exact recovery evidence, conditionally commits the target as durable owner, and only then
exposes it through live routing.

This operation is available only on a `distributed` build running an authoritative **resolve-only
remote coordinator**: `--route-by-assignments`, at least one `--control-endpoint`, the committed
`--shards` count, and no `--shard-endpoint`. Static endpoint-order and CLI-seeded assignment
topologies are rejected before work admission because their next restart cannot safely follow a
changed map.

## Compatibility boundary

This is a native API, not an alias for Elasticsearch or OpenSearch `POST /_cluster/reroute`.
Reroute accepts a list of named-index allocation commands such as move, cancel, allocate replica,
and allocate primary, together with allocation deciders, dry-run/explain, retry, metric, and overall
timeout controls. Reverse Rusty has one global matcher and this route moves exactly one global
position to a numeric membership node. Advertising the broader path would make existing allocation
automation appear to work while changing different state.

The exact shared concepts are available as additive aliases: `shard` for `position`, `to_node` for
`node`, and `master_timeout` / `cluster_manager_timeout` for manager-start admission. See the
[Elasticsearch reroute API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute)
and [OpenSearch reroute API](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)
for the intentionally unsupported contract.

## Request

```http
POST /_cluster/reassign?cluster_manager_timeout=30s
Content-Type: application/json

{"position": 0, "node": 2}
```

The body is required, strict JSON (`application/json` or `+json`), limited to 64 KiB, and must
arrive within 250 ms. Unknown, duplicate, missing, null, or incorrectly typed fields are rejected.

| Field | Type | Meaning |
|---|---|---|
| `position` | unsigned 32-bit integer | Global logical position to move. `shard` is an alias; do not send both. |
| `node` | unsigned 64-bit integer | Target registered membership ID. `to_node` is an alias and may also be a decimal JSON string; do not send both. |

Only one query parameter is accepted:

| Parameter | Default / maximum | Meaning |
|---|---|---|
| `cluster_manager_timeout` or `master_timeout` | `30s` / `30s` | Bounds waiting for the one-operation admission slot, topology/cluster access, and move-ledger reservation before the atomic start gate. `0` performs one immediate attempt. Send at most one spelling. |

`timeout` is deliberately rejected. Once recovery or a control proposal starts, cancelling it cannot
promise unchanged state. A manager timeout before the start gate returns `408 reassign_timeout` and
guarantees that no move or assignment commit starts later. After the gate, the request waits for the
terminal result even if the manager deadline passes; a client disconnect drops only the response.
The independently supervised operation continues, and graceful coordinator shutdown waits for it.

## Response

```json
{
  "took": 42,
  "took_ms": 42.731,
  "acknowledged": true,
  "moved": true,
  "committed": true,
  "reconciled": false,
  "position": 0,
  "node": 2,
  "generation": 4
}
```

`took` is whole elapsed milliseconds; `took_ms` preserves fractional precision. Every response is
`Cache-Control: no-store`. Detailed membership endpoints and mesh failures stay in server logs;
client failures preserve the typed status without exposing the internal topology.

The terminal flags distinguish three successful states:

| State | `acknowledged` | `moved` | `committed` | `reconciled` | Meaning |
|---|---:|---:|---:|---:|---|
| Moved and committed | `true` | `true` | `true` | `false` | This invocation physically flipped routing; the durable target is committed, either already or by this invocation. |
| Already converged | `true` | `false` | `true` | `false` | Live routing and the durable assignment already named the target; no new proposal was needed. |
| Durable authority reconciled | `true` | `false` | `true` | `true` | Live routing already named a different authority, so this invocation durably reconciled it without stale recopy before completing the requested placement. |

The `acknowledged`, `committed`, and optional `warning` fields retain their original response shape,
but the native durable mover no longer returns a successful live-but-uncommitted state. An ambiguous
control, fence, endpoint, or recovery-evidence outcome is a structured non-200 failure with its move
intent preserved. Retrying after quorum/endpoint recovery is idempotent; a resolve-only coordinator
restart also inspects and resolves the recorded phase before serving, with no manual pre-restart
retry. Unprovable authority fails startup loud.

A proven clean failure before readiness automatically unfences the source and aborts its preparing
intent. A position with committed replicas is rejected by this single-target route; use
`/_cluster/rebalance` or `/_cluster/reconcile`, which dispatch the group-aware durable movement path.

Common transport and topology failures include:

| Status / type | Meaning |
|---|---|
| `400 validation_error` | Invalid strict body/query, unsupported in-process topology, or a replicated position. |
| `408 request_timeout` | The request body missed its delivery deadline. |
| `408 reassign_timeout` | Admission or topology access timed out before start; nothing was started. |
| `409 reassign_routing_not_authoritative` | Static remote routing cannot follow the committed map. |
| `409 reassign_resolve_only_required` | Assignment routing was seeded from CLI endpoints; restart resolve-only first. |
| `413 payload_too_large` | Body exceeds 64 KiB. |
| `415 unsupported_media_type` | Missing or non-JSON content type. |
| `501 not_supported_in_cluster_mode` | Server was built without `distributed`. |
| `502 shard_unreachable` / `invalid_shard_response` | Required mesh recovery or attestation failed. |
| `503 control_plane_error` / `durability_unavailable` | Membership, assignment, durable-intent, or conditional-commit proof was unavailable. No ambiguous authority is acknowledged. |
| `503 reassign_unavailable` | Admission is closed or the worker could not start. |

Cross-topology assembly is documented in [coordinator mode](../server/coordinator-mode.md), the
operator procedure in [cluster deployment](../../../operations/cluster-deployment.md#5-scaling),
and the movement/failure model in
[clustering and scaling](../../../design/clustering-and-scaling.md#9-movement-and-failure-recovery).
Rationale and proof are recorded in
[ADR-090](../../../decisions/adr-090-data-moving-reassignment.md) and
[ADR-171](../../../decisions/adr-171-cluster-reassign-api-contract.md), with the durable transition
specified by
[ADR-175](../../../decisions/adr-175-durable-reassignment-intent-and-conditional-cutover.md).
