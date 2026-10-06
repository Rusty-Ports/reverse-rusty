# `POST /_cluster/resync` — Re-drive partial-apply repairs

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md) · [ADR-169](../../../decisions/adr-169-cluster-resync-api-contract.md)

Run one exact pass over the coordinator's in-memory partial-apply repair queue. Each queued mutation
failed on one or more target positions, and its writer was answered with a retryable 503; the pass
re-drives only those failed positions. It is safe to repeat.

```http
POST /_cluster/resync?cluster_manager_timeout=30s
```

The request has no body. A successful response is:

```json
{
  "took": 4,
  "took_ms": 4.72,
  "acknowledged": true,
  "repaired": 3,
  "still_pending": 1
}
```

`acknowledged:true` means this repair pass ran to completion and the counts are terminal for that
pass. It does **not** claim every target recovered: `still_pending` is the number of mutations that
still had at least one unreachable position and remain queued for another pass. `repaired` counts
mutations, not physical shard copies. Every response carries `Cache-Control: no-store`.

## Request contract

Only `POST` is accepted; other methods return `405` with `Allow: POST`. The request must have an
empty body. Delivery is capped at 64 KiB and 250 ms so an administrative no-body route cannot inherit
the server-wide 100 MiB allowance or hold a connection indefinitely.

| Query parameter | Default | Contract |
|---|---:|---|
| `cluster_manager_timeout` | `30s` | Maximum wait for shared administrative admission and exclusive REST-writer/cluster access before the pass starts. Accepts `0` or an integer plus `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`; capped at 30 seconds. |
| `master_timeout` | — | Elasticsearch and legacy OpenSearch alias for `cluster_manager_timeout`; specifying both is rejected. |

Zero performs non-waiting admission and lock probes. A positive deadline that expires before start
returns `408 resync_timeout` and guarantees no pass was started. Once the worker starts, the manager
timeout no longer applies: a pass can repair some positions before reaching a slow peer, so
cancelling it cannot promise the queue and shards stayed unchanged. The request waits for the exact
terminal report; a client disconnect does not cancel the independently supervised worker.

`timeout` is rejected rather than presented as an overall cancellation guarantee. Unknown or
duplicate query parameters are rejected.

## Errors and admission

| Status | Error type | Meaning |
|---:|---|---|
| `400` | `validation_error` | Unknown/duplicate/conflicting query control, invalid or over-limit duration, or non-empty body. |
| `405` | `method_not_allowed` | Method other than `POST`. |
| `408` | `request_timeout` | The empty request body did not finish within 250 ms. |
| `408` | `resync_timeout` | Admission or exclusive access expired before the pass started. |
| `413` | `payload_too_large` | Request body exceeded 64 KiB. |
| `500` | `resync_unavailable` | An admitted worker panicked or its completion supervisor failed. |
| `503` | `resync_unavailable` | Administrative admission or the dedicated worker is unavailable. |

Repair shares the server's single expensive corpus-administration slot with stats, vocabulary,
membership mutation, and in-process resize. The permit and writer/cluster guards remain owned by the
worker through completion, including after disconnect. Shutdown retains that same admission
boundary before its final durability cleanup, so an admitted repair cannot begin after cleanup.

## Elasticsearch/OpenSearch boundary

Elasticsearch and OpenSearch expose `POST /_cluster/reroute?retry_failed=true`, but that retries
failed **shard allocation**. Reverse Rusty resync retries delivery of already-logged query mutations
to their original failed positions; it does not change allocation, routing, or cluster state. The
native path is therefore intentional. `retry_failed`, reroute commands, `dry_run`, `explain`,
`metric`, and a fabricated `/_cluster/reroute` alias are rejected rather than silently given another
meaning. The manager-timeout names and additive `acknowledged` field are adopted because those
ergonomics map without changing the operation.

## Recovery limits

Use this after a cluster write reports a partial apply (503 `"result": "partial"`), or repeat the
write: a partial `PUT /_doc/{id}`, bulk item or DELETE is idempotent, and repeating it converges
the document on any coordinator (ADR-194). Repeat a partial `op_type=create` as an index
operation. A re-drive replaces the document on each position it reaches, so a position that had
in fact applied the write before its reply was lost ends with one copy.

The queue exists only in the same coordinator process, and nothing in the server drains it on its
own. A stateless remote coordinator restart does not restore it: the writes it held stay missing
from the positions that refused them until their writers repeat them. As it stops, a coordinator
logs the document ids it never converged. An in-process durable coordinator's log replay is its
recovery backstop, and its shard writes do not fail part-way. A successful no-op pass proves only that this process currently has no queued repairs; it
cannot attest that a prior stateless coordinator never observed a partial apply. See ADR-047,
ADR-067, and ADR-125. Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md); the system invariants are canonical in
[clustering and scaling](../../../design/clustering-and-scaling.md).
