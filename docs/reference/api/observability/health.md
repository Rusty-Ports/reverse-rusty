# `GET` / `HEAD /_health` — Native readiness

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

```bash
curl 'localhost:9200/_health?wait_for_status=green&timeout=30s'
```

Standalone response:

```json
{
  "status": "green",
  "mode": "standalone",
  "timed_out": false,
  "total_queries": 3,
  "wal_healthy": true,
  "persistence_healthy": true,
  "skipped_segments": 0,
  "stale_segments": 0
}
```

| Status | Meaning |
|---|---|
| `green` | Single-node durability is healthy, or every cluster position answers with no queued repair and every replica in sync |
| `yellow` | Single-node load skipped/stale segments, or cluster partial applies are queued for resync, or a replica is outside the in-sync set so reads cannot fail over to it (ADR-195) |
| `red` | Single-node WAL/persistence failure (including a commit whose manifest was renamed into place and whose directory could not be synced, ADR-222: the node serves reads and logs writes by id, commits nothing more, and needs a restart), or a required cluster shard/control/topology check failed |

Cluster health uses a deliberately smaller native payload:

```json
{
  "status": "green",
  "mode": "cluster",
  "timed_out": false,
  "shards": 8,
  "pending_repairs": 0,
  "out_of_sync_replicas": 0,
  "rebuild_in_progress": false
}
```

`out_of_sync_replicas` counts the replicas reads may not fail over to: a replicated write to them
failed, or they were not proven equal to their primary when the coordinator connected. It is 0 on
a cluster without replicas. A yellow or red response also includes `reason`. Detailed shard/control-plane errors are logged but
the unauthenticated response uses a stable generic red reason.

The route is a strict, bodyless GET/HEAD with a 64 KiB extraction ceiling. It rejects unknown query
parameters and unsupported values, returns structured 400/405/413 errors, sends
`Allow: GET, HEAD` on 405, strips the body for HEAD, and includes `Cache-Control: no-store` on every
response. `GET` and `HEAD` remain open even with `--auth-protect-reads` so orchestrator probes do
not need bearer credentials. Admission occurs before body buffering and allows at most eight
concurrent health requests; additional work fails immediately with 429, `Retry-After: 1`, and a
structured `rejected_execution_exception`. The cap therefore also covers slow request bodies, and
body buffering has an independent 250 ms deadline. A body that does not complete by then returns
408 with a structured `request_timeout`, releasing its health permit. Health duration telemetry
starts before method validation, admission, and body extraction, so it includes all transport work
and rejections as well as successful probes and status waits.

Supported query controls:

| Control | Contract |
|---|---|
| `wait_for_status=red\|yellow\|green` | Wait until the observed status reaches at least this ordered color; yellow accepts yellow or green |
| `timeout=<time>` | Bound the wait, stats admission, and coordinator probe result wait; default `30s`; units are `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d` |
| `level=cluster` | Accepted familiar spelling for this cluster-level native response; index/shard levels are rejected |

Green and yellow return HTTP 200. Red returns 503. If coordinator collection cannot complete by the
deadline, or `wait_for_status` is not reached, the latest response returns 408 with
`"timed_out":true`. The response preserves the last completed observation rather than replacing it
with a synthetic failure at the deadline, and a status first seen after the deadline remains timed
out. The coordinator rechecks the wall clock after each blocking result rather than relying only on
the async timeout race. Explicit status waits and dependency-probe deadlines have distinct stable
reasons. A coordinator request that times out cannot forcibly stop already-running blocking/network
work; that work retains its single shared stats permit until its own transport bounds complete.
A vocabulary change or a resize does not hold that permit, so in coordinator mode a probe answers
while one rebuilds the cluster
([ADR-210](../../../decisions/adr-210-the-coordinator-serves-beside-a-rebuild.md)). A rebuild
does not change the colour, because searches answer exactly throughout. The cluster payload
reports it in `rebuild_in_progress`, which is `true` while a vocabulary change or a resize is
rebuilding the cluster or is about to; writes wait until it is `false` again. While it is `true`,
a difference between the committed topology and the serving shards is not treated as a failure,
because the rebuild replaces the one and then the other.

Coordinator green requires a successful committed control-state read, a count from every logical
serving position, matching committed/ring shard counts, and exactly one in-range committed
assignment per position. The probe shares bounded stats admission with `/_stats` and CAT stats and
runs off the async request workers.

This is deliberately not Elasticsearch/OpenSearch `/_cluster/health`. Those APIs describe Lucene
index-shard allocation; Reverse Rusty has no honest equivalent for their index, active-primary,
relocating, or unassigned-shard fields, so no `/_cluster/health` alias is exposed (ADR-144).

## Probe routes

`GET`/`HEAD /_health/live` and `GET`/`HEAD /_health/ready` answer for this process and nothing
else ([ADR-211](../../../decisions/adr-211-probes-answer-for-the-process.md)). They are what a
Kubernetes liveness probe and readiness probe should call.

| Route | 200 means | Body |
|---|---|---|
| `/_health/live` | the process is up and its runtime is answering | `{"status":"alive"}` |
| `/_health/ready` | the process has assembled its engine or cluster and is accepting connections | `{"status":"ready"}` |

They take no admission, read nothing from the engine, and call no shard and no control plane, so
they answer while `/_health` is red or waiting. They accept no parameters and no body, answer
`HEAD` without a body, send `Cache-Control: no-store`, refuse other methods with 405 and
`Allow: GET, HEAD`, and need no credentials even under `--auth-protect-reads`.

`/_health` remains the status to watch and alert on. Do not point a liveness probe at it: it is red
when a shard is down, and restarting the coordinator does not bring a shard back.
