# `POST /_cluster/resize` — Resize a cluster

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This strict native operation rebuilds a cluster under a fresh consistent-hash ring and atomically
replaces the serving shard set: in place for an in-process cluster (ADR-078/167), or onto fresh
nodes for a resolve-only remote cluster (ADR-180, [below](#strictness-topology-and-errors)). Every live query is re-extracted from its
stored source and re-placed under the new ring; vocabulary, the frozen feature/tag spaces, query
tags, ranking values, and Boolean semantics are preserved.

```bash
curl -X POST \
  'localhost:9200/_cluster/resize?cluster_manager_timeout=5s' \
  -H 'Content-Type: application/json' \
  -d '{"num_shards":16,"operation_id":"grow-to-16","if_placement_generation":4}'
```

The required `num_shards` is an integer from 1 through 1024. Two optional fields make the request
safe to automate (ADR-179):

- `operation_id` (1–64 ASCII letters, digits, `-`, `_`, `.`, or `:`) names the operation. Repeating
  the same request under a retained ID replays the recorded success with `"replayed": true` and
  does not rebuild; an operation that is still queued or running returns `409 resize_in_progress`;
  different parameters under a retained ID return `409 operation_id_conflict`. A failed or
  not-started operation re-executes under its ID, which keeps retry-to-heal available: if the
  failed attempt had already swapped the serving layout, the retry passes its
  `if_placement_generation` precondition at that operation's own uncommitted generation and
  finishes the commit. When the
  field is omitted, the server generates an ID and returns it, in error responses after admission
  as well as on success.
- `if_placement_generation` is a compare-and-set precondition checked under the exclusive guards
  before the rebuild starts. A mismatch returns `409 placement_generation_mismatch` and changes
  nothing. Read the current value from the last resize response or
  [`GET /_cluster/state`](../observability/cluster-state.md).

Operation records are process-local and bounded to the 64 most recent; active records, and failed
records that still hold an uncommitted swap, are never evicted. A restart forgets them; use `if_placement_generation` when a retry must stay safe across a
coordinator restart. Progress and outcomes are readable through
[`GET /_cluster/resize`](resize-operations.md).

The target shard count may grow or shrink the ring by an arbitrary amount; it need not be a factor or multiple of the current count. Repeating an already
attested current count is an acknowledged no-op in memory. A same-count retry repairs a prior
post-swap control-count failure before acknowledgement; on a durable cluster it also re-checkpoints
and repairs the on-disk shard-directory set.

A successful response is terminal:

```json
{
  "acknowledged": true,
  "shards_acknowledged": true,
  "version": 47,
  "old_num_shards": 8,
  "num_shards": 16,
  "rebuilt": 1200000,
  "placement_generation": 5,
  "operation_id": "resize-1790000000000-1"
}
```

- `acknowledged` means the blue/green rebuild, atomic serving swap, control-state shard-count
  proposal, and required durable checkpoint all completed.
- `shards_acknowledged` is the familiar ES/OpenSearch field and is always true on a 200: unlike
  their target-index operations, this synchronous endpoint does not return while target shards are
  unassigned or recovering.
- `version` is the final observed `ClusterState` application version, shared with
  [`GET /_cluster/state`](../observability/cluster-state.md). It is not a Raft term/log index,
  checkpoint epoch, feature-model version, or placement generation. Before returning it, resize
  also attests that the committed placement generation exactly matches the serving shards.
- `old_num_shards` and `num_shards` report the serving ring transition. `rebuilt` is the number of
  unique live logical queries rebuilt; it is zero for a same-count retry.
- `placement_generation` is the attested serving placement generation, suitable for the next
  request's `if_placement_generation`. `operation_id` names this operation; `replayed: true`
  appears only on a replay of a recorded success.

## Execution and timeout contract

The rebuild is `O(corpus)` and temporarily needs blue and green state. One shared administrative
slot admits it alongside stats and vocabulary work. After admission, one independently supervised
OS thread acquires exclusive topology, REST-write, and cluster guards, rebuilds the corpus, swaps
the ring/shards, commits control state, checkpoints when durable, and reads the final version. Tokio
request workers never wait on those blocking locks or perform the rebuild.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`,
default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a non-waiting admission and
exclusive-lock probe. A positive value covers admission, dedicated-worker dispatch, and lock
waiting until the rebuild atomically starts.

A deadline before start returns `408 resize_timeout` and guarantees no delayed resize can begin.
Once all exclusive guards are held and the rebuild starts, the manager timeout does not cancel it:
arbitrary cancellation could strand a swapped in-memory ring, control state, and durable manifest
at different generations. The request waits for the exact terminal result. If the client
disconnects after start, the supervised worker retains admission and completes; graceful shutdown
acquires and retains the shared corpus-administration admission slot before its final checkpoint,
so an admitted worker cannot start after cleanup. Inspect `/_health` and `/_cluster/state` after any
connection loss before retrying.

A failed control proposal can occur after the serving swap. The next request first repairs only
that exact one-generation resize predecessor; it cannot advance to a different shard count until
the prior serving/control transition is committed and attested. Any other control/live divergence
fails loud instead of being reinterpreted as a resize retry. On a durable cluster, adds and upserts
return `503 durability_unavailable` while the swapped layout is uncommitted, because the previous
manifest remains the crash-recovery point; reads and removes continue, and the same-count retry or
any successful checkpoint lifts the pause (ADR-178).

The familiar overall `timeout`, `wait_for_active_shards`, asynchronous task controls, and target
index settings are rejected because their ES/OpenSearch meanings do not match this synchronous
in-place rebuild.

## Strictness, topology, and errors

`POST` requires `application/json` or `application/*+json`, caps the body at 64 KiB, and gives body
delivery 250 ms. It requires one object with the required `num_shards` field and the optional
fields above; unknown/duplicate/null fields, non-object JSON, fractional/string counts, zero, counts
above 1024, malformed operation IDs, and negative or non-integer generations are rejected before
admission. `GET` reads [operation records](resize-operations.md); any other method returns `405`
with `Allow: GET, POST`. Every route-reached response is structured
JSON, `Cache-Control: no-store`, and observed under the fixed `cluster_resize` metric label.

An in-process cluster rebuilds in place and rejects `targets`.

A **resolve-only remote coordinator** (`--route-by-assignments`, `--control-endpoint`, no
`--shard-endpoint`) resizes onto fresh nodes and requires `targets` (ADR-180):

```bash
curl -X POST 'localhost:9200/_cluster/resize' -H 'Content-Type: application/json' -d '{
  "num_shards": 12, "operation_id": "grow-to-12",
  "targets": [{"id": 21, "endpoint": "https://shard-21:50051"},
              {"id": 22, "endpoint": "https://shard-22:50051"}]}'
```

The targets must be empty shard servers that host no slot of the current layout. Position `p` of the
new layout goes to `targets[p % len(targets)]`, and unknown target ids are registered. The operation
then runs these steps:

1. Record a durable resize intent.
2. Pause writes: adds, upserts, removes, and repair are refused with `503` while reads keep serving
   the old layout.
3. Copy the live corpus onto the targets under the new ring.
4. Prove each new position's content fingerprint and count.
5. Commit the new shard count, placement generation, and assignments in one control-plane
   transition.
6. Swap routing.
7. Fence the old slots so a stale writer fails loud.

A failure before the commit aborts and reopens writes, leaving the targets holding an unrouted layout
that must be wiped before reuse. After a coordinator crash, startup aborts an uncommitted intent or
finishes a committed one before serving. Replication factor above 1 is refused. Decommission the old
nodes once the resize succeeds.

A static or CLI-seeded remote coordinator returns `501 not_supported_in_cluster_mode` before
admission: its routing follows the CLI endpoint list, so changing the ring there would make routing
disagree with stored placement. Use the separate-cluster blue/green procedure in
[cluster deployment](../../../operations/cluster-deployment.md#5-scaling) for those topologies.
Remaining remote-resize work is tracked in the
[roadmap](../../../roadmap.md#remote-cluster-resize).

Invalid input is 400, a pre-start deadline is 408, an operation-ID conflict, in-progress duplicate,
or failed precondition is 409, an oversized body is 413, a missing/wrong media type is 415, a
registry whose every retained record is active is 429, closed/failed worker admission is 503, and
the remote-topology boundary is 501.
Underlying rebuild, control, or durability failures fail loud with a typed non-200 response and a
sanitized reason; inspect server logs, health, and cluster state before retrying.

## Elasticsearch/OpenSearch boundary

Elasticsearch and OpenSearch resize a named source index into a distinct target index through
`/{index}/_split/{target}` or `/{index}/_shrink/{target}`. Their split/shrink operations preserve the
source, constrain the target shard count to a multiple/factor, require index-state preparation, and
may acknowledge cluster-state creation before target recovery completes
([Elasticsearch shrink](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink),
[OpenSearch split](https://docs.opensearch.org/latest/api-reference/index-apis/split/),
[OpenSearch shrink](https://docs.opensearch.org/latest/api-reference/index-apis/shrink-index/)).

Reverse Rusty has one reverse-query corpus rather than named source/target indices. Its operation
mutates the serving ring in place, accepts arbitrary bounded shard counts, remains synchronous, and
does a full source rebuild instead of hard-linking Lucene segments. Aliasing it to `_split` or
`_shrink`, accepting target-index settings, or returning a fabricated index name would therefore be
false compatibility. The native path stays explicit; only manager-timeout spellings and
`shards_acknowledged` are shared where their semantics are exact.

Cross-topology assembly rules are documented in
[coordinator mode](../server/coordinator-mode.md). The operator procedure lives in
[cluster deployment](../../../operations/cluster-deployment.md#5-scaling); cluster design and
failure invariants are canonical in
[clustering and scaling](../../../design/clustering-and-scaling.md).

---

Back to the [REST API reference](../../api.md).
