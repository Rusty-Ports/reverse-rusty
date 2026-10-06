# Resource sizing

How to turn a representative corpus capture into a deployment shape. Exact, dated measurements live
only in [`../performance/results.md`](../performance/results.md) and
[`../performance/benchmark-results.txt`](../performance/benchmark-results.txt); this page owns the
method.

> **Do not size from the old 256 B/query process-RSS capture.** Current persisted-profile captures
> separate engine-accounted resident memory from durable bytes and show that source retention changes
> the result by an order of magnitude. Measure the profile you will actually deploy.

## 1. Pick the source-retention profile first

`--retain-source` defaults to `true`. Select the low-memory profile with `--retain-source false`
and a `--data-dir`: on `server` for single-node and in-process clusters, and on every
`shardserver` for a remote cluster (Helm `shard.retainSource=false`, Compose
`RR_SHARD_RETAIN_SOURCE=false`). The coordinator's flag does not reach remote shards.

| Profile | What the engine keeps | Sizing consequence |
|---|---|---|
| `retain_source=true` (the default, and what a node runs unless told otherwise) | canonical query text resident with compiled state | easiest source/explain/admin reads; current captures are roughly a little over 100 B/query of engine-accounted resident memory, which counts string capacities and not the allocator's size-class and hash-table slack around them |
| `retain_source=false` + durable `--data-dir` | compiled state resident; canonical source in the durable source store and read lazily | current captures report roughly 5–6 B/query of engine-accounted resident memory, but source pages, filesystem cache, and source-read I/O still consume host resources |

Those rounded ranges describe the current pinned workloads, not constants. Vocabulary size, tag
count, predicate shape, duplicate bodies, lane mix, allocator behavior, and source length all move
them. The durable 1M capture is also much larger per query than the no-source resident number because
it includes source and file-format data. Use the exact current figures in the performance pages only
as a bootstrap estimate.

## 2. Capture the quantities your topology needs

Run the persisted benchmark/profile with:

- the real `retain_source` setting;
- representative DSL/source lengths and tags/rank fields;
- representative class A/B/C/D/H distribution;
- the intended shard count and replication factor;
- a steady snapshot after flush/checkpoint, plus a compaction/rebuild peak.

Record:

```
B_engine   = reverse_rusty_memory_bytes / live logical queries
B_rss      = process RSS / live logical queries
B_durable  = committed data-dir bytes / live logical queries
P99_shard  = shard service-time p99 under the expected title mix
fanout     = routed logical positions per title
```

`B_engine` is useful for component attribution. `B_rss` includes allocator overhead and resident mmap
pages. The published throughput figures come from engines built in memory; a durable node reaches
them only with its segment working set in the page cache, so budget page cache up to the committed
segment bytes on top of `B_engine`. Neither predicts filesystem cache perfectly; observe the node over a realistic source/explain
read workload. `B_durable` must come from file sizes, not from the memory gauge.

## 3. Separate selective and replicated query cost

Selective A/B/H rows divide across ring positions. Class C, accepted D, top-64 class-B pairs, and
required-phrase proxy rows are replicated to every logical position. Therefore the simple
`total_queries / K` formula is incomplete.

For one logical position, start with:

```
Q_position ≈ Q_selective / K + Q_replicated
M_position ≈ measured_bytes_for_that_mix(Q_position)
```

At RF>1, multiply physical copies by the position's replica count. Adding logical positions can
reduce selective rows per position while increasing total replicated storage across the deployment.
Use `class_counts`, per-position query counts, and actual data-dir/RSS measurements after a trial
build; class B contains both selective and replicated shapes, so its aggregate count alone cannot
model the split exactly.

## 4. Convert the capture into nodes and positions

Choose a target steady-state memory utilization `U` that leaves room for the OS and transients. A
conservative first pass is `U = 0.5`:

```
usable_memory_per_node = node_memory × U
positions_per_node     = floor(usable_memory_per_node / M_position)
nodes_min              = ceil(K × replication_factor / positions_per_node)
```

Then validate failure-domain constraints: replicas of one position must not share a node, and loss of
one node must leave enough CPU/memory for failover traffic.

Logical positions and pods are not inherently 1:1 (ADR-093). A `ShardServer` can host several
`shard_id` slots, so choose:

- `K` for selective working-set size, placement granularity, and movement cost;
- physical node count for memory, CPU, replicas, and failure domains.

The shipped Helm chart is RF=1 and models a simple topology. More complex RF placement is currently
operator-managed; see [`deployment-modes.md`](deployment-modes.md).

## 5. Reserve transient memory and disk

Steady state is not the peak:

- compaction copies every segment in its merge range onto the heap (also the mmap-backed ones),
  builds the merged segment in memory beside them, and reads the file it wrote back to checksum
  it: plan for about three times the merge range, in memory, while the old segments still serve.
  A full merge makes that range the whole engine: `POST /_compact`, or
  `POST /_forcemerge?max_num_segments=1` (a bare `POST /_forcemerge` only runs the merge
  policy, which may merge nothing). Both are standalone routes; a coordinator answers 501.
  A shard node merges only by policy, and the range the policy picks can include the shard's
  largest segment;
- every flush (by default every 100,000 writes) rebuilds the complete source store file in heap
  buffers before writing it, whatever the size of the flush;
- opening a node with `retain_source=true` reads the whole source store into memory and then
  builds the resident map from it, so it briefly holds about twice the sources;
- in-process vocabulary rebuild and resize build replacement state before swapping;
- peer recovery temporarily holds source and target copies;
- open PITs retain snapshots and may keep unlinked mmap segments alive;
- ingest bursts grow the memtable before its flush threshold.

Two-times steady-state memory/disk is a useful **starting reserve**, not a guarantee, and a forced
merge of one large engine or shard exceeds it. Large source stores can make disk the dominant
dimension, and the largest compaction/rebuild unit determines the temporary copy; sharding divides
that unit. Measure the peak of a full merge (`POST /_forcemerge?max_num_segments=1` on a
standalone engine holding one shard's share of the data), a vocabulary rebuild, a resize, and a
peer recovery against the representative data before tightening headroom.

For disk, budget separately:

```
committed segments + source store
+ WAL/coordinator-log/translog tail
+ largest expected compaction/rebuild/recovery transient
+ backup staging if backups share the volume
```

Do not assume mmap segment bytes equal resident bytes: untouched mappings consume address space and
disk without being resident, while hot pages enter RSS/page cache.

## 6. CPU, routing, and broad work

The selective benchmark has substantial throughput headroom in the pinned captures, but CPU can still
become the constraint when:

- C/D traffic enables the broad lane frequently;
- θ moves many fat postings into the always-visible H batch lane;
- title routing reaches many positions;
- ranking/source/explain enrichment dominates;
- ingest, compaction, recovery, or reconciliation overlaps search.

Track `reverse_rusty_shard_rpc_duration_seconds`, broad/hot candidate counters, routed `_shards.total`,
transport queue/error metrics, and node CPU. Batch broad-heavy traffic through `/_mpercolate` or
`/v2/_mpercolate` so the columnar evaluator can amortize postings.

Content routing usually reaches only a few positions in the captured product-title workload, but
fan-out is input-dependent and can grow with the number of non-top-64 features. Measure it; do not use
“2–5 regardless of K” as a capacity invariant.

## 7. Other components

- **Coordinator:** remote deployments do not persist shard corpus files in the coordinator, but the
  process still holds dictionaries, the logical-ID/admission directory when authoritative, control
  and repair state, request buffers, and enrichment data. Measure it under bulk ingest and concurrent
  search; do not call it zero-memory or universally stateless.
- **Control plane:** cluster-state traffic is low-rate, but each manager needs durable storage and
  enough CPU/IO to avoid election churn. Size for reliability, not corpus bytes.
- **Network:** title fan-out is small payload/high request rate; peer recovery and source fetch are
  bulk paths. Ensure recovery cannot starve interactive RPCs.
- **Kubernetes:** set requests from observed steady state and limits above observed peak. The chart
  intentionally ships without environment-specific resource defaults.

## 8. Re-size when the profile changes

Repeat the capture after normalizer/vocabulary changes, a materially different tag/source mix, hot or
broad reclassification, a shard-count/RF change, or persistent body-dedup work. The roadmap's
[`memory-headroom`](../roadmap.md#memory-headroom-for-100m-query-deployments) item may change the
profile; when it does, update the canonical performance capture first and recompute topology from the
formula here.
