# ADR-225 — What a rebuild costs, and who frees the layout it replaced

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A resize and a vocabulary change rebuild the cluster beside the layout that is serving, and
then publish the new one (ADR-208, ADR-209, ADR-210). Searches go on; writes wait. Nothing
said how long a rebuild takes, where the time goes, how much memory it needs, or what it does
to a search or a write that runs beside it. The roadmap carried that as an open measurement,
and the answer decides whether writes have to be carried into a rebuild.

Taking the measurement found a defect. Beside a rebuild of two million queries one search
took 200 to 290 ms, right after the new layout was published, where the median is 55 µs.
A layout is held through shared handles and freed by whoever drops the last one. The rebuild
dropped its own within microseconds of publishing, so a search that had loaded the old layout
a moment before was often the last holder, and that one search freed every shard of the old
corpus.

## Decision

1. **A rebuild keeps the time of each of its parts.** `ClusterEngine::last_rebuild` returns a
   `RebuildTimings`: the queries rebuilt, and the time spent gathering the live corpus,
   extracting, placing, building the new shards, publishing, and committing (the control
   state, readers of the old layout finishing, the checkpoint).
2. **`clusterbench rebuild` measures a rebuild on a serving coordinator.** It builds a durable
   in-process cluster and rebuilds it twice, to another shard count and under another
   vocabulary, with one thread searching and one thread writing beside each. It reports the
   parts above, search latency against the same searches with nothing else running, the
   longest search and when it happened, how long a write waited, and peak resident memory.
3. **A replaced layout is freed on a thread of its own.** When a layout is published, the one
   it replaces is handed to a thread that keeps a handle until nothing else holds it, and
   then lets go. A reader's handle is never the last, so a reader never frees a corpus. An
   operation that holds the old layout longer than the grace period becomes its last holder
   as before; it is long-running already.

## What was measured

One machine (Apple M4 Max, 16 cores), release build, durable in-process cluster, 4 to 8
shards. Every number depends on the machine; the shape does not.

| Queries | Rebuild | gather | extract | place | build | publish | commit | Longest write wait | Peak resident |
|---|---|---|---|---|---|---|---|---|---|
| 100k | 0.9 s | 0.03 | 0.16 | 0.05 | 0.41 | <0.001 | 0.24 | 0.9 s | 1.7x |
| 500k | 2.7 s | 0.19 | 0.88 | 0.24 | 0.90 | 0.001 | 0.34 | 2.6 s | 2.4x |
| 1M | 5.1 s | 0.51 | 1.79 | 0.68 | 1.54 | 0.001 | 0.46 | 5.0 s | 2.1x |
| 2M | 11.7 s | 1.79 | 4.02 | 1.40 | 3.29 | 0.008 | 0.91 | 11.4 s | see below |

(A resize; a vocabulary change is within a tenth of it, with more of the time in extract.)

- **Time is linear in the corpus,** about five seconds for a million queries here. Extracting
  every query again is the largest part, building the shards the second.
- **Searches are not slowed.** The median and the 99th percentile beside a rebuild are those
  of the same searches with nothing else running (1M: 50 µs and 153 µs against 49 µs and
  140 µs).
- **Writes wait for the whole rebuild.** The longest write waited as long as the rebuild
  took, at every size.
- **Memory:** two corpora are resident while the new one is built. Peak resident memory was
  1.7 to 2.4 times what the process held before the rebuild. At 2M the process still held
  memory from building the corpus, and the ratio read 1.0 to 1.2; plan for 2.5 times.
- **The longest search beside a 2M rebuild** was 205 to 293 ms before the third point of the
  decision and 18 to 84 ms after it, and it no longer falls at the publication.

## What changes for a deployment

- Nothing in the API. A library caller can read `last_rebuild` after a resize or a
  vocabulary change.
- A search no longer stalls when a rebuild publishes. Each rebuild starts one short-lived
  thread, `rr-layout-release`.
- Operators have numbers to plan a resize or a vocabulary change by:
  [cluster deployment, scaling](../operations/cluster-deployment.md).

## Alternatives considered

- **Free the old layout on the rebuild's thread,** by waiting for the readers there. The
  rebuild holds the layout lock, so every write would wait for the teardown as well.
- **Keep replaced layouts in a list and free them at the next checkpoint.** An in-memory
  cluster has no checkpoint, and a whole corpus would stay resident until the next one.
- **An event with the timings.** `EngineEvent` is a public enum and a new variant breaks
  every match on it. A record that is read when wanted costs nothing to those who do not.

## Prior art

RCU: "The basic idea behind RCU is to split updates into 'removal' and 'reclamation'
phases", and "the reclamation phase must not start until readers no longer hold references
to those data items". The updater reclaims, by blocking until the readers finish or by a
callback run after them, and "it is often helpful for an entirely different thread to do the
reclamation" (kernel documentation, *What is RCU?*, read 2026-10-08). A reader never
reclaims. The layout's handles already gave the first half; the release thread is the
second.

## Proven

- `layout_change::a_reader_is_not_the_one_that_frees_a_replaced_layout`: after a resize, a
  handle taken as a search takes one is not the only one left on the replaced layout, and
  the layout is freed after that handle is dropped. Without the release thread the test
  fails.
- `layout_change::a_rebuild_keeps_the_time_of_each_of_its_parts`: a resize and a vocabulary
  change are timed, the commit of a durable cluster is counted, the record is of the last
  rebuild, and a resize that rebuilds nothing leaves it.
- The capture above, in `docs/performance/benchmark-results.txt`.

**See also:** ADR-208 (the published layout), ADR-209 and ADR-210 (what runs beside a
rebuild).
