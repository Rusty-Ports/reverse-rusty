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
   `RebuildTimings` for the last resize or vocabulary change: the queries rebuilt, and the
   time spent gathering the live corpus, extracting, placing, building the new shards,
   publishing, and committing (the control state, readers of the old layout finishing, the
   checkpoint). The rebuild a start runs for an older compiler is not timed.
2. **`clusterbench rebuild` measures a rebuild on a serving coordinator.** It builds a durable
   in-process cluster and rebuilds it twice, to another shard count and under another
   vocabulary, with one thread searching and one thread writing beside each. It reports the
   parts above, search latency against the same searches with nothing else running, the
   longest search and when it happened, how long a write waited, and peak resident memory.
   Only what begins after the rebuild has begun is counted.
3. **A replaced layout is freed on a thread of its own.** When a layout is published, the one
   it replaces is handed to a thread that keeps a handle until nothing else holds it, and
   then lets go. A reader's handle is never the last, so a reader never frees a corpus. The
   thread waits thirty seconds, which is longer than any search should run; an operation that
   holds the old layout longer becomes its last holder as before.
4. **A thread that cannot be started is reported.** The layout is then freed by its last
   holder, as it was before there was a thread, and the coordinator raises a durability event
   with a new operation, `thread_start`, which is not data at risk. The move of several
   shards at once (ADR-095) already ran a move on the calling thread when it could not start
   one for it, and reported that as `replica_desync`, which names something else; it reports
   `thread_start` now.

## What was measured

One machine (Apple M4 Max, 16 cores), release build, durable in-process cluster, 4 to 8
shards. Every number depends on the machine; the shape does not.

| Queries | Rebuild | gather | extract | place | build | publish | commit | Longest write wait | Peak resident |
|---|---|---|---|---|---|---|---|---|---|
| 100k | 1.0 s | 0.03 | 0.16 | 0.05 | 0.42 | <0.001 | 0.24 | 0.9 s | 254 MB (152 when it began) |
| 500k | 2.8 s | 0.21 | 0.83 | 0.25 | 0.90 | 0.001 | 0.36 | 2.6 s | 1,039 MB (375) |
| 1M | 5.0 s | 0.50 | 1.68 | 0.56 | 1.61 | 0.002 | 0.45 | 4.8 s | 1,762 MB (922) |
| 2M | 10.7 s | 1.50 | 3.47 | 1.48 | 3.36 | 0.012 | 0.89 | 10.7 s | 2,365 MB (1,505) |

(A resize. A vocabulary change is within a tenth of it, with more of the time in extract:
2.20 s of 5.2 s at 1M. The rebuild's wall time includes waiting for the layout lock, which
the parts do not.)

- **Time is linear in the corpus,** about five seconds for a million queries here. Extracting
  every query again is the largest part, building the shards the second.
- **Searches are not slowed.** The median and the 99th percentile beside a rebuild are those
  of the same searches with nothing else running (1M: 50 µs and 152 µs against 49 µs and
  138 µs; 2M: 57 µs and 241 µs against 54 µs and 231 µs).
- **Writes wait for the whole rebuild.** The longest write waited as long as the rebuild
  took, at every size.
- **Memory:** two corpora are resident while the new one is built. Peak resident memory was
  1.3 to 2.8 times what the process held when the rebuild began. That reading depends on
  what the allocator has kept, so the ratio moves between runs. Plan for three times.
- **The longest search beside a 2M rebuild** was 205 to 293 ms before the third point of the
  decision, right after the publication, and 17 to 84 ms after it.

## What changes for a deployment

- Nothing in the API. A library caller can read `last_rebuild` after a resize or a
  vocabulary change.
- A search no longer stalls when a rebuild publishes. Each rebuild starts one short-lived
  thread, `rr-layout-release`.
- `durability_failures_total` has a new `op`, `thread_start`: a thread the engine starts to
  keep work off its callers could not be started and the work ran on the caller. Nothing is
  lost. The shipped alert on that counter fires for it, as it does for every `op`: a process
  that cannot start a thread is at a limit an operator should know of.
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

- `layout_release::a_reader_is_not_the_one_that_frees_a_replaced_layout`: after a resize, a
  handle taken as a search takes one is not the only one left on the replaced layout, and
  the layout is freed after that handle is dropped. Without the release thread the test
  fails.
- `layout_release::a_release_thread_that_cannot_be_started_is_reported`: the change still
  publishes, one `thread_start` failure is reported, and the layout is freed by its last
  holder. Fails when nothing is reported.
- `layout_release::replaced_layouts_are_forgotten_once_released`: once the release threads
  have let go, the next change remembers only the layout it replaced.
- `layout_change::a_rebuild_keeps_the_time_of_each_of_its_parts`: a resize and a vocabulary
  change are timed, the commit of a durable cluster is counted, the record is of the last
  rebuild, and a resize that rebuilds nothing leaves it.
- The capture above, in `docs/performance/benchmark-results.txt`.

**See also:** ADR-208 (the published layout), ADR-209 and ADR-210 (what runs beside a
rebuild).
