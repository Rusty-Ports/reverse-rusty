# ADR-224 — A write is under way on every shard of a step at once

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The coordinator applies a mutation to each shard it touches. An upsert installs the new
version on its placement shards and then removes copies on the others (ADR-185); a query on
the broad lane is placed on every shard (ADR-080); a delete goes to every shard. Each of
those steps was a loop that called one shard, waited for its answer, and called the next.

For shards in the coordinator's process that is the right loop: a call is microseconds. For
shards on other machines a write cost one round trip for every shard it touched. A delete
against sixteen shards was sixteen round trips, one after the other, with the request's
locks and its place in write admission held the whole time, and a moving upsert held its
move fence, which readers wait behind, for as long.

Measured on one machine over loopback gRPC, where a round trip is about 55 µs:

| Shards | Operation | Before | After |
|---|---|---|---|
| 3 | delete | 172 µs | 80 µs |
| 8 | delete | 434 µs | 120 µs |
| 16 | delete | 874 µs | 181 µs |
| 8 | upsert that moves the query | 443 µs | 214 µs |
| 16 | upsert that moves the query | 870 µs | 275 µs |
| 8 | upsert that keeps its placement | 89 to 122 µs | 82 µs |

On loopback most of what is left is CPU. Between machines the round trip is ten times that
or more, and the "before" column grows with it while the "after" column does not grow with
the number of shards.

## Decision

1. **A shard can start a write and return what finishes it.** `Shard::start_write` takes one
   of the three writes a mutation is made of (a replace, a delete, an insert) and returns a
   `Started`, whose `wait` gives the shard's answer. The default applies the write before it
   returns, so a shard in the coordinator's process behaves as it did.
2. **A remote shard sends the request and returns.** The request runs as a task on the RPC
   runtime (ADR-183); `wait` blocks for that task and then does everything the blocking call
   does with the outcome (the lease reclaim, the metrics, the error it maps to). The request
   and the reading of the reply are built by the same functions the blocking calls use. The
   per-call deadline runs from the send, and the latency recorded is the call's own, measured
   in the task, whatever the caller waited for first.
3. **The coordinator starts a step on all its shards, then reads the answers.**
   `fanout::on_each` replaces each of the loops. Nothing about a mutation's order changes:
   - a step is answered on every shard before the next step is started (install on every
     placement shard, and only then remove copies elsewhere);
   - answers are read in the order the shards were named, so the error reported for a step
     is still that of the first shard that failed in that order, and the lists of shards that
     applied and that failed are what they were;
   - every shard of a step is still asked, whatever another one answered.
4. **Writes are started in ascending shard position.** A replicated shard takes its lock
   when it starts a write and keeps it until the answer has been waited for, and only then
   writes its replicas, so a replica still sees writes in the order its primary did. One
   mutation therefore holds the locks of several shards at once, and every mutation taking
   them in the same order is what keeps two from waiting for each other.
5. **A shard whose backing can be exchanged names the backing.** The handoff wrapper cannot
   return something that borrows a backing it has only loaded, so it answers
   `Shard::write_target` with the backing in place. The coordinator keeps it until the answer
   has been waited for, as a blocking call kept the backing it loaded for as long as it ran.
   A backing exchanged in between does not receive the write or its answer.

## What changes for a deployment

- A coordinator of remote shards answers a write in about one round trip for each step of
  it, not one for each shard. Nothing changes for an in-process cluster.
- A write that fails on some shards reports the same shards and the same first error as
  before.
- A coordinator sends a shard node up to as many requests at once as it has writes in
  flight, as before; they now arrive closer together.

## Alternatives considered

- **A thread for each call** (`std::thread::scope`), as data-moving reassignment does
  (ADR-095). A reassignment is seconds long and a thread is nothing beside it. A write over
  loopback is 55 µs a call, and starting a thread costs about as much.
- **The search pool, or a pool of its own.** A blocking call parked on the search pool
  starves the percolate fan-out, and a pool shared with requests that can wait for each
  other is how ADR-199 and ADR-207 found their deadlocks. A pool of its own caps the calls
  in flight below what the write path has today unless it is sized for the worst case.
- **Make `Shard` asynchronous.** Every shard, lock and caller would change, and the
  administrative paths rely on bounded blocking waits (ADR-183).
- **Parallel replicas inside a replicated shard.** Left as it is: the supported topology has
  one copy of each shard, and the order of primary and replicas is what the shard's lock is
  for.

## Consequences

- A shard that does not answer costs a step one write timeout, as it cost one before; the
  other shards of the step are no longer waited for behind it.
- A replicated shard's lock is held for the longest call of a step, not for its own call
  only. For a remote shard both are one round trip.
- `Started` is not `Send`. The thread that starts a write is the one that waits for it.

## Proven

- `cluster/coordinator/tests/write_concurrency/fanned.rs`, with shards that answer a started
  write only when they are waited for: a delete is sent to every shard before an answer is
  read; an upsert that moves a query answers its install everywhere before the removal is
  sent; with two shards failing, the error is that of the lower one and every shard was
  asked; a shard that refuses the send fails alone; writes named out of order are started in
  shard order and answered in the order named; a shard in this process applies the write
  when it is asked to start it; a replicated shard holds its lock from the send until the
  answer, a second write waits for it, and the replica is written after its primary
  answered; a replica is sent what its primary did, so a conditional replace the primary
  declined is not sent to it, started or called; a write started before its shard's backing
  is exchanged is answered by the backing it was sent to.
- The gRPC suites (`cluster_grpc_oracle`, 138 tests) run every write through the started
  form, including the partial-apply, upsert-visibility and block-on-in-runtime guards.

## Prior art

Elasticsearch's `ReplicationOperation` sends a write to its replication targets in a loop
that does not wait:

```java
for (final ShardRouting shard : replicationGroup.getReplicationTargets()) {
    if (shard.isSameAllocation(primaryRouting) == false) {
        performOnReplica(shard, replicaRequest, globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes, pendingReplicationActions,
            pendingActionsListener);
    }
}
```

Each `performOnReplica` dispatches a request with a listener, and the operation completes
when the reference-counted listener has heard from all of them (source read 2026-10-08).
The request is issued on the I/O layer and the caller waits for the answers; no thread is
taken for a request. That is the shape taken here, with a blocking wait where Elasticsearch
has a callback, because this coordinator's write path is synchronous.

**See also:** ADR-080 (the broad lane is on every shard), ADR-183 (the RPC runtime, and
writes off the async workers), ADR-185 (install, then remove elsewhere), ADR-194 (a write
that failed on some shards), ADR-206 (writes share admission).
