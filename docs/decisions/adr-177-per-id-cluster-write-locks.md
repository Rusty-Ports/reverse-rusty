# ADR-177 — Lifecycle-managed per-ID cluster write locks

**Status:** Accepted

**Date:** 2026-09-19

## Problem and prior art

The coordinator's 256 logical-ID stripes hold a lock through log append and the complete shard
fan-out. Unrelated IDs with the same stripe therefore wait through one another's durable I/O and
remote calls. Increasing the stripe count changes collision probability without removing the
coupling; retaining one lock for every historical ID would instead grow with corpus churn.

[Guava's striping design](https://github.com/google/guava/wiki/StripedExplained) describes that
memory/concurrency tradeoff and the requirement that equal keys always share a lock. Its weak-lock
lifetime approach motivates reclaiming idle locks, but a waiter must keep its entry registered until
it has both acquired and released the lock. Checking an Arc reference count without synchronizing
retirement can leave an idle entry behind or permit two live locks for one ID.

## Decision

Replace the fixed stripes with a private, standard-library-only table keyed by the complete `u64`
logical ID. Each registration contains a per-ID condition-variable lock and an explicit count of
holders plus waiters. One short registry mutex serializes registration and retirement, and is never
held while waiting for an ID or while appending/applying a mutation. The final registered caller
removes the entry. A `BTreeMap` releases its nodes as the active set contracts instead of retaining
a hash table sized for historical peak concurrency.

The per-ID lock uses a mutex-protected held predicate and
[`Condvar::wait_while`](https://doc.rust-lang.org/std/sync/struct.Condvar.html#method.wait_while),
so spurious wakeups cannot admit a second holder. RAII release wakes a waiter and retires its
registration on every return or unwind. As with the previous mutexes, waiter order is unspecified.

A separate read/write barrier preserves whole-directory exclusion for initial bulk load. The
global order is:

1. PIT/exhaustive mutation barrier;
2. bulk barrier (shared for an individual mutation or repair, exclusive for bulk load);
3. per-ID registration and lock;
4. directory/log/shard operations inside the existing mutation scope.

Bulk load never acquires individual ID locks. Same-ID creates, upserts, removes, and repair re-drives
retain their full log-and-apply critical sections. No caller takes multiple ID locks at once.

## Bounds and compatibility

Live registrations and wait objects are bounded by currently active mutation callers, including
waiters, rather than stored or previously seen IDs. There is no new fixed admission cap: the serving
layer retains its admission policy, and library embedders control their own concurrent calls.

This introduces allocation and short registry work on the mutation path. It removes collision waits
but does not parallelize actual shared log or shard storage locks. Matching, visibility, ownership,
WAL/segment formats, wire protocols, and public APIs are unchanged. Rollback needs no migration.

## Validation

The lock tests cover formerly colliding IDs, same-ID contention and retirement, high-cardinality
churn, bulk exclusion, and unwind cleanup. Coordinator regressions must also preserve log/apply
ordering, failure recovery, and bulk admission. Timing evidence belongs in the
[performance capture log](../performance/benchmark-results.txt); production throughput claims still
require the representative-corpus acceptance work in the [roadmap](../roadmap.md).

## Implementation outcome

The coordinator regression reproduced a pre-existing repair-selection race: draining payloads
before taking the ID lock could replay an old upsert after a newer successful write had cleared
its repair entry, making the live result differ from durable reopen. `resync` now snapshots IDs
and takes the current payload only while holding that ID's lock. The regression failed before
this correction and passes afterward; the pass remains bounded to its initial ID snapshot.
