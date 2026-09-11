# ADR-176: Remote logical-ID directory reconstruction

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted (2026-09-10)

## Context

A fresh coordinator attached to populated remote slots cannot reconstruct create-only admission.
The shards retain the queries, but the coordinator's compact logical-ID directory is empty and
unauthoritative. Rejecting every create preserves safety but prevents normal ingestion after restart.
This is a bounded lifecycle prerequisite for larger remote automation; the higher-priority cost
changes remain gated on representative-corpus measurements.

The directory historically also certified initial corpus convergence for exhaustive delivery. A
union of IDs cannot prove that a former coordinator completed every cross-shard mutation. Losing
its repair journal must not turn a partial corpus into a successful exhaustive response.

## Alternatives and prior art

- Per-create shard probes repeat cluster-wide work and do not supply a durable absence check.
- Independent offset pages can miss IDs when writes move page boundaries. Both
  [etcd ranges](https://etcd.io/docs/v3.6/learning/api/#key-value-api) and
  [Elasticsearch pagination](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results)
  tie consistent enumeration to a fixed revision or point in time. A persistent cursor registry
  would add lifecycle state solely for this startup operation.
- A unary vector couples supported corpus size to the gRPC message cap. Copying query sources
  adds text allocation and source-store I/O that membership does not require.

## Decision

Add an additive `LiveLogicalIds` server-streaming RPC. Capture one shard's live exact-row IDs under
its engine mutation lock, then release the lock before transmitting. Check the size before
allocation, sort IDs, and refuse duplicate live physical rows within one position. Enumerate the
integer index directly, including memtable and immutable/mmap rows; source availability does not
determine whether an ID exists.

The request attests shard position, feature and tag fingerprints, and placement configuration.
Frames contain strictly increasing fixed-width IDs and repeat the position, placement, and snapshot
count. Exactly one empty terminal frame attests completion. Reject missing/duplicate completion,
unexpected identity, unordered or duplicate IDs, count mismatch, and data after completion. No
partial vector escapes the client on failure, including a retry or replica failover.

Bound each stream and the union directory to 100 million IDs, and each data frame to 4,096 IDs plus
the configured gRPC encoded-result cap. One node-local admission permit covers snapshot construction
and consumption.
The caller's existing mesh read timeout bounds the entire transfer and all retries; the server also
caps a request at one minute. Snapshot work runs off the async runtime, checks its deadline while
waiting for the engine lock, scanning, sorting, and validating, and never holds that lock during
network backpressure. Sorting uses in-place bytewise radix partitioning, polling every 256 counting
or partitioning steps; standard sorting is limited to leaves of at most 4,096 IDs. This avoids a
second corpus-sized sort buffer and bounds cancellation work even for hostile ID orderings.
The coordinator temporarily holds the union plus one position's ID vector; each serving node holds
at most one bounded snapshot. Compact the deduplicated allocation before installation so the
resident directory does not retain capacity for physical placement copies. Compaction allocation
failure leaves admission unavailable. This is maintenance work, outside the allocation-free
matching path.

Delegate through `HandoffShard` to the pinned current backing and through `ReplicatedShard` using
its existing primary/in-sync-read-failover rule. Remote constructors collect each position's IDs,
sort and deduplicate physical placement copies, then atomically install admission membership before
returning the coordinator. The existing single-writer deployment contract remains necessary;
exclusive coordinator builders additionally enforce it through the existing mesh lease.

Keep membership authority separate from convergence. A populated remote attach installs membership
without certifying initial convergence, so create-only writes work while exhaustive reads retain
their existing refusal. Proven-empty assemblies and successful fresh builds/bulk loads retain their
existing convergence proof. A failed or unsupported enumeration leaves create-only admission
unavailable and records the reason; matching and explicit upserts keep their existing behavior.

## Compatibility and consequences

There is no segment, manifest, mutation-log, or control-plane format change, and no new dependency.
Old servers return `UNIMPLEMENTED` for the new RPC; that cannot be mistaken for an empty shard.
The rollout can therefore preserve the existing fail-closed attach behavior until every required
position supports enumeration. No query visibility, feature normalization, routing, ranking, or
per-row verification rule changes.

The RPC proves membership at attach time, not an ongoing subscription or historical convergence.
Remote exhaustive recovery after loss of coordinator repair state still needs separate evidence.
Larger-than-limit corpora retain explicit upserts until a separately justified enumeration strategy
is implemented.

## Validation

Unit and real gRPC coverage includes multi-frame and empty transfers; integer ID boundaries;
deletion, upsert, mmap/translog reopen, and co-location; primary/in-sync replica failover; missing and
malformed completion; frame/count/deadline/admission limits; and atomic fallback when any required
enumeration fails. Reattach tests reject existing-ID creates, accept fresh IDs, preserve matches,
and keep unattested exhaustive reads refused. Sorting is compared against the standard sort across
random, ordered, duplicate-heavy, and boundary ID sets, with deterministic mid-operation cancellation
checks for sorting and validation. The standard default/distributed and crash gates remain required.
