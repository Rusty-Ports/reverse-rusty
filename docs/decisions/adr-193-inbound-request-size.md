# ADR-193 — Inbound request size on the shard mesh

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

gRPC refuses an inbound message above the receiver's limit. tonic's default is 4 MiB, and no
shard node raised it. ADR-110 dealt with the same ceiling for *replies*, by capping what a shard
sends back. Nothing bounded what a coordinator *sends*, and two requests grow with the corpus:

- **Bulk ingest.** The coordinator buckets a batch by shard and sent each bucket as one
  `IngestExtracted` request. Past a few tens of thousands of queries a bucket no longer fit.
- **`AdoptDict`.** The whole dictionary is shipped in one request at every connect, including
  each coordinator restart. A dictionary of a few hundred thousand features no longer fit; the
  20M-query synthetic run has 683,877.

The failure was loud (`OutOfRange` from the shard), never a wrong result. But a remote cluster
could not take the documented `--load-file` bootstrap beyond a small corpus, and past the
dictionary threshold a coordinator could not connect or restart at all. No test reached the
limit: the gRPC oracle loads 4,000 queries, and the scale soak builds in-process.

## Decision

1. **A bulk bucket is one staged load of bounded messages.** `RemoteShard::ingest_extracted`
   sends a bucket on the `StageIngest` stream that a remote resize already uses to fill a
   target (ADR-180), split into messages of at most `INGEST_REQUEST_BUDGET_BYTES` (3 MiB of
   encoded size). The node seals segments of its own flush threshold as the messages arrive,
   and when the stream closes it compacts to its segment policy, writes its source store once
   and then its checkpoint sidecar. The budget is below tonic's default, so no node setting
   bounds a load. A replica receives the bucket through the same client. A single query that
   exceeds the budget on its own fails before anything is sent, naming the query. The load's
   deadline is one write timeout per message plus one for the node to finish. A node that
   restarted since the coordinator connected refuses the stream for a lease it no longer
   holds; the load reclaims the lease and sends the bucket once more, as a unary write does.
   The node checks the lease before any handler runs, so the refused call applied nothing.
2. **A shard node's inbound limit is a named setting.** `shardserver
   --max-grpc-request-bytes` (default 64 MiB) is applied as the service's decode limit. It
   bounds the dictionary a coordinator can ship, and what one caller can make the node buffer.
   The mesh token is verified on headers, before a body is read, so with a token configured
   only authenticated callers reach the larger limit.
3. **A refused dictionary names the setting.** When a node rejects `AdoptDict` for its size,
   the coordinator reports a configuration error with the request size, the node's limit and
   the flag, instead of the transport status.
4. **Replies are unchanged.** The ADR-110 result cap still holds every reply at or under
   4 MiB, which is what the coordinator's client decodes.

## Alternatives considered

- **Only raise the limit.** It moves the cliff. A bucket's size has no natural bound; a
  bounded request does.
- **Separate bounded `IngestExtracted` requests,** which was the first cut of this change and
  needs nothing from the node. Each request is its own bulk ingest: it seals one segment and
  rewrites the node's whole source store, so a bucket of N requests writes the store N times
  (quadratic in the bucket), and nothing compacts the N segments, because a node that only
  bulk-loads never flushes a memtable. The staged load already solved both for resize.
- **A chunked or streamed `AdoptDict`,** so no single message bounds the dictionary. The right
  long-term shape; the configurable limit covers dictionaries two orders of magnitude larger
  than the default did without a wire change.
- **Skip the dictionary bytes when the node already holds that dictionary.** It would spare
  the re-ship at every restart, but the ownership claim and placement checks ride on the same
  request today.
- **Have the coordinator check the size before sending.** It does not know the node's limit
  without asking. The node rejects on the frame header, before buffering the body, so the
  check is cheap where it is.

## Consequences

- The remote `--load-file` bootstrap works for buckets of any size, and leaves each shard with
  the segments its own policy allows and one write of its source store.
- A bulk load that fails part-way leaves the rows it had sealed in the slot's memory, outside
  the slot's checkpoint sidecar and source store. The coordinator already treats a failed bulk
  load as not converged; wipe the shard nodes and load again.
- The coordinator's `ingest` transport metric is one call per bucket, as it was. The node times
  a staged load as the slot's `ingest` latency, so that histogram now also covers a resize
  target's load.
- The unary `IngestExtracted` RPC stays for a coordinator that predates this change.
- Dictionaries up to the configured limit can be adopted. Past it the operator gets a message
  naming the flag. Shard nodes that predate this change still refuse a dictionary over 4 MiB:
  upgrade the shard nodes before a coordinator whose dictionary needs the larger limit.
- A node accepts, and may buffer, requests up to the limit from any caller the mesh admits.

## Proven

- `tests/cluster_grpc_oracle/large_payload.rs`, over real loopback gRPC: a 30,000-query bucket
  (about 9 MB encoded) loads on a node with the default limit and on one held at 4 MiB, as one
  `ingest` call, and the loaded cluster answers like an in-process build; a durable node with a
  2,000-row flush threshold and a two-segment policy ends that load with at most two segments;
  with a replica, both copies hold the whole bucket; a dictionary of about 11 MB is adopted,
  and re-adopted by a second connect to the populated node; a dictionary above a node's limit
  is refused with a configuration error that names `--max-grpc-request-bytes` and the limit.
  The bulk load and the dictionary adoption failed with `OutOfRange` before this change.
- `cluster/remote/ingest_chunks.rs`: every message stays within the budget as a whole
  request, order is kept, messages are filled before a new one starts, an empty batch is one
  empty message, an item above the budget is refused by id, and pre-resolved tag ids are
  refused.
- `cluster/server/tests/stage_ingest.rs`: a bulk load sent to a node that no longer holds the
  coordinator's lease reclaims it and loads the bucket. `cluster/remote/stage_ingest.rs`: the
  retry is taken once, only for a lost lease, and only by a client that can claim.
- `tests/cluster_grpc_oracle/transport.rs`: the node's `ingest` count still equals the
  coordinator's.
- `cluster/server/tests/stage_ingest.rs` (ADR-180) already proves the node's half: segments of
  the flush threshold, the source store written once when the stream closes, compaction to
  the policy, and a failed sidecar write failing the load.

**See also:** ADR-180 (the staged load), ADR-110 (the result cap, the outbound half), ADR-034
(dictionary shipping),
ADR-071 (mesh token), ADR-085 (transport hardening).
