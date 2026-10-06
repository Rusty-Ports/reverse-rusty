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

1. **Bulk ingest is sent as bounded requests.** `RemoteShard::ingest_extracted` splits a
   bucket into consecutive requests of at most `INGEST_REQUEST_BUDGET_BYTES` (3 MiB of encoded
   size), sends them in order and sums their reports. The budget is below tonic's default, so
   a shard node of any version or configuration accepts them; there is no wire change and no
   upgrade order. A replica receives the bucket through the same client, so it gets the same
   requests. A single query that exceeds the budget on its own fails before anything is sent,
   naming the query.
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
- **A client-streaming ingest RPC** that builds one segment per shard. Cleaner on the shard
  (chunked requests leave one base segment per request until compaction merges them), but it
  is a wire change that needs capability negotiation and a fallback for old nodes.
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

- The remote `--load-file` bootstrap works for buckets of any size. A bulk load is no longer
  one request per shard: if a later request fails, earlier ones on that shard have been
  applied. The coordinator already treats a failed bulk load as not converged.
- A shard that bulk-loads a large bucket starts with one base segment per request (about one
  per 3 MiB of DSL) until compaction merges them.
- Dictionaries up to the configured limit can be adopted. Past it the operator gets a message
  naming the flag. Shard nodes that predate this change still refuse a dictionary over 4 MiB:
  upgrade the shard nodes before a coordinator whose dictionary needs the larger limit.
- A node accepts, and may buffer, requests up to the limit from any caller the mesh admits.

## Proven

- `tests/cluster_grpc_oracle/large_payload.rs`, over real loopback gRPC: a 30,000-query bucket
  (about 9 MB encoded) loads on a node with the default limit and on one held at 4 MiB, and
  the loaded cluster answers like an in-process build; with a replica, both copies hold the
  whole bucket; a dictionary of about 11 MB is adopted, and re-adopted by a second connect to
  the populated node; a dictionary above a node's limit is refused with a configuration error
  that names `--max-grpc-request-bytes` and the limit. The first and third failed with
  `OutOfRange` before this change.
- `cluster/remote/ingest_chunks.rs`: every request stays within the budget, order is kept,
  requests are filled before a new one starts, an empty bucket yields none, and an item above
  the budget is refused by id.

**See also:** ADR-110 (the result cap, the outbound half), ADR-034 (dictionary shipping),
ADR-071 (mesh token), ADR-085 (transport hardening).
