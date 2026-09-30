# `POST /_checkpoint` — Cluster durability commit

> [Ingest & lifecycle APIs](../ingest.md) · [REST API hub](../../api.md)

`/_checkpoint` is the strict native commit point for a durable in-process cluster (ADR-161):

```bash
curl -X POST localhost:9200/_checkpoint
```

```json
{
  "took": 4,
  "took_ms": 4.37,
  "acknowledged": true,
  "durable": true,
  "epoch": 7,
  "shards_checkpointed": 3
}
```

The commit seals or reseals every logical shard position, atomically publishes the coordinator
manifest and its source/segment registry, advances `epoch`, and only then allows the committed
mutation-log prefix and orphaned segment files to be reclaimed. A shard persistence or manifest
failure returns a typed error (a durability failure is 503 `durability_unavailable`) without
advancing or acknowledging a new epoch.

An in-memory cluster has no coordinator `data_dir`. Its acknowledged maintenance boundary only
compacts the derived logical-ID directory; the absence of a durability commit is explicit:

```json
{
  "took": 0,
  "took_ms": 0.08,
  "acknowledged": true,
  "durable": false,
  "epoch": 0,
  "shards_checkpointed": 0,
  "message": "no durable checkpoint was created because the coordinator has no data directory"
}
```

On a stateless remote coordinator, the request seals each current **primary** on its data node through
the `Seal` RPC. It persists that slot's checkpoint and trims only the translog prefix permitted by
retention leases. `shards_checkpointed` reports the number of primary positions sealed; `durable`
remains `false`, `epoch` remains zero, and `message` explains that the remote primary checkpoints
were committed without creating a cluster recovery point. Replicas are not part of that sweep.
A required node's failure or a peer without `Seal` support fails the request; partial completion is
not acknowledged as success. Upgrade data nodes before the coordinator.

`acknowledged: true` means the requested operation completed. Inspect `durable` before treating it
as a cluster recovery point. Independent remote seals do not create a cross-shard snapshot barrier;
quiesce writes and snapshot every shard and control-plane volume as one set for a consistent backup.

The request accepts only `POST`, no query parameters, and an empty body. Route bodies are capped at
64 KiB and must complete within 250 ms; other methods return 405 with `Allow: POST`. Responses are
`Cache-Control: no-store`. Checkpoint and backup share one durability-work slot and the cluster
writer boundary, so either can wait for the other while existing immutable read snapshots continue
serving. The disk work and lock wait run off the async runtime. Once admitted, a client disconnect
does not cancel the checkpoint; completion is still logged and counted.

This endpoint is not an Elasticsearch/OpenSearch flush alias. Their familiar flush contract is
already implemented by `GET`/`POST /_flush`; it seals memtables but does not commit the coordinator
manifest or advance the cluster mutation-log checkpoint.
