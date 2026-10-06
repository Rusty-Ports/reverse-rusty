# ADR-194 — A cluster write that not every shard took is a failure its caller retries

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A coordinator write fans out to the shards that own the query. Against remote shards an RPC can
fail after another shard has applied the write. ADR-047 made that visible: the coordinator
returns `PartiallyApplied`, emits an event, and queues the failed shards for `resync`.

The REST layer then told the caller the write was safe. `PUT /_doc/{id}` and the cluster bulk
items answered **200** `"result": "partial"`, said the write was "durably logged", and warned
against repeating it ("a re-PUT would double-log"). All of that describes a coordinator with a
durable log. The only coordinator that can produce a partial write does not have one:

- A remote or replicated-remote coordinator is built with a null log, and the server refuses
  `--data-dir` with `--shard-endpoint`. An in-process coordinator does have a log, and its shard
  writes cannot fail part-way.
- The repair queue is a map in the coordinator's memory. Nothing in the server drains it; only a
  manual `POST /_cluster/resync` does.
- A coordinator restart therefore loses the repair. The new process reports no pending repairs
  and a green `/_health`, and the shards that refused the write never receive it. A title routed
  to one of them misses the query, permanently, for a write the client was told not to repeat.

The same answer covered a write that no shard took: a single-target write to a shard that is
down is also `PartiallyApplied`, with nothing applied, and was also a 200.

ADR-125 had already moved `DELETE` to a retryable 503 for this reason. PUT and bulk were left on
the old contract, and the documents kept describing the repair as durable.

Two details made the retry itself unsafe to ask for:

- **A create.** A partly applied `op_type=create` keeps its id reserved, so a retried create
  answered 409 "already exists" while some shard did not hold the document.
- **An insert the shard had applied.** A failed shard write is ambiguous: the shard may have
  applied it before the error came back. `resync` re-drove a queued create as a plain insert, so
  that shard ended with two rows for one id. Matching was unaffected, but the shard then refused
  to enumerate its ids, and after the next coordinator attach every create-only write in the
  cluster was refused with no message naming the id.

## Decision

1. **A partial write is a retryable failure.** `PUT /_doc/{id}` answers **503**
   `"result": "partial"` with the applied and pending positions and no `_version`. A cluster bulk
   item answers status 503, error type `partial_write`, and is not counted as accepted.
   `ShardError::write_http_class` maps `PartiallyApplied` to 503 for every other caller. The
   guidance is the same as ADR-125 gives for DELETE: repeat the idempotent write, or resync on the
   same coordinator while it stays up. Nothing says "durably logged" or "reopen".
2. **A create is retried as an index operation.** That is what the response says, because it
   converges on any coordinator: a coordinator that restarted in between finds the id on the
   shards that took it and would answer a create with 409.
3. **A create never answers 409 for an id it knows is unconverged.** When the id is reserved and
   a repair is queued for it, `create_query_with_tags` re-drives that repair first, under the same
   barrier and id lock as any write. While a shard still refuses, the create answers the same
   retryable failure. Once the repair converges the id is a true conflict (409), or, if the
   queued write was a delete, free.
4. **A repair replaces; it does not insert.** A queued create is re-driven as a replace of that
   id on each failed shard (the atomic shard-side replace from ADR-185). The row is stored when
   the shard never received it and stays one row when it had. Peer recovery and translog replay
   still apply an `Add` as an insert, since they replay a shard's own history.
5. **A stopping coordinator names what it leaves unconverged.** After its final flush it logs,
   at error level, the document ids still queued for repair.

## Alternatives considered

- **Make the repair intent durable** (a coordinator-side log, or a record on the shards that took
  the write). This is the end state ADR-047 names. It conflicts with the stateless remote
  coordinator and needs its own design; a client retry closes the correctness gap without it.
- **Roll back the shards that applied.** The shard that failed may have applied the write too,
  so the rollback is as ambiguous as the write, and it removes a document that is at least
  partly serving.
- **Keep 200 and fix the text.** An ingest pipeline acts on the status code. A 2xx is an
  acknowledgement, and this write is not acknowledged.
- **Answer the retried create 201 when its body matches the queued one.** Truthful, but 409
  after a converged retry is the answer an idempotent create already expects, and it needs no
  comparison of bodies.
- **Resync automatically, or at shutdown.** A background pass would shorten the window in which a
  restart loses repairs, and a shutdown pass would rescue a planned roll. Neither changes what
  the caller must be told, and a pass against a shard that is still down waits a write timeout
  per queued id, which a shutdown cannot afford unbounded. Left as follow-up work.

## Consequences

- **Behaviour change.** A client that treated 200 `partial` as success now sees 503 and must
  repeat the write. Clients that retry 503s need no change. A bulk response that carried a 200
  `partial` item now carries a 503 item, and `errors` is true as before.
- A create retried on the coordinator that queued its repair converges; on a coordinator that
  restarted in between it can still answer 409 for a document some shard lacks. The response
  therefore tells the caller to retry as an index operation.
- After a coordinator restart, a 503 that nobody repeated stays missing from the shards that
  refused it. The stopping coordinator's log names the ids; nothing else does.
- A create for an id whose delete is half applied no longer answers 409 while the delete is
  queued. It finishes the delete and then creates, or answers 503.
- The duplicate-row trap is closed for new repairs. A duplicate that an earlier `resync` already
  left on a shard is not repaired by this change; upserting or removing that id heals it.

## Proven

- `cluster/coordinator/tests/retry_contract.rs`, over fault-injecting shards: a retried create
  answers the retryable failure while its shard refuses and 409 only after it delivered the
  earlier attempt; a create after a half-applied delete finishes the delete first; repairing an
  insert the shard had applied (its acknowledgement lost) leaves one live row and an enumerable
  shard; `pending_repair_ids` names the unconverged ids until a retry or a resync converges them.
- `tests/cluster_grpc_oracle/partial_apply.rs`, over real gRPC: a write a fenced shard refused
  is queued on its coordinator; a coordinator that replaces it reports no pending repair and
  does not match the query, and the retried upsert stores it.
- Server unit tests: the PUT and bulk renderings are 503 `partial` without a version, with the
  index or create guidance and none of the old claims; the write status class is 503; the
  shutdown report names the ids, and cuts a long list while still counting it.
- `deploy/harness.sh` legs 3b and 4: writers repeat a 503 until it is accepted, and every
  accepted write is matchable after a shard is killed mid-write and after a live handoff.

**See also:** ADR-047 (partial-apply detection and `resync`), ADR-125 (the DELETE contract this
matches), ADR-185 (the shard-side replace a repair uses), ADR-136 (bulk item contract).
