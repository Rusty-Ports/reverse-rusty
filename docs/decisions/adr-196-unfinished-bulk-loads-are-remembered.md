# ADR-196 — A bulk load that did not finish is remembered by the shards

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A remote coordinator started with `--load-file` loads the file in bulk when the cluster is
empty, one shard's bucket after another. On every later start it skips the load, and the only
thing it checked to decide that was whether the cluster holds any query.

A load that stops part-way leaves some queries. The coordinator exits with an error, its
supervisor (Compose `restart:`, a Kubernetes Deployment) starts it again, the cluster is no
longer empty, the load is skipped with a warning that the cluster "is already populated", and
the coordinator serves whatever part of the corpus had landed. Every query that had not is a
silent false negative from then on. The first failure is loud; the loss is silent, because the
second start succeeds without anyone doing anything.

The coordinator cannot be the one to remember: a remote coordinator is stateless, and it is
the process that stopped. Nor can the content of the cluster say whether a load finished. A
cluster that has taken writes since its bootstrap legitimately differs from the file, so "does
the cluster hold the file's queries" cannot tell a partial load from a cluster that has moved
on.

## Decision

1. **The shards carry a mark for the duration of a bulk load.** Before the first bucket is
   sent, `ClusterEngine::ingest` marks every shard as holding a bulk load in progress
   (`SetBulkLoadState`). After the last bucket has landed (and, on a durable in-process
   cluster, after the checkpoint that commits it) it clears the marks. A shard node keeps the
   mark on disk, in the slot's directory, before it answers, so it survives a restart of the
   node as well as of the coordinator.
2. **A coordinator refuses a cluster that carries a mark.** Both remote builders ask every
   shard (and every replica) for its mark (`BulkLoadState`) once the cluster is assembled, and
   fail with a configuration error that names the first marked position and says the load did
   not complete. The server therefore does not start, on the first restart or on any later
   one, until the shard nodes' data is reset and the corpus is loaded again.
3. **A load that cannot be marked does not start.** If any shard cannot be marked, nothing
   has been loaded yet: the marks already set are taken back and the load fails. A node that
   predates the mark answers `UNIMPLEMENTED`, which is reported as a configuration error naming
   the node and asking for an upgrade.
4. **A node that predates the mark reads as unmarked.** It cannot hold one, so a coordinator
   can still attach to it.

In-process shards record nothing: they stop with their coordinator, whose own checkpoint
decides whether a bulk load happened.

## Alternatives considered

- **Compare the cluster with the file** (a count, or a content fingerprint per shard). It
  cannot distinguish a partial load from a cluster that was written to afterwards, so it would
  either refuse healthy restarts or accept partial loads.
- **Record completion instead of incompleteness** (a "bootstrap complete" record written last).
  Every cluster loaded before this change would lack the record and be refused at its next
  restart. A mark that exists only while a load is unfinished needs no migration.
- **Make the load itself atomic across shards** (stage every bucket, then commit all). A real
  two-phase commit with the coordinator as recovery driver, for a load that happens once per
  cluster. The mark gets the property that matters (a partial load is never served) for two
  small RPCs.
- **Finish or redo the load automatically at the next start.** It needs a way to empty a slot
  that a node will accept (dropping a slot makes the node expect a recovery, ADR-189), and the
  coordinator would have to be sure that no client write happened in between. Left as an
  operator action for now: reset the shard volumes and start again.

## Consequences

- After an interrupted bootstrap the coordinator fails at every start with a message that
  says why, until an operator resets the shard nodes' data. That replaces a cluster that
  silently served part of its corpus.
- A bootstrap needs every shard reachable before it starts, which it already did.
- A coordinator that stops between the last bucket and the last cleared mark leaves a
  complete cluster that is refused. The window is a few round trips; the remedy is the same
  reset and reload.
- Shard nodes must be upgraded before a coordinator that bulk-loads. The standard
  shards-before-coordinator order covers it. Attaching to older nodes without loading is
  unaffected.
- Two unary RPCs per shard copy around a bulk load, and one per copy at each connect.

## Proven

- `tests/cluster_grpc_oracle/bulk_load_marker.rs`, over real gRPC with durable nodes: a load
  that reaches shard 0 and is refused by shard 1 leaves a non-empty cluster that the next
  coordinator refuses, also after both nodes restart (it was handed out and served before); a
  completed load leaves no mark and the next coordinator serves the whole corpus; a load that
  cannot mark one shard does not start and takes back the marks it set on the others; a load
  with a shard down does not start and runs once the shard is back; a mark on the last shard
  alone refuses the cluster; a node that predates the mark can be attached and is not loaded
  in bulk.
- `cluster/server/tests/bulk_load.rs`: a slot is unmarked until told; the mark is per hosted
  slot; a durable node remembers it across a restart and forgets it once cleared.

**See also:** ADR-193 (how a bucket travels), ADR-189 (another thing a node remembers so that
a coordinator with a stale view fails loudly), ADR-176 (logical-id reconstruction at attach).
