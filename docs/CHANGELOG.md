# Changelog

This is the chronological record of material changes shipped in Reverse Rusty. Entries are
reverse chronological and describe outcomes, not the current architecture or future plans.

- Current design → [design documentation](design/README.md)
- Current API and DSL → [reference documentation](reference/api.md)
- Decision rationale and proof → [ADR hub](DECISIONS.md)
- Unfinished ideas and priorities → [roadmap](roadmap.md)
- Exact performance captures → [performance results](performance/results.md)

## 2026-10-06 — `--include-broad` applies on every search surface

- **Behaviour change for servers started with `--include-broad`.** `/v2/_search`,
  `/v2/_mpercolate` and exhaustive jobs now evaluate a request that names no `query_scope` in
  the server's default scope, as `/_search` and `/_mpercolate` always have
  ([ADR-201](decisions/adr-201-server-default-scope-on-every-surface.md)). They used to fall
  back to `standard`, so a consumer that relied on the server flag and moved to one of them
  silently lost every class C and accepted class D candidate. A request that names a scope gets
  it, and a server without the flag is unchanged.
- The flag's help text, the server reference, the coordinator reference and the Helm and
  Compose comments now say which surfaces it governs. The coordinator reference no longer says
  `include_broad` is a v2 request field; it is rejected there.

## 2026-10-06 — A merge no longer rewrites the stored documents

- A standalone commit that changed no stored document now selects the source sidecar it already
  has ([ADR-200](decisions/adr-200-keep-an-exact-source-sidecar.md)). A compaction used to write
  every stored document to a new file under the write lock, and a flush or bulk load that also
  merged wrote them twice. A flush and a bulk load of new ids still write them once.
- New event `SourceCommit` and metrics `source_commits_total`, `source_commit_bytes_total`,
  `source_commit_time_seconds_total`: each complete write of the stored documents, which
  `flush_time_seconds_total` never included.

## 2026-10-06 — The request limit is per endpoint, and the record now says so

- No behaviour change. The 256-request limit has always applied to each endpoint (a route and
  method) separately, while ADR-062, ADR-099, ADR-144, the threat model and the server
  reference called it server-wide. The documents are corrected
  ([ADR-199](decisions/adr-199-request-limit-per-endpoint.md)).
- The limit stays per endpoint on purpose. One pool shared by every route, and then one pool
  per class of request, were both built and both stalled: writes waiting behind a compaction,
  job-status polls waiting for their own stream, and searches and checkpoints waiting for a
  lock a job holds can each fill a shared pool and keep out the request they are waiting for.
- Both HTTP routers are now built by functions the tests call, so the limit, its isolation
  between endpoints and the position of auth are tested on the layer stack that is served.

## 2026-10-06 — Sizing and memory documentation corrected

- The sizing guide now says what compaction, a flush and a resident-source open hold on the heap
  (its inputs and output together for a compaction, and the whole source store file for every
  flush and every standalone compaction commit), which requests run a full merge, that the default
  profile is `retain_source=true`, and that the throughput captures need the segment working set
  in the page cache on a durable node. It gives no multiplier for the compaction peak: measure it.
- Two claims were wrong and are corrected where they appeared (roadmap, ADR-020 outcome, the
  capture notes and a code comment): aliveness is one byte per row, not bit-packed; and the
  dictionary does not saturate in the captures, where its cost per query is flat and its total
  grows with the corpus.
- The roadmap's memory item lists the heap copies, the byte-per-row aliveness and the ungated
  default profile as candidates. No behaviour change.

## 2026-10-06 — The WAL is replaced, not truncated

- Fix a single-node server that could refuse to start after a crash during a flush: the WAL was
  reset by truncating it to zero bytes and then writing its header, and a crash between the two
  left a log "too small" to open, with no data missing. The log is now reset, and created, by
  renaming a complete empty log into place
  ([ADR-198](decisions/adr-198-wal-is-replaced-not-truncated.md)).
- A `wal.log` that an older binary left cut inside its header now opens as an empty log, so
  such a node starts without manual repair.

## 2026-10-06 — A checkpoint excludes writes

- Fix a library-level race in the cluster coordinator: `ClusterEngine::checkpoint`, `flush`
  and `backup_to` took no lock that a write takes. A checkpoint that ran while a write was
  between its log append and its shard could truncate that write out of the log (an
  acknowledged write lost after a restart) or commit it in a segment and keep it in the log
  (a cluster that fails to reopen with a duplicate id). All three now wait for writes in
  flight and hold new ones back
  ([ADR-197](decisions/adr-197-checkpoint-excludes-mutations.md)). The server was not affected:
  it serializes these calls itself.
- `backup_to` no longer asks its caller to hold a lock.

## 2026-10-06 — An interrupted bootstrap is not served

- Fix a remote cluster serving part of its corpus after a `--load-file` bootstrap that stopped
  part-way: the coordinator exited, was restarted by its supervisor, found the cluster "already
  populated", skipped the load and served what had landed. The shard nodes now carry a mark
  from before the first bucket until after the last, kept on disk, and a coordinator that finds
  one refuses to start and says the load did not complete
  ([ADR-196](decisions/adr-196-unfinished-bulk-loads-are-remembered.md)). Reset the shard
  nodes' data and load again.
- Upgrade shard nodes before a coordinator that bulk-loads: a node that cannot record the mark
  is not loaded in bulk. Attaching to older nodes is unaffected.

## 2026-10-06 — Replicas are proven before they are trusted

- Fix a silent miss on the remote replicated topology: every coordinator that connected marked
  every replica in sync without checking, so a replica that had missed writes while it was down,
  or one started on an empty volume, was served again after the next coordinator restart, and a
  read that failed over to it answered without those queries. A connecting coordinator now
  compares each replica's content fingerprint with its primary's and trusts only an exact match
  ([ADR-195](decisions/adr-195-replicas-are-proven-at-connect.md)). A replica that is not proven
  is left as it is and never served; a read with no primary and no proven replica fails with 502.
- `--recover-divergent-replicas` on the coordinator re-recovers such replicas from their
  primaries at startup. It is off by default: recovery discards the replica's data, which is the
  wrong thing when the primary is the copy that lost its volume.
- `/_health` and `/_stats` report `out_of_sync_replicas`, and health is yellow while it is
  non-zero.

## 2026-10-06 — A partial cluster write is a failure to retry

- **Behaviour change (remote clusters).** A `PUT /_doc/{id}` or bulk item that not every shard
  took now answers **503** `"result": "partial"` (bulk: item status 503, type `partial_write`)
  with no `_version`, and tells the caller to repeat it
  ([ADR-194](decisions/adr-194-partial-cluster-writes-are-retryable-failures.md)). It used to
  answer 200, say the write was "durably logged" and warn against repeating it. A remote
  coordinator has no log and keeps its repair queue in memory, so a coordinator restart before a
  manual resync left the write missing from the shards that refused it, for good. `DELETE`
  already worked this way (ADR-125).
- A retried `op_type=create` no longer answers 409 "already exists" while an earlier write of
  the id is still queued for repair: it re-drives the repair and answers 503
  `earlier_write_unconverged` until it converges. Retry a partial create as an index operation,
  which converges on any coordinator.
- Fix `resync` storing a second row when the shard had applied a create whose acknowledgement
  was lost. That left the shard unable to enumerate its ids, and after the next coordinator
  attach every create-only write was refused. A repair now replaces the id on the shard.
- A stopping coordinator logs the document ids whose repairs it never completed.
- The documents that described a remote partial write as durably logged are corrected (ADR-047
  later outcome, the coordinator, bulk and resync references, the design note, the runbooks).

## 2026-10-06 — Requests larger than one gRPC message

- Fix the remote `--load-file` bootstrap failing with `OutOfRange` once a shard's bucket passed
  4 MiB: the coordinator sent each bucket as one request, and a shard node accepts at most
  tonic's default. A bucket is now one staged load, the stream a remote resize already uses,
  in messages of at most 3 MiB ([ADR-193](decisions/adr-193-inbound-request-size.md)). The
  node compacts to its segment policy and writes its source store once when the load ends.
- Fix a coordinator being unable to connect, or restart, once its dictionary serialized to more
  than 4 MiB: `AdoptDict` ships the dictionary in one request. Shard nodes now accept requests
  up to `shardserver --max-grpc-request-bytes` (default 64 MiB; Helm
  `shard.maxGrpcRequestBytes`), and a dictionary above a node's limit is refused with an error
  that names the flag. **Upgrade shard nodes before a coordinator whose dictionary exceeds
  4 MiB.**
- Replies are unchanged: the ADR-110 result cap still holds them at or under 4 MiB.

## 2026-10-06 — Boolean server flags that could not be set to false

- Fix `--retain-source`, `--broad-columnar` and `--broad-materialize` on `server`: each was a
  switch that only set `true`, its default, so it could never be turned off and passing `false`
  was a startup error. The documented low-memory `retain_source=false` profile could not be
  selected on the shipped binary. All three now take a value (`--retain-source false`), like
  `--tag-segment-skipping`. A bare `--retain-source` with no value, which did nothing, is now an
  error.
- `shardserver` takes `--broad-columnar <true|false>` and `--broad-materialize <true|false>`
  beside `--retain-source`; a remote coordinator warns that its own copies do not reach the
  shards. Helm: `shard.broadColumnar`, `shard.broadMaterialize`. Compose:
  `RR_SHARD_RETAIN_SOURCE`.
- `server --retain-source false` without `--data-dir` warns that the setting saves nothing.

## 2026-10-06 — Shard-local engine settings

- Fix the remote topology having no way to turn on power-loss durability: `shardserver` built
  every shard from default engine settings, and the coordinator's `--wal-sync-on-write` reached
  nothing while `/_settings` reported it as the shards' configuration. `shardserver` now takes
  `--wal-sync-on-write`, `--retain-source`, `--max-segments` and `--memtable-flush-threshold`
  ([ADR-192](decisions/adr-192-shard-local-engine-settings.md)).
- **Behaviour change:** a coordinator of remote shard nodes refuses `--wal-sync-on-write` and
  names the shard flag; it warns about `--max-segments`, `--memtable-flush-threshold` and
  `--retain-source`, which also have no effect there.
- `GET /_settings` on a remote coordinator lists, under `shard_local`, the `per_shard` keys each
  shard node sets for itself.
- Shard nodes print their sync policy at startup and export
  `reverse_rusty_shard_translog_sync_on_write{shard}`.
- Helm: `shard.walSyncOnWrite`, `shard.retainSource`, `shard.maxSegments`,
  `shard.memtableFlushThreshold`. Compose: `RR_SHARD_WAL_SYNC_ON_WRITE`.

## 2026-10-06 — Data-plane handlers wait off the runtime

- Fix the server becoming unresponsive, `/_health` included, when writes queued behind
  maintenance: standalone `PUT`/`DELETE /_doc`, `/_bulk` and `/_flush` waited for the engine mutex
  on async workers, so during a compaction, backup or vocabulary rebuild as many waiting writes as
  there are CPUs stopped every other request. They now wait, and run, on blocking threads under a
  32-permit admission ([ADR-191](decisions/adr-191-data-plane-handlers-wait-off-the-runtime.md)).
- The same for the coordinator's brief cluster reads (`GET`/`HEAD /_doc`, `GET /`, the
  `/v2/_search` and `/v2/_mpercolate` compile step, job creation), which waited on a worker
  whenever a vocabulary rebuild or resize held or queued for the exclusive cluster lock.
- A standalone write whose client disconnects after admission still completes and is published;
  shutdown waits for such writes before its final flush.

## 2026-10-06 — First read after a shard restart, and wider test margins

- Fix a read failing with a transport error right after a shard node restarted: the coordinator
  could write the request to its old connection before noticing the close, and that failure was
  not classified as retryable. An idempotent read whose connection was lost is now retried
  (ADR-085, later outcome). Writes still never retry.
- Widen four wall-clock bounds in the tests (a 10 ms and two 100 ms margins, and one of 750 ms)
  to at least an order of magnitude by slowing the path each test must beat. Each still fails
  against a build that ignores its deadline or timeout.
- `docs/testing.md` records what to do when the gate goes red, and the two test shapes behind
  most of the intermittent failures.

## 2026-10-06 — Three flaky gate tests

- Fix three tests that failed the gate intermittently, on `main` as well as on branches:
  - the shard node's exhaustive-stream admission test relied on its first stream staying open,
    but that stream matched nothing (its queries are opt-in and it read without the broad lane),
    so it finished at once and the test raced it. It now reads a stream longer than the response
    queue and checks that it did;
  - a job whose stream is dropped with the completion record still queued could fail with
    either "consumer disconnected" or "completion frame was not consumed", depending on which
    side noticed first. Both paths now report that the completion was not consumed;
  - the control-plane wiring tests waited for one node to name a leader and then read through a
    follower that might not know it yet. They now wait until every node names the same leader.

## 2026-10-05 — No commit around an in-memory segment

- Fix loss of acknowledged writes on the single-node engine after a transient storage error: a
  flush that could not write its segment kept the rows in memory and in the WAL, but the next
  successful flush, bulk batch or compaction committed a manifest without them and reset the WAL,
  so a restart came back without those rows. After a failed vocabulary rebuild the same sequence
  lost every query in the store except the ones written since. Every commit now writes such a
  segment to disk first, or does not happen
  ([ADR-190](decisions/adr-190-no-commit-around-in-memory-segments.md)).
- An explicit flush commits a segment left in memory even when there is nothing new to seal.
- Fix a restart deleting the wrong query for library callers: `Engine::tombstone` logs a memtable
  position, which named a different row at replay when an earlier flush had failed. It now
  returns an error until the next commit; deletes by logical id were never affected.
- The first bulk batch on an engine with such a segment is no longer refused once the segment can
  be written. `persistence_healthy` still stays false until the engine is reopened.

## 2026-10-05 — Dropped shards await recovery

- Fix a silent false negative with a stale cluster topology: after orphan GC dropped a shard from
  a node, a coordinator that still named that node could re-adopt the shard, get an empty slot and
  read no matches from it. The node now remembers the shards it dropped, and a slot re-created for
  one refuses every read and write as an ownership mismatch until a peer recovery fills it
  ([ADR-189](decisions/adr-189-dropped-shards-await-recovery.md)).
- Moving a shard back to a node that gave it up is unchanged. Re-seeding such a node from scratch
  under the same dictionary and placement generation now requires wiping its data directory.

## 2026-10-05 — Any-of cover and visibility-preserving rebuilds

- Fix default-read false negatives for queries shaped `<top-64 term> (<variants>)`: a query whose
  only required feature is top-64 now anchors on an any-of group that has no top-64 member, instead
  of going to the opt-in broad lane. Activating an alias on a rare term (`acme mouse` with
  `acme ≡ acm`) therefore no longer removes the query from `include_broad=false` reads
  ([ADR-187](decisions/adr-187-anyof-cover-and-visible-rebuilds.md)).
- Choose the any-of cover from the frozen top-64 mask, not from frequency order, so two compiles of
  one query agree on visibility.
- Keep a default-visible query visible through every single-node rebuild (vocabulary change,
  compiler-semantics migration), including a query whose only term turned top-64 after it was
  compiled and an alias that leaves only top-64 anchors.
- **Upgrade:** compiler semantics version 7. Single-node stores rebuild from retained source on
  open; cluster data follows the existing compiler-semantics procedure (rebuild through the
  coordinator, or reseed remote shard volumes).

## 2026-10-05 — The top-64 mask is assigned once

- Fix silent false negatives after a second initial build: `Dict::finalize_mask` re-ranked the
  top-64 mask on every call, and the initial-build path called it on engines that already held
  queries. Stored rows keep their required top-64 features as mask bits, so after a re-rank they
  stopped matching in both `include_broad` modes. The mask is now assigned once
  ([ADR-188](decisions/adr-188-mask-assigned-once.md)).
- The single-node server no longer applies `--load-file` to a populated data directory. It did so
  on every restart, which re-ranked the mask and stored every query in the file again.
- Fix a query disappearing from default reads after a restart: a live insert made before the
  first mask assignment was re-planned by WAL replay under the finalized mask. The memtable is now
  sealed before the first assignment.
- A store that was restarted with `--load-file` before this fix may hold rows with stale mask
  bits. They are repaired by the next rebuild from source (a vocabulary change, or a
  compiler-semantics migration on open).

## 2026-10-05 — Memtable deletes survive a commit and a restart

- Fix an acknowledged delete coming back after a restart: deleting a query that was still in the
  memtable, followed by a compaction or a bulk ingest and then a restart, replayed the insert from
  the WAL but skipped the delete, because the commit had advanced the WAL watermark past it without
  sealing the memtable. Recovery now always applies a delete to memtable copies and leaves only the
  segment copies to the watermark rule (ADR-066, later outcome).

## 2026-10-05 — Reader-atomic cluster upsert

- Fix silent false negatives during cluster upserts: `PUT /_doc` and every `_bulk` index item
  tombstoned the query on every shard and then inserted it, so a title matched in between saw
  neither version. A shard now replaces a query in one step, and an upsert that keeps its placement
  (a re-put, a tag or version edit, a bulk re-index) needs no reader coordination
  ([ADR-185](decisions/adr-185-reader-atomic-cluster-upsert.md)).
- Fence upserts that move a query between shards or lanes with an optimistic sequence counter:
  overlapping reads repeat instead of seeing the move half-done, and ordinary reads still take no
  lock.
- Install before removing, also under failure: when a shard write of an upsert fails, the old
  copies on other shards keep serving and are queued for removal with the repair, which installs
  the new version first. A queued repair re-drives as the same atomic replace, and the next
  upsert sweeps a stale copy a failed tombstone left behind.
- Wire: add the `ReplaceExtracted` RPC, an `upsert` translog entry for peer recovery, and an
  `atomic_replace` attestation on every handshake. Upgrade shard servers before the coordinator.

## 2026-10-05 — Visibility-partitioned dedup

- Fix a default-read false negative on the single-node engine: a query with no required feature
  could be hidden from `include_broad=false` reads when it shared a dedup group with an identical
  query that had planned class C (after frequency drift, or because one copy was compiled before
  the first mask finalize). A dedup member now joins only a leader on its own side of the opt-in
  boundary, at the memtable write and in both compaction merges
  ([ADR-186](decisions/adr-186-visibility-partitioned-dedup.md)). The reverse case, a class-C query
  exposed on default reads by a visible leader, is closed by the same rule.
- Rows hidden before this change stay class C on disk. With `compaction_reanchor = true` they
  return to the main lane at the next merge when their body currently plans visible; otherwise
  recompile from retained source. `include_broad=true` reads and cluster shards were not affected.

## 2026-10-05 — Multi-machine harness: converge handoff-window repairs

- Fix an intermittent "multi-machine harness" failure: a write that reached a handoff source just
  as it was fenced is acknowledged as a partial apply and queued for repair, which keeps `/_health`
  yellow, and the leg that restarts the handoff target then waited for green without a resync.
  Leg 4 now converges repairs before its green gate and checks every acknowledged write.
- Add `RR_HARNESS_PORT`, `RR_HARNESS_WRITERS` and `RR_HARNESS_WRITE_GAP` to run the harness beside a
  local server and to stress the handoff fence.

## 2026-10-01 — Recorded feature model

- Fix silent false negatives after a restart under a different vocabulary: the single-node manifest
  (v8) now records the vocabulary and a normalizer fingerprint with every commit, and the cluster
  manifest (v8) records the fingerprint. A reopen restores the recorded vocabulary — so a runtime
  alias, `PUT /_vocab`, or the documented backup restore survives without `--vocab-file` — and a
  normalizer that differs from the recorded one fails with `FeatureModelMismatch`
  ([ADR-184](decisions/adr-184-recorded-feature-model.md)).
- Treat `--vocab-file` as a seed for both modes: a store that recorded a vocabulary keeps it and
  warns when the file differs, and a populated in-process cluster built without a vocabulary
  reopens under the stock normalizer its queries were compiled with instead of the file's.
- Commit every vocabulary change before acknowledging it, including on an empty engine and for
  alias candidates and feedback evidence; those responses now report `persisted: true` on durable
  engines. A commit while a vocabulary change awaits its recompile is refused.
- Reject unknown fields in nested vocabulary entries and any `format_version` other than `1`.

## 2026-09-30 — Cluster RPC runtime isolation

- Fix a remote-coordinator deadlock under concurrent writes: write handlers waited on coordinator
  locks on HTTP worker threads while the lock holder's shard RPCs needed those same workers. Cluster
  RPCs now run on a dedicated runtime, and PUT, DELETE, bulk, and flush wait for their locks on
  blocking threads ([ADR-183](decisions/adr-183-cluster-rpc-runtime-isolation.md)).
- Bound queued cluster writes: at most 32 writes hold blocking threads at once, and a write keeps
  its admission until it finishes even if its client disconnects.

## 2026-09-30 — Remote blue/green resize

- Resize a resolve-only remote cluster online: `POST /_cluster/resize` with `targets` builds the new
  layout on fresh shard servers, pauses writes while reads keep serving, streams the deduplicated
  corpus through the new bounded `LiveSources` export RPC into one `StageIngest` load stream per
  target (full-size segments, one source-store write), proves each new position's fingerprint,
  durably retires the old nodes (they then refuse every request from any coordinator), commits the
  shard count, placement generation, and assignments in one control-plane transition, and swaps
  routing ([ADR-180](decisions/adr-180-remote-blue-green-resize.md)).
- Record the transition as a replicated, idempotent resize intent behind a one-way control format 5
  and `RRL5` log header. Resolve-only startup resolves a recorded resize before choosing routes
  (aborting an uncommitted one only after claiming its nodes, and returning never-committed
  retirements to service), adopts the committed shard count and placement generation, and attests
  that it connected to exactly that layout.

## 2026-09-30 — Validated log recovery before append

- Repair and sync incomplete final WAL, coordinator/translog, and Raft writes before reopening for
  append, so a later acknowledged mutation remains reachable on the next restart. Retain exact
  repaired-byte diagnostics for standalone, coordinator, and data-node observers.
- Refuse complete CRC failures, unknown or malformed payloads, and future WAL headers without
  changing the original log. Encode each frame before writing and disable later appends after any
  write/flush/sync failure ([ADR-182](decisions/adr-182-validated-log-recovery.md)).
- Propagate pre-first-manifest initialization failures from fallible engine open. Reject oversized
  live WAL tag fields as client errors without degrading storage health or replacing prior queries.

## 2026-09-30 — Durable peer-recovery targets

- Commit a recovered data node's restart checkpoint before acknowledging recovery, so its copied
  corpus and later translog mutations survive restart even when replacing a previous checkpoint.
  A checkpoint write failure fails recovery before publishing the new slot.
- Add a durable remote `Seal` RPC and drive it for current primaries from `POST /_checkpoint`.
  Remote responses report the primary count while retaining `durable: false`, since individual
  node commits do not create a coordinator manifest or cross-shard snapshot
  ([ADR-181](decisions/adr-181-durable-recovery-target-checkpoints.md)).

## 2026-09-30 — Governed resize operations

- Name and observe in-process resizes: `POST /_cluster/resize` accepts an idempotent `operation_id`
  (a retained success replays without rebuilding; a failed attempt re-executes to heal) and an
  `if_placement_generation` compare-and-set precondition checked before the rebuild starts.
  Responses report the operation ID and attested placement generation, and the new
  `GET /_cluster/resize` and `GET /_cluster/resize/{operation_id}` read progress without the
  cluster lock ([ADR-179](decisions/adr-179-governed-resize-operations.md)).
- Add an opt-in governed growth loop (`--autoscale-resize-interval-secs`) for in-process clusters.
  A pure resize governor accepts a split recommendation only after it persists across observations
  and a cooldown, bounds each step and the final shard count, and stops growing when a resize fails
  to relieve the hottest shard. Accepted operations run through the ordinary resize path with the
  observed placement generation as a precondition.

## 2026-09-29 — Safe retry window after a failed rebuild commit

- Pause adds and upserts on a durable coordinator while a swapped resize or vocabulary rebuild is not
  yet committed. Previously, a write accepted in that window could make the cluster fail to reopen
  after a crash and strand the acknowledged mutation. Reads and removes continue, and the existing
  retry or any checkpoint lifts the pause ([ADR-178](decisions/adr-178-uncommitted-rebuild-write-fence.md)).
- Reclaim generation-named source sidecars superseded by a committed rebuild in every primary and
  in-process replica directory, so repeated resizes or vocabulary changes no longer keep one full
  source corpus per shard copy for every past generation. The canonical `sources.dat`, which a
  peer-recovered replica serves from, is never reclaimed.

## 2026-09-19 — Independent cluster writes by logical ID

- Replace fixed write stripes with per-ID locks that preserve complete same-ID log/apply ordering
  while allowing formerly colliding IDs to proceed independently. Reclaim idle entries and retain
  exclusive bulk-load admission without keeping a lock for every stored or historical ID
  ([ADR-177](decisions/adr-177-per-id-cluster-write-locks.md)).
- Select repair payloads under the ID lock so a delayed repair pass cannot resurrect a mutation
  superseded by a newer successful write. Add stalled-log/fan-out, bulk exclusion, bounded-churn,
  and durable-replay regressions plus a reproducible write-concurrency capture.
- Refresh the locked TLS dependency chain to address
  [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285), retaining the existing
  dependency ranges and security policy.

## 2026-09-11 — Remote create-only admission after restart

- Reconstruct the coordinator's logical-ID membership from complete, bounded shard snapshots on
  populated remote attach. Existing IDs still conflict and new IDs can be created after coordinator
  restart, durable shard reopen, or reattachment to surviving replicas. IDs retained only on stale
  writable replicas remain reserved until explicitly replaced or removed.
- Add the streaming `LiveLogicalIds` RPC with fixed snapshot identity, explicit completion,
  frame/count/deadline limits, and one snapshot admission slot per node. Enumeration uses live
  integer index rows; incomplete, malformed, or unsupported transfers leave admission unavailable
  while explicit upserts remain usable.
- Separate admission membership from historical convergence evidence. Reconstructing IDs does not
  authorize exhaustive reads after loss of coordinator repair state
  ([ADR-176](decisions/adr-176-remote-logical-id-directory.md)).
- Refresh locked transport and build dependencies to clear current security advisories and a
  yanked transitive release, preserving existing dependency ranges and policy gates.

## 2026-08-12 — Durable reassignment intent and conditional cutover

- Added versioned per-position move intents with assignment and placement generations, normalized
  member identities, exact source-fence generations, and full-member recovery evidence. Replicated
  `Begin`/`Ready`/`Commit`/`Abort`/`Finish` transitions reserve overlapping endpoint footprints
  across coordinators and atomically compare the complete move predicate.
- Reordered RF=1 and RF>1 cutover to recover, fence, drain, record evidence, conditionally commit
  the assignment, and only then expose the new live route. Cold startup resolves every recorded
  phase before serving, preserves already-live and third-source authority without stale recopy,
  quiesces an already-live target under a reconstructible fence until commit, and fails loud on
  missing quorum or ambiguous endpoint, placement, fence, or evidence state.
- Added the one-way `RRL4`/move-control-format-4 compatibility fence, control replay/snapshot/race
  coverage, RF=1 phase and commit-quorum crash recovery, and RF>1 cutover, reconcile, failover, and
  post-move restart coverage
  ([ADR-175](decisions/adr-175-durable-reassignment-intent-and-conditional-cutover.md)).

## 2026-08-10 — Fail-open filtered segment skipping

- Added exact immutable per-segment `TagId` unions that let filtered scalar, ranked, exhaustive, and
  columnar reads skip a segment only when a request predicate group is provably absent. Missing and
  cross-group-inconclusive summaries fail open, the memtable always probes, and per-row exact tag
  verification remains authoritative ([ADR-174](decisions/adr-174-fail-open-tag-segment-summaries.md)).
- Rebuild summaries at seal, compaction, and mmap open without changing the segment format; expose a
  dynamic single-node and cluster-startup result-preserving kill switch, merged local/gRPC skip
  telemetry, and resident-memory
  accounting. Differential coverage spans writes, compaction, reopen, synthetic tag IDs, batch
  evaluation, cluster fan-out, and real gRPC transport.
- Added `tagbench`; its eight-segment seeded capture preserved all 11,349 result rows while reducing
  postings and candidates by 87.5% with 32 bytes of summary payload.

## 2026-08-05 — Cluster GC API hardening

- Hardened native `POST /_cluster/gc` with strict bodyless transport, assignment-routed topology
  safety, shared reconcile/GC admission, manager-bounded atomic start, independently supervised
  disconnect-safe and shutdown-joined completion, truthful skipped-node/slot and pending-disk
  semantics, final control-version/timing attestation, sanitized partial failures, no-store
  telemetry, and an explicit non-dangling-index Elasticsearch/OpenSearch boundary
  ([ADR-173](decisions/adr-173-cluster-gc-api-contract.md)).

## 2026-08-05 — Cluster reconcile API hardening

- Hardened native `POST /_cluster/reconcile` with strict bounded transport, a resolve-only topology
  contract for both manual and unattended passes, shared single admission, manager-bounded atomic
  start, independently supervised disconnect-safe completion, shutdown joining through optional GC,
  final control-version/timing attestation, truthful commit-only and uncommitted semantics, sanitized
  partial failures, fixed no-store telemetry, and an explicit non-reroute Elasticsearch/OpenSearch
  boundary ([ADR-172](decisions/adr-172-cluster-reconcile-api-contract.md)).

## 2026-08-05 — Cluster reassign API hardening

- Hardened native `POST /_cluster/reassign` with strict bounded transport, truthful no-op and
  reconciliation outcomes, checked position identity, current-live-primary authority under the
  move ledger, commit-only recovery from a prior uncommitted flip, resolve-only topology safety,
  manager-bounded start admission, independently supervised disconnect-safe completion, shutdown
  quiescence, sanitized failures, terminal timing/telemetry, and an explicit non-reroute
  Elasticsearch/OpenSearch boundary
  ([ADR-171](decisions/adr-171-cluster-reassign-api-contract.md)).

## 2026-08-05 — Cluster handoff API hardening

- Hardened native `POST /_cluster/handoff` with strict bounded JSON, explicit uncommitted-routing
  acknowledgement, current-live-primary and fresh-target attestation under the move ledger,
  manager-bounded start admission, idempotent retry, independently supervised disconnect-safe
  completion, shutdown quiescence, terminal timing/status fields, fixed no-store telemetry, and an
  honest non-alias boundary from Elasticsearch/OpenSearch allocation reroute
  ([ADR-170](decisions/adr-170-cluster-handoff-api-contract.md)).

## 2026-08-05 — Cluster resync API hardening

- Hardened native `POST /_cluster/resync` with strict bodyless transport, bounded
  manager-timeout admission, independently supervised off-runtime repair, disconnect-safe terminal
  reporting, additive acknowledgement/timing fields, fixed no-store telemetry, and an explicit
  non-alias boundary from Elasticsearch/OpenSearch allocation reroute
  ([ADR-169](decisions/adr-169-cluster-resync-api-contract.md)).

## 2026-08-05 — Dependency-gate qualification

- Qualified `RUSTSEC-2026-0235` as an inactive optional `rkyv` lockfile edge, retained the
  lockfile-wide RustSec scan, and added an all-feature/all-target graph guard that fails before any
  `rkyv` version can enter a shipped build under the exception
  ([ADR-168](decisions/adr-168-inactive-rkyv-advisory.md)).

## 2026-07-26 to 2026-07-29 — Documentation, module boundaries, and API parity

- Replaced the monolithic ADR index with an area hub, nine compact catalogs, and one canonical
  page per ADR.
- Split large Rust implementation and test files along existing responsibility boundaries without
  changing public behavior.
- Standardized every ADR area catalog on the same four-column table and reduced its summary cells
  to short outcome statements.
- Reframed project tracking: this changelog owns shipped history, while the roadmap owns unfinished
  work and its full proposal text.
- Added startup-loaded, fingerprinted `static_v1`, linear, and quantized-tree CPU ranking profiles
  for native bounded/exhaustive delivery, with deterministic post-match scoring, strict model
  bounds, title-dependent batch support, pre-dedup semantic feature persistence plus source-driven
  legacy migration, benchmark selection, and fail-loud remote-wire refusal
  ([ADR-162](decisions/adr-162-versioned-cpu-ranking-profiles.md)).
- Extended named CPU profiles across remote top-K, batch, and exhaustive gRPC delivery with
  request/terminal fingerprint attestation, fail-closed version skew, shared Compose mounts, and
  Helm ConfigMap or generic PVC/CSI-capable volume sources
  ([ADR-163](decisions/adr-163-distributed-ranking-profile-attestation.md)).
- Aligned document deletion with the ES/OpenSearch shape and refresh controls, made logical delete
  counts placement-independent, and exposed the existing remote partial-repair contract accurately
  ([ADR-125](decisions/adr-125-delete-document-contract.md)).
- Hardened compatibility `GET`/`POST /_search` with strict native/ES request parsing, supported
  ES/OS controls and response identity, snapshot-generation-safe enrichment, and complete
  multi-document profile semantics ([ADR-126](decisions/adr-126-search-api-contract.md)).
- Hardened exact bounded `POST /v2/_search` with strict request parsing, honest ES/OS control
  aliases and timing fields, structured extractor failures, and mutation-fenced cluster winner
  enrichment ([ADR-127](decisions/adr-127-v2-search-api-contract.md)).
- Hardened exact bounded `POST /v2/_mpercolate` with a strict shared-options envelope, truthful
  ES/OS control aliases and batch timing/status fields, structured extractor failures, and
  mutation-fenced cluster union enrichment
  ([ADR-128](decisions/adr-128-v2-mpercolate-api-contract.md)).
- Hardened `POST /v2/_pit` with strict body/query controls, ES/OpenSearch keep-alive and
  fail-loud partial-creation aliases, structured extractor failures, and a dual-dialect response
  carrying both token names, creation time, and truthful shard counts
  ([ADR-129](decisions/adr-129-v2-open-pit-api-contract.md)).
- Hardened `DELETE /v2/_pit` with strict ES/OpenSearch/native scalar and batch identities,
  pre-decode body bounds, all-token pre-validation, structured extractor failures, and a truthful
  response carrying aggregate, per-PIT, and logical-context release results
  ([ADR-130](decisions/adr-130-v2-close-pit-api-contract.md)).
- Hardened `POST /_percolate/jobs` with strict bounded input, optional server-generated identity,
  native and ES/OpenSearch execution-control aliases, fail-loud unsupported async controls, and
  familiar async identity/status fields without weakening exact terminal delivery
  ([ADR-131](decisions/adr-131-exhaustive-job-create-api-contract.md)).
- Hardened `GET /_percolate/jobs/{id}` with strict bounded async waiting, fail-loud retention
  controls, no-store caching, and native plus familiar status/timing/error fields while preserving
  terminal stream attestation
  ([ADR-132](decisions/adr-132-exhaustive-job-status-api-contract.md)).
- Hardened `DELETE /_percolate/jobs/{id}` with strict input, a native/ES-compatible acknowledged
  response, cooperative running cancellation, and atomic terminal record plus event-id removal
  ([ADR-133](decisions/adr-133-exhaustive-job-delete-api-contract.md)).
- Hardened `GET /_percolate/jobs/{id}/stream` with strict query-free single-consumer semantics,
  cache-safe newline-delimited responses, pre-claim HEAD rejection, and standalone/coordinator
  route parity while keeping the terminally attested protocol explicitly native
  ([ADR-134](decisions/adr-134-exhaustive-job-stream-api-contract.md)).
- Hardened full-result `POST /_mpercolate` with one strict native/ES-shaped request, truthful
  source/timeout/fail-closed controls and batch timing/status fields, generation-consistent
  standalone enrichment, and an explicit coordinator profile boundary
  ([ADR-135](decisions/adr-135-mpercolate-api-contract.md)).
- Hardened `POST /_bulk` with strict NDJSON framing and controls, consistent ordered
  replace-or-create/create-only semantics, source-version and response metadata preservation, and a
  safe fresh-corpus immutable-segment fast path
  ([ADR-136](decisions/adr-136-bulk-api-contract.md)).
- Hardened `GET`/`POST /_flush` with strict body-free controls, exact non-waiting admission,
  ES/OpenSearch shard results, shared standalone/coordinator metrics, and fail-loud local-shard
  durability
  ([ADR-137](decisions/adr-137-flush-api-contract.md)).
- Made native `POST /_compact` actually force all sealed segments, added strict
  Elasticsearch/OpenSearch-familiar `POST /_forcemerge` controls and shard results, moved merge work
  off async runtime workers, and preserved fail-closed rollback
  ([ADR-138](decisions/adr-138-compaction-api-contract.md)).
- Hardened native `POST /_backup` with one strict bounded standalone/coordinator contract,
  synchronous timing and checkpoint-epoch results, single-slot blocking-worker admission that
  survives disconnects with independently supervised outcomes, unique staging, and fail-closed
  atomic no-clobber promotion that refuses dangling or raced destination entries
  ([ADR-139](decisions/adr-139-backup-api-contract.md)).
- Hardened native `POST /_checkpoint` with strict bounded transport, supervised off-runtime
  durability work shared with backup, no-store telemetry, fail-loud persistence errors, and
  explicit `durable`/`shards_checkpointed` results that cannot disguise a stateless coordinator
  maintenance no-op as a recovery point
  ([ADR-161](decisions/adr-161-checkpoint-api-contract.md)).
- Hardened coordinator `GET`/`HEAD /_cluster/state` with strict bounded no-store transport,
  authoritative off-runtime reads, shared introspection admission, sanitized fail-loud errors, an
  exact familiar `version` projection and manager-timeout aliases, and explicit rejection of
  nonexistent index-state semantics
  ([ADR-162](decisions/adr-162-cluster-state-api-contract.md)).
- Hardened native coordinator `POST /_cluster/nodes` with strict endpoint identity and mesh-origin
  validation, exact committed versions, bounded off-runtime consensus writes, outcome-aware
  timeouts, sanitized failures, and explicit separation from voter membership, placement, and data
  movement ([ADR-164](decisions/adr-164-node-registration-api-contract.md)).
- Hardened native coordinator `DELETE /_cluster/nodes/{id}` with strict bodyless identity, exact
  committed versions, reserved bootstrap and in-use voter/assignment protection, bounded
  off-runtime consensus writes, outcome-aware timeouts, sanitized failures, and explicit
  separation from voter membership, placement, data movement, and safe node shutdown
  ([ADR-165](decisions/adr-165-node-deregistration-api-contract.md)).
- Hardened native coordinator `POST /_cluster/rebalance` with strict bounded transport,
  topology-safe defaults that move data before committing resolve-only remote routing and reject
  restart-unsafe CLI-seeded or non-authoritative static routing, positive conflict-free parallelism, one
  supervised off-runtime workflow, manager-start timeouts, final control-state attestation,
  resumable partial reports, shutdown-budget deployment controls, sanitized failures, and an
  explicit non-reroute ES/OpenSearch boundary
  ([ADR-166](decisions/adr-166-cluster-rebalance-api-contract.md)).
- Hardened native coordinator `POST /_cluster/resize` with strict bounded transport, a fixed
  public shard-count ceiling, one supervised off-runtime blue/green rebuild, manager-start
  timeouts, disconnect-safe terminal completion with shutdown admission quiescence,
  predecessor-safe retry repair, exact final control-version/shard-count/placement-generation
  attestation, fail-loud remote-topology refusal, no-store telemetry, and an explicit
  non-split/non-shrink ES/OpenSearch boundary
  ([ADR-167](decisions/adr-167-cluster-resize-api-contract.md)).
- Hardened native `GET /_stats` with a strict no-store transport, truthful physical/live/tombstone
  and resident-memory/WAL projections, familiar timing and shard metadata, single-slot blocking
  collection, fail-loud cluster aggregation, and one shard-count fan-out instead of two
  ([ADR-140](decisions/adr-140-stats-api-contract.md)).
- Reworked native `GET /_cat/stats` into a truthful `metric` / `value` table with strict
  text/JSON, header, column, help, and sort controls; shared its corpus-wide collection admission
  with `/_stats` and moved the scan off async workers
  ([ADR-141](decisions/adr-141-cat-stats-api-contract.md)).
- Hardened native `GET /_cat/segments` with strict bodyless transport, no-store responses, shared
  CAT header/column/help/sort rendering, numeric byte-unit controls, consistent string-valued JSON,
  and honest LSM fields plus exact ES/OpenSearch aliases
  ([ADR-142](decisions/adr-142-cat-segments-api-contract.md)).
- Hardened coordinator `GET /_cat/shards` with strict shared CAT controls, no-store responses,
  bounded blocking-worker admission, string-valued JSON, and fail-loud shard/topology collection
  that no longer disguises control-plane failure as empty node assignments
  ([ADR-143](decisions/adr-143-cat-shards-api-contract.md)).
- Hardened native `GET`/`HEAD /_health` with strict bounded transport, familiar status waiting,
  fail-loud HTTP readiness, complete coordinator serving/control-plane attestation, blocking-worker
  admission, independently bounded pre-body unauthenticated requests and body-read deadlines,
  sanitized failures, deadline-checked observations, and whole-route no-store telemetry
  ([ADR-144](decisions/adr-144-health-api-contract.md)).
- Hardened native `GET`/`HEAD /_metrics` with strict bounded no-store transport, Prometheus text
  0.0.4 semantics, whole-route telemetry, lock-free standalone snapshots, and fail-loud
  coordinator collection that runs one complete shard-count pass off async workers and removes
  stale per-position labels
  ([ADR-145](decisions/adr-145-metrics-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab` with strict bounded no-store transport, complete
  round-trippable JSON, GET-only request limits, whole-route telemetry, shared blocking-work
  admission, lock-free standalone snapshot capture, and brief off-runtime coordinator locking
  ([ADR-146](decisions/adr-146-get-vocab-api-contract.md)).
- Hardened native `PUT /_vocab` with strict bounded JSON transport, synchronous timing and
  standalone/coordinator response parity, shared off-runtime rebuild admission, complete
  post-recompile verification, and fail-loud durable acknowledgement
  ([ADR-147](decisions/adr-147-put-vocab-api-contract.md)).
- Hardened native `POST /_vocab/learn` with one strict caller-corpus contract in standalone and
  coordinator modes, distinct-query evidence counting, bounded DSL/config/input validation, shared
  blocking-work admission, round-trippable no-store output, and explicit separation from
  ES/OpenSearch synonym management
  ([ADR-148](decisions/adr-148-vocab-learn-api-contract.md)).
- Hardened native `POST /_vocab/learn_and_apply` with strict bodyless controls, timed
  standalone/coordinator response parity, shared off-runtime rebuild admission, complete standalone
  post-recompile verification, and fail-loud durable acknowledgement
  ([ADR-149](decisions/adr-149-vocab-learn-apply-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab/aliases` with strict bounded no-store transport, familiar
  `from`/`size` review paging, total `count`, whole-registry summaries, shared blocking-work
  admission, lock-free standalone snapshot capture, and brief off-runtime coordinator locking
  ([ADR-150](decisions/adr-150-alias-registry-read-api-contract.md)).
- Split the unchanged core and distributed release/LTO code-gate commands across independent CI
  runners, retained the complete local `check.sh` entry point and one required aggregate result,
  and stopped producing empty test harnesses for binary targets without binary-local tests
  ([ADR-151](decisions/adr-151-parallel-ci-code-gate-lanes.md)).
- Hardened native `POST /_vocab/aliases/import` with strict atomic Solr parsing, familiar
  Elasticsearch rule objects and synchronous refresh plus OpenSearch Solr/expansion controls,
  bounded no-store transport, true no-op retries that finish pending engine, control-plane, and
  durable coordinator state without overwriting incompatible manifests, timed
  standalone/coordinator parity, shared off-runtime mutation admission, complete standalone rebuild
  verification, and fail-loud durable acknowledgement
  ([ADR-152](decisions/adr-152-alias-import-api-contract.md)).
- Hardened native `POST /_vocab/aliases/learn_and_apply` with strict bodyless evidence controls,
  bounded no-store transport, timed standalone/coordinator response parity, shared off-runtime
  corpus/rebuild admission, complete standalone rebuild verification, fail-loud durable
  acknowledgement, and an explicit native boundary from Elasticsearch/OpenSearch synonym management
  ([ADR-153](decisions/adr-153-alias-learn-apply-api-contract.md)).
- Hardened native `POST /_vocab/aliases/discover` with strict optional-JSON transport, validated
  distinct-query evidence and bounded controls, timed no-store standalone/coordinator parity,
  shared off-runtime admission, brief stored-source capture, deterministic response limits, and an
  explicit native boundary from Elasticsearch/OpenSearch synonym management
  ([ADR-154](decisions/adr-154-alias-discover-api-contract.md)).
- Hardened native `POST /_vocab/aliases/discover_and_record` with strict controls-only transport,
  timed no-store output, truthful live-only persistence, shared off-runtime admission, brief
  source capture and registry installation locks, success-only snapshot publication, and a
  validated fail-loud coordinator alternative
  ([ADR-155](decisions/adr-155-alias-discover-record-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab/aliases/feedback` with strict positive evidence controls,
  familiar bounded `from`/`size` paging, total counts, timed no-store output, shared off-runtime
  admission, page-only evidence snapshots, and an observed fail-loud coordinator alternative
  ([ADR-156](decisions/adr-156-alias-feedback-read-api-contract.md)).
- Hardened native `POST /_vocab/aliases/feedback/reset` with strict bounded bodyless transport,
  timed no-store output, shared off-runtime admission, a linearizable in-place evidence clear that
  preserves tracked candidates, and an observed fail-loud coordinator alternative
  ([ADR-157](decisions/adr-157-alias-feedback-reset-api-contract.md)).
- Hardened native `POST /_vocab/aliases/validate_and_apply` with strict positive evidence
  controls, bounded bodyless transport, timed no-store output, idempotent stamping, shared
  off-runtime admission, success-only publication, fail-loud activation durability, and a
  validated coordinator alternative
  ([ADR-158](decisions/adr-158-alias-feedback-validate-apply-api-contract.md)).
- Hardened native `GET`/`HEAD /_settings` with strict familiar controls, bounded bodyless
  transport, no-store telemetry, shared off-runtime admission and serialization, and coordinator
  lock/default parity while keeping its native compatibility boundary explicit
  ([ADR-159](decisions/adr-159-get-settings-api-contract.md)).
- Hardened native `PUT /_settings` with strict duplicate-safe JSON, bounded familiar controls,
  no-store telemetry, shared off-runtime admission and lock waiting, and coherent mutation/snapshot
  publication while preserving the explicit live-only and coordinator boundaries
  ([ADR-160](decisions/adr-160-put-settings-api-contract.md)).

## 2026-07-25 — Semantic correctness, durability, and performance gates

- Fixed clause-boundary lowering so aliases, phrases, and numeric context cannot leak
  across intervening query clauses ([ADR-118](decisions/adr-118-clause-boundary-compiler-semantics.md)).
- Preserved OR-of-AND semantics for multi-token any-of members
  ([ADR-119](decisions/adr-119-multi-token-anyof-member-semantics.md)).
- Made quoted phrases exact analyzed token-graph adjacency predicates
  ([ADR-120](decisions/adr-120-quoted-phrase-token-graph-semantics.md)).
- Made source sidecar replacement atomic with the manifest-selected segment set
  ([ADR-121](decisions/adr-121-atomic-source-sidecar-commit.md)).
- Rejected stale positional tombstone addresses before WAL append
  ([ADR-122](decisions/adr-122-fail-closed-positional-tombstones.md)).
- Bounded cooperative cancellation latency inside dense segment scans
  ([ADR-123](decisions/adr-123-bounded-in-segment-cancellation.md)).
- Added a variance-aware merge-blocking performance gate and a scheduled 10M-query soak
  ([ADR-124](decisions/adr-124-variance-tolerant-performance-gate.md)).
- Hardened the independent matcher so compiler-semantic regressions cannot cancel out between the
  engine and its oracle ([ADR-087](decisions/adr-087-independent-correctness-oracle.md)).

## 2026-07-23 to 2026-07-24 — Delivery and document API parity

- Added bounded exhaustive background jobs with idempotent streamed chunks and an exact terminal
  checksum ([ADR-114](decisions/adr-114-exhaustive-job-stream-delivery.md)).
- Added source metadata readback and honest `GET` and `HEAD` document behavior
  ([ADR-116](decisions/adr-116-get-document-source-readback.md)).
- Added strict create/index controls, conflict semantics, refresh parsing, and response metadata to
  document writes ([ADR-117](decisions/adr-117-put-document-index-contract.md)).

## 2026-07-17 to 2026-07-18 — Exact ranked delivery

- Split exact Boolean truth from bounded delivery through an explicit ranked result contract
  ([ADR-107](decisions/adr-107-ranked-percolation-result-contract.md)).
- Added typed priority and bounded local top-K collection
  ([ADR-108](decisions/adr-108-typed-priority-local-bounded-ranking.md)).
- Added deterministic distributed emission ownership so one shard emits each logical match
  ([ADR-109](decisions/adr-109-deterministic-distributed-emission-ownership.md)).
- Added distributed top-K merge and winner-only source fetch
  ([ADR-110](decisions/adr-110-distributed-top-k-query-then-fetch.md)).
- Added typed ranked wire errors with a legacy compatibility fallback
  ([ADR-111](decisions/adr-111-typed-ranked-wire-errors.md)).
- Added streamed distributed title batching with one-credit winner fetch
  ([ADR-112](decisions/adr-112-distributed-title-batching.md)).
- Added point-in-time snapshots and signed cursor pagination
  ([ADR-113](decisions/adr-113-pit-cursor-pagination.md)).

## 2026-07-02 to 2026-07-03 — Scale, cost control, and deployability

- Added review-first distributional alias discovery and behavioral match-feedback validation
  ([ADR-102](decisions/adr-102-distributional-alias-discovery.md),
  [ADR-103](decisions/adr-103-match-feedback-alias-validation.md)).
- Proved the durable K=8 cluster path at 20 million stored queries, including mutation and reopen
  ([ADR-104](decisions/adr-104-cluster-scale-soak.md)).
- Added the always-visible columnar hot tier under the two-axis placement rule
  ([ADR-105](decisions/adr-105-hot-tier-two-axis-placement.md)).
- Added in-memory canonical-body posting sharing and cross-segment regrouping
  ([ADR-106](decisions/adr-106-canonical-body-dedup-stage-a.md)).
- Added deployable-mode contracts, local and remote smoke gates, and versioned image publishing
  ([ADR-098](decisions/adr-098-deployable-gate-and-release-pipeline.md)).
- Added cooperative request cancellation and bounded search concurrency
  ([ADR-099](decisions/adr-099-cooperative-cancellation-bounded-concurrency.md)).
- Added per-shard RPC latency histograms and broad-lane cost counters
  ([ADR-100](decisions/adr-100-shard-rpc-latency-histogram.md),
  [ADR-101](decisions/adr-101-shard-broad-lane-cost-counters.md)).
- Added group-aware reassignment, parallel move scheduling, orphan-slot collection, and
  fingerprint-based retained-member reuse
  ([ADR-094](decisions/adr-094-replicated-group-reassignment.md) through
  [ADR-097](decisions/adr-097-content-fingerprint-skip.md)).

## 2026-06-24 to 2026-07-01 — Distributed operations and recovery

- Hardened gRPC transport deadlines, keepalive, retries, and metrics
  ([ADR-085](decisions/adr-085-grpc-transport-hardening.md)).
- Made committed control-plane assignments the routing source of truth with endpoint failover
  ([ADR-086](decisions/adr-086-control-plane-routing-and-failover.md)).
- Added real-process crash injection and a documented security review
  ([ADR-088](decisions/adr-088-crash-injection-harness.md),
  [ADR-089](decisions/adr-089-security-review.md)).
- Added live data-moving reassignment, unattended reconciliation, and multi-shard-per-node hosting
  ([ADR-090](decisions/adr-090-data-moving-reassignment.md) through
  [ADR-093](decisions/adr-093-multi-shard-per-node.md)).

## 2026-06-19 to 2026-06-23 — Packaging, backup, and platform integration

- Added engine-driven consistent backup and restore for durable single-node and in-process-cluster
  data directories
  ([ADR-079](decisions/adr-079-backup-restore.md)).
- Replicated broad and class-D queries across shards to remove the shard-0 hotspot
  ([ADR-080](decisions/adr-080-cluster-replicate-broad-to-all.md)).
- Added release container packaging and the distributed operations runbook
  ([ADR-081](decisions/adr-081-deployment-packaging-runbook.md)).
- Closed advertise-URL and coordinator class-D packaging gaps
  ([ADR-082](decisions/adr-082-packaging-deploy-correctness.md)).
- Connected the coordinator to the durable control quorum
  ([ADR-083](decisions/adr-083-control-plane-coordinator-wiring.md)).
- Added Helm packaging and native gRPC health/readiness endpoints
  ([ADR-084](decisions/adr-084-kubernetes-helm-health.md)).

## 2026-06-10 to 2026-06-11 — Percolator parity and distributed-v1 surfaces

- Established the drop-in translation contract and explicit distributed-v1 graduation criteria
  ([ADR-064](decisions/adr-064-percolator-drop-in-parity-audit.md),
  [ADR-065](decisions/adr-065-distributed-v1-graduation.md)).
- Made base-segment tombstones durable and document replacement atomic
  ([ADR-066](decisions/adr-066-tombstone-durability-at-commit.md),
  [ADR-067](decisions/adr-067-atomic-upsert-put.md)).
- Added the opt-in class-D lane and configurable parity number context
  ([ADR-068](decisions/adr-068-class-d-always-candidate-lane.md),
  [ADR-069](decisions/adr-069-parity-number-context-words.md)).
- Added the cluster REST coordinator, mesh TLS/authentication, and the multi-process single-host
  container-network lifecycle harness ([ADR-070](decisions/adr-070-cluster-rest-surface.md) through
  [ADR-072](decisions/adr-072-multi-machine-harness.md)).
- Closed REST parity gaps, enabled tagged-cluster vocabulary rebuilds, added cluster ranking, and
  made multi-word alias routing lossless
  ([ADR-073](decisions/adr-073-rest-parity-hardening.md) through
  [ADR-076](decisions/adr-076-cluster-multiword-aliases-vocab-shipping.md)).
- Added tag-space recovery attestation and durable in-process cluster resize
  ([ADR-077](decisions/adr-077-tagdict-recovery-fingerprint.md),
  [ADR-078](decisions/adr-078-cluster-resize.md)).

## 2026-06-03 to 2026-06-09 — Vocabulary, parity, and adversarial testing

- Added per-query tags and filtered percolation locally and across shards
  ([ADR-049](decisions/adr-049-percolator-parity-tags.md),
  [ADR-055](decisions/adr-055-cluster-tags-filtered-percolation.md)).
- Added golden front-end tests, fail-closed replacement operations, and review-driven hardening
  ([ADR-050](decisions/adr-050-golden-front-end-tests.md) through
  [ADR-052](decisions/adr-052-external-review-hardening.md)).
- Added corpus phrase learning, lossless equivalence expansion, compaction re-anchoring, and
  versioned frozen dictionaries
  ([ADR-053](decisions/adr-053-corpus-phrase-vocab-source.md) through
  [ADR-057](decisions/adr-057-frozen-dict-format-versioning.md)).
- Added punctuation folding, ranking/pagination, governed learned aliases, and multi-word title
  views ([ADR-058](decisions/adr-058-punctuation-equivalence-folding.md) through
  [ADR-061](decisions/adr-061-token-graph-multiword-aliases.md)).
- Added HTTP bearer authentication and adversarial test generation
  ([ADR-062](decisions/adr-062-server-bearer-auth.md),
  [ADR-063](decisions/adr-063-adversarial-test-hardening.md)).

## 2026-05-31 to 2026-06-03 — Cluster v1

- Added the in-process multi-shard core and the lean-core build boundary
  ([ADR-027](decisions/adr-027-in-process-multi-shard-core.md),
  [ADR-028](decisions/adr-028-lean-core-feature-gate.md)).
- Added the local/remote shard seam, dictionary attestation and shipping, coordinator log, and
  per-shard durable segments
  ([ADR-029](decisions/adr-029-grpc-shardserver-shard-seam.md) through
  [ADR-034](decisions/adr-034-cross-process-dict-shipping.md)).
- Added replication, no-quiesce recovery, retention leases, and a durable Raft control plane
  ([ADR-035](decisions/adr-035-per-shard-replication-peer-recovery.md) through
  [ADR-041](decisions/adr-041-durable-raft-log-recovery.md)).
- Added rendezvous allocation, live handoff, autoscaling policy, and dynamic vocabulary
  ([ADR-042](decisions/adr-042-shard-node-allocator.md) through
  [ADR-046](decisions/adr-046-dynamic-vocabulary.md)).
- Added fail-closed repair for partial distributed writes
  ([ADR-047](decisions/adr-047-remote-partial-apply-resync.md),
  [ADR-048](decisions/adr-048-reliability-hardening.md)).

## 2026-05-27 to 2026-05-30 — Engine foundation

- Established semantic signatures, integer-only verification, broad-query classes, and the
  append-oriented LSM write path ([ADR-001](decisions/adr-001-semantic-signatures.md) through
  [ADR-004](decisions/adr-004-lsm-write-path.md)).
- Added typed errors, structural exclusion of forbidden gates, deterministic generation, and the
  initial specialized dependency set ([ADR-005](decisions/adr-005-typed-errors.md) through
  [ADR-008](decisions/adr-008-deterministic-data-generation.md)).
- Added score-based compaction, fallible normalization, segment filters, mmap segments, WAL
  recovery, and source persistence ([ADR-009](decisions/adr-009-score-based-compaction.md) through
  [ADR-014](decisions/adr-014-query-source-store.md)).
- Added runtime vocabulary, lock-free snapshots, durable bulk ingest, and per-item outcomes
  ([ADR-015](decisions/adr-015-runtime-vocabulary-learning.md) through
  [ADR-018](decisions/adr-018-bulk-ingest-per-item-outcomes.md)).
- Reduced resident memory and added observable durability failures, runtime settings, segment
  introspection, CI, query limits, and columnar broad evaluation
  ([ADR-020](decisions/adr-020-resident-memory-reduction.md) through
  [ADR-026](decisions/adr-026-broad-lane-batch-evaluation.md)).
