# ADR-180 — Remote blue/green resize onto fresh nodes

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted (2026-09-30)

## Context

ADR-078 resizes an in-process cluster by rebuilding the live corpus under a new ring, and
ADR-179 made that an idempotent, governed operation. A remote (gRPC) cluster could not change its
shard count at all: operators had to stand up a separate cluster, re-ingest the corpus from their
own source of truth, and cut traffic over by hand.

The obvious alternative, staging the new layout beside the old one on the same shard servers, is
blocked by eleven separate assumptions. The main ones:

- a node adopts one node-wide placement space, persisted as a single generation and shard count
  and validated by every slot RPC;
- a slot's key and directory are its logical position;
- restart validates every row against the node's one generation;
- orphan GC classifies slots by position against the single committed map.

Lifting those would mean a new node format and a slot-addressing change across the whole RPC
surface.

## Decision

Resize a remote cluster by building the complete new layout on **fresh, empty target nodes**, then
committing it and swapping routing. Blue and green never share a node, so the node format and slot
addressing stay unchanged, and retiring the old layout means retiring whole nodes.

### Corpus export

A new `LiveSources` streaming RPC exports one slot's live corpus. The server snapshots the sorted
live logical ids from the index rows; the lock wait, scan, and sort all observe the export deadline.
It then fetches documents (source, stored version, raw tags) in pages of 256 under short lock holds,
each lock wait also bounded by the deadline, and sends byte-capped frames through a bounded channel
followed by an explicit completion frame. The export fails loud when:

- a document disappears or its source disagrees with its exact row;
- the deadline passes;
- a single document exceeds the result cap;
- the receiver goes away.

The client checks frame identity, a stable total, strictly increasing ids, and a completion count
equal to everything delivered. It never retries a partially consumed stream. It shares the node's
single snapshot permit with `LiveLogicalIds`. Every send waits for channel capacity no later than
the deadline and failure delivery never waits, so a stalled reader cannot pin the producer or the
permit; the client bounds consumption with the same absolute deadline.
`ClusterEngine::export_live_corpus` dedups replicated rows across positions, and copies that
disagree fail the export.

### Staged load

Each target position receives one client-streaming `StageIngest` call. The target seals segments
of its memtable flush threshold as rows arrive and, when the stream closes, compacts them to its
`max_segments` policy and writes its source store and checkpoint sidecar once, failing if either
write fails. A dropped, unfinished load cancels the call rather than closing it, so a target never
persists a partial load as complete, and each segment and finish job holds the node's installation
barrier, so a cancelled call's detached worker can never write over a replaced slot. Placement
force-accepts, as log replay does, so a stored class-D query survives even when the current
admission knob is off; a stored query that no longer parses or places fails the resize.

### Layout authority: durable node retirement

Which layout may serve is enforced by the data nodes, not by any coordinator's memory. `Retire`
durably marks a whole node as superseded by a successor placement generation, keyed by the resize
operation id; the record is written before it takes effect, and every durable startup path restores
it (an unreadable record fails closed). A retired node:

- refuses every slot RPC, reads included, as a superseded placement;
- refuses adoption and any new slot, so it can never be re-adopted empty and answer with silently
  empty results;
- still answers the fingerprint handshake, reporting which operation retired it, so startup
  resolution can claim it.

Only `Unretire` by the same operation lifts it, and only when the control plane proves that
resize did not commit. A finished resize leaves its old nodes retired until an operator wipes or
decommissions them. Both RPCs are ordinary lease-checked owner RPCs.

### Replicated resize intent

The control document gains one `ResizeIntent`, nested with the move state. It records:

- the expected layout: shard count, placement generation, and complete assignments;
- the desired layout at generation `N + 1`, with position `p` on `targets[p % n]`;
- the normalized endpoint of every desired node;
- a `Preparing`, `Ready(evidence)`, or `Committed(evidence)` phase.

`Begin`, `MarkReady`, `Commit`, `Abort`, and `Finish` are idempotent by operation id, and their
preconditions run inside the state machine:

- **`Begin`** requires well-formed, gap-free layouts. The new generation must be exactly one more
  than the old. Every desired node must be registered at its recorded endpoint, and no desired
  endpoint may appear in the expected layout. There must be no other resize or move intent, and
  the expected layout must still be committed.
- **`MarkReady`** requires evidence for every desired position.
- **`Commit`** re-checks the expected layout and member identities. It then replaces the shard
  count, generation, and assignments in one transition, bumps every position's assignment
  generation, and removes the old layout's (already retired) data nodes from membership, so no
  rebalance or reconcile picks them as a destination.
- **`Begin` on a move** is refused while a resize intent exists.

A resize command raises the snapshot fence to control format 5, which never lowers. It also
installs an `RRL5` Raft log header before the first resize entry. Older binaries, which accept at
most format 4 and `RRL4`, reject both instead of silently dropping the intent.

### Orchestration

`prepare_remote_resize` runs under a shared reference, so reads keep serving the old layout during
the copy:

1. It validates the request and the cluster: remote, assignment-routed, exclusively owned by this
   coordinator, replication factor 1, no queued partial writes, and live routing naming exactly
   the committed layout's nodes (after a raw handoff or map-only reassignment they differ, and the
   export would read nodes that retirement never reaches). The routing check runs under the move
   reservation, which keeps handoffs over those nodes out for the rest of the resize.
2. Before any network call, it raises a **resize write fence** and briefly takes the mutation
   barrier exclusively, so every accepted write lands before the export and every later add,
   upsert, remove, bulk load, or resync is refused. A resize that finds the fence already raised
   refuses to start. It then registers the targets, reserves every participating endpoint in the
   move ledger, and records `Begin`. A committed intent left by a failed `Finish` is finished
   first; an uncommitted intent left by an earlier attempt of the same operation is aborted first.
3. It fingerprints every source position, builds the staged layout at generation `N + 1` on the
   empty targets, and streams the corpus into it.
4. When the current layout is durable (each slot reports it through the additive `durable` field
   on `Flush`), every target position must commit an ADR-181 `Seal`, which a volatile target
   refuses. Each target's content fingerprint and count are checked against what was loaded.
5. It **retires every node of the current layout**. Each retirement reports its slots'
   fingerprints, which must equal those taken in step 3, so a write that reached a source by any
   path after the export (for example after a node restart dropped this coordinator's lease) fails
   the resize instead of being lost. From here until installation, reads of the old layout fail
   loud at the nodes.
6. It records `Ready` with the target evidence and commits. An ambiguous commit is resolved by
   reading the committed layout back.

`install_remote_resize` then takes `&mut self` briefly and makes no network call, since request
threads may be waiting on that exclusive lock; preparation already confirmed the commit. It installs
the exported logical ids as an authoritative, converged directory, swaps the ring, shards, handoff
handles, metrics, and generation, clears PITs, and lowers the fence. `finish_remote_resize` records
`Finish`. If installation fails, or a committed preparation is dropped (for example by a cancelled
caller), the retired old nodes keep refusing requests, so nothing answers from the superseded
layout; a restart routes to the committed one.

Only this coordinator's `Commit` can make the new layout the layout of record, because startup
aborts every uncommitted intent. So a failure before `Commit` is proposed always unretires the old
nodes, aborts the intent, and reopens writes; a node whose unretire fails keeps refusing (failing
loud, never answering wrongly) until startup lifts it. After `Commit` was proposed, the old nodes
are unretired and writes reopen only when the control plane proves the commit did not apply: the
abort was accepted and the served layout is still the committed one. Otherwise writes stay fenced
and the old nodes stay retired until a coordinator restart routes to the committed layout. In
every case the targets keep an unrouted staged layout that must be wiped before reuse.

The HTTP route holds the REST write serializer only until the fence is up, returns the single
administrative admission slot (which health probes share) once it holds the exclusive topology
guard, and holds a dedicated remote-resize permit that shutdown joins.

### Startup

A resolve-only coordinator treats the committed document as the layout of record: it connects
every node at the committed placement generation (threaded through a new
`ClusterConfig::remote_placement_generation`) and adopts the committed shard count, even when
`--shards` differs. CLI-seeded and static modes still require their CLI topology to match; a
static coordinator never resolves a resize (it does not route by the committed layout) and refuses
to start while one is recorded.

Before choosing any route, `recover_durable_resize` runs, like durable-move recovery:

1. An uncommitted intent is aborted, but only after this coordinator has claimed every node of the
   layout it would have retired. A coordinator still running that resize holds those claims, so
   this startup fails instead of aborting the resize underneath it.
2. A committed intent is finished.
3. Every node of the committed layout must serve. A committed resize retires only nodes outside
   its layout, so a retired node inside it was retired by a resize that never committed; its
   retirement is lifted, but only after re-reading, while holding the node's claim, that it is
   still in the committed layout and no resize is in flight. Positions still on an unseeded
   genesis placeholder are skipped, so a fresh quorum bootstraps as before.

After assembly, the coordinator attests that the layout it connected to matches the committed
shard count, placement generation, and each position's primary node, and fails to start
otherwise.

### API

`POST /_cluster/resize` accepts `targets: [{id, endpoint}]` on a resolve-only remote coordinator,
and requires it there. An in-process coordinator rejects `targets`, and other remote topologies
keep the `501`. The operation ID, precondition, records, and status reads are ADR-179's. `targets`
is part of the request identity, and the ID's FNV-1a hash is the control-plane intent key.

## Review history

Eleven Codex rounds found about 33 real issues. Most were ordinary hardening of new code (the
export stream, the staged load path, the durability proof) and converged. One class kept
regenerating: every round from the eighth on found another way the old layout could keep
answering after the commit (an unproven commit, a failed installation, a dropped preparation, the
commit-to-install window). Each fix had added another flag in the resizing coordinator's memory,
because the old nodes themselves never learned they were superseded; the protection across
processes rested on a volatile 30-second lease.

The design was therefore revised to the durable node retirement above. It replaces the in-memory
serving refusal, the abandonment guard, and the post-cutover volatile fencing of retired slots,
and it adds the fingerprint check that makes the copy's completeness independent of lease
exclusivity. A separate cluster of findings (locks held across network calls) traced to a
pre-existing server-wide hazard, request threads blocking the async runtime on synchronous locks,
which is tracked outside this ADR.

The first review of the revised design found three more, each fixed with a mutation-checked test:
startup resolution ran before a fresh quorum was seeded and refused its placeholder assignments;
the retirement sweep could lift a retirement that a concurrently committed resize had just made;
and a resize after an uncommitted route change would export from one node and retire another. The
next review found that the pre-built durable startup path ignored a retirement record and that the
routing check ran before the move reservation that excludes concurrent handoffs; both are fixed.
The one after found two availability gaps, both fixed: retired nodes stayed registered as
allocation candidates, and a static coordinator swept a committed layout it does not route by.

## Alternatives

- **Stage beside the old layout on the same nodes.** Deferred: it needs per-slot placement
  configuration, a new node format, and layout-aware slot addressing and GC.
- **Dual-write while copying.** Rejected for this step: every write path and its failure handling
  would need a second fan-out and a green-divergence repair. Refusing writes for the copy window
  matches the in-process resize, which also excludes writes for its rebuild.
- **Hold `&mut self` for the whole resize.** Rejected: reads would stop for an `O(corpus)` network
  copy, and it would not resolve an ambiguous commit.
- **Enforce layout authority in the resizing coordinator's memory.** Rejected after review: every
  crash, cancellation, lost reply, or lease lapse between the commit and installation needed its
  own patch, and no patch could protect against another process.
- **Delete retired nodes' data automatically.** Rejected: retirement already makes them refuse
  everything, and the previous layout remains a manual rollback source until an operator wipes it.

## Consequences

A remote cluster changes shard count online with one API call. Writes are unavailable for the copy
window; reads continue during the copy and fail loud (`503`) only from the retirement until
installation. The resize needs as many spare nodes as the new layout uses, and its old nodes stay
retired until wiped.

Replication factor above 1, same-node staging, a catch-up copy that keeps writes open, and a
targeted online split remain [roadmap](../roadmap.md#remote-cluster-resize) work. The ADR-179
governor stays in-process, because provisioning target nodes is an external decision.

## Proof

- **Retirement tests:** a retired node refuses reads, writes, and adoption; its handshake reports
  the retiring operation; the record survives restart; only the retiring operation lifts it,
  durably; a mismatched layout or a missing successor is refused.
- **Startup resolution tests:** an uncommitted intent is aborted and its nodes serve again; a
  second coordinator cannot abort a resize whose nodes another coordinator still holds; a committed
  intent is finished and its old nodes stay retired; a leftover retirement inside the committed
  layout is lifted. A gRPC test runs the same resolution against real nodes.
- **Fingerprint check:** a retirement must report every hosted position unchanged since the export.
- **Wire and export tests** cover the collector (completion, ordering, count, limit, identity,
  visitor refusal), deadline-bounded snapshots and page reads, and exact, deduplicated, versioned,
  tagged gRPC exports.
- **Staged-load tests** show threshold-sized segments, compaction to the policy without losing
  rows, a single source-store write when the stream closes, a failed sidecar write failing the
  load, and every job holding the installation barrier.
- **Resize-intent state-machine and Raft restart tests** cover idempotent transitions, invalid and
  co-located intents, the move exclusion, the format fence, and an `RRL5` restart.
- **gRPC resize tests:**
  - K=3 moves to K=5 with no false negatives against the brute-force oracle and the pre-resize
    results, carrying versioned upserts and removes;
  - writes are refused between prepare and install and reopen on the new layout;
  - the old nodes refuse reads after the commit, and a fresh coordinator pointed at them is
    refused;
  - failures before `Commit` (a lost `Begin`, a refused `MarkReady`, an unreachable control plane)
    return the old nodes to service and reopen writes;
  - a refused `Commit` that the control plane proves did not apply returns the old layout to
    service;
  - after an ambiguous commit or an abandoned committed preparation, the old nodes refuse reads and
    a retry cannot reopen writes;
  - installation needs no control-plane call;
  - a coordinator attached to populated shards regains exhaustive delivery after the rebuild;
  - a shared coordinator, a co-located target, and a dirty target are refused cleanly.
- **Handler tests** cover `targets` validation by topology and origin, record the failed remote
  operation, and show a remote resize returns the administrative slot health probes share while
  holding the permit shutdown joins.

The design-changing mutations were each checked against these tests: ignoring retirement in slot
lookup, not persisting it, skipping the retirement, skipping the claims before an abort, and never
unretiring on failure or at startup.

**See also:** ADR-043, ADR-078, ADR-086, ADR-175, ADR-176, ADR-179, ADR-181.
