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
addressing stay unchanged.

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
`ClusterEngine::export_live_corpus` dedups replicated rows across positions.

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
  count, generation, and assignments in one transition and bumps every position's assignment
  generation.
- **`Begin` on a move** is refused while a resize intent exists.

A resize command raises the snapshot fence to control format 5, which never lowers. It also
installs an `RRL5` Raft log header before the first resize entry. Older binaries, which accept at
most format 4 and `RRL4`, reject both instead of silently dropping the intent.

### Orchestration

`prepare_remote_resize` runs under a shared reference, so reads keep serving the old layout:

1. It validates the request and the cluster: remote, assignment-routed, exclusively owned by this
   coordinator, replication factor 1, no queued partial writes. Exclusive shard claims keep every
   other coordinator off the source slots, which the local write fence alone cannot.
2. Before any control-plane or mesh call, it raises a **resize write fence** and briefly takes the
   mutation barrier exclusively. Every mutation checks the fence under that barrier, so each
   accepted write lands before the export, and every later add, upsert, remove, bulk load, or
   resync is refused. The HTTP route holds the REST write serializer only until the fence is up.
   Request threads blocked on that serializer could otherwise starve the runtime these calls
   need, and queued writers are then refused at once instead of waiting out the copy. Vocabulary
   and alias rebuilds check the fence before asking for exclusive access; queueing for it behind
   the copy would stall every read. The route also returns the single administrative admission
   slot, which health probes share, once it holds the exclusive topology guard that keeps other
   resizes out, so `/_health` keeps answering while reads continue.
3. It registers the targets, reserves every participating endpoint in the move ledger, and
   records `Begin`. A committed intent left behind by a failed `Finish` is finished first, after
   its retired slots are fenced again, when the served layout is exactly the one it committed; an
   uncommitted intent left by an earlier attempt of the same operation is aborted first.
4. It builds the staged layout with the ordinary remote builder at generation `N + 1`, refusing
   targets that already hold data.
5. It streams the corpus into the staged layout, placing byte-bounded, versioned batches under the
   new ring. Each target position receives one client-streaming `StageIngest` call: the target seals
   segments of its memtable flush threshold as rows arrive and writes its source store and
   checkpoint sidecar once, when the stream closes. A dropped, unfinished load cancels the call
   rather than closing it, so a target never persists a partial load as complete. Each segment
   and finish job holds the node's installation barrier, so a cancelled call's detached worker
   can never write over a slot that adoption, recovery, or removal replaced. Placement
   force-accepts, as log replay does, so a stored class-D query survives even when the current
   admission knob is off; a stored query that no longer parses or places fails the resize.
6. When the current layout is durable (each slot reports it through the additive `durable`
   field on `Flush`), every target position must commit an ADR-181 `Seal`, which a volatile
   target refuses. Only then are its content fingerprint and count checked against what was
   loaded and recorded as `MarkReady`, so evidence never names rows a target restart could lose.
7. It commits. An ambiguous commit is resolved by reading the committed layout back.

`install_remote_resize` then takes `&mut self` briefly. It swaps the ring, shards, handoff handles,
metrics, and generation, clears PITs, and lowers the fence. `finish_remote_resize` fences every
retired slot, so a stale writer fails loud, and records `Finish`.

A failure aborts the intent. Only this coordinator's `Commit` can make the new layout the layout of
record, since startup aborts every uncommitted intent, so a failure before `Commit` is proposed
always reopens writes, even when the abort itself is lost; retrying the same operation clears the
leftover intent. After `Commit` was proposed, writes reopen only when the control plane proves it
did not apply: the abort was accepted and the served layout is still the committed one. The old
layout then keeps serving and is writable, and the targets keep an unrouted staged layout that must
be wiped before reuse. An unproven outcome keeps writes paused, because consensus may already name
the new layout; a coordinator restart resolves the recorded intent and routes to the committed
layout.

### Startup

A resolve-only coordinator treats the committed document as the layout of record:

- It connects every node at the committed placement generation. A new
  `ClusterConfig::remote_placement_generation` threads it into the plain and replicated builders
  that previously hard-coded the initial generation.
- It adopts the committed shard count, even when `--shards` differs.
- Once it holds exclusive claims on the shards it routes to, it aborts an uncommitted intent and
  finishes a committed one, then logs which nodes to wipe or decommission. Claiming first means a
  live coordinator still running the resize makes this startup fail to connect instead of having
  its resize aborted.
- Before resolving an intent, and before serving at all, it attests that the layout it connected
  to matches the committed shard count, placement generation, and each position's primary node.
  A coordinator that read the topology just before another coordinator committed fails to start
  instead of finishing the intent and serving the retired layout.

CLI-seeded and static modes still require their CLI topology to match.

### API

`POST /_cluster/resize` accepts `targets: [{id, endpoint}]` on a resolve-only remote coordinator,
and requires it there. An in-process coordinator rejects `targets`, and other remote topologies
keep the `501`. The operation ID, precondition, records, and status reads are ADR-179's. `targets`
is part of the request identity, and the ID's FNV-1a hash is the control-plane intent key.

## Codex review

The first review found seven real issues, all fixed with regression tests; the class-D,
ambiguous-commit, and stalled-reader fixes were mutation-checked:

- an ambiguous commit reopened writes on the old layout;
- target durability was unproven before the commit;
- the staged load re-applied the class-D admission knob;
- the replicated builder ignored the committed generation;
- a failed `Finish` blocked every later resize and move;
- a stalled export reader could pin the server producer and snapshot permit, and the client did not
  bound stream consumption by its deadline.

The second review found six more, all fixed:

- a volatile target could attest a checkpoint; targets must now persist to disk whenever the
  source layout does, proved by an ADR-181 `Seal` and detected through the additive `durable`
  field on `Flush`;
- the export silently kept the first of several disagreeing copies; copies must now match
  exactly, or the export fails;
- the operator docs implied a failed response meant nothing committed; they now require checking
  the committed state before wiping either layout;
- startup could abort another live coordinator's in-flight resize; resolution now runs only after
  this coordinator holds exclusive shard claims;
- the load batcher could overshoot its byte budget; a document that would overflow now starts the
  next batch;
- the export did not reclaim a restarted source's coordinator lease; opening the stream (never
  consuming it) is now retried once after a reclaim.

The third review found five more, all fixed with regression tests; the attestation, exclusivity,
fence-ordering, and staged-load fixes were mutation-checked:

- startup could finish a committed intent while serving the layout it retired; it now attests
  the served layout first;
- a shared (non-exclusive) coordinator could resize while another coordinator still wrote to the
  source slots; resize now requires exclusive ownership;
- the HTTP route held the REST write serializer for the whole copy, pinning runtime workers of
  every queued writer; it is released once the fence is up;
- every load batch rewrote the target's whole source store and added a small segment; the load is
  now one `StageIngest` stream per target with full-size segments and a single store write;
- the export's id snapshot and page reads waited on the engine lock without a deadline; every lock
  wait and the snapshot sort now observe it.

The fourth review found two more, both fixed with mutation-checked regression tests:

- a staged load ignored a failed checkpoint-sidecar write, so a durable target loaded from a
  volatile layout (which skips `Seal`) could reopen empty; the load now fails instead;
- a malformed target endpoint was registered as a data node before the connect failed, leaving
  membership that later rebalances would target; targets are now validated as mesh origins, like
  node registration.

The fifth review found one more, fixed with a mutation-checked regression test: a cancelled
`StageIngest` detached its blocking segment and finish workers without the node's installation
barrier, so an adoption, recovery, or removal could replace the slot while the old engine still
wrote the same files. Each worker now confirms the slot is unchanged and unfenced under the barrier
and holds it until the job finishes, as `Seal` does.

The sixth review found three more, all fixed with mutation-checked regression tests:

- the route held the single administrative admission slot for the whole copy, so health probes
  timed out and a readiness or liveness probe could pull or restart the coordinator mid-resize;
  it is returned once the exclusive topology guard is held;
- the REST write serializer stayed held through target registration, the durability probe, and
  `Begin`, so request threads waiting on it could starve the runtime those calls needed; the fence
  is now raised, and the serializer released, before any network call;
- a lost `Begin` reply returned without aborting, stranding a `Preparing` intent that refused even
  a retry of the same operation; any failure before `Commit` now reopens writes, and a retry of
  the same operation aborts its own leftover intent.

## Alternatives

- **Stage beside the old layout on the same nodes.** Deferred: it needs per-slot placement
  configuration, a new node format, and layout-aware slot addressing and GC.
- **Dual-write while copying.** Rejected for this step: every write path and its failure handling
  would need a second fan-out and a green-divergence repair. Refusing writes for the copy window
  matches the in-process resize, which also excludes writes for its rebuild.
- **Hold `&mut self` for the whole resize.** Rejected: reads would stop for an `O(corpus)` network
  copy. The write fence gives the same consistency while reads continue.
- **Delete retired slots automatically.** Rejected: fencing makes stale writes fail loud without
  destroying the previous layout, which remains a manual rollback source until an operator
  decommissions it.

## Consequences

A remote cluster changes shard count online with one API call. Writes are unavailable for the copy
window, and the resize needs as many spare nodes as the new layout uses.

Replication factor above 1, same-node staging, a catch-up copy that keeps writes open, and a
targeted online split remain [roadmap](../roadmap.md#remote-cluster-resize) work. The ADR-179
governor stays in-process, because provisioning target nodes is an external decision.

## Proof

- **Wire tests** cover the export collector: completion, ordering, count, limit, identity, and
  visitor refusal. Shard tests show the export snapshot collapses duplicate rows and gives up at
  its deadline while the engine lock is held, for both the id snapshot and page reads.
- **Staged-load tests** show a stream seals threshold-sized segments (not one per request), loads
  nothing from an empty stream, refuses a second shard id, writes a durable slot's source store
  only when the stream closes, fails when its checkpoint sidecar cannot be written, and runs each
  job under the installation barrier, refusing a slot replaced since the stream began.
- **Resize-intent state-machine tests** cover:
  - atomic idempotent commit and generation bumps;
  - invalid, co-located, unregistered, and unnormalized intents;
  - an assignment race blocking commit;
  - a single intent with no concurrent move;
  - the format fence and its downgrade refusal.
- **Raft restart test.** A durable control node installs `RRL5` on the first resize command,
  keeps a `Ready` intent across restart, then commits and finishes it.
- **gRPC export tests.** Exports are exact, deduplicated, versioned, and tagged; many small frames
  complete, and an oversized source fails loud.
- **gRPC resize tests:**
  - K=3 moves to K=5 on fresh nodes with no false negatives against the brute-force oracle and the
    pre-resize results;
  - a versioned upsert and a remove carry over;
  - writes reopen on the new layout;
  - the export count is exact after the cutover;
  - every write kind is refused between prepare and install;
  - a co-located target, and a retired node that still holds data, are refused cleanly: nothing is
    committed, the intent is aborted, and writes stay open;
  - the fenced callback runs only once writes are refused, and the fence check vocabulary rebuilds
    rely on fails until install;
  - a shared coordinator is refused;
  - a coordinator that still serves the retired layout refuses to resolve the committed intent;
  - the fence goes up before the first control-plane call, and writes reopen after a failure
    before `Commit`;
  - a lost `Begin` reply with a failed abort reopens writes, refuses a different operation, and
    lets a retry of the same operation complete.
- **Startup recovery tests.** An uncommitted intent is aborted and the committed one finished.
- **Handler tests** cover `targets` validation by topology and origin, record the failed remote
  operation, and show a remote resize returns the administrative slot health probes share.

**See also:** ADR-043, ADR-078, ADR-086, ADR-175, ADR-176, ADR-179, ADR-181.
