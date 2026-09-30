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
live logical ids under the engine lock. It then fetches documents (source, stored version, raw tags)
in pages of 256 under short lock holds, and sends byte-capped frames through a bounded channel
followed by an explicit completion frame. The export fails loud when:

- a document disappears or its source disagrees with its exact row;
- the deadline passes;
- a single document exceeds the result cap;
- the receiver goes away.

The client checks frame identity, a stable total, strictly increasing ids, and a completion count
equal to everything delivered. It never retries a partially consumed stream. It shares the node's
single snapshot permit with `LiveLogicalIds`. `ClusterEngine::export_live_corpus` dedups replicated
rows across positions.

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

1. It validates the cluster: remote, assignment-routed, replication factor 1, no queued partial
   writes.
2. It registers the targets, reserves every participating endpoint in the move ledger, and
   records `Begin`.
3. It raises a **resize write fence** and briefly takes the mutation barrier exclusively. Every
   mutation checks the fence under that barrier, so each accepted write lands before the export,
   and every later add, upsert, remove, bulk load, or resync is refused. The HTTP route also holds
   the REST write serializer.
4. It builds the staged layout with the ordinary remote builder at generation `N + 1`, refusing
   targets that already hold data.
5. It streams the corpus into the staged layout in byte-bounded, versioned batches placed under
   the new ring.
6. It checks each target position's content fingerprint and count against what was loaded, and
   records `MarkReady`.
7. It commits. An ambiguous commit is resolved by reading the committed layout back.

`install_remote_resize` then takes `&mut self` briefly. It swaps the ring, shards, handoff handles,
metrics, and generation, clears PITs, and lowers the fence. `finish_remote_resize` fences every
retired slot, so a stale writer fails loud, and records `Finish`.

A failure before the commit aborts the intent and lowers the fence, leaving the old layout serving
and writable. The targets keep an unrouted staged layout that must be wiped before reuse.

### Startup

A resolve-only coordinator treats the committed document as the layout of record:

- It connects every node at the committed placement generation. A new
  `ClusterConfig::remote_placement_generation` threads it into the builder that previously
  hard-coded the initial generation.
- It adopts the committed shard count, even when `--shards` differs.
- Before routing, it aborts an uncommitted intent and finishes a committed one, then logs which
  nodes to wipe or decommission.

CLI-seeded and static modes still require their CLI topology to match.

### API

`POST /_cluster/resize` accepts `targets: [{id, endpoint}]` on a resolve-only remote coordinator,
and requires it there. An in-process coordinator rejects `targets`, and other remote topologies
keep the `501`. The operation ID, precondition, records, and status reads are ADR-179's. `targets`
is part of the request identity, and the ID's FNV-1a hash is the control-plane intent key.

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
  visitor refusal.
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
    committed, the intent is aborted, and writes stay open.
- **Startup recovery tests.** An uncommitted intent is aborted and the committed one finished.
- **Handler tests** cover `targets` validation by topology and record the failed remote
  operation.

**See also:** ADR-043, ADR-078, ADR-086, ADR-175, ADR-176, ADR-179.
