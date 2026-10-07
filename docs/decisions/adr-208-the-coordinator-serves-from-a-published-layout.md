# ADR-208 — The coordinator serves from a published layout

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The coordinator kept what it serves from as separate fields of `ClusterEngine`: the normalizer,
the dictionary, the vocabulary, the ring, the shards, the source-sidecar names, the handoff
handles and the placement generation. A vocabulary change or a resize replaced them one
assignment after another, which is safe only while nothing else can look. So those operations
take the engine exclusively (`&mut self`), and the server takes a write lock on the whole
engine for the length of the rebuild. Every search, write and health probe waits for it, and a
search has to hold a read lock on the engine for each title it matches, which is what made the
search pool need a gate (ADR-207).

The fields always change together and are written in two places only (the in-process rebuild
and the cutover of a remote resize). They were read in about 240 places across 30 files.

## Decision

1. **The serving state is one value, the layout.** `Layout` holds the normalizer, the
   dictionary, the vocabulary, the ring, the shards, the source-sidecar names, the handoff
   handles and the placement generation. It is immutable once built.
2. **It is published atomically.** The engine holds the current layout behind an atomic
   pointer swap. A rebuild builds a whole new layout and publishes it in one step; the old one
   lives until the last operation that loaded it returns.
3. **One operation, one layout.** An operation that routed a title by one layout and matched it
   in another would be wrong, and a rebuild may publish at any moment. So every function that
   reads the layout is one of three kinds:
   - a **helper** is handed `layout: &Layout` and never loads another;
   - an **operation** (a public method) loads once, at its entry, and calls no other method
     that loads;
   - a **writer** (`&mut self`, or the engine by value while it is assembled) holds the engine
     alone and may load whenever it likes.
   A public method that other operations call has an inner form that takes the layout, and the
   public method is a wrapper that loads and calls it.
4. **The rule is checked.** `coordinator/tests/layout_discipline.rs` reads the coordinator's
   source and fails when a helper loads, when an operation loads twice, or when an operation
   calls another method that loads.
5. **Accessors hand out shared handles.** `ClusterEngine::normalizer`, `dict` and `vocab`
   return `Arc`s, because a reference could not outlive the layout it came from.

This is the published-snapshot design ADR-207 names as the one the coordinator is meant to
reach: Lucene's `ReferenceManager`, and this server's own single-node mode.

## What this step does not change

A rebuild still takes the engine exclusively and the server still holds its cluster lock and
the search pool's gate, so nothing is served during a rebuild yet. This step makes the next one
possible: build the new layout beside the old one while only writes are held out, publish it,
and then take the cluster lock and the gate off the search path (the roadmap item "In-process
rebuilds that keep serving").

## Alternatives considered

- **Keep the fields and guard each with its own lock or atomic.** A reader could then see the
  new ring with the old shards. They have to move together.
- **Pin the layout per thread instead of handing it down.** A thread-local pin would make
  nested loads return the same layout without changing any signature. It fails for the same
  reason the first fix of ADR-207 did: an operation fans out to other threads, and those would
  load for themselves.
- **Load wherever a field is needed.** It compiles and is correct while a rebuild is
  exclusive. It is wrong the moment a rebuild is not, and nothing would show it until a search
  crossed a swap.

## Consequences

- While an operation runs, the layout it loaded stays alive, shards included. Once rebuilds
  stop being exclusive, an old layout will be freed when the last operation that holds it
  returns, not at the swap.
- Loading costs one reference-count increment per operation, which replaces nothing yet and
  will replace the read lock the server takes per title.
- Helpers that no longer touch the engine became associated functions that take the layout
  (`route`, the match passes, the corpus gather).
- `matching.rs` was split; its introspection and document reads are in `matching/inspect.rs`.

## Proven

- No behaviour change: the cluster unit tests, `cluster_oracle`, `cluster_durability_oracle`
  and `cluster_grpc_oracle` are unchanged and pass.
- `layout_discipline.rs` passes on the tree, and fails with the right message when a helper is
  made to load, when an operation is made to load twice, and when an operation is made to call
  another that loads.

**See also:** ADR-046 (the vocabulary rebuild), ADR-078 (in-process resize), ADR-180 (remote
resize), ADR-207 (the gate this design retires).
