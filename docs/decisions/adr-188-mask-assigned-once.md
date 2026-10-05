# ADR-188 — The top-64 mask is assigned once

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The top-64 common mask is baked into every compiled row. A row stores its required top-64
features only as bits of that mask, and its cost class (so its default visibility) depends on
which features hold a bit. The design has always said the mask is frozen after the first
finalize. Nothing enforced it on the single-node engine.

- **Any later finalize re-ranked it.** `Dict::finalize_mask` cleared all 64 bits and reassigned
  them by current frequency on every call. `bulk_ingest` guarded its call; the initial-build path
  (`try_build_from_queries`) did not, and it could run on an engine that already held rows. After
  a re-rank, the bits in every existing row meant different features, so the exact verifier
  rejected true matches. A stored query stopped matching in **both** `include_broad` modes, and
  the damage was written to disk.
- **The server took that path on every restart.** With `--load-file`, single-node startup always
  ran the initial build, including on a reopened, populated `--data-dir`. Cluster mode skipped the
  load in that case; single-node did not. Each restart also stored every query in the file again.
  Frequencies only grow, so a reload moves the ranking as a matter of course.
- **A row could straddle the first assignment.** A live insert made before the first finalize is
  compiled with no mask and is default-visible. A bulk ingest does not flush the memtable, so that
  insert stayed in the WAL as text. After a restart, replay compiled it again under the finalized
  mask; if its only term now held a bit, it was class C and gone from default reads.

## Decision

1. **`Dict::finalize_mask` assigns once.** It is a no-op on a finalized dictionary, including one
   restored from disk (the flag is persisted). The invariant is enforced in the dictionary, the
   one place every caller goes through, not at each call site. Every path that legitimately ranks
   a mask does so on a fresh dictionary (cluster build, a cluster vocabulary rebuild, the shard
   server's seed), so none needs to re-rank.
2. **A later build is an ordinary batch.** `try_build_from_queries` on an engine that already has
   a mask compiles against it, exactly like `bulk_ingest`.
3. **The memtable is sealed before the first assignment.** On an engine with a WAL, the first
   batch that will assign the mask seals a non-empty memtable first (`seal_before_first_mask`).
   The rows' classes are then stored in a segment and their WAL frames retired, so no row is ever
   compiled on both sides of the assignment. The seal is all-or-nothing: the segment is built
   from a copy, and the memtable is replaced only once the manifest names it. If the segment
   write or the commit fails, the engine is exactly as it was, the batch fails with the mask
   unassigned, and a retry seals again. It deliberately does not reuse `flush`, which hands the
   memtable over before it knows whether the write succeeds.
4. **The first batch is refused while a failed flush's rows exist only in memory.** A flush
   whose segment write fails leaves its rows in an in-memory segment (ADR-051), durable only as
   WAL frames. Before the mask exists those rows are in the same position as memtable rows, but
   the seal cannot make them durable, and it must not retire their frames. So the batch fails
   with the mask unassigned; a restart replays the rows into the memtable, where the next batch
   seals them. (What a later *successful* flush does to such rows is a separate, older defect in
   `flush` itself and is not changed here.)
5. **`--load-file` seeds an empty engine.** The single-node server skips the file, with a warning,
   when the reopened data directory already holds queries (`preload_queries`), as cluster mode
   does.

## Alternatives considered

- **Record each row's class in the WAL insert frame** and have replay honor it. It fixes the
  straddling row without a flush but changes the WAL format, and does nothing for the re-rank.
- **Guard each call site.** One unguarded caller was the bug.
- **Upsert the load file by id on every restart.** A different feature: it would overwrite live
  edits with the file's contents. Operators who want that can post the file to `_bulk`.

## Consequences

- A stored query's mask bits and class are stable for the life of the store. Live compile and WAL
  replay agree on the mask for every row.
- The first bulk ingest on a durable engine with live rows pays one flush.
- Restarting with `--load-file` no longer changes the store. To load a changed file into a
  populated store, send it through `_bulk`.
- **A store that already went through a re-rank is not repaired by this change**, and cannot be
  detected from its segments: rows compiled before the re-rank hold stale mask bits. A rebuild
  from retained source repairs them. A vocabulary change does one, and so does the next
  compiler-semantics migration on open.
- The mask is still chosen by whatever the first finalize saw, with no frequency floor. Changing
  how it is chosen remains a blue/green rebuild concern.

## Proven

- `dict::tests::finalize_mask_assigns_once_and_never_reranks`: inverting the ranking and adding
  a far more frequent feature moves no bit.
- `tests/persistence/mask_stability.rs`: a second, much heavier build on a reopened engine
  leaves a stored two-mask-bit query matching (and still requiring both features), across
  another reopen; a query inserted before the first finalize is default-visible before and after
  a restart while a later copy stays opt-in, for both batch entry points; a seal that fails at
  the segment write or at the commit changes nothing, a retry succeeds, and no acknowledged
  query is lost by a later flush; the first batch is refused while a failed flush has
  unpersisted rows, and a restart recovers them; an in-memory engine is not sealed.
- `server::preload::tests`: two restarts with the same load file leave the query count and a
  match result unchanged.

**See also:** ADR-017 (bulk ingest bypasses the WAL), ADR-051 (fail-closed flush), ADR-056 and
ADR-187 (a stored query never leaves default reads on its own), ADR-184 (the manifest records the
feature model).
