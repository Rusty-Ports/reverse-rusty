# ADR-185 — Reader-atomic cluster upsert

> [Clustering — core & transport decisions](areas/clustering-core-and-transport.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A cluster upsert was atomic for crash replay (one coordinator log frame, ADR-070) but not for
readers. `apply_upsert` tombstoned the id on every shard and then inserted the new version on its
placement shards. Each step published its own shard snapshot, and ordinary reads take no barrier,
so a title matched between the two passes saw **neither** version: a silent false negative on
every `PUT /_doc` re-put and every `_bulk` index item, for any title that satisfies both the old
and the new version. The gap existed with one shard and with an unchanged placement. ADR-067's
"never both, never neither" was claimed for the cluster (ADR-070, ADR-075, ADR-117) but held only
single-node.

Inserting first does not fix it. Ownership is decided per row from that row's own placement: the
lowest common position for a selective row, the first routed position for an always-visible
replicated row, and the per-title broad evaluator for a broad row. Two versions with different
placements can therefore both be emitted (the exact ranked merges fail closed on the duplicate id),
and for some mode changes every fixed order of per-shard steps leaves a title with neither.

## Decision

1. **A per-shard atomic replace.** `Shard::replace_placed` tombstones the prior live copies and
   inserts the new version as one visibility step: one engine critical section
   (`Engine::replace_extracted_with_placement`: capture, insert, then tombstone, so a rejected
   replace never deletes), one translog frame (a whole `Upsert`), and one published snapshot. The
   method is required on the trait, with no default, so no implementation can fall back to delete
   plus insert. `ReplicatedShard` applies it to the primary and mirrors it to in-sync replicas;
   `RemoteShard` uses the new `ReplaceExtracted` RPC.
2. **The placement-preserving path needs no reader coordination.** The coordinator asks each
   placement shard to replace *only if every live copy it holds already carries the new
   placement*. When all do, the upsert is complete: the row's owner for any title is the same
   shard before and after, and that shard switched versions in one step. This is the common case:
   a re-put, a tag or version edit, a bulk re-index. A new id (per an authoritative logical-id
   directory) is simply placed.
3. **A placement-moving upsert runs inside an optimistic move fence.** A shard that declines the
   conditional replace changed nothing, so the coordinator can still fence the whole rewrite. The
   fence is a sequence counter: the mover makes it odd before its first shard call and even after
   its last; a reader samples an even value, fans out, and re-checks, repeating the read when the
   value changed. Readers pay two atomic loads and never block a writer; a read that overlaps
   four moves excludes movers for one pass rather than spin. A read that starts during a move
   waits for it, but only until its own deadline: a bounded top-K read then fails with its
   deadline error rather than being held by the mover. Inside the fence the new placement
   is written before stale copies are tombstoned, so an unfenced point read (`GET /_doc`) always
   finds a version.
4. **Strays are swept.** When a repair is queued for the id or the directory is not converged, a
   shard outside the new placement may hold a stale copy. After a placement-preserving replace the
   coordinator tombstones the id on the other shards without the fence: removing an extra copy can
   only take a duplicate away.
5. **Repair and recovery use the same step.** `resync` and replica catch-up re-drive an `Upsert`
   as an unconditional replace on a position the placement covers and as a tombstone elsewhere.
   Self-restart replays the single translog frame through the same engine funnel (live ≡ replay).
6. **Wire.** `ReplaceExtracted` is additive. `FetchTranslog` gains an `upsert` entry so peer
   recovery ships the whole frame; a receiver that predates it decodes an unset entry and fails
   its recovery loud. `DictFingerprintReply.atomic_replace` attests the capability, and a
   coordinator refuses to connect to a shard server that does not, because against it an upsert
   could only be the reader-visible two-step.

## Alternatives considered

- **Generation-tagged insert-first** (a coordinator-minted generation on every row; merges keep the
  newer duplicate). It changes the log, translog, segment and wire formats, and a duplicate
  resolved after per-shard top-K truncation can still leave the merged page one row short, so
  exact ranked reads would have to over-fetch or mark totals approximate.
- **Epoch MVCC** (rows carry added/removed epochs; readers pin a watermark). Complete, and large:
  it needs a reader-epoch registry so compaction never purges a row a pinned reader still needs.
- **A reader/writer lock around moves.** Simple in-process, but readers would take a shared lock
  on every title and a mover would wait for every in-flight read, including slow remote ones.
- **Fencing every upsert.** One primitive, no new RPC, but a bulk re-index would invalidate every
  concurrent read (the concern behind RR-020 and RR-062).

## Consequences

- An unfenced read during an upsert returns the query exactly once: never neither, never both.
  Ordinary reads still take no lock and never wait on a placement-preserving write.
- A read that overlaps a moving upsert is repeated, and one that starts during a move waits for
  it, up to its deadline if it has one. On remote shards a move spans several RPCs, so reads
  without a deadline wait for them.
- An upsert of an existing id costs one conditional RPC per placement shard instead of one delete
  per shard plus the inserts; a move adds its rewrite.
- **Upgrade order: shard servers before the coordinator.** A new coordinator refuses an old shard
  server at connect. Old and new shard servers can peer-recover from each other only until the
  first atomic replace is logged; an old target then fails that recovery loud.
- A partial failure still leaves a documented window (ADR-047): a shard whose replace failed keeps
  serving the old version, and a failed tombstone leaves a stale copy until `resync`, during
  which exact ranked reads fail closed on the duplicate id.

**See also:** ADR-067 (single-node atomic upsert), ADR-070 (cluster REST surface and the upsert
frame), ADR-047 (partial apply and resync), ADR-109 (ownership), ADR-113 (the PIT barrier, which
fenced reads keep using), ADR-177 (per-id write locks).
