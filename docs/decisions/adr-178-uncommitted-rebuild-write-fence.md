# ADR-178 — Uncommitted-rebuild write fence and source-sidecar reclamation

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted (2026-09-29)

## Context

An in-process resize (ADR-078) and a vocabulary change (ADR-046) share one blue/green rebuild: build
the green shards, swap the serving ring/model/shards under `&mut self`, update control state, and
then checkpoint the green layout. A failure in either of the last two steps leaves the previous
manifest authoritative while the process keeps serving the green layout. ADR-167 and the alias
import retry path make an identical retry heal that window.

Nothing stopped writes during the window. A live add or upsert is stamped with its placement
decision, including the serving placement generation, and appended to the coordinator log. If the
process crashed before a retry or checkpoint committed the green layout, reopen selected the old
manifest and replay rejected the green-stamped record with `PlacementDecisionMismatch`. The cluster
became unopenable and the acknowledged write was stranded in the log. A durability test reproduced
this for both resize and alias import.

Separately, every committed rebuild wrote a generation-named source sidecar
(`sources_g<generation>.dat`) for each primary and in-process replica directory, but nothing removed
the sidecar it superseded. Each resize or vocabulary change therefore left one complete source
corpus per shard copy on disk indefinitely.

## Decision

- **Fence placement-stamped writes, not reads or removes.** A durable coordinator mirrors the
  placement generation of its last successfully written manifest in an atomic. Add, create, and
  upsert compare it with the serving generation after computing their placement and before taking
  the mutation barrier or logging. A mismatch fails with a typed durability error (HTTP 503
  `durability_unavailable`) naming the retry. Reads keep serving the swapped layout. Removes carry
  no placement and replay correctly over either manifest, so they remain available.
- **Any successful commit lifts the fence.** The existing same-count resize retry, the identical
  alias-import retry, and an ordinary checkpoint all publish a manifest carrying the serving
  generation and update the mirrored value. No new state, format, or recovery step is introduced,
  and in-memory clusters are unaffected.
- **Reclaim superseded sidecars after commit.** After the manifest write succeeds, checkpoint
  removes every exactly shaped source sidecar (`sources.dat` or `sources_g<20 digits>.dat`) in each
  position's primary and `replica_*` directories other than the one the committed manifest selects.
  Removal is best effort and retried by the next checkpoint. Readers that still hold an older
  store keep their existing memory mapping, the same lifecycle already used for superseded segment
  files. While the previous manifest remains authoritative, its sidecar is never touched, which
  preserves ADR-118's crash-window invariant.

## Alternatives

- **Log a layout-change record so replay can follow the green generation.** Rejected: replay would
  need the complete green segment set and control state that the failed commit did not publish,
  turning a rare retry window into a second recovery protocol.
- **Refuse all writes, including removes.** Rejected as unnecessary unavailability; a remove is
  placement-free and replays identically.
- **Delete the blue sidecar immediately after the swap.** Rejected: until the manifest commits, the
  blue sidecar is part of the only recoverable generation.

## Consequences

A failed post-swap commit now costs write availability for adds and upserts until an operator (or a
later checkpoint) completes it, instead of risking an unopenable cluster. Disk use after repeated
rebuilds stays at one source corpus per shard copy.

## Proof

`tests/cluster_durability_oracle/resize_commit_fence.rs` injects a control failure after the swap
and proves: add and upsert are refused while removes still apply; a crash reopens the previous
layout with the remove replayed; a same-count retry or checkpoint lifts the fence and the next write
survives reopen; alias import has the same fence; and repeated committed resizes leave exactly one
source sidecar per shard. Disabling the fence fails three of those tests. The source-generation
suite now checks that the blue sidecar stays intact while authoritative and is reclaimed after the
green commit.

**See also:** ADR-046, ADR-078, ADR-118, ADR-167.
