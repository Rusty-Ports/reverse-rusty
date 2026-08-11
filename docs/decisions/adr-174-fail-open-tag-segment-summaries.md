# ADR-174: Fail-open tag summaries for filtered segment skipping

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Done (2026-08-10)

## Context

Filtered percolation already compiles request metadata to integer `TagId` groups and checks every
retrieved row during exact verification (ADR-049/055). That preserves the semantic signature cover,
but a filtered read still enters every immutable segment. The reference workload's dominant read
shape narrows to one category, so category-local segments pay candidate-index and verifier cost even
when no row in them can pass the request.

Tags must remain unavailable to the semantic signature optimizer. Any earlier rejection must be an
exact proof about the request filter, not a probabilistic claim that a semantically matching query is
unlikely to exist.

## Decision

1. Every sealed segment carries the sorted, deduplicated union of all `TagId`s in its exact tag
   column. The mutable memtable has no summary and always probes.
2. A request predicate remains AND across groups and OR within each group. A segment is skipped only
   if at least one predicate group has no intersection with the segment union. If every group is
   represented, the summary is inconclusive and ordinary candidate retrieval plus per-row exact tag
   verification runs unchanged. In particular, the summary never infers that tags from different
   groups coexist on one row.
3. Summaries are rebuilt off the hot path at seal, compaction, and mmap open. Construction
   scans into a set before sorting the distinct IDs, so category-local temporary state and sort cost
   scale with tag cardinality rather than row count. The mmap reader derives the union from the existing
   validated tag blob, so segment format v10 and all compatibility stamps remain unchanged. Pre-v3
   segments are known to contain no tags and therefore have an exact empty union.
4. Tombstones do not delete IDs from a summary. Dead or replaced rows can only make the union more
   permissive, which loses a skip opportunity but cannot create an incorrect rejection. Compaction
   naturally rebuilds the union from its surviving destination rows.
5. `tag_segment_skipping` is a default-on read-path kill switch: dynamic in single-node mode and
   startup-wired on both cluster binaries. `false` bypasses summaries without changing stored state.
   `MatchStats.tag_segments_skipped` counts avoided immutable-segment
   traversals and is merged across shards; protobuf field 21 carries it additively. Resident summary
   payload is exposed as `tag_summary_bytes` in engine, REST, CAT, and Prometheus memory accounting.
6. Scalar, ranked, exhaustive, and batch entry points share the same segment proof. In the columnar
   path, one skipped segment pass covers the active title chunk; the scalar selective passes retain
   their per-title counts.

## Correctness argument

For segment rows `R`, let `U` be the union of every stored row's tag set. A predicate accepts a row
only if, for every group `G`, `tags(row) ∩ G` is non-empty. If the summary finds some `G` where
`U ∩ G` is empty, then `tags(row) ⊆ U` makes that intersection empty for every row in `R`; no row can
pass, so skipping the segment is result-equivalent. If no such group exists, the implementation
probes normally. Missing, stale-additive, or cross-group-inconclusive state therefore fails open.

This proof is independent of title features and query Boolean semantics. Tags never enter signature
construction, title signature generation, cost-class placement, or the positive/negative verifier.
The lossless semantic cover is unchanged, while the caller-requested result scope is preserved
exactly.

## Alternatives

- **Physically partition or route by one tag.** This can avoid more work but couples layout and
  rebalancing to one workload key, complicates multi-key predicates, and requires a migration. Keep it
  as a future option only if exact summaries leave material cost on representative deployments.
- **Persist a new summary section.** Rejected for this increment: deduplicating the already-mapped tag
  blob once at open avoids a format/version transition and gives older readable segments the same
  proof.
- **Bloom or min/max tag summaries.** A Bloom filter could remain false-positive-only, but the current
  tag cardinality makes an exact `u32` union simpler, smaller to reason about, and directly auditable.
- **Skip from per-row tag correlation.** Rejected until evidence warrants a richer structure. Separate
  union membership cannot prove that two groups occur on one row, so it deliberately falls through.

## Consequences and evidence

The resident payload is four bytes per distinct tag ID per sealed segment, plus allocation metadata;
mmap open and sealing pay one set build plus a sort of the distinct union. Segments containing many mixed tag values may be
inconclusive, especially after compaction, but never become less correct.

Differential coverage pins enabled/disabled equality for scalar and columnar matching, compaction,
mmap reopen, post-freeze synthetic tag IDs, in-process cluster fan-out, and real gRPC stats transport.
The seeded `tagbench 160000 2000 8 0.05 372` capture on 2026-08-10 used eight category-local segments:
the summary skipped 7.00 segments/title, reduced postings and candidates by 87.5%, used 32 payload
bytes, and preserved every per-title result. Three consecutive final-binary paired captures showed a
3.27–3.60× throughput improvement despite heavy absolute timing variance on the interactive host;
timings are machine-specific, while equality and work counts are deterministic. The canonical
command and full capture context live in
[`benchmark-results.txt`](../performance/benchmark-results.txt).
