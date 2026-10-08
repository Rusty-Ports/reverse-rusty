# Matching & verification decisions

> [Architecture decision hub](../../DECISIONS.md)

Candidate retrieval, lossless signature cover, exact verification, broad-query handling, and query-cost controls.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [001](../adr-001-semantic-signatures.md) | Semantic signatures | Gates on small semantic feature combinations instead of raw terms to keep retrieval selective and lossless. | Accepted |
| [002](../adr-002-integer-exact-verification.md) | Integer-only exact verification | Pushes parsing and interpretation to compile time so matching uses only integer masks and sorted IDs. | Accepted |
| [003](../adr-003-broad-query-quarantine.md) | Broad-query cost classes | Classifies non-selective queries into explicit lanes so broad work cannot dominate selective matching. | Accepted |
| [006](../adr-006-forbidden-features-never-gate.md) | Forbidden features never gate | Keeps negative features out of candidate retrieval and checks them only during exact verification. | Accepted |
| [011](../adr-011-cache-line-blocked-bloom.md) | Cache-line blocked bloom filter | Skips impossible segment probes with one cache-line-sized bloom lookup. | Accepted |
| [019](../adr-019-query-family-factoring-declined.md) | Query-family factoring | Declines a shared-prefix DAG because it targets a non-bottleneck at high format and rebuild cost. | **Declined** |
| [025](../adr-025-query-complexity-limits.md) | Query-complexity limits | Enforces front-door policy limits while keeping durable recovery governed by format ceilings. | Accepted |
| [026](../adr-026-broad-lane-batch-evaluation.md) | Columnar broad-lane evaluation | Evaluates broad queries once per title batch through bitmap algebra while preserving scalar results. | Accepted |
| [187](../adr-187-anyof-cover-and-visible-rebuilds.md) | Any-of cover and visible rebuilds | Anchors a top-64-required query on an any-of group with no top-64 member, keyed to the frozen mask, and keeps a default-visible query visible through every single-node rebuild. | Accepted |
| [203](../adr-203-cluster-rebuilds-keep-visible-queries.md) | Cluster rebuilds keep visible queries | Replicates a default-visible query always-visible, in each shard's main lane, when a cluster vocabulary change, resize or compiler migration re-plans it as class C. | Accepted |
| [217](../adr-217-random-queries-use-the-whole-grammar.md) | Random test queries use the whole grammar | Adds a generator of queries with every clause kind in any order and a title built to satisfy each, and drives four oracles with it: the independent reference, the built title retrieves its query, subset relations between a query and its edits, and an ungrouped twin as the reference for default-read visibility across the merges. | Accepted |
| [219](../adr-219-the-reference-says-where-it-comes-from.md) | The reference matcher says where it comes from | Records that the reference's parser, cleaner, normalizer and phrase selection were ported from the engine, so the differential finds drift there and not a shared misreading; states the front-end rules in full as normative text; puts copied tables in one file; and adds gate lanes for the reference's provenance and its own tests. | Accepted |
| [220](../adr-220-the-reference-front-end-is-written-from-the-specification.md) | The reference matcher's front end is written from the specification | Replaces the reference's ported parser, cleaner and normalizer with ones written by an author who was given the specification and not the engine's source, in a different shape; settles in the specification every question that author could not answer from it; and leaves the provenance lane with no exceptions. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
