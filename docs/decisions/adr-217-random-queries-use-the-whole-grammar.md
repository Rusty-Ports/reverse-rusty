# ADR-217 — Random test queries use the whole grammar

> [Matching & verification decisions](areas/matching-and-verification.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The contract is universal: if a title could satisfy a query, the title retrieves it. The
tests that explore it at random did not sample the query language. `gen::generate`, which
every at-scale differential is built on, writes one shape: bare required terms with
single-term negations at the end. It never writes an any-of group, a quoted phrase, a negated
group, or a negation between two positive terms.

The shapes where clauses interact were covered by hand-written cases only, and that is where
the escapes have been:

- three false negatives from misread clause boundaries and member semantics (ADR-118, ADR-119,
  ADR-120);
- a dedup join that let one copy of a body take another's visibility (ADR-186), which needs a
  body made of any-of groups alone, copies stored on both sides of the frozen frequency mask,
  and a merge;
- an any-of body whose only cover was opt-in (ADR-187).

Each has its own regression test now. Nothing generated the next one.

There was a second gap. The differentials that have a reference (brute force, and the
independent reference matcher) read with `include_broad=true` only. Which rows a *default*
read returns was compared engine against engine with dedup on and off, and for the
re-anchoring merge that comparison is empty: that merge regroups identical bodies whatever the
dedup switch says, so "dedup off" is not a reference for it.

## Decision

1. **A grammar generator** (`gen::grammar`). A query is a random sequence of clauses in
   random order: runs of bare terms, quoted phrases, any-of groups with one- and two-token
   members, and the negated form of each. A share of the queries have no bare term and no
   phrase, only any-of groups. Tokens come from a small hot pool and a large rare pool, so a
   corpus fills the top-64 mask and has features on both sides of it; token names depend on
   their index only, so corpora from different seeds share a vocabulary and one can be loaded
   after another to move frequencies. Positive and negated clauses never share a token, so
   every query can be satisfied.
2. **A title built to satisfy each query**, from the language rules and not from the engine:
   every bare term, every phrase in order and unbroken, one whole member of each group, and
   no forbidden clause complete. A forbidden phrase contributes one of its tokens, or all of
   them with a word between each two; a forbidden two-token member contributes one token. So
   an engine that rejects on part of a negated clause fails on the title that was built to
   match.
3. **Four oracles are driven with it.**
   - *The independent reference* (`tests/independent_oracle/grammar.rs`): engine against
     `reverse-rusty-ref-matcher` on the built titles, on near-misses (a required token
     dropped, a phrase broken or reversed, a forbidden clause completed) and on random bags
     of tokens. Zero false negatives and zero false positives, with the hot tier off and on,
     and on the same titles with surface noise.
   - *The built title retrieves its query*, on an engine built in one batch, one loaded in
     three phases across segments, and that one compacted. No reference is involved, so this
     holds where the engine and the reference could share a misreading. With broad off, a
     read of the built title leaves out the opt-in class and nothing else: exactly as many
     queries are missing as the engine counts in that class.
   - *Relations between a query and an edit of it*: one more negation matches a subset, one
     more group member a superset, and a phrase a subset of its words as bare terms.
   - *The ungrouped twin* (`tests/oracle/grammar.rs`): the corpus with duplicates, stored
     before and after the mask is frozen with other queries in between, through the
     memtable, flushes, the deletion and the upsert of group leaders, the grouped merge and
     the re-anchoring merge, with the hot tier off and on. The twin is the same
     corpus with one more negation in each query: a forbidden phrase that is its own. A
     negation takes no part in planning and is not counted toward frequency, so each copy
     plans as in the original, and no two bodies are equal, so nothing in the twin can be
     grouped. Both reads of every title must return the same from the corpus and from its
     twin at every step. This is the reference for default-read visibility that the
     re-anchoring merge did not have.
4. **Each test says what it needs to be worth running.** The generator test fails if a
   clause kind, a groups-only body, a multi-group body, a negation between positive clauses
   or a two-token member stops being generated. The twin tests fail unless at least ten
   bodies have copies on both sides of the opt-in boundary, before the merge and after it.
5. **Seeds stay fixed in the gate** (ADR-008). `RR_GRAMMAR_SEED` runs the reference
   differential and the built-title property under any other seed.

## What it found

No new defect in the engine at this size (about 100,000 true matches per run of the reference
differential). Two things about the tests:

- **"Dedup off" was not a reference for the re-anchoring merge.** A planted bug in it (a
  copy joins a leader on the other side of the opt-in boundary) passed a dedup-on against
  dedup-off comparison on a corpus with 63 split bodies, because that merge regroups with the
  switch off as well. The twin catches it.
- **The frozen mask breaks frequency ties by feature id.** The first twin gave each query a
  new word of its own, and whole queries changed class: the new words shifted the ids of the
  features interned after them, and the sort that picks the top 64 breaks ties by id. The
  twin is now written in words that one priming query, stored first in both engines, has
  already interned. Which of two equally frequent features holds a mask bit depends on which
  was seen first. That is deterministic, and it is one more way in which a query's default
  visibility depends on the corpus around it (ADR-187, ADR-203).

## Alternatives considered

- **A property-testing crate** (proptest). It would add shrinking and persisted failing
  seeds. The corpus tests here need thousands of queries that share a vocabulary and a
  frequency distribution, which is a generator of corpora, not of single values; and the
  crate would be the first dev-dependency that is not the reference matcher. A failure found
  under another seed is kept as a hand-written test, which is what Elasticsearch does too.
- **Extend `generate`** with the new shapes. Every benchmark and every pinned structural
  number is built on its output, byte for byte.
- **A fresh random seed in every run**, with the seed printed for reproduction, as Lucene and
  Elasticsearch do. It finds more over time and makes the gate non-deterministic, which
  ADR-008 decided against. The seed can be set by hand; running a scheduled job under a
  random one is on the roadmap as a decision to take.
- **Only the reference differential.** It reads with broad on, so it cannot see a visibility
  defect; and a misreading shared by the engine and the reference passes it. ADR-118 to 120
  were of that kind. The built title and the edit relations do not go through a reference.

## Consequences

- About 35 seconds more in the gate's debug lanes, most of it the reference differential
  under three seeds.
- New differentials should take their corpus from `gen::grammar` unless they need the
  product-shaped corpus `generate` writes.
- **What this leaves:** the grammar corpus is clean text under the default vocabulary. It
  does not go through the messy surfaces (case, punctuation, Unicode), a vocabulary with
  synonyms, phrases and aliases, or the cluster oracles. On the
  [roadmap](../roadmap.md#test-infrastructure).

## Proven

- `tests/independent_oracle/grammar.rs` and `tests/oracle/grammar.rs`, in the gate.
- Planted engine bugs, each after an unmutated baseline, and which tests fail: listed in the
  pull request. They cover the exact predicate (a forbidden two-token member rejecting on
  one token, a group satisfied by anything, a two-token member satisfied by one token, a
  forbidden phrase and a required phrase not checked), the candidate cover (an any-of body's
  broad cover leaving a member out), and visibility (a memtable join, a grouped merge and a
  re-anchoring merge crossing the opt-in boundary, and a re-anchoring merge moving a visible
  query into the opt-in lane).

## Prior art

How other projects test a matcher of stored queries, or a query engine, with random input
(sources read 2026-10-08).

- **Elasticsearch's percolator** (`CandidateQueryTests`). `testDuel` stores about a thousand
  random queries, most from `createRandomBooleanQuery` (nested to depth 3, "many iterations
  with boolean queries, which are the most complex queries to deal with when nested"), and
  compares the percolator's candidate path with a `ControlQuery` that runs every stored query
  against the document. It asserts equal hits. It has no separate assertion that candidates
  are a superset, and no document built to match. A failure is kept as a hand-written test:
  "Recreates a similar scenario that made testDuel() fail randomly".
- **Lucene** (`SearchEquivalenceTestBase`): "Simple base class for checking search
  equivalence", with `assertSubsetOf` and `assertSameSet` over random terms. Its relations
  include `A ⊆ (A B)`, `(A -B) ⊆ A` and `"A B" ⊆ (+A +B)`.
- **SQLancer.** Pivoted query synthesis: "It randomly selects a row, called a pivot row, for
  which a query is generated that is guaranteed to fetch the row. If the row is not
  contained in the result set, a bug has been detected." NoREC: "It translates a query that
  is potentially optimized by the DBMS to one for which hardly any optimizations are
  applicable, and compares the two result sets."
- **Seeds.** Lucene: "Tests tasks will be (by default) re-executed on each invocation because
  we pick a random global `tests.seed`", with a "NOTE: reproduce with:" line on failure.
  proptest: "Later runs of tests will replay those test cases before generating novel cases."
  Hypothesis: "If you want to ensure an input is run every time, use @example."

What is taken from them: the duel against a brute-force control (the reference differential);
Lucene's subset relations (the edits); the pivot, turned round, since here the stored thing is
the query and the title is built to fetch it; and NoREC's idea of comparing with a form the
optimisation cannot apply to, which is the ungrouped twin. The percolator's tests have no
counterpart of the built title or of a default-read reference.

**See also:** ADR-008 (deterministic data generation), ADR-063 (adversarial test hardening),
ADR-087 (the independent reference), ADR-106 and ADR-186 (dedup and its visibility
partition), ADR-118 to ADR-120 (the clause-interaction escapes), ADR-187 (the any-of cover).
