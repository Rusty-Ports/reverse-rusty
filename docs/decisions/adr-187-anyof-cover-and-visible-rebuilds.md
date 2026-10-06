# ADR-187 — The any-of cover, and rebuilds that never hide a query

> [Matching & verification decisions](areas/matching-and-verification.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Class C means "only a top-64 anchor is available", and class C is opt-in: a default read
(`include_broad = false`) does not return it. Two things sent queries there that had a better
anchor, or that default reads were already returning.

- **The planner ignored any-of groups once a query had a required feature.** `anchor_plan` used
  an any-of group as the cover only when there was no required feature at all. A query with one
  required feature that is top-64 went to class C even when one of its any-of groups had no
  top-64 member. `new (acme, zenith)` was opt-in while the broader `(acme, zenith)` was
  default-visible: adding a required term made the query *less* visible.
- **Alias expansion triggers the same shape.** ADR-054 moves an aliased required feature into an
  any-of group. `acme mouse` (anchored on the rare `acme`) recompiles, after `acme ≡ acm`
  activates, to required `mouse` plus the group `(acme, acm)`. With `mouse` top-64 that was class
  C, so activating an alias silently removed a query from default reads. The claim that an
  alias's match set "can only grow" held only with `include_broad = true`.
- **The any-of cover was chosen by live frequency.** With no required feature the planner took
  the group whose most frequent member was least frequent, and only then asked whether it had a
  top-64 member. Frequencies keep moving after the mask freezes, so a group with a top-64 member
  could win over one without, and two compiles of one body could disagree on visibility
  (the drift case in ADR-186).
- **Every rebuild re-planned from scratch.** A vocabulary change and a compiler-semantics
  migration both recompile each stored query against today's dictionary. A query compiled before
  the first mask finalize, whose term later turned top-64, sits in the main lane and is
  default-visible; re-planned, it is class C. Compaction refuses that move (ADR-056), but the
  rebuild did not, so an unrelated vocabulary change or an upgrade could hide it.

## Decision

1. **One any-of cover, keyed to the frozen mask.** `anyof_cover` picks, among the any-of groups
   with **no top-64 member**, the one whose most frequent member is least frequent, and anchors
   arity-1 on each of its members: class B, or class H when θ is on and that member reaches θ.
   Both planner branches use it:
   - no required feature: the cover if one exists, otherwise class C on the most selective group;
   - one required feature that is top-64: a required-phrase proxy first (unchanged), then the
     cover, otherwise class C on the required feature.
   It is lossless because a satisfying title carries a member of every group, and the main and
   hot indexes are probed arity-1 with every title feature. Whether a group qualifies is read
   from the mask alone; frequency only chooses among qualifying groups and between the main lane
   and the hot tier.
2. **Compiler semantics version 7.** The cover, class and cluster placement of affected rows
   change, so stored rows are re-derived through the existing migration: a single-node store
   rebuilds from retained source on open, an in-process cluster rebuilds through the
   coordinator, and a remote shard server that still holds version-6 state is refused.
3. **A single-node rebuild never moves a query out of default reads.**
   `recompile_stale_segments`, the one function behind both the vocabulary recompile and the
   semantics migration, records which queries a default read can return before it rebuilds. For
   those it sets `CompileKnobs::keep_visible`, and `add_compiled` then stores a class-C plan's
   signatures in the main lane as class B (`SigPlan::pin_visible`). The main lane is probed
   arity-1 with every title feature, so the same signatures are an equally lossless cover there.
   This is the state a query compiled before the first finalize is already in, and the one the
   compaction guard preserves. A new write never sets the knob: a fresh query takes the lane its
   plan gives it.

## Alternatives considered

- **An arity-2 pair rescue** (`{required, member}` pairs when every group has a top-64 member).
  It would keep `new phone` + `phone ≡ mobile` visible for fresh compiles too. Not taken here:
  the roadmap's pair-escalation item requires that pair escalation never change visibility and
  gates it on real-corpus evidence. Stored queries of that shape stay visible through item 3.
- **Refuse a vocabulary change that would hide a stored query.** Loud, but it turns one affected
  query into a failed alias install, and it cannot apply to the semantics migration: a store that
  will not open after an upgrade is worse than a fat posting.
- **Hide and report.** Leaves the recall loss in place.

## Consequences

- A query is opt-in only when no required feature and no any-of group offers an anchor outside
  the top 64. Adding a positive required term never makes a default-visible query opt-in.
- Activating an alias on a single-node engine never removes a stored query from default reads.
- For a body with no required feature compiled after the mask is frozen, visibility no longer
  depends on live frequency.
- A kept-visible row rides a top-64 feature's main posting, which costs a verification for every
  title carrying that feature. Only rebuilt rows that were already visible take this path.
- Re-putting a kept-visible query compiles it fresh, so it takes the lane of its current plan.
- **Upgrade.** Version 7 follows the existing compiler-semantics procedure
  ([rolling upgrade](../operations/rolling-upgrade.md)): single-node stores migrate on open,
  and remote shard volumes are rebuilt or reseeded under the new binary.
- **Not covered: cluster rebuilds.** A cluster vocabulary change or resize re-mints the dictionary
  and re-ranks the top-64 mask over the live corpus, so it can still move a default-visible query
  into the replicated broad lane. Keeping it visible needs the mask preserved across that rebuild
  first; that is tracked with the mask-stability work, not here.

## Proven

- `compile/tests/anyof_cover.rs`: the cover for the mixed shape, under θ, with only top-64
  anchors available, against drifted frequencies, and an exhaustive sweep showing that adding a
  required term never turns a default-visible plan opt-in.
- `tests/oracle/anyof_cover.rs`: the mixed query through live, bulk, flush and both merges;
  an alias on the rare term; an alias that leaves only top-64 anchors (kept visible through the
  rebuild, a merge and a durable reopen, while a new query of the same text is opt-in); and an
  unrelated rebuild over a query whose term turned top-64.
- `compiler_migration_tests`: a version-6 store reopens with the mixed query default-visible and
  with every previously visible query still visible.
- `tests/cluster_oracle/placement.rs`: the mixed query ring-places on its group members' shards
  and is returned by a default cluster read.

**See also:** ADR-054 and ADR-060 (alias expansion), ADR-056 (the compaction guard this extends to
rebuilds), ADR-105 (the two-axis rule), ADR-186 (dedup joins, which already tolerate copies of one
body on both sides of the boundary).
