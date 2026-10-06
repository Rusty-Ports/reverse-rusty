# ADR-204 — A vocabulary change interns the names it introduces

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A feature name has two possible ids. A name in the dictionary has its dense id. A name that is
not there resolves, on every read-only path, to a deterministic synthetic id (ADR-046), so that
a term first seen after the dictionary is frozen is absorbed and not dropped. The two never meet
as long as a name stays on one side.

On a single-node engine a vocabulary change moved a name from one side to the other.

1. `set_vocab` installs the new normalizer, and `recompile_stale_segments` compiles every stored
   query **read-only**. A name the new vocabulary produces for the first time, such as a
   synonym's canonical or a phrase's entity, is not in the dictionary. The recompiled queries
   hold its synthetic id. Titles resolve it the same way, so everything matches.
2. Later, an ordinary insert whose text contains that name compiles through the interning path
   and gives the name a dense id.
3. From then on a title resolves the name to the dense id. Every query recompiled in step 1
   still holds the synthetic id and can no longer be reached through it.

Reproduced on `main` (`ff71842`):

- Store `refurb widget`. Install the synonym `refurb → term:refurbished`. The title
  `refurb widget` matches. Insert `refurbished gadget`. The title `refurb widget` no longer
  matches the first query.
- Store `north star lamp`. Install the phrase `north star → term:northstar`. Insert
  `northstar shade`. The title `north star lamp` no longer matches the first query.

On a generated corpus of 4,000 queries with four such synonyms and two phrases, one ordinary
title lost 13 matches after six inserts. Nothing fails and nothing is logged; a restart does not
help, because the sealed rows keep the synthetic id and the dictionary keeps the dense one.

The hazard was known in two narrower places and handled only there: equivalence forms are
interned before a vocabulary is installed (ADR-060, `Vocab::intern_equivalence_forms`), and the
compiler-semantics migration runs the interning extractor over the stored corpus before it
recompiles. A plain vocabulary change had neither.

A cluster is not affected: its dictionary is frozen and both sides resolve read-only, so a name
keeps one id.

## Decision

1. **`set_vocab` interns every name the new normalizer produces for the stored queries, before
   anything is installed.** It already builds the next dictionary off to the side. It now runs
   the interning extractor over the live sources into that dictionary
   (`intern_live_names`), so the read-only recompile that follows finds every name dense, with
   the id a later insert would give it. Every single-node path that changes the vocabulary goes
   through `set_vocab`: `PUT /_vocab`, the alias import, learn-and-apply and feedback
   activation.
2. **Nothing the dictionary already held changes.** The extractor counts each query's
   features, so every existing feature gets its frequency and mask bit back afterwards. The
   top-64 mask stays as it was assigned (ADR-188), and no stored query's anchor choice or
   visibility moves because of the pass. A new name keeps the count of the stored queries that
   now carry it and holds no mask bit.
3. **One function serves both rebuilds.** The compiler-semantics migration calls the same
   function in place of its own copy of the pass.

## Alternatives considered

- **Intern each kind of name the vocabulary can introduce** (synonym canonicals, phrase
  entities), as ADR-060 did for equivalence forms. The set of kinds grows with the vocabulary
  model, and a kind that is forgotten fails silently. The stored queries are the complete list
  of names that matter, and they are already in hand.
- **Compile the recompile through the interning path.** It would also change frequencies as
  it went and intern into the live dictionary before the change is known to be acceptable. The
  pass into the proposed dictionary keeps `set_vocab` all-or-nothing.
- **Make `Dict::intern` reuse a synthetic id that is in use.** The dictionary does not know
  which synthetic ids stored rows hold.

## Consequences

- An insert after a vocabulary change no longer changes what an earlier query matches.
- A vocabulary change compiles the stored corpus three times where it compiled it twice (the
  new pass, the existing preflight, the recompile). It is an operator action on the write path,
  not a read-path cost.
- A name introduced by a vocabulary change now has a real frequency for anchor selection,
  where a synthetic id always looked like the rarest feature of its query.
- **Repairing a store that already split.** A store on which the sequence above already
  happened keeps the split until its rows are recompiled. Re-applying the current vocabulary
  (`GET /_vocab`, then `PUT /_vocab` with the same document) recompiles every stored query, and
  with this change every name comes out dense. Any other vocabulary change does the same.
- No stored format changes and the compiler-semantics version is unchanged: the fix is in
  which id a rebuild writes, not in what a query means.

## Proven

- `tests/oracle/vocab_ids.rs`: both reproductions; a generated corpus whose vocabulary change
  rewrites many stored queries to new names, followed by inserts that use the names, with the
  engine equal to a brute-force evaluation of every query on every title; existing ids,
  frequencies and mask bits identical before and after a change; and the same sequence across
  a durable reopen.
- `handlers/vocab/write_tests.rs`: `PUT /_vocab` followed by a write that uses the new
  canonical.
- `segment/lifecycle/vocab/intern.rs`: the pass interns new names with their counts, restores
  existing features, and reports a stored query that no longer parses.
- The existing compiler-migration tests, which now run through the shared function.

**See also:** ADR-046 (synthetic ids), ADR-060 (the same fix for equivalence forms), ADR-184
(the recorded feature model), ADR-188 (the mask is assigned once).
