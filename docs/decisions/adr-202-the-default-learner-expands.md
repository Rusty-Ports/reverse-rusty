# ADR-202 — The default vocabulary learner applies what it learns by expansion

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

`POST /_vocab/learn_and_apply` with no parameters, and `Engine::learn_and_apply` and
`ClusterEngine::learn_and_apply` behind it, ran the ADR-015 learner: any pair of forms seen
together in an any-of group in two or more stored queries became a **collapse** rule, was
installed without review, and the whole corpus was recompiled under it.

A collapse rule rewrites both the title and the query to one canonical feature. That changes
what a stored query means, and it can remove matches the query had:

- **A negation widens.** After learning `pkg` → `new`, a query `widget -new` also refuses every
  title that says `pkg`. It matched `widget pkg` before.
- **A phrase swallows its words.** After learning `new in box` as a collapse phrase, the title
  `widget new in box` no longer carries `box`, and a query `box widget` stops matching it.

The engine's contract holds for each vocabulary on its own terms: under the new vocabulary every
title still retrieves every query it satisfies. But the stored queries were written against the
old meaning, and to the system that consumes the candidates these are false negatives, caused by
an unreviewed default. ADR-054 chose expansion for learned relationships for exactly this
reason, and left collapse as the default of the learner.

## Decision

1. **The learner has a mode, and the default is expansion.** `CorpusLearnConfig.anyof_mode` is
   `AnyOfLearnMode::Expansion` unless the caller asks for `Collapse`. In expansion mode a
   learned pair becomes an equivalence group (ADR-054): a query that names one member also
   accepts the others. A stored query keeps every match it had.
2. **Collapse is on request only.** `anyof_mode=collapse` on `POST /_vocab/learn_and_apply`
   (a query parameter) and on `POST /_vocab/learn` (a body field). The preview and the apply
   route take the same control, so a preview shows what applying would install.
3. **The old control is kept as an alias.** `learn_equivalences=true` means expansion and
   `learn_equivalences=false` means collapse, as before. A request that sends both controls and
   has them disagree is refused with a 400.
4. **The library wrappers follow.** `learn_and_apply(min_count)` on the engine and on the
   cluster use the default configuration, so they expand.

## Alternatives considered

- **Keep collapse as the default and document the risk.** The warning is needed either way, but
  a default that silently removes candidates from a recall-first engine stays a trap.
- **Install nothing; route learned pairs to the alias registry as candidates for review**
  (ADR-060). Safer still for multi-word pairs, and more work; it duplicates the aliases route,
  which exists for that.
- **Make learned collapse phrases additive.** That stops a phrase from swallowing its words. It
  does nothing for the negation case.

## Consequences

- **Behaviour change.** A `learn_and_apply` call that names no mode now installs equivalences,
  not collapse synonyms and phrases. Send `anyof_mode=collapse` to get the previous behaviour.
- Expansion skips a form that does not resolve to exactly one feature. A multi-word pair such as
  `(nib, new in box)` therefore teaches nothing under the default unless `new in box` is already
  a phrase. Multi-word relationships belong on the governed alias route
  (`/_vocab/aliases/learn_and_apply`, ADR-060 and ADR-061).
- Expansion adds members to any-of groups, which can fan a query's anchors out to more postings
  and, in a cluster, to more shards. That costs work and never a match.
- Expansion can leave a stored query with only top-64 anchors, which is the opt-in class C. The
  rebuild that applies the learned rules keeps such a query in default reads: ADR-187 on the
  single-node engine, ADR-203 on a cluster. So the default learner removes nothing from a read
  that leaves the broad lane out either.
- Collapse rules installed by earlier calls stay installed. `GET /_vocab` lists them
  (`synonyms`, and `phrases` without `additive`); remove the ones that are not wanted and
  `PUT /_vocab`.
- The ADR-015 default is superseded. ADR-054 and ADR-149 describe collapse as safe under
  symmetry; that holds for the lossless cover under one vocabulary and not for the matches of
  queries written before the rule. Each carries a dated note.

## Proven

- `tests/oracle/learn_default.rs`: on a corpus whose any-of groups teach `pkg` ≡ `new` and
  `nib` ≡ `new in box`, with queries that exclude one member or name a phrase's words apart,
  the default learner removes no match any stored query had, and a query naming one member now
  matches the other; the collapse learner, asked for by name, does remove matches on the same
  corpus (so the first test shows a difference that exists).
- `tests/cluster_oracle/vocab_learning.rs` and a coordinator route test: on a cluster with the
  broad lane off, the default learner leaves `widget pkg` in the default read of `widget pkg`
  when it learns `pkg` ≡ `package`, and the query now also matches `widget package` there.
- Server tests: with no control the apply route installs an equivalence and no synonym, in both
  modes; `anyof_mode=collapse` and `learn_equivalences=false` install synonyms; the preview
  reports the same per control; disagreeing controls and an unknown mode are 400s.

**See also:** ADR-015 (the collapse learner), ADR-054 (expansion), ADR-149 (the route contract),
ADR-060 and ADR-061 (governed aliases, where multi-word relationships go).
