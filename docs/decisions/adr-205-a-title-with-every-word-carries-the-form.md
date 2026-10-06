# ADR-205 — A title with every word of an alias form carries the form

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A multi-word alias form (`new york` in `ny ≡ new york`) is registered as an alias-mode phrase.
On the query side the phrase consumes its words: a stored query that spells the form out
compiles to the form's entity, and equivalence expansion widens that entity to the alias's other
forms. On the title side a form was recognised only where its words stand together.

Before the alias existed the same query required each word, wherever it stood. So activating an
alias removed matches the query had:

| Stored query | Title | Before | After |
|---|---|---|---|
| `wireless mouse` | `wireless optical mouse` | match | **no match** |
| `wireless mouse` | `mouse, wireless` | match | **no match** |
| `new york inventory` | `new seasonal york inventory` | match | **no match** |
| `wireless mouse` | `cordless mouse` | no match | match |

ADR-061 recorded this on purpose (its "semantics of activation": the zero-false-negative
contract holds per vocabulary) and sketched a query-side alternative as a follow-on. Six
reference and design documents, four other ADRs and three oracle tests nevertheless described
activation as widening-only. The at-scale test named for the property could not fail: its
generated queries never contained the alias text, and its injected queries could match no
generated title.

Every path that activates an alias was affected: a Solr or Elasticsearch synonym import, an
edited registry installed through `PUT /_vocab`, and feedback activation, on a single node and
on a cluster. To the system that consumes the candidates the lost matches are false negatives.

## Decision

**The positive title view holds a multi-word alias form's entity whenever it holds every word
of the form, wherever the words stand.**

1. **What counts as holding a word.** The view holds `term:<token>` for every cleaned token
   of the title, whatever its context makes of the token, so a word written in the title
   counts. So does whatever the word compiles to as a token of its own: a synonym's canonical,
   a year. A title that says `refurbished unit` therefore carries the form `refurb unit` when
   `refurb` is a synonym of `refurbished`, and `unit #1995` carries `1995 unit`. The words are
   looked for in the complete view, including what an overlapping phrase contributes.
2. **Only the positive view.** The canonical view, which negation reads, keeps the adjacent
   reading: `inventory -(new york, boston)` rejects `new york inventory` and still accepts a
   title that has the two words apart. A quoted `"new york"` is checked against positions and
   keeps requiring adjacency (ADR-120).
3. **Nothing stored changes.** Query compilation, plans, cost classes, cluster placement, the
   feature-model fingerprint and the compiler-semantics version are as they were. The rule
   takes effect for every stored query at once, in both server modes.
4. **One place.** Every title path builds its positive view in `match_features_dual` when a
   multi-word alias is active. Every feature enters that view through one function, which also
   notes the feature's name. When the view is complete, the forms whose words it holds add
   their entities. With no multi-word alias nothing is noted and the path is not taken.
5. **A form is examined only when the title carries its rarest word.** Each form is keyed on
   the one of its words that the fewest forms share. Ten thousand forms `wireless <model>` cost
   a title that says `wireless` nothing; a title that names a model looks at that model's
   form. Names are compared by a 64-bit hash, so a title's names are kept without strings; a
   collision could only add a candidate.

**Why no match is lost.** Take a title that matched a query before the alias, and a form the
query spells out. Before the alias the query required, for each word of the form, the feature
the word compiled to there. If that feature was a synonym's canonical or a year, it is what the
word compiles to alone, the title holds it, and the rule counts the word. Otherwise the feature
came from a token equal to the word (a plain term, a number its context left untyped) or from
a phrase whose tokens include the word; either way the title has the word as a token, and the
view holds `term:<word>`. Every word of the form is counted, the view gets the entity, and the
entity satisfies the rewritten query.

## Alternatives considered

- **Rewrite the query** to "the entity, or all of the form's words" (ADR-061's sketch: one
  any-of group per word). This was implemented first. It worked for the cases in the finding,
  and review kept finding paths around it, seven in two rounds: a per-form list of word
  features cannot be computed away from the query (a number typed by its context, `#1995 unit`);
  two forms that share an entity; a word that itself resolves to the entity; the list is not part
  of the feature-model fingerprint; a query that is only a form of very common words, or has
  such a form in an any-of group, plans as class C and leaves default reads unless the planner
  gains a new pair cover, which itself produced a self-pair that lost a match. It also changes
  every stored plan, so it needs a compiler-semantics bump and a reseed of remote shards. Each
  is a consequence of moving the rule into stored rows. On the title side none arises.
- **Keep the collapse and correct the documents** (the finding's option B), refusing automated
  activation of multi-word pairs. It leaves the recall loss in place for the documented import
  path.

## Consequences

- Activating a multi-word alias never removes a match from a stored query. The statements to
  that effect in the reference documents and in ADR-102, ADR-103, ADR-152 and ADR-158 now hold
  for multi-word forms; each carries a dated note.
- **A wider reading than the query-side one.** A query that names only another form of the
  alias (`ny catalog`) now also matches a title that has the words of `new york` apart
  (`new seasonal york catalog`). The engine is a recall-first candidate generator and the
  consumer's own matcher decides; an alias form made of very common words will add candidates.
- While a multi-word alias is active a title pays one hash and one lookup per feature of its
  positive view, and nothing otherwise. On 200,000 generated queries with three aliases active
  that was about 0.2 µs on a 1.9 µs title (Apple M4 Max, release build); classes and candidates
  per title were identical to `main`.
- No migration and no upgrade order: stored rows, the manifest and the wire formats are
  unchanged. During a rolling upgrade a node on the old binary answers as before.

## Proven

- `normalize/alias_words_tests.rs`: the positive view gets the entity for words apart,
  reordered, written through a synonym, typed by their context, consumed by another phrase in
  the canonical view, and restored by an overlapping phrase; not for a missing word, and not
  through an unrelated phrase; the canonical view is unchanged; forms that share an entity,
  nested forms and a repeated word; five thousand forms that share a word are keyed on the
  other one; no multi-word alias, no change.
- `tests/oracle/alias_components.rs`: every match a query set had before `wireless mouse =>
  cordless mouse` and `ny => new york` survives activation, over titles with the words
  adjacent, apart and reordered; the alias matches; one word of a form does not; quoted and
  negated forms keep their results; the engine equals a brute-force evaluation of every stored
  query; a form over an existing additive or collapse phrase; a number typed by its context;
  a later alias whose form overlaps an earlier one removes no match;
  queries written under the alias keep the class and default visibility they have on `main`;
  and the same at scale on generated queries the aliases rewrite, against the no-alias brute
  force.
- `tests/independent_oracle/aliases.rs`: the engine equals the independent reference matcher,
  which applies the rule with plain scans and shares no code with the engine, with no false
  negative and no false positive, on fixed and randomized corpora.
- `tests/cluster_oracle/alias_components.rs`: the same through a cluster rebuild and live
  writes, at one, three and eight shards, in both scopes, equal to a single engine.
- `tests/oracle/alias_feedback.rs` and `alias_discovery.rs`: a pair with a multi-word form,
  activated by feedback and by the operator.

**See also:** ADR-061 (multi-word aliases and the two title views), ADR-054 (expansion),
ADR-120 (quoted phrases), ADR-046 (synthetic ids).
