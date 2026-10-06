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

**The positive title view holds a multi-word alias form's entity whenever it holds some reading
of the form's text, wherever the pieces stand.** For a form of plain words that is: whenever it
holds every word.

1. **A reading, and what counts as holding a piece.** A reading cuts the form's text into
   pieces: single words, and phrases the vocabulary already has for several of the words.
   Those are the ways a query could have asked for the text before the form existed.
   - The view holds a *word* when it holds `term:<word>`, which it holds for every cleaned
     token of the title whatever its context makes of the token, or whatever the word compiles
     to as a token of its own (a synonym's canonical, a year). `refurbished unit` carries the
     form `refurb unit` when `refurb` is a synonym of `refurbished`, and `unit #1995` carries
     `1995 unit`.
   - The view holds a *phrase* when it holds the phrase's feature.
   - And it holds either when it holds a name a query takes for it. With `pkg ≡ package`
     active, `deal package` carries the form `pkg deal`. With `ny ≡ new york`, a title that
     carries `new york` carries the word `ny` of the form `ny catalog`. With
     `new york ≡ big apple`, `big seasonal apple catalog` carries the form `new york catalog`.

   The pieces are looked for in the complete view, including what an overlapping phrase
   contributes, and the rule is applied until it adds nothing: a form the title carries is
   itself in the view, and may be a piece of another form.
2. **Only the positive view.** The canonical view, which negation reads, keeps the adjacent
   reading: `inventory -(new york, boston)` rejects `new york inventory` and still accepts a
   title that has the two words apart. A quoted `"new york"` is checked against positions and
   keeps requiring adjacency (ADR-120).
3. **Nothing stored changes, and each kind of data has one home.** Query compilation, plans,
   cost classes, cluster placement, the feature-model fingerprint and the compiler-semantics
   version are as they were. The rule takes effect for every stored query at once, in both
   server modes. A form's readings are built from the normalizer's phrases, so the normalizer
   still holds nothing outside its fingerprint. The equivalents are read where the compiler
   reads them: `Vocab::resolve_equivalences` now resolves each class by feature name as well
   as by feature id, and the two are installed on the dictionary together. A title is widened
   through the classes of whatever dictionary it is matched against, each class once however
   many of its members the title carries. Two names that share a
   feature id (a synthetic id is a hash, ADR-046) are one feature to the compiler, and the
   class by name keeps both.
4. **One place.** Every title path builds its positive view in `match_features_dual` when a
   multi-word alias is active. Every feature enters that view through one function, which also
   notes the feature's name. When the view is complete, the forms it holds a reading of add
   their entities. With no multi-word alias nothing is noted and the path is not taken.
5. **The work follows the forms a title touches.** Every reading crosses each gap between two
   of the form's tokens with exactly one piece. A form is keyed on the pieces over one gap,
   the gap whose names the fewest forms share, and is not looked at before one of those names
   is in the view. Ten thousand forms `wireless <model>` cost nothing to a title that says
   `wireless`, or that gets `wireless` from a form `wire less`; a title that names a model
   looks at that model's form. A name enters the view once, from the title or from a
   completed form, and one thing is done with it: the forms keyed on it are examined, and so
   are the forms an earlier look found waiting on it. An examination walks into the form only
   as far as the view reads it, and a form that then lacks a piece only another form can
   supply waits on that piece, noted once. Nothing bounds the length of an alias form, so
   building one is a single scan of its text for the phrases inside it, and choosing its key
   is one sweep. What the completion remembers for a title
   is empty until the title touches a form, so a scratch made for one title (cluster routing
   makes one per request) costs nothing to size or clear, whatever the number of forms. A
   title's names are reduced to the distinct ones first and compared by a 64-bit hash, so
   they are kept without strings; a collision could only add a candidate.

**Why no match is lost.** Take a query that spells a form out and a title that matched it
before the form existed. What the query compiled that text to was a reading: words, and phrases
the vocabulary had then, each as its feature or one of the feature's equivalents. A title that
held a word's feature has the word as a token, or holds what the word compiles to alone. A title
that held a phrase's feature holds it still. An equivalent is counted as such. So the view holds
that reading, gets the entity, and the entity satisfies the rewritten query. Readings only
accumulate as forms are added, so the order in which aliases are activated does not matter.

## What remains

One shape still narrows, and the title side cannot repair it. A new form that *cuts through* a
phrase an earlier compile used changes what the query asks for outside the form. With
`yc ≡ york city` active, the stored query `new york city` asks for `new` and for `york city`
or `yc`, and matches the title `new yc`. Activate `ny ≡ new york`: the query's text is now read
as the form `new york` followed by the word `city`, and `new yc` has no `city`. Titles that have
the words, together or apart, keep matching.

Repairing it on the title side would mean that a title carrying a phrase under another name
also carries the phrase's words, so that `yc` matched a query for `city` or for `york`. That is
a far wider reading than the one this decision takes, and it is left as a separate question. A
test pins the present behaviour (`a_form_that_cuts_through_an_earlier_phrase_can_still_narrow`).

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

- Activating a multi-word alias removes no match from a stored query, except in the shape
  under "What remains". The statements to that effect in the reference documents and in
  ADR-102, ADR-103, ADR-152 and ADR-158 now hold for multi-word forms with that exception;
  each carries a dated note.
- **A wider reading than the query-side one.** A query that names only another form of the
  alias (`ny catalog`) now also matches a title that has the words of `new york` apart
  (`new seasonal york catalog`). The engine is a recall-first candidate generator and the
  consumer's own matcher decides; an alias form made of very common words will add candidates.
- While a multi-word alias is active a title pays one hash per feature of its positive view,
  a sort of those hashes, one equivalence lookup and one form lookup per distinct name, and
  nothing otherwise. On 200,000 generated queries with three aliases active that was
  2.24–2.30 µs per title against 1.89–1.91 µs before (Apple M4 Max, release build); classes
  and candidates per title were identical.
- No migration and no upgrade order: stored rows, the manifest and the wire formats are
  unchanged.
- **Remote clusters are not affected.** A remote cluster runs the stock vocabulary on every
  node and refuses any other (ADR-076), so it has no multi-word alias and the rule is never
  active there. When vocabularies are shipped across processes, the title view and the
  equivalence classes have to be shipped and fenced with them; the roadmap item says so.
- `Dict::set_equivalences` takes the `Equivalences` that `Vocab::resolve_equivalences`
  returns, which dereferences to the map by feature id it used to be.

## Proven

- `normalize/alias_words_tests.rs`: the positive view gets the entity for words apart,
  reordered, written through a synonym, typed by their context, consumed by another phrase in
  the canonical view, and restored by an overlapping phrase; not for a missing word, and not
  through an unrelated phrase; the canonical view is unchanged; a piece counts under what a
  query takes it for, through classes that share a member, and not when the dictionary holds
  no such class; a phrase inside a form is a piece, alias or not, and is part of the form's
  key; two phrases that overlap inside a form do not join into a reading; a form of two
  thousand tokens is built and carried like any other; no multi-word alias, no change.
- `vocab/tests.rs`: the class by name keeps every name of an id two names share.
- `normalize/alias_words_tests/completion.rs` counts the work: a title with the shared word
  of five thousand forms examines none; a chain of two thousand forms is examined once each;
  a word repeated fifty thousand times is looked at once; an entity that ten thousand forms
  share as a word wakes none of them; a form that waits on a piece is looked at once when the
  piece arrives, also when an equivalent brings it; a class of two thousand names widens a
  title that carries all of them once; a title that touches no form leaves the completion's
  scratch unallocated; and nothing of one title is left for the next.
- `tests/oracle/alias_components.rs`: every match a query set had before `wireless mouse =>
  cordless mouse` and `ny => new york` survives activation, over titles with the words
  adjacent, apart and reordered; the alias matches; one word of a form does not; quoted and
  negated forms keep their results; the engine equals a brute-force evaluation of every stored
  query; a form over an existing additive or collapse phrase; a number typed by its context;
  a later alias whose form overlaps an earlier one, or is built on its entity, removes no
  match; queries written under the alias keep the class and default visibility they have on
  `main`; and the same at scale on generated queries the aliases rewrite, against the
  no-alias brute force.
- `tests/oracle/alias_chains.rs`: a chain of ordinary imports (`ny => new york`, then
  `nycat => ny catalog`), a form over a single-word alias or a declared equivalence, and a
  form that contains an earlier alias (`new york => big apple`, then `nyc => new york
  catalog`, in either order) remove no match; and the shape under "What remains" is pinned.
- `tests/independent_oracle/alias_forms.rs` and `aliases.rs`: the engine equals the
  independent reference matcher, which applies the rule with plain loops and shares no code
  with the engine, with no false negative and no false positive, on fixed and randomized
  corpora, including words carried through equivalences, forms that contain a phrase or two
  overlapping ones, and a number typed by its context.
- `tests/cluster_oracle/alias_components.rs`: the same through a cluster rebuild and live
  writes, and through chains of imports, at one, three and eight shards, in both scopes,
  equal to a single engine; and a group whose members share a synthetic id in the cluster's
  frozen dictionary.
- `tests/oracle/alias_feedback.rs` and `alias_discovery.rs`: a pair with a multi-word form,
  activated by feedback and by the operator.

**See also:** ADR-061 (multi-word aliases and the two title views), ADR-054 (expansion),
ADR-120 (quoted phrases), ADR-046 (synthetic ids).
