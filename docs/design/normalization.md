# Normalization — DSL, shared normalizer, feature dictionary

*Scope: how stored query text and incoming document text become dense integer feature IDs. Siblings:
[`matching.md`](matching.md), [`ingestion-and-updates.md`](ingestion-and-updates.md), and
[`clustering-and-scaling.md`](clustering-and-scaling.md). See the
[overview](README.md) for the correctness contract.*

> **Implementation status:** Fully implemented and tested.

**TL;DR (for agents)**

- **Owns:** DSL parsing (`dsl.rs`), shared normalization (`normalize.rs`), feature dictionary
  (`dict.rs`), and runtime vocabulary (`vocab.rs`).
- **Key invariant:** queries and documents must use the same normalizer and vocabulary.
- **Default behavior:** generic tokens plus four-digit year recognition. There is no built-in
  product taxonomy, named-entity list, category policy, or domain composite.
- **Configured behavior:** phrases, synonyms, equivalences, aliases, punctuation rules, and numeric
  context arrive through `NormalizerBuilder`, `Vocab`, or the vocabulary REST APIs.
- **Quoted clauses:** zero-slop contiguous paths through analyzed token graphs (ADR-120).

---

## 1. Query DSL

The DSL is deliberately constrained so every query can compile to an integer predicate and the
compiler can identify queries with no selective positive gate.

```text
Grammar (EBNF-ish):
  query        := clause+
  clause       := positive | negative
  positive     := term | phrase | anyof
  negative     := '-' term | '-' phrase | '-' anyof
  anyof        := '(' member (',' member)* ')'
  member       := term+
  phrase       := '"' term+ '"'
  term         := word | normalized-entity-literal

Semantics:
  bare term / phrase            → MUST
  ( a b , c )                   → MUST ((a AND b) OR c)
  -term                         → MUST_NOT
  -( a b , c )                  → MUST_NOT ((a AND b) OR c)
```

The compiler jointly normalizes each maximal consecutive run of positive bare terms. This lets a
configured phrase such as `wireless mouse` become one entity without joining across another clause:
`wireless -used mouse`, `wireless "compact" mouse`, and `wireless (black,white) mouse` remain
separate runs (ADR-118).

An unquoted multi-token any-of member is one conjunctive branch, not a bag of interchangeable
features. `(red shoe,boot)` means `(red AND shoe) OR boot`. Candidate retrieval may use one necessary
feature as a proxy for a branch, but exact verification preserves the complete branch (ADR-119).

A quoted clause retains the analyzer's position graph. Each edge is
`(start, end, FeatureId alternatives)`. A normal token spans one position; a configured collapsed
phrase can span several. `"red shoe"` therefore rejects both `red leather shoe` and `shoe red`.
Required phrase edges may be widened by active equivalences. Forbidden phrases use the canonical
leftmost-longest title view and are never widened (ADR-120).

Worked example with caller-supplied vocabulary:

```text
2024 (north star,ns) wireless mouse (package,pkg)
-(used,damaged) -refurbished
```

Given:

```text
phrase:  north star      → brand:north_star
phrase:  wireless mouse  → entity:wireless_mouse
synonym: ns              → brand:north_star
synonym: pkg             → term:package
```

the query compiles to:

```text
REQUIRED:   year:2024, brand:north_star, entity:wireless_mouse, term:package
FORBIDDEN:  term:used, term:damaged, term:refurbished
```

Each any-of group collapses to one canonical feature and is therefore promoted to a required
feature by the compiler.

The AST is compile-time only; matching never interprets source strings.

---

## 2. Shared query and document normalizer

The same `Normalizer` processes stored queries and incoming titles. The pipeline uses caller-owned
scratch buffers:

1. **Byte cleaning.** ASCII is lowercased, supported diacritics fold to ASCII, and the punctuation
   table classifies each character as `split`, `fold`, `keep`, or `marker`. By default `.` is kept,
   `#` and `/` are marker tokens, and other non-alphanumeric characters split words. Separators
   are merged: however many stand between two words, they are one token boundary (ADR-218). Operators may,
   for example, fold apostrophes and hyphens so `O'Brien`, `O-Brien`, and `OBrien` converge.
2. **Tokenization.** Cleaned text becomes spans into the reusable buffer, not owned strings.
3. **Phrase and alias scan.** A daachorse Aho-Corasick automaton emits configured multi-token
   features. A phrase is its words as consecutive tokens, and where phrases overlap the earliest
   wins, then the longest, among those that start and end on token boundaries (ADR-218).
   Collapse, additive, and alias modes control whether component tokens remain visible.
4. **Number typing.** Four-digit values in `1900..=2099` emit `year:N`. Other numbers remain generic.
   A caller-supplied `number_context` word makes an immediately following number generic too:
   with `["model"]`, `model 1995` emits `term:1995`, while `series 1995` emits `year:1995`.
   The default context list is empty.
5. **Synonyms and fallback.** A configured single-token synonym emits its canonical feature.
   Everything else emits `term:<normalized-token>`.
6. **Dense IDs.** Feature names resolve to `FeatureId(u32)`. Strings do not enter candidate
   retrieval or exact verification.

There is intentionally no inference for product categories, brands, model names, conditions,
or other business concepts. A caller can submit those semantics as phrases and synonyms,
declare equivalences, import an alias file, learn repeated any-of relationships, or opt into corpus
phrase induction. The same vocabulary is then applied to both query compilation and title analysis.

`match_features_dual` writes canonical `N(T)` and positive-superset `P(T)` views. `P(T)` includes
overlapping alias paths so a positive requirement is not hidden by a longer leftmost-longest phrase.
`N(T)` remains canonical so forbidden predicates are not accidentally widened. When quoted
predicates exist, `match_phrase_views` also writes reusable position-arc buffers.

### 2.1 The front-end rules, stated in full

This section is normative. It says everything the analyzer does to a query or a title before
features exist, completely enough to implement it without reading the engine. The independent
reference matcher is checked against it (ADR-219), and a difference between the two is a defect in
one of them or a gap here. Stage numbers match the list above.

**Cleaning (stage 1).** The text is read one Unicode scalar at a time, in order.

1. The scalar is folded by this table; any other scalar is left as it is.

   | To | From |
   |---|---|
   | `a` | `á à â ä ã å ā ą Á À Â Ä Ã Å` |
   | `e` | `é è ê ë ē ė ę É È Ê Ë` |
   | `i` | `í ì î ï ī į Í Ì Î Ï` |
   | `o` | `ó ò ô ö õ ø ō Ó Ò Ô Ö Õ` |
   | `u` | `ú ù û ü ū Ú Ù Û Ü` |
   | `n` | `ñ ń Ñ` |
   | `c` | `ç ć č Ç Ć Č` |
   | `s` | `š ś Š Ś` |
   | `z` | `ž ź ż Ž Ź Ż` |
   | `y` | `ý ÿ Ý` |
   | `l` | `ł Ł` |

2. If the result is an ASCII letter or digit, it is written in lower case.
3. Otherwise its punctuation class decides what is written: `split` writes one space, `fold`
   writes nothing, `keep` writes the character, and `marker` writes a space, the character and
   a space.

The default classes are: `.` is `keep`; `#` and `/` are `marker`; every other character is
`split`. That covers all whitespace, all other punctuation, and every non-ASCII character the
table does not fold. A vocabulary may give single characters another class.

A separator is never written twice in a row, and never first (ADR-218). So two `split`
characters in a row leave one space, a `marker` after a space does not add another, and a text
that begins with separators begins with its first token. A space that a vocabulary classes as
`keep` or `marker` is still just a separator. Classed as `fold` it is deleted like any folded
character, and the words on either side join.

**Tokens (stage 2).** The cleaned text is cut at spaces. A token is a maximal run of
characters that are not spaces; there are no empty tokens. Positions count tokens from 0. How
many separators stood between two tokens in the original text is not kept and changes nothing:
`north, star`, `north - star` and `north star` are the same two tokens.

**Phrases (stage 3).** A vocabulary phrase is a sequence of tokens, a feature name, and a mode.
Its tokens are tokens in the sense of stage 2. An alias is declared as text, and its tokens are
that text cleaned and cut by the rules above, under the vocabulary's punctuation classes as they
finally stand, in whatever order the vocabulary was declared. A phrase declared as a list of
tokens is taken as given, so the list has to be in that form already: a token that cleaning
cannot produce (one with an ASCII upper-case letter, a letter the fold table changes, or a
character that splits) makes a phrase that occurs nowhere. A phrase with no tokens is ignored. A vocabulary has one phrase for a
sequence of tokens: of two declarations the first stands and the other is ignored, in every
view.

- *An occurrence* of a phrase is a place where its tokens are consecutive tokens of the text.
  (In the cleaned text: its tokens joined by single spaces, starting at the start of the text
  or right after a space, and ending at the end of the text or right before a space.) A match
  of the phrase's characters that starts or ends inside a token is not an occurrence.
- *Selection* takes occurrences in order of start; of two that start together, the longer; and
  drops any that starts before the end of one already taken. (Leftmost, then longest, never
  overlapping.)
- A selected occurrence emits the phrase's feature once, at the position of its first token.
- The mode says what happens to the tokens inside it. `collapse`: they are consumed and emit
  nothing else. `additive`: they go on through stage 4 as if no phrase were there. `alias`:
  consumed on the query side, kept on the title side.

**Each remaining token (stages 4 and 5),** by the first rule that applies:

1. A token that is exactly `#` or `/` emits nothing. It still has a position. The rule is about
   those two tokens and not about the `marker` class: a character that a vocabulary classes as
   `marker` becomes a token of its own and is then a token like any other (`@` classed so emits
   `term:@`, and does nothing to a number beside it).
2. A *number* is a token of ASCII digits with at most one `.` and at least one digit.
   - It emits `term:<token>` when the token before it is `#`; when the token before it or
     after it is `/`; or when the token before it is one of the vocabulary's number-context
     words (compared without regard to ASCII case). "Before" and "after" are by position,
     whether or not a phrase consumed that neighbour.
   - Otherwise, when it is exactly four digits and between 1900 and 2099, it emits
     `year:<token>`.
   - Otherwise it emits `term:<token>`.
3. A token that a vocabulary synonym names emits that synonym's canonical feature name. The
   synonym's token is compared as it was declared, character for character, so it too has to
   be in cleaned form. A vocabulary has one synonym for a token: of two, the first stands.
4. Any other token emits `term:<token>`.

One consequence of rule 2: `#1999` and `/1999` are the term `1999`, and a bare `1999` is the
year `1999`. They are different features. Under the canonical view neither matches the other.
Under the wide positive view (below: a vocabulary with an alias) a title's bare `1999` is
carried as `term:1999` as well, so the query `#1999` matches it; the query `1999` still does
not match the title `#1999`.

**The two views of a title.** The canonical view `N(T)` is the set of features the stages above
emit for the title. Forbidden clauses are checked against it. The positive view `P(T)` is what
required clauses and any-of groups are checked against. When the vocabulary has no phrase in
`alias` mode, `P(T)` is `N(T)`. When it has one, whether or not that phrase occurs in the title,
`P(T)` is the union of:

- `N(T)`;
- what the stages emit when no phrase consumes its tokens, whatever its mode;
- `term:<token>` for every token of the cleaned title except `#` and `/`;
- the feature of every phrase, in any mode, that has an occurrence in the title, overlapping
  occurrences included;
- the feature of an `alias` phrase whose form the title carries in pieces (ADR-205). The form's
  tokens are cut, left to right, into pieces, and the title must carry every piece. A piece is
  one token, carried as `term:<token>` or as any feature that token emits when analyzed by
  itself as a title; or it is a vocabulary phrase of two or more tokens, shorter than the form,
  carried as its feature. A feature is also carried when the title holds another feature of the
  same equivalence class. A form found this way is itself carried and can be a piece of another
  form, so the rule is applied until it adds nothing. Carrying is a question about the set of
  features gathered so far, not about positions: the pieces need not stand in the title in the
  form's order, and one feature can carry more than one piece. "Analyzed by itself as a
  title" means the canonical view of a title that is that one token. The rule adds features
  only; it adds no arc to either graph.

**Equivalence classes.** A declared equivalence group lists forms. A form takes part only when
it analyzes, as a query, to exactly one distinct feature (`a a` is one). Groups with fewer than
two such features are dropped, and groups that share a feature are merged.

**A quoted clause (ADR-120)** is analyzed with positions. The text is cleaned, once, and the
stages run: on the query side for a clause, on the title side for a title. Every emitted feature
is an arc from the position of its first token to the position after
its last; a phrase's arc spans its tokens. A position that no arc starts at and no arc passes
over gets an arc `term:<token>` for the token there, markers included.

- *The query's graph.* Arcs with the same start and end are alternatives of one edge. On a
  required clause an edge's alternatives are widened by equivalence classes; on a forbidden
  clause they are not.
- *A title's canonical graph* is its arcs as above.
- *A title's positive graph* is the union of the canonical graph's arcs; the arcs of the same
  analysis with no phrase consuming its tokens, whatever its mode (so a number or a synonym
  inside a phrase keeps the arc it would have by itself, and every phrase's own arc is
  there); an arc `term:<token>` for every token that is not a marker; and an arc for every
  phrase occurrence, overlapping ones included. This is so with or without an alias in the
  vocabulary.
- *Matching.* A quoted clause matches a graph when its edges can be followed from its first
  position to its last along arcs that carry one of each edge's alternatives and join end to
  start, beginning at any title position. A required clause is matched against the positive
  graph and a forbidden clause against the canonical graph.

**A clause that analyzes to nothing is dropped.** A quoted clause with no token, a bare term
or an any-of member made only of `split` characters, and a group left with no member neither
require nor forbid anything.

---

## 3. Feature dictionary

- One `Dict` belongs to an engine. A cluster shares one frozen dictionary across shards so a
  `FeatureId` has one meaning everywhere.
- Interned IDs are dense in first-seen order below the reserved synthetic-ID range. Parallel arrays
  hold names, kinds, frequencies, and top-64 mask positions.
- Query-document frequency drives anchor selection independently of ID order. Finalization freezes
  the 64 highest-frequency features used by the exact verifier's common mask.
- Read-only paths resolve an absent name to a deterministic synthetic ID. A collision may
  over-retrieve, but cannot remove a true candidate.
- A name must keep one of the two ids. A cluster's dictionary is frozen, so a name absent from
  it stays synthetic everywhere. A single-node dictionary grows with every insert, so a rebuild
  that compiles read-only (a vocabulary change, a compiler migration) first interns every name
  the current normalizer produces for the stored queries (ADR-204).
- `FeatureKind` is descriptive vocabulary metadata: `year`, `brand`, `entity`, `category`, `flag`,
  or `generic`. Candidate choice is frequency-based; it does not contain category-specific rules.
- Active equivalences widen a positive requirement to an any-of group. Expansion can add matches,
  but cannot remove an existing match (ADR-054).

Multi-word aliases are asymmetric by design (ADR-061). On the query side they collapse to an entity
that equivalence expansion can widen. On the title side they are additive, and an overlapping scan
adds nested alias entities to `P(T)`. `P(T)` also gets a form's entity when the complete view
holds some reading of the form's text anywhere. A reading cuts the text into words and phrases
the vocabulary already has; a word is held as `term:<word>`, which the view holds for every
token, or as what the word compiles to alone, a phrase as its feature, and either as anything
an active equivalence makes a query accept in its place (ADR-205). That keeps every match a query had before the alias, because the query used to require
exactly those words. `N(T)`, which negation reads, and the positions quoted phrases read keep the
adjacent form only. With no active multi-word alias, the flat title paths are identical.

---

## 4. Vocabulary sources and lifecycle

`Normalizer::default_vocab()` is domain-neutral and empty apart from generic normalization rules.
Semantics can be supplied through:

- `NormalizerBuilder` for embedded callers;
- a serialized `Vocab` loaded at startup;
- `PUT /_vocab` for explicit runtime replacement;
- `POST /_vocab/aliases/import` for operator-declared surface forms;
- any-of learning from query text;
- opt-in NPMI phrase induction; and
- review-first distributional alias discovery.

The review-first REST learner requires one explicit caller corpus in both local modes. It rejects
duplicate IDs and invalid DSL before counting cross-query evidence, bounds corpus cardinality,
relationship expansion, phrase tokens and growth passes, body/result size, and body time, and runs
validation, learning, and serialization on the shared administrative blocking slot. It does not
inspect stored queries or apply its result.

The REST replacement, alias-import, vocabulary learn-and-apply, and governed-alias learn-and-apply
paths perform any required O(corpus) rebuild through the same one-slot blocking-work boundary and
recompile stored queries before publishing the new normalizer. Alias imports parse atomically, bound
rules and forms, and skip installation entirely when an identical registry declaration is retried.
Both stored-corpus learners accept only bounded, bodyless, validated query controls; every mutation
returns the same timed `recompiled` result in standalone and coordinator modes. A successful durable
response means the rebuilt query state committed; a coherent live rebuild whose storage commit
fails is published but explicitly not acknowledged. Retrying the identical coordinator import in
that state recommits the live vocabulary generation and repairs any pending feature-model control
transition before it returns a no-op acknowledgement. The retry may replace only the exact
pre-import manifest retained by that attempt; if the new manifest was renamed before directory sync
failed, it re-attests and syncs that exact next-epoch commit, including its segment registry, next
segment IDs, source sidecars, and log replay cursor. Every other unreadable, divergent, or newer
manifest remains a fail-loud incompatibility.
Embedded imports also complete any stale-plan rebuild left between the public split apply steps.
Alias-registry review shares that administrative slot for potentially large JSON snapshots. A
standalone read captures one immutable engine snapshot; a coordinator read clones the registry
under a brief cluster guard inside the blocking worker and releases the guard before paging and
serialization. Optional `from`/`size` controls page stored order without changing the total
registry `count` or whole-registry lifecycle summary.
Compute-only distributional discovery shares the same admission and blocking-work boundary. An
explicit corpus is validated as distinct, valid DSL with bounded size and controls; standalone
stored-corpus discovery briefly clones live sources under the engine guard and releases it before
analysis. Coordinator discovery requires an explicit corpus until cross-shard source gathering
exists. The deterministic proposals are never recorded or activated by that route.
The separate standalone discover-and-record mutation accepts only bounded discovery controls. Its
blocking worker clones stored sources briefly, runs discovery without the engine guard, then
reacquires the guard only to install never-active candidates through the metadata-only seam and
publish a snapshot. Matching and the vocabulary epoch remain unchanged. The seam commits the updated
registry to the manifest (keeping the previous WAL watermark) or, if it cannot, leaves the registry
unchanged and reports the failure; coordinator mode returns the dry-run, review, and `PUT /_vocab` alternative instead of performing a
full blue/green rebuild for review metadata.
Feedback evidence review shares the administrative slot and accepts strict positive thresholds plus
bounded `from`/`size` paging. A blocking worker clones only the requested evidence page under the
capture mutex, captures an immutable engine snapshot, and releases the mutex before source
resolution, exclusion filtering, overlap calculation, and serialization. The standalone response
is timed and no-store; coordinator mode validates the same read contract before returning the
single-node capture alternative.
Evidence validation and application use that same one-slot, off-runtime boundary. The default
operation stamps changed evidence metadata only and treats an identical retry as a no-op.
`activate=true` remains explicit and promotes only eligible candidates through a complete,
durability-checked recompile before publication. Both are committed to the manifest like every other
vocabulary change.
Embedded callers use the deliberately split `set_vocab()` then `recompile_stale_segments()` sequence
and must not publish a snapshot between those calls; a durable standalone engine refuses to commit
in between, because no single recorded model describes that corpus.

### The recorded feature model

Every manifest commit records the feature model the committed corpus was compiled under
([ADR-184](../decisions/adr-184-recorded-feature-model.md)): the `Vocab` document (single-node
manifest v8; the cluster manifest since v3) and `Normalizer::fingerprint()`, a stable hash of every
phrase, synonym, punctuation rule, and number-context word. On reopen a recorded vocabulary is
authoritative — the normalizer is rebuilt from it and its equivalences are installed before the WAL
or log tail replays — and a normalizer whose fingerprint differs from the recorded one fails the
open with `FeatureModelMismatch`, unless a compiler-semantics migration is about to rebuild every row
from source. `--vocab-file` therefore only seeds a new store (`Engine::open_seeded`,
`ClusterEngine::open_seeded`); a store that recorded no vocabulary reopens under the stock
normalizer and takes a file only while it holds no queries. See
[`../reference/api/vocab.md`](../reference/api/vocab.md).

Compiler semantics version 5 removed earlier special-purpose feature categories; version 6 also
preserves pre-dedup semantic any-of counts for deterministic ranking. This project has no
compatibility requirement for prototype data: persisted query state written with an earlier
compiler semantics version must be rebuilt from source rather than upgraded in place.
