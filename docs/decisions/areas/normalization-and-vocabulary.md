# Normalization & vocabulary decisions

> [Architecture decision hub](../../DECISIONS.md)

Shared query/title normalization, dictionaries, learned vocabulary, aliases, and feature-space evolution.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [010](../adr-010-normalizer-builder-fallible.md) | Fallible normalizer builder | Replaces hardcoded vocabulary and panicking construction with a configurable, typed builder. | Accepted |
| [015](../adr-015-runtime-vocabulary-learning.md) | Runtime vocabulary learning | Learns synonyms from any-of groups and tracks the epoch used to compile each segment. | Accepted |
| [046](../adr-046-dynamic-vocabulary.md) | Dynamic vocabulary | Hashes post-freeze terms and rebuilds aliases blue/green so new vocabulary cannot cause false negatives. | Accepted |
| [053](../adr-053-corpus-phrase-vocab-source.md) | Corpus phrase induction | Adds opt-in NPMI phrase learning as another source for the runtime vocabulary. | Accepted |
| [054](../adr-054-equivalence-expansion.md) | Alias expansion | Widens required features into compile-time any-of groups instead of collapsing meanings. | Accepted |
| [058](../adr-058-punctuation-equivalence-folding.md) | Punctuation folding | Makes punctuation treatment configurable and shared across query and title normalization. | Accepted |
| [205](../adr-205-a-title-with-every-word-carries-the-form.md) | A title with every word of an alias form carries the form | Puts a multi-word alias form's entity in the positive title view whenever the title holds a reading of the form's text, so activating an alias removes no match (one overlap shape excepted) and no stored row changes. | Accepted |
| [204](../adr-204-vocabulary-change-interns-its-names.md) | A vocabulary change interns the names it introduces | Gives every feature name a single-node vocabulary change produces for stored queries its dense id before the read-only recompile, so a later insert cannot give the name a second id and strand the recompiled queries. | Accepted |
| [202](../adr-202-the-default-learner-expands.md) | The default learner expands | Makes the vocabulary learner apply what any-of groups teach by expansion unless `anyof_mode=collapse` is asked for, because a collapse rule can remove matches stored queries had. | Accepted |
| [218](../adr-218-a-phrase-is-consecutive-tokens.md) | A phrase is its words as consecutive tokens | Merges separators in cleaning so a vocabulary phrase is found whatever stands between its words (`north, star`), and selects phrases leftmost-longest among occurrences on token boundaries in every mode, so a match inside a word hides nothing. Fixes two false negatives; compiler semantics 8. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
