# ADR-218 — A phrase is its words as consecutive tokens

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A vocabulary phrase (`north star` → `brand:north_star`) is recognised by the analyzer in
queries and in titles. Two things in how it was looked for lost matches.

**Separators.** Cleaning turned every separator into a space of its own and did not merge
them: `north, star` became `north`, two spaces, `star`. The phrase was registered as its words
joined by one space and was looked for as that string in the cleaned text. So it was found in
`north star` and in `North-Star`, and not in `north  star`, `north, star` or `north - star`.
For a phrase that consumes its words (every manual phrase), a title written that way carried
the two words and not the phrase's feature, while a query `north star` required the feature:
the query did not match the title. A quoted `"north star"` did, because quoted analysis
reduces runs of spaces on both sides (ADR-120). The same on the query side: an any-of member
typed with two spaces required the words, which a title carrying the phrase no longer had.

The query side reduced runs when a multi-word alias was active (ADR-061), and the title's
positive view found aliases across runs. Neither applied to a vocabulary without aliases, and
the title's canonical view never did.

**Matches inside words.** Without an alias, phrases were selected by one leftmost-longest
pass of the automaton over the cleaned text, and each match was checked afterwards for
starting and ending on a token boundary. The automaton commits to a match before that check.
A pattern found inside a word (`north star` in `xnorth star lamp`) or ending inside one
(`new york city` in `new york cityscape`) was taken, was then discarded, and had already
hidden the valid phrase it overlapped (`star lamp`, `new york`). The alias path had been given
a boundary-aware selection for this reason (ADR-061); the alias-free path kept the old pass.

Both are false negatives, reproduced. The first was invisible to every differential, because
the brute-force oracle uses the engine's analyzer and the independent reference's cleaner was
ported from it. The surface-noise property that would have shown it (extra spaces and
punctuation between a title's words do not change what it matches) ran only without phrases.
It was found by writing the analyzer's rules down in full and trying one the text implied.

## Decision

1. **A phrase is its words as consecutive tokens, whatever separates them.** Cleaning merges
   separators: the cleaned text never has two spaces in a row and never starts with one. The
   tokens are the same as before. A comma and a space, a hyphen between spaces, a tab and two
   spaces are each one token boundary, and a phrase is found across them as across one space,
   in a query and in a title, in every view.
2. **A character that is kept is part of a token.** `north . star` has three tokens under the
   default punctuation classes and does not carry `north star`. A marker (`#`, `/`) is a token
   too. The space is the exception: it is the separator whatever class a vocabulary gives it,
   so no configuration brings runs of separators back.
3. **Selection is leftmost-longest over the occurrences that sit on token boundaries**, in
   every mode. The fast leftmost-longest pass runs first; the first match it reports that is
   not on token boundaries hands the text to the boundary-aware selection
   (`PhraseOverlap::select_phrases`), which collects every occurrence on boundaries, takes the
   earliest, the longest of those that start together, and drops what overlaps one already
   taken. When every match of the fast pass is on boundaries the two give the same answer:
   its first match is then the earliest occurrence and the longest at that start.
4. **Stored queries are recompiled.** The compiler-semantics version goes from 7 to 8. A
   stored query whose text has two separators inside a phrase, or a phrase beside a pattern
   that only matches inside a word, compiled to the phrase's words and now compiles to the
   phrase. Left as it was, such a row would stop matching titles that now carry the phrase's
   feature in place of the words. A store written under an older version is rebuilt from its
   sources before it serves, as for every earlier version.
5. **The reference matcher follows the rule**, and its hand-written truth table gets the rows
   for both shapes.

## What changes for a deployment

- With a vocabulary that has phrases (manual, learned or alias): a title or a query in which
  two or more separators stand between a phrase's words now carries the phrase. Queries that
  use the phrase unquoted match such titles; they did not before.
- For a phrase that consumes its words, such a title no longer carries the words on their
  own, exactly as a title with one separator never did: `-north` does not reject
  `north, star`, and a query for `north` alone does not match it.
- Without phrases in the vocabulary nothing changes.
- **Upgrade:** compiler semantics 8. A single-node store or an in-process cluster written by
  an earlier release is rebuilt from its sources at the first open. A remote mesh must be
  upgraded as a whole: a peer on an older version is refused.

## Alternatives considered

- **Reduce runs of spaces after cleaning** (what the alias path did on the query side). One
  more pass over every title, and a rule that has to be remembered at each new caller. Not
  writing the second space is free and cannot be forgotten.
- **Match phrases on tokens instead of on text** (a trie over token ids, as Lucene's synonym
  filter walks its FST token by token). That is the rule stated directly, and a larger
  change to the analyzer's hot path. With separators merged, a string match that starts and
  ends on token boundaries is a match on consecutive tokens.
- **Always use the boundary-aware selection.** It scans for overlapping occurrences and sorts
  them, for every title. The fast pass gives the same answer whenever all of its matches are
  on boundaries, which is nearly always, so it stays in front.
- **Fix the title side only and leave stored queries alone**, to avoid the rebuild. A row
  compiled to a phrase's words would then lose titles it matched by accident of both sides
  missing the phrase. The version exists for this.
- **Keep the phrase's words visible on the title side** (make every phrase additive). That is
  a different decision about what a manual phrase means, and it would not have fixed the
  query side.

## Consequences

- The earlier statement that cleaned text is byte-identical across versions (ADR-061) no
  longer holds, by decision; what it protected, stored queries staying in step with titles,
  is what the version bump is for.
- The surface-noise property now holds, and is tested, under a vocabulary with phrases.
- **What this leaves, by the rules as written:** a manual phrase consumes its words, so a
  query for `star` does not match a title that carries `north star`; and where two phrases
  overlap in a title the earlier one wins in the canonical view (`north star lamp` carries
  `north star` and not `star lamp`) unless an alias is active, when the positive view has
  both. Those are what the vocabulary's modes mean, not defects of selection.

## Proven

- `normalize/tests.rs`: a phrase is found across two spaces, a comma and a space, a hyphen
  between spaces, tabs, and a hyphen alone; not across a kept character or another word.
  Cleaned text has no two separators in a row and no leading one, also when a vocabulary
  classes the space itself as kept or as a marker. A run inside an alias form
  is the form on the query side and in both title views. A pattern found inside a word hides
  no phrase, and where the longer or the earlier phrase is an occurrence it still wins.
- `tests/independent_oracle/gotcha.rs`, engine and reference against hand-written
  expectations: the phrase across every separator, for the bare, any-of and quoted query;
  a member typed with two spaces; what a forbidden term then sees; and both shapes of a match
  inside a word.
- `tests/adversarial/perturbation.rs`: under a vocabulary where the generator's multi-word
  brands are phrases, widening whitespace, sprinkling split punctuation, case, diacritics and
  appended junk leave every title's match set identical.
- The stored-query rebuild is the existing version migration; its tests run at version 8.
- Mutation checks, each after an unmutated baseline: listed in the pull request.

## Prior art

How analyzers define a multi-word entry, and how they choose among overlapping ones (sources
read 2026-10-08).

- **Lucene.** `SynonymGraphFilter` "applies single- or multi-token synonyms … to an incoming
  TokenStream": matching is over tokens. `StandardTokenizer` implements the Unicode word-break
  rules and writes no token and no position gap for what stands between words ("Not numeric,
  word, ideographic, hiragana, emoji or SE Asian -- ignore it"), so `north, star` is the
  adjacent tokens `north` and `star`. Rule words are joined by a sentinel, not a space. On
  overlap: "parsing is greedy, so whenever multiple parses would apply, the rule starting the
  earliest and parsing the most tokens wins", and a match can only start and end with a
  token, so boundaries are part of matching and not a filter after it.
- **Elasticsearch.** Multi-word synonyms are "a graph token stream" over token positions.
- **spaCy `PhraseMatcher`** matches "sequences of tokens". It keeps punctuation as tokens, so
  it is defined on tokens without ignoring separators.
- **Character-level tools.** flashtext matches "complete words (words with boundary
  characters on both sides)" during its scan, and a keyword's space is a character of its
  trie. The Rust `aho-corasick` crate is literal search; its non-overlapping iteration drops
  what overlaps an earlier match ("The `abcd` match is never reported since it overlaps with
  the `b` match"), and for word boundaries its maintainer suggests to "use overlapping
  matches along with a buffer of matches. Then fix up the matches after the fact", which is
  what the boundary-aware selection does.

The rule taken: entries are over token positions, separators are not positions, and the
choice among overlapping entries is made among candidates on token boundaries, earliest then
longest.

**See also:** ADR-058 (punctuation classes), ADR-061 (aliases, the two title views, and the
boundary-aware selection), ADR-063 (the surface-noise property), ADR-120 (quoted clauses),
ADR-187 (the previous compiler-semantics version).
