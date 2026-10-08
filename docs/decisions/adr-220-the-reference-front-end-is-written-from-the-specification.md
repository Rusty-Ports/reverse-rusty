# ADR-220 — The reference matcher's front end is written from the specification

> [Matching & verification decisions](areas/matching-and-verification.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

ADR-219 recorded that four modules of the independent reference matcher (the parser, the
cleaner, the normalizer and the phrase selection) had been translated from the engine's code,
function by function. Two programs that run the same algorithm agree on every reading that
algorithm has, right or wrong, so for those modules the differential could report a change
made on one side and not a rule both had wrong. ADR-218 was such a rule, and it was found by
writing the rules down, not by any differential.

ADR-219 wrote the rules down as normative text and left the re-write as its fifth decision.
This is the re-write.

## Decision

1. **Two rooms.** The specification (`design/normalization.md` §2.1, "Parsing rules" in
   `reference/dsl.md`) was written by someone who had read the engine. The front end was
   written by an author who was given those two documents, the reference's modules that
   already came from the specification (`features`, `tables`, `vocab`, `semantic`, `matcher`)
   and stubs holding the signatures the evaluator calls. It was not given the engine's source,
   the ported modules, or the tests that had been written beside the port. It worked in a
   directory that held nothing else, under the instruction to read nothing outside it, and its
   session was recorded. The record of its four rounds (136 commands) holds no command that
   reads outside that directory and none that uses the network.
2. **A different author.** The port and the specification came from one author. The re-write
   was done by another: a language model from a different provider. Authors of one family
   agree on wrong answers more often than chance (see Prior art), so the second author was
   chosen to be as unlike the first as was available.
3. **A different shape.** The interface between the evaluator and the front end is stated in
   the specification's terms: `query_features`, `title_views`, `quoted_clause` and
   `phrase_graph_matches` replace an `emit(text, side, force_additive)` that had the engine's
   shape. Behind it the new front end works on sequences of tokens. Cleaning returns tokens; a
   phrase occurs where its tokens are a slice of the text's tokens; there are no byte offsets,
   no boundary checks and no automaton. The engine works on byte spans into one cleaned buffer
   with two Aho-Corasick automata.
4. **What the text does not settle is a deliverable.** The author was told not to guess
   silently: every sentence that could be read two ways went into a list, with an input that
   shows the difference and the reading chosen. Each entry was settled by finding what the
   engine does, deciding whether that is the rule, and writing the rule into the specification.
   The author then brought its code in line with the clarified text. The table below is that
   list.
5. **A vocabulary is declared, not analyzed.** `RefVocab` used to clean every phrase's form
   when it was declared, under whatever punctuation classes had been set so far, so the order
   of declarations changed what a phrase was. It now keeps what was declared, in the two ways
   the specification gives: a list of tokens, taken as given, or an alias form, which is
   text. `phrases_in_force()` answers what the declarations amount to when a text is
   analyzed: the first of two lists with the same tokens stands; an alias form is cut under
   the classes as they stand, is no phrase with fewer than two tokens, over the tokens of a
   declared list makes that phrase an alias and leaves it its feature, and otherwise is a
   phrase named from its tokens. `synonym_for()` gives the first synonym declared for a token.
6. **The gate keeps it.** The `ref-matcher provenance` lane now has no list of exceptions: a
   module of the reference that cites engine code as the source of its logic fails, in any
   directory of the crate, and so does crate documentation that stops saying where the modules
   come from.

## What the author asked, and what the specification now says

| Question | Settled as |
|---|---|
| Do clauses need whitespace between them? | No: a `(` or `"` ends a bare term, and a closing `)` or `"` ends its clause. `a(b,c)"d"e` is four clauses. |
| Is a `(` inside a group an error? | No. It is an ordinary character, and the group ends at the first `)`. The author had rejected it; the engine accepts it. See Consequences. |
| Which characters are whitespace, and is a run of them one space? | Any Unicode white-space character; inside a member each one is one space. |
| Are empty clauses and empty members counted against the limits? | Clauses are counted as written, members after empty ones are dropped. A string with no clause at all is not an error and is not stored. Which of several broken rules is reported is not specified. |
| Does `--used` negate twice? | No. One `-` negates; the rest belongs to the bare term. |
| Two phrases with the same tokens, or two synonyms for one token? | The first declared stands and the other is ignored in every view. |
| Is a synonym's token cleaned? | No. It is compared as declared, and so is a phrase declared as a list of tokens. See Consequences. |
| Does an alias widen a title it does not occur in? | Yes: the positive view is the wide one whenever the vocabulary has an alias. |
| Must the pieces of an alias form stand in order in the title? | No. Carrying is about the set of features, one feature can carry two pieces, and no arc is added. |
| Is `a a` one feature or two for an equivalence form? | One: distinct features are counted. |
| Is a character classed `marker` treated like `#` and `/`? | No. The rule that a token emits nothing is about the tokens `#` and `/`. |
| Is a bare term analyzed on the title side? | By the same rules, on the query side: an alias consumes its words. |
| Is a phrase's form cut again when the classes change? | An alias's form is cut under the classes as they finally stand; a list of tokens is taken as given. Text is cleaned once. |

Two reviews of the result added six more. One reviewer wrote a third implementation from the
two passages and ran it against the re-write (300,000 query and title pairs, 600,000 query
strings): they agreed, and what it found were things the text did not say.

| Found by review | Settled as |
|---|---|
| The reference vocabulary cleaned every declared form, and the text says a list of tokens is taken as given. | The vocabulary type has both ways of declaring a phrase. An alias form over a declared list makes that phrase an alias and keeps its feature; a form of one token is no phrase. |
| Nothing said an equivalence class widens an unquoted clause. | A class widens what any required clause accepts, quoted or not, and never a forbidden one. |
| Nothing said which side a member of a group is analyzed on. | Every piece of a query is analyzed on the query side. |
| Is `1999.` a year? | No: four digits with no `.`. |
| Nothing said a query with only forbidden clauses is not stored. | It is not, unless the deployment accepts such queries. |
| The author's tests pinned which of several errors is reported. | The text says that is not specified, and the tests now assert only the rejection. |

The author then found one in the vocabulary type itself: the text names a standalone alias
form from its tokens, and the type took the name from its caller. The type now derives it.

## What changes for a deployment

Nothing. The engine is unchanged.

## Alternatives considered

- **The same author, told not to look.** There would be no barrier that can be checked and
  no difference in who reads the text.
- **A person in the clean room.** That is the method this copies, and it would be better
  still. The setup can be repeated by one: the two documents, the five modules and the stubs
  are all there is to hand over.
- **Give the author the engine-versus-reference tests.** They depend on the engine's crate
  and carry its behaviour. The author wrote its tests from the specification's examples and
  rules; the engine-versus-reference suites were run afterwards, unchanged.
- **Keep the ported modules' unit tests.** They were written beside the port and say what it
  did. They were replaced by the author's, and the ported front end was instead run against
  the new one directly.
- **Make the engine clean declared phrase and synonym tokens in this change.** It changes
  what stored queries compile to under some vocabularies. It is on the roadmap.

## Consequences

- For the parser and the normalizer the two sides now share a specification and not an
  algorithm. A rule the engine implements differently from the text, as an independent reader
  understands the text, is a divergence.
- This is not proof. Independent authors fail together on the hard parts, both authors here
  are language models, and a rule the specification has wrong is wrong in both. The layers
  with no reference (ADR-063, ADR-217, the hand-written tables) stay the arbiter there.
- A change to the analyzer is now made in three places, in this order: the sentence in the
  specification, the engine, and the reference from the sentence. Writing the reference's
  side from the engine's diff would undo this decision, and only review can see that.
- Two behaviours of the engine were written down as rules because they are what it does, and
  both are on the roadmap as questions. A vocabulary's declared phrase tokens and synonym
  tokens are never cleaned, so one with a capital letter or a hyphen loads and matches
  nothing. A query that tries to nest groups is stored as a different query.
- One place where the engine does not follow the rule is on the roadmap as a defect: a second
  declaration of a phrase through `NormalizerBuilder::add_phrase_alias` is half applied (it
  widens every title's positive view and adds its own feature there). A vocabulary document,
  the REST API and alias import cannot produce it, and no suite builds one.
- The reference asks the vocabulary for its phrases and its equivalence classes at every
  analysis, and its rule for alias forms carried in pieces is quadratic in the number of alias
  forms. The suites take the same time as before. A vocabulary of thousands of aliases would
  not: a reviewer measured 320 ms a title at 8,000. That has to be prepared once for each
  vocabulary before the reference is run over a real corpus, and it is on the roadmap.

## Proven

- **The port against the re-write.** A scratch harness built both from one random vocabulary
  description and compared tokens, query features, both title views, both title graphs, quoted
  graphs, parses and whole-matcher answers. Over 60,000 vocabularies, 240,000 texts (80,000
  with a phrase occurrence) and 1.4 million match questions it found two kinds of difference.
  The port cleaned a text twice on the quoted path, which shows only when a vocabulary classes
  the space as `fold` (the second pass deletes the separators the first wrote): a defect of
  the port, since the engine cleans once. And the `(` inside a group. With the first corrected
  in the copy of the port and the second settled, two further runs found none.
- **The engine against the re-write.** Every `tests/independent_oracle/` suite, the quoted
  phrase suite and the crash-injection oracle pass unchanged.
- **A third reading.** A reviewer who was given the two passages and the reference, and told
  not to read the engine, wrote its own implementation from the text and compared: 306,584
  query and title pairs under 6,000 random vocabularies, 48,000 titles (tokens, both views,
  both graphs, quoted graphs, equivalence classes), every query string of up to six characters
  over nine significant ones (597,871), and the fold table for every Unicode scalar. The one
  difference was the widening of unquoted clauses, which the text had not stated.
- **The clarified rules.** `tests/independent_oracle/clarified.rs` asserts hand-written
  expectations for fourteen of them against the engine and the reference.
  `a_text_is_cleaned_once` is the port's defect.
- **Mutants, and what they showed about the suites.** Fifty single changes to the new front
  end and the vocabulary type, each run against the reference's own tests and against the
  engine-versus-reference suites. Two are equivalent. The reference's tests catch 46 of the
  other 48, and the suites the remaining two. The suites alone at first caught little more
  than half: a rule the reference could lose without any of them failing is a rule the
  engine was not being held to either. `tests/independent_oracle/rules.rs` adds a
  hand-written case for each such rule that can be seen in a match (additive phrases, a
  number beside `/`, which numbers are years, tokens with two dots, the fall-back arc, the
  fixed point of forms carried in pieces, overlapping occurrences and literal tokens in the
  positive graph and view, the marker tokens, and four parsing rules), and the suites now
  catch 45. The engine agreed with the specification on every one. The three the suites
  still miss cannot be seen in a match through the declaration interfaces, or need a marker
  inside a phrase.
- **The gate.** The provenance lane passes on the tree and fails for a citation of engine
  code added to a module in a subdirectory, and for crate documentation without the
  statement.

## Prior art

Sources read 2026-10-08, in addition to those in ADR-219.

- **Clean-room design.** One team examines the original and writes a specification; a second,
  with no connection to the first and no exposure to the original, implements from the
  specification alone. Phoenix's 1984 PC BIOS is the usual example: its implementer had never
  worked with the processor family.
- **IETF, RFC 2026 §4.1.2.** A specification advances when "at least two independent and
  interoperable implementations from different code bases have been developed", and an option
  that has not been shown in two is removed from it. The second implementation is how the
  text is tested, and what it cannot support comes out of the text.
- **Kim, Garg, Peng and Garg, ICML 2025**, "Correlated Errors in Large Language Models":
  across more than 350 models, two models that are both wrong agree on the wrong answer far
  more often than chance, and more so when they come from the same provider.

What is taken from them: the author of the second implementation must not have seen the
first; what the author cannot derive from the text goes back into the text; and the second
author should be as unlike the first as can be had, without counting that as independence.

**See also:** ADR-087 (the reference), ADR-217 (grammar corpora), ADR-218 (the rule found by
writing the rules down), ADR-219 (the provenance record and the normative text).
