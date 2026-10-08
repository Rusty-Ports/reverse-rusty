# ADR-219 — The reference matcher says where it comes from

> [Matching & verification decisions](areas/matching-and-verification.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

The independent reference matcher (ADR-087) is the arbiter of the largest differentials: the
engine and the reference are given the same queries and titles and must agree. Its crate
documentation said it was written "purely from the spec", and ADR-087 said its front end was
"never" written from engine code.

For four of its modules that was not so. The parser, the cleaner, the normalizer and the
phrase selection were translated from the engine's `dsl.rs` and `normalize/`, function by
function, when the crate was created, and their own comments said it ("Faithful translation of
`core.rs::emit`", "Mirrors `dsl::parse`"). They share no code with the engine. They share its
algorithm.

A differential reports where two implementations disagree. Where both run the same algorithm
it reports a change one side makes and the other does not, and it cannot report a reading of
the language that the algorithm already had. ADR-218 is an instance: a title with two
separators between a phrase's words did not carry the phrase, in the engine and in the
reference alike, and every differential was green.

The specification was also too thin to write a normalizer from. `normalization.md` gave the
stages in six sentences; rules the code implements (how a number beside `#` or `/` is typed,
what a run of spaces does to a phrase, how overlapping phrases are chosen, what an any-of
member's whitespace becomes) were written down nowhere. That is probably why the port
happened.

## Decision

1. **The crate says where each module comes from.** Its documentation has a table: the
   predicate tree and its evaluator, the feature names and the vocabulary type are written
   from the specification; the tables are the specification's data; the parser, cleaner,
   normalizer and phrase selection are ported from the engine. It says what "ported" costs:
   for those modules the differential finds drift and not a shared misreading.
2. **The front-end rules are stated in full**, as normative text: `design/normalization.md`
   §2.1 (cleaning with the whole fold table, tokens, phrases and their selection, each
   remaining token's rule including number typing, the two title views, equivalence classes,
   quoted clauses, clauses that analyze to nothing) and "Parsing rules" in `reference/dsl.md`
   (limits, negation, groups and members, quoted clauses, bare terms, runs).
3. **Copied data lives in one file.** The diacritic fold, the default punctuation classes, the
   year range and the query limits are in `ref-matcher/src/tables.rs`, which a reviewer checks
   against the specification line by line. A table has one right answer; copying it shares no
   logic.
4. **The gate keeps it true.** A `ref-matcher provenance` lane fails when a module that is not
   on the ported list cites engine code as the source of its logic, or when the crate
   documentation stops naming a ported module. A `ref-matcher tests` lane runs the reference's
   own unit tests, which no lane ran.
5. **The ported modules are to be written again from the specification alone**, by an author
   who is given the two documents and the tests and not the engine's source, in a shape
   different from the engine's, with every divergence triaged against the specification. The
   provenance lane's ported list then goes to nothing. That is a separate change, on the
   roadmap; this one makes the claim true as it stands and makes the re-write possible.

## What changes for a deployment

Nothing. No behaviour changes; the gate has two more lanes.

## Alternatives considered

- **Keep the port and say so, and stop there.** The claim would be true and the blind spot
  would stay. The layers that involve no reference cover clause semantics (ADR-217) and
  surface noise (ADR-063, ADR-218); they do not cover number typing, phrase selection or
  parsing edge cases systematically.
- **Re-write first and document afterwards.** The re-write needs the rules in writing, and
  writing them is what found ADR-218.
- **Treat a second implementation as proof.** It is not: see Prior art. A re-write removes
  the channel a port keeps open, and independent authors still tend to fail together on the
  hard parts. So the reference stays one layer of several, and examples taken from the
  specification stay the arbiter when the two implementations agree with each other and not
  with it.

## Consequences

- The differential's claim is narrower than the documents said, and now it is the documented
  claim.
- A rule that exists only in code now has a place where its absence shows: the normative
  text. A divergence between the engine and that text is a defect in one of them.
- Until the re-write, a change to the analyzer has to be made twice, in the engine and in the
  reference's port, as ADR-218 was.

## Proven

- `check.sh`: the provenance lane passes on the tree; it fails when a module outside the
  ported list is given a citation of engine code, and when the crate documentation's row is
  changed to leave a ported module out (both tried by hand).
- The reference's own unit tests run in the gate.
- The independent-oracle suites pass against the restructured crate.

## Prior art

What a second implementation is worth as an oracle, and what projects use to arbitrate
(sources read 2026-10-08).

- **Knight and Leveson, 1986**, "An Experimental Evaluation of the Assumption of Independence
  in Multiversion Programming": 27 versions written independently from one specification,
  one million tests. Each was reliable alone; tests on which more than one failed were far
  more frequent than independence predicts, and about half of all faults involved two or more
  versions. They did not find the common faults in the specification: their explanation is
  that some parts of a problem are harder and lead different programmers to the same mistake.
- **NASA-GB-8719.13 §7.4.2.1:** "Even if separate teams develop the software, studies have
  shown that the software is still often not truly independent."
- **Csmith** (Yang et al., PLDI 2011, §2.6): had every compiler produced the same wrong
  output it would have gone unseen, which they call a limitation inherent in differential
  testing without an oracle; they credit the diversity of the compilers' internals for not
  meeting it.
- **WebAssembly:** a specification, a reference interpreter written for clarity, and an
  official test suite, with engines fuzzed against the interpreter and against each other.
- **CommonMark:** the specification's own examples "are intended to double as conformance
  tests".
- **Wine's clean-room guidelines** say what a contributor must not have looked at, and that
  the way to learn a behaviour is to write a test for it.

What is taken from them: say how the reference was produced and what its author saw; do not
count agreement between two implementations as correctness where they share an origin, and
only partly where they do not; make them differ in structure; and keep examples drawn from
the specification as the arbiter.

**See also:** ADR-050 (the shared-front-end blind spot of the in-tree oracle), ADR-063
(properties that involve no reference), ADR-087 (the reference), ADR-217 (grammar corpora and
their reference-free properties), ADR-218 (the defect found by writing the rules down).

## Later outcome (2026-10-08, ADR-220)

Decision 5 is done. The parser, the cleaner and the normalizer were written again from the
normative text by an author who was not given the engine's source, and the phrase-selection
module went with the port. The provenance lane has no ported list any more: no module of the
reference may cite engine code. Twenty-two questions the text did not settle were settled in it,
and the comparison of the port with the re-write found one defect in the port (it cleaned a
text twice on the quoted path). See
[ADR-220](adr-220-the-reference-front-end-is-written-from-the-specification.md).
