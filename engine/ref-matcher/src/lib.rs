//! # reverse-rusty-ref-matcher — the front-end-independent correctness reference (ADR-087)
//!
//! A second implementation of Reverse Rusty's matching language — the DSL parser, the shared
//! query/title normalizer, a grammar-preserving predicate tree, and its direct evaluator. It
//! reuses **none** of the `reverse-rusty` crate and contains no retrieval proxies, cost
//! classes, or storage lowering.
//!
//! ## Where each module comes from
//!
//! Every module is written from the specification. None is derived from the engine's code.
//!
//! | Module | Written from |
//! |---|---|
//! | [`semantic`], [`matcher`] | `docs/reference/dsl.md`, ADR-118/119/120 |
//! | [`features`], [`vocab`] | `docs/design/normalization.md` §2.1 |
//! | [`tables`] | the specification's tables, copied as data |
//! | [`clean`], [`normalize`], [`parse`] | `docs/design/normalization.md` §2.1 and "Parsing rules" in `docs/reference/dsl.md` |
//!
//! The last row was a port of the engine's `dsl.rs` and `normalize/` until ADR-220. It was
//! written again by an author who was given those two documents and this crate's other modules,
//! and not the engine's source, and who works on sequences of tokens where the engine works on
//! byte offsets and automata. So for the parser and the normalizer the two sides now share a
//! specification and not an algorithm: the differential can find a rule the engine gets wrong,
//! and not only a change that one side makes and the other does not.
//!
//! What it still cannot find is a rule the specification itself has wrong, or one that two
//! authors both misread. That is covered by tests that involve no reference at all: titles
//! built to satisfy a query and relations between a query and its edits (ADR-217), the
//! adversarial properties (ADR-063), and the hand-written truth tables.
//!
//! ## Why this exists
//! The in-tree differential oracle (`engine/tests/oracle/`) compares the engine to a
//! "brute-force" reference, but that reference calls the engine's OWN `dsl::parse`,
//! `compile::extract`, and `Normalizer`. So a bug in the parser/normalizer/extractor corrupts
//! both sides identically and the oracle stays green — the documented shared-front-end blind
//! spot (ADR-050). Diffing the engine against THIS reference, which shares no front-end code,
//! catches engine-vs-spec drift the in-tree oracle structurally cannot. The semantic tree also
//! prevents this reference from copying production lowering choices while using different code.
//!
//! ## The independence contract
//! This crate has **zero dependencies** — no `daachorse`, no `serde`, and above all no
//! `reverse-rusty`. That is enforced by the `ref-matcher independence` lane in `engine/check.sh`
//! (`cargo tree` must show no `reverse-rusty` edge). The algorithms are deliberately naive
//! (linear phrase scans instead of an Aho-Corasick automaton): a test oracle optimizes for
//! correctness and independence, not speed, and a second independent implementation of the same
//! algorithm is more likely to expose an integration bug than a shared library would be.
//!
//! ## Comparison is by canonical feature STRING
//! The reference compares matches by the engine's canonical feature names (`year:1994`,
//! `term:wireless`, `brand:acme`, …) — never the engine's interned integer
//! `FeatureId`s. That is what lets it reuse none of the dictionary machinery (synthetic hashing
//! included): two titles match a query iff they produce the same canonical feature set, by name.
//!
//! ## Layout
//! - [`features`] — the feature kinds + their canonical string forms.
//! - [`vocab`] — [`vocab::RefVocab`], the reference's own plain-data vocabulary (phrases,
//!   synonyms, number-context words, aliases, equivalences, and punctuation).
//!   The differential harness builds this AND the engine's `Vocab` from one neutral description.
//! - [`tables`] — the diacritic fold, the default punctuation classes, the year range and the
//!   query limits: the data the specification enumerates.
//! - [`clean`] — byte cleaning: lowercase + diacritic fold + the punctuation-class table.
//! - [`normalize`] — analysis of query text and titles: phrases, token typing, the ADR-061
//!   two title views `N(T)` / `P(T)`, and the graphs of quoted clauses (ADR-120).
//! - [`parse`] — the DSL parser (AND clauses, any-of groups, phrases, adjacent-`-` negation).
//! - [`semantic`] — AST → [`semantic::RefSemanticQuery`], retaining term, phrase, any-of, and
//!   forbidden predicates as grammar nodes; direct evaluation against canonical title views.
//! - [`matcher`] — [`matcher::RefMatcher`]: build semantic queries + a vocab, then
//!   `matches(title)`.

pub mod clean;
pub mod features;
pub mod matcher;
pub mod normalize;
pub mod parse;
pub mod semantic;
pub mod tables;
pub mod vocab;

pub use matcher::RefMatcher;
pub use vocab::RefVocab;
