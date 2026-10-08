//! # reverse-rusty-ref-matcher — the front-end-independent correctness reference (ADR-087)
//!
//! A second implementation of Reverse Rusty's matching language — the DSL parser, the shared
//! query/title normalizer, a grammar-preserving predicate tree, and its direct evaluator. It
//! reuses **none** of the `reverse-rusty` crate and contains no retrieval proxies, cost
//! classes, or storage lowering.
//!
//! ## Where each module comes from
//!
//! | Module | Written from |
//! |---|---|
//! | [`semantic`], [`matcher`] | the specification (`docs/reference/dsl.md`, ADR-118/119/120) |
//! | [`features`], [`vocab`] | the specification (`docs/design/normalization.md` §2.1) |
//! | [`tables`] | the specification's tables, copied as data |
//! | [`clean`], [`normalize`], [`parse`], [`phrases`] | **ported from the engine's code** |
//!
//! "Ported" means translated from the engine's `dsl.rs` and `normalize/` function by function
//! when this crate was created. Those modules share no code with the engine, and they do share
//! its algorithm. So for the parser and the normalizer the differential detects a change that
//! one side makes and the other does not; it does **not** detect a reading of the
//! specification that the engine had at the time of the port, because the reference has the
//! same one. That class is covered by tests that involve no reference at all: titles built to
//! satisfy a query and relations between a query and its edits (ADR-217), the adversarial
//! properties (ADR-063), and the hand-written truth tables. The predicate tree and its
//! evaluator are independent in the full sense. ADR-219 records this and the plan to
//! re-write the ported modules from the specification alone.
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
//! - [`normalize`] — the two-phase emit pipeline producing canonical features, including the
//!   ADR-061 two title views `N(T)` / `P(T)`.
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
pub mod phrases;
pub mod semantic;
pub mod tables;
pub mod vocab;

pub use matcher::RefMatcher;
pub use vocab::RefVocab;
