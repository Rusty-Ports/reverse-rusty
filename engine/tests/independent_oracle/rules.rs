//! Front-end rules the engine-versus-reference suites did not exercise.
//!
//! Found by changing the reference one rule at a time (ADR-220): each case here is a rule
//! whose removal from the reference left every other suite in this directory green, which
//! means the engine was not being held to it either. The expectations are written by hand
//! from `docs/design/normalization.md` §2.1 and "Parsing rules" in `docs/reference/dsl.md`,
//! and asserted against both sides.

use super::gotcha::{check, check_pair};
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty::segment::Engine;
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

fn plain_norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}

/// `north star -> brand:ns` in the given mode, with `ns` as a synonym for the same feature.
fn north_star(mode: PhraseMode, with_alias: bool) -> (Normalizer, RefVocab) {
    let mut b = NormalizerBuilder::new();
    match mode {
        PhraseMode::Additive => {
            b.add_phrase_additive(&["north", "star"], "brand:ns", FeatureKind::Brand);
        }
        _ => b.add_phrase(&["north", "star"], "brand:ns", FeatureKind::Brand),
    }
    b.add_synonym("ns", "brand:ns", FeatureKind::Brand);
    let mut vocab = RefVocab::default_vocab()
        .phrase("north star", "brand:ns", mode)
        .synonym("ns", "brand:ns");
    if with_alias {
        b.add_alias_form("big apple");
        vocab = vocab.alias_form("big apple");
    }
    (b.build().expect("normalizer"), vocab)
}

/// An additive phrase emits its feature and leaves its tokens to emit theirs, in the canonical
/// view too: a forbidden word inside it still rejects.
#[test]
fn an_additive_phrase_keeps_its_words() {
    let norm = || north_star(PhraseMode::Additive, false).0;
    let vocab = || north_star(PhraseMode::Additive, false).1;
    check(norm, vocab, "north", &[("north star", true)]);
    check(
        norm,
        vocab,
        "ns",
        &[("north star", true), ("star north", false)],
    );
    check(
        norm,
        vocab,
        "x -north",
        &[("x north star", false), ("x star", true)],
    );
}

/// A token a phrase consumed has no arc in the canonical graph, so a forbidden quoted word
/// does not find it there. A required one finds it in the positive graph.
#[test]
fn a_consumed_token_has_no_arc_in_the_canonical_graph() {
    let norm = || north_star(PhraseMode::Collapse, false).0;
    let vocab = || north_star(PhraseMode::Collapse, false).1;
    check(
        norm,
        vocab,
        "x -\"star\"",
        &[("x north star", true), ("x star", false)],
    );
    check(norm, vocab, "\"star\"", &[("north star", true)]);
}

/// The wide positive view holds what the stages emit when no phrase consumes its tokens: a
/// year inside a consuming phrase is there as a year once the vocabulary has an alias.
#[test]
fn the_wide_view_holds_what_a_consumed_token_emits() {
    let offer = |with_alias: bool| {
        let mut b = NormalizerBuilder::new();
        b.add_phrase(&["sale", "1999"], "entity:offer", FeatureKind::Entity);
        let mut vocab =
            RefVocab::default_vocab().phrase("sale 1999", "entity:offer", PhraseMode::Collapse);
        if with_alias {
            b.add_alias_form("big apple");
            vocab = vocab.alias_form("big apple");
        }
        (b.build().expect("normalizer"), vocab)
    };
    check(
        || offer(true).0,
        || offer(true).1,
        "1999",
        &[("sale 1999", true)],
    );
    check(
        || offer(false).0,
        || offer(false).1,
        "1999",
        &[("sale 1999", false)],
    );
}

/// The wide positive view holds every token as a term. So once the vocabulary has an alias,
/// a query for the term `1999` (`#1999`) matches a title whose `1999` is a year. Without an
/// alias it does not, and a query for the year never matches a title's `#1999`.
#[test]
fn the_wide_view_holds_every_token_as_a_term() {
    let wide = || {
        let mut b = NormalizerBuilder::new();
        b.add_alias_form("big apple");
        b.build().expect("normalizer")
    };
    let wide_vocab = || RefVocab::default_vocab().alias_form("big apple");
    check(
        wide,
        wide_vocab,
        "#1999",
        &[("1999", true), ("#1999", true), ("2001", false)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        "#1999",
        &[("1999", false), ("#1999", true)],
    );
    check(wide, wide_vocab, "1999", &[("#1999", false)]);
}

/// The positive graph has every token's own `term:` arc, with or without an alias: a quoted
/// clause whose edge an equivalence widened to `term:1999` finds a title's year there.
#[test]
fn the_positive_graph_holds_every_token_as_a_term() {
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_equivalence(&["#1999", "mm"]);
    let queries = vec![(1u64, "\"mm lamp\"".to_string())];
    let mut eng =
        Engine::with_vocab(vocab, reverse_rusty::config::EngineConfig::default()).expect("engine");
    eng.build_from_queries(&queries);
    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab().equivalence(&["#1999", "mm"]),
    );
    check_pair(
        &eng,
        &reference,
        "\"mm lamp\"",
        &[
            ("mm lamp", true),
            ("#1999 lamp", true),
            ("1999 lamp", true),
            ("1999 x lamp", false),
        ],
    );
}

/// The positive graph has an arc for every phrase occurrence, the ones selection passed over
/// included. The flat view has them only when the vocabulary has an alias.
#[test]
fn an_overlapping_occurrence_is_in_the_positive_graph() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_phrase(&["new", "york"], "entity:ny", FeatureKind::Entity);
        b.add_phrase(&["york", "city"], "entity:yc", FeatureKind::Entity);
        b.build().expect("normalizer")
    };
    let vocab = || {
        RefVocab::default_vocab()
            .phrase("new york", "entity:ny", PhraseMode::Collapse)
            .phrase("york city", "entity:yc", PhraseMode::Collapse)
    };
    check(
        norm,
        vocab,
        "\"york city\"",
        &[("new york city", true), ("york new city", false)],
    );
    check(norm, vocab, "york city", &[("new york city", false)]);
}

/// The tokens `#` and `/` emit nothing, so a clause that is only one of them is dropped.
#[test]
fn a_marker_alone_requires_and_forbids_nothing() {
    for query in ["x /", "x #", "x -/", "x -#"] {
        check(
            plain_norm,
            RefVocab::default_vocab,
            query,
            &[("x", true), ("x / #", true), ("y", false)],
        );
    }
}

/// Number typing: a number beside `/` is a term on either side of it; only exactly four
/// digits make a year; and a token with two dots is not a number, so a synonym can name it,
/// while a number is typed before any synonym is looked at.
#[test]
fn which_tokens_are_numbers_and_which_numbers_are_years() {
    check(
        plain_norm,
        RefVocab::default_vocab,
        "1999",
        &[("1999/5", false), ("5/1999", false), ("1999 5", true)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        "01999",
        &[("#01999", true), ("01999", true), ("1999", false)],
    );
    // Four digits and a dot are a number and not a year.
    check(
        plain_norm,
        RefVocab::default_vocab,
        "#1999.",
        &[("1999.", true), ("1999", false)],
    );
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_synonym("1.2.3", "term:release", FeatureKind::Generic);
        b.add_synonym("1.5", "term:minor", FeatureKind::Generic);
        b.build().expect("normalizer")
    };
    let vocab = || {
        RefVocab::default_vocab()
            .synonym("1.2.3", "term:release")
            .synonym("1.5", "term:minor")
    };
    check(norm, vocab, "release", &[("1.2.3", true), ("1.2", false)]);
    check(norm, vocab, "minor", &[("1.5", false), ("minor", true)]);
    check(norm, vocab, "1.5", &[("1.5", true)]);
}

/// A number-context word is compared without regard to ASCII case, however it was declared.
#[test]
fn a_number_context_word_is_compared_without_case() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.set_number_context_words(&["MODEL"]);
        b.build().expect("normalizer")
    };
    let vocab = || RefVocab::default_vocab().number_context(&["MODEL"]);
    check(
        norm,
        vocab,
        "1995",
        &[
            ("model 1995", false),
            ("Model 1995", false),
            ("series 1995", true),
        ],
    );
}

/// A synonym's token is compared as it was declared. One that cleaning cannot produce names
/// nothing.
#[test]
fn a_synonym_token_is_used_as_declared() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_synonym("PKG", "term:package", FeatureKind::Generic);
        b.build().expect("normalizer")
    };
    let vocab = || RefVocab::default_vocab().synonym("PKG", "term:package");
    check(
        norm,
        vocab,
        "package",
        &[("pkg", false), ("PKG", false), ("package", true)],
    );
}

/// A form carried in pieces is itself carried and can be a piece of another form, whatever
/// order the forms were declared in: the rule is applied until it adds nothing.
#[test]
fn forms_carried_in_pieces_reach_a_fixed_point() {
    let queries = vec![(1u64, "q".to_string())];
    let mut eng = Engine::new(plain_norm());
    eng.build_from_queries(&queries);
    // `x c` is declared before `a b`, and `x` stands for `a b`.
    eng.import_alias_synonyms("q => x c\nx => a b")
        .expect("import + apply aliases");
    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab()
            .alias_form("x c")
            .alias_form("a b")
            .equivalence(&["q", "x c"])
            .equivalence(&["x", "a b"]),
    );
    check_pair(
        &eng,
        &reference,
        "q",
        &[
            ("x c", true),
            ("a b c", true),
            ("b c a", true),
            ("a c", false),
            ("b a", false),
        ],
    );
}

/// A `"` ends a bare term; a group with no member left rejects the whole query; a query with
/// nothing it requires is not stored; and a query may have exactly 256 clauses.
#[test]
fn where_a_bare_term_ends_and_what_rejects_a_query() {
    check(
        plain_norm,
        RefVocab::default_vocab,
        "x\"d e\"f",
        &[("x d e f", true), ("x e d f", false), ("d e f", false)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        "x (,)",
        &[("x", false)],
    );
    // A query needs something it requires.
    check(
        plain_norm,
        RefVocab::default_vocab,
        "-used",
        &[("x", false), ("used", false)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        "x (a,,b)",
        &[("x a", true), ("x b", true), ("x", false)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        &"x ".repeat(256),
        &[("x", true)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        &"x ".repeat(257),
        &[("x", false)],
    );
}
