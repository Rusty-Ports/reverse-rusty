//! Rules that writing the reference's front end again made the specification state (ADR-220).
//!
//! Each is a place where an author working from the text alone had to choose a reading, or
//! where the ported reference differed from the engine. The expectations are written by hand
//! from `docs/design/normalization.md` §2.1 and "Parsing rules" in `docs/reference/dsl.md`,
//! and asserted against both the engine and the reference, as in `gotcha.rs`.

use super::gotcha::{check, check_pair};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder, PunctClass};
use reverse_rusty::segment::Engine;
use reverse_rusty_ref_matcher::clean::PunctClass as RefPunctClass;
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

fn plain_norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}

fn classed_norm(ch: char, class: PunctClass) -> Normalizer {
    let mut b = NormalizerBuilder::new();
    b.set_punct_class(ch, class);
    b.build().expect("normalizer")
}

fn classed_vocab(ch: char, class: RefPunctClass) -> RefVocab {
    let mut vocab = RefVocab::default_vocab();
    vocab.punct.set(ch, class);
    vocab
}

/// A text is cleaned once. With the space classed `fold`, cleaning a second time would delete
/// the separators the first pass wrote, so `t,e` would become the one token `te`. It is the
/// tokens `t` and `e`, in a quoted clause as everywhere else. (The ported reference cleaned
/// twice on the quoted path.)
#[test]
fn a_text_is_cleaned_once() {
    let norm = || classed_norm(' ', PunctClass::Fold);
    let vocab = || classed_vocab(' ', RefPunctClass::Fold);
    check(
        norm,
        vocab,
        "\"te\"",
        &[("te", true), ("t e", true), ("t,e", false), ("t-e", false)],
    );
    check(
        norm,
        vocab,
        "\"t,e\"",
        &[("t,e", true), ("t-e", true), ("te", false), ("e,t", false)],
    );
    check(
        norm,
        vocab,
        "x -\"te\"",
        &[("x,t,e", true), ("x,te", false)],
    );
}

/// Groups do not nest: inside a group a `(` is an ordinary character, and the group ends at
/// the first `)`.
#[test]
fn a_paren_inside_a_group_is_an_ordinary_character() {
    check(
        plain_norm,
        RefVocab::default_vocab,
        "(a,(b,c)) x",
        &[
            ("a x", true),
            ("b x", true),
            ("c x", true),
            ("x", false),
            ("a", false),
        ],
    );
    // Not the nesting it looks like: `(a OR (b AND c) OR d) AND e`.
    check(
        plain_norm,
        RefVocab::default_vocab,
        "(a,(b c,d) e)",
        &[
            ("a e", true),
            ("b c e", true),
            ("d e", true),
            ("b e", false),
            ("a", false),
        ],
    );
}

/// Only the first `-` negates, and a `(` or a `"` ends a clause without whitespace.
#[test]
fn one_dash_negates_and_delimiters_end_a_clause() {
    check(
        plain_norm,
        RefVocab::default_vocab,
        "x --used",
        &[("x", true), ("x used", false), ("used", false)],
    );
    // `--` is the negated bare term `-`, which has nothing in it and is dropped.
    check(
        plain_norm,
        RefVocab::default_vocab,
        "x --",
        &[("x", true), ("y", false)],
    );
    check(
        plain_norm,
        RefVocab::default_vocab,
        "a(b,c)\"d e\"f",
        &[
            ("a b d e f", true),
            ("f d e c a", true),
            ("a b e d f", false),
            ("a d e f", false),
            ("a b d e", false),
        ],
    );
}

/// Whitespace is any Unicode white-space character, between clauses and inside a member. A
/// string with no clause at all is not stored.
#[test]
fn unicode_whitespace_and_a_string_with_no_clause() {
    check(
        plain_norm,
        RefVocab::default_vocab,
        "(red\u{2003}shoe,boot)\u{2003}x",
        &[("red shoe x", true), ("boot x", true), ("red x", false)],
    );
    for nothing in ["", " \t\u{a0}", "\"\"", "(!!!)"] {
        check(
            plain_norm,
            RefVocab::default_vocab,
            nothing,
            &[("a", false), ("", false)],
        );
    }
}

fn twice_declared(with_alias: bool) -> (Normalizer, RefVocab) {
    let mut b = NormalizerBuilder::new();
    b.add_phrase(&["north", "star"], "brand:first", FeatureKind::Brand);
    b.add_phrase_additive(&["north", "star"], "brand:second", FeatureKind::Brand);
    b.add_synonym("nf", "brand:first", FeatureKind::Brand);
    b.add_synonym("nsec", "brand:second", FeatureKind::Brand);
    b.add_synonym("pkg", "term:first", FeatureKind::Generic);
    b.add_synonym("pkg", "term:second", FeatureKind::Generic);
    let mut vocab = RefVocab::default_vocab()
        .phrase("north star", "brand:first", PhraseMode::Collapse)
        .phrase("north star", "brand:second", PhraseMode::Additive)
        .synonym("nf", "brand:first")
        .synonym("nsec", "brand:second")
        .synonym("pkg", "term:first")
        .synonym("pkg", "term:second");
    if with_alias {
        b.add_alias_form("big apple");
        vocab = vocab.phrase("big apple", "term:big_apple", PhraseMode::Alias);
    }
    (b.build().expect("normalizer"), vocab)
}

/// A vocabulary has one phrase for a sequence of tokens and one synonym for a token: the
/// first declared stands, and the other is ignored in every view, the wide positive one
/// included.
#[test]
fn the_first_of_two_declarations_stands() {
    for with_alias in [false, true] {
        let norm = || twice_declared(with_alias).0;
        let vocab = || twice_declared(with_alias).1;
        check(norm, vocab, "nf", &[("north star", true)]);
        check(
            norm,
            vocab,
            "nsec",
            &[("north star", false), ("nsec", true)],
        );
        check(norm, vocab, "first", &[("pkg", true)]);
        check(norm, vocab, "second", &[("pkg", false)]);
    }
    // Nor does the second declaration's mode apply: the first one consumes the words.
    let norm = || twice_declared(false).0;
    let vocab = || twice_declared(false).1;
    check(norm, vocab, "north", &[("north star", false)]);
}

/// The positive view is the wide one when the vocabulary has an alias, whether or not the
/// alias occurs in the title.
#[test]
fn an_alias_anywhere_in_the_vocabulary_widens_every_title() {
    let wide = || twice_declared(true);
    check(|| wide().0, || wide().1, "star", &[("north star", true)]);
    check(
        || wide().0,
        || wide().1,
        "x -star",
        &[("x north star", true)],
    );
}

/// "A token that is exactly `#` or `/` emits nothing" is about those two tokens, not about
/// the `marker` class: another character classed `marker` is a token of its own and then a
/// token like any other.
#[test]
fn the_marker_rule_is_about_two_tokens_and_not_the_class() {
    let norm = || classed_norm('@', PunctClass::Marker);
    let vocab = || classed_vocab('@', RefPunctClass::Marker);
    check(
        norm,
        vocab,
        "1999",
        &[("@1999", true), ("1999", true), ("#1999", false)],
    );
    check(
        norm,
        vocab,
        "@",
        &[("a @ b", true), ("a@b", true), ("a b", false)],
    );
    check(
        norm,
        vocab,
        "\"@ 1999\"",
        &[("@1999", true), ("@ x 1999", false)],
    );
}

/// An alias's form is cut under the punctuation classes as they finally stand, in whatever
/// order the vocabulary was declared.
#[test]
fn an_alias_form_is_cut_under_the_final_classes() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_alias_form("wi-fi router");
        b.add_synonym("wr", "term:wifi_router", FeatureKind::Generic);
        b.set_punct_class('-', PunctClass::Fold);
        b.build().expect("normalizer")
    };
    let vocab = || {
        RefVocab::default_vocab()
            .phrase("wi-fi router", "term:wifi_router", PhraseMode::Alias)
            .synonym("wr", "term:wifi_router")
            .fold_punct('-')
    };
    check(
        norm,
        vocab,
        "wr",
        &[
            ("wi-fi router", true),
            ("wifi router", true),
            ("wi fi router", false),
            ("router", false),
        ],
    );
}

/// An equivalence form takes part when it analyzes to exactly one distinct feature: `zeta
/// zeta` is one.
#[test]
fn an_equivalence_form_counts_distinct_features() {
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_equivalence(&["zeta zeta", "omega"]);
    let queries = vec![(1u64, "zeta".to_string())];
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&queries);
    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab().equivalence(&["zeta zeta", "omega"]),
    );
    check_pair(
        &eng,
        &reference,
        "zeta",
        &[("omega", true), ("zeta", true), ("alpha", false)],
    );
}
