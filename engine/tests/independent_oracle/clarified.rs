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
        vocab = vocab.alias_form("big apple");
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
            .alias_form("wi-fi router")
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

/// A phrase declared as a list of tokens is taken as given: it is not cut again when the
/// classes change. With the space classed `fold`, `north-star` is still the tokens `north`
/// and `star`, and the list still names them.
#[test]
fn a_list_of_tokens_is_taken_as_given() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_phrase(&["north", "star"], "brand:ns", FeatureKind::Brand);
        b.add_synonym("ns", "brand:ns", FeatureKind::Brand);
        b.set_punct_class(' ', PunctClass::Fold);
        b.build().expect("normalizer")
    };
    let vocab = || {
        let mut vocab = RefVocab::default_vocab()
            .phrase("north star", "brand:ns", PhraseMode::Collapse)
            .synonym("ns", "brand:ns");
        vocab.punct.set(' ', RefPunctClass::Fold);
        vocab
    };
    check(
        norm,
        vocab,
        "ns",
        &[
            ("north-star", true),
            ("north star", false),
            ("northstar", false),
        ],
    );
}

/// An alias form whose tokens are those of a phrase declared as a list makes that phrase an
/// alias and leaves it its feature, whichever of the two is declared first.
#[test]
fn an_alias_over_a_declared_phrase_makes_it_an_alias() {
    for alias_first in [false, true] {
        let norm = || {
            let mut b = NormalizerBuilder::new();
            if alias_first {
                b.add_alias_form("North Star");
            }
            b.add_phrase(&["north", "star"], "brand:ns", FeatureKind::Brand);
            if !alias_first {
                b.add_alias_form("North Star");
            }
            b.add_synonym("ns", "brand:ns", FeatureKind::Brand);
            b.build().expect("normalizer")
        };
        let vocab = || {
            let mut vocab = RefVocab::default_vocab();
            if alias_first {
                vocab = vocab.alias_form("North Star");
            }
            vocab = vocab.phrase("north star", "brand:ns", PhraseMode::Collapse);
            if !alias_first {
                vocab = vocab.alias_form("North Star");
            }
            vocab.synonym("ns", "brand:ns")
        };
        // It keeps its feature.
        check(norm, vocab, "ns", &[("north star", true)]);
        // As an alias it keeps its words on the title side, in the canonical view too.
        check(norm, vocab, "star", &[("north star", true)]);
        check(norm, vocab, "x -star", &[("x north star", false)]);
        // On the query side it consumes them, and a title carries the form in pieces.
        check(
            norm,
            vocab,
            "north star",
            &[("ns", true), ("star north", true), ("north", false)],
        );
    }
}

/// Every piece of a query is analyzed on the query side, a member of a group included: an
/// alias consumes its words there.
#[test]
fn a_member_of_a_group_is_analyzed_on_the_query_side() {
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_alias_form("open box");
        b.add_synonym("obx", "term:open_box", FeatureKind::Generic);
        b.build().expect("normalizer")
    };
    let vocab = || {
        RefVocab::default_vocab()
            .alias_form("open box")
            .synonym("obx", "term:open_box")
    };
    check(
        norm,
        vocab,
        "x -(open box)",
        &[("x obx", false), ("x open box", false), ("x open", true)],
    );
    check(
        norm,
        vocab,
        "(open box,zz)",
        &[("obx", true), ("open", false)],
    );
}

/// An equivalence class widens what a required clause accepts, quoted or not, and never a
/// forbidden one.
#[test]
fn a_class_widens_required_clauses_only() {
    let pair = |query: &str| {
        let mut vocab = reverse_rusty::vocab::Vocab::new();
        vocab.add_equivalence(&["ny", "nyc"]);
        let queries = vec![(1u64, query.to_string())];
        let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
        eng.build_from_queries(&queries);
        let reference = RefMatcher::build(
            &queries,
            RefVocab::default_vocab().equivalence(&["ny", "nyc"]),
        );
        (eng, reference)
    };
    let cases: [(&str, &[(&str, bool)]); 4] = [
        ("ny", &[("nyc", true), ("ny", true), ("la", false)]),
        ("(ny,zz)", &[("nyc", true), ("la", false)]),
        (
            "\"ny office\"",
            &[("nyc office", true), ("nyc x office", false)],
        ),
        ("x -ny", &[("x nyc", true), ("x ny", false)]),
    ];
    for (query, expected) in cases {
        let (eng, reference) = pair(query);
        check_pair(&eng, &reference, query, expected);
    }
}
