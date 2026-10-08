use super::*;
use crate::clean::PunctClass;
use crate::parse::parse;
use crate::semantic::{analyze, resolve_equivalences, RefTitle};
use crate::vocab::{Phrase, PhraseMode, RefPhrase};

fn names(features: &[Feature]) -> Vec<&str> {
    features.iter().map(Feature::as_str).collect()
}

fn query(vocab: &RefVocab, text: &str) -> Vec<String> {
    unique(query_features(vocab, text))
        .into_iter()
        .map(|feature| feature.0)
        .collect()
}

fn matches(vocab: &RefVocab, source: &str, title: &str) -> bool {
    analyze(&parse(source).unwrap(), vocab, &resolve_equivalences(vocab))
        .matches(&RefTitle::analyze(vocab, title))
}

fn has(arcs: &[RefPositionArc], name: &str, start: usize, end: usize) -> bool {
    arcs.contains(&arc(Feature::raw(name), start, end))
}

mod aliases;
mod graphs;

#[test]
fn phrase_forms_are_cleaned_and_match_whole_consecutive_tokens() {
    let vocab = RefVocab::default().phrase("Nórth, - stár", "brand:ns", PhraseMode::Collapse);
    for text in ["north, star", "north - star", "north star"] {
        assert_eq!(query(&vocab, text), ["brand:ns"]);
    }
    for text in ["north starlight", "upnorth star", "north bright star"] {
        assert!(!query(&vocab, text).contains(&"brand:ns".to_owned()));
    }
    let mut empty = RefVocab::default().phrase("!!!", "ignored", PhraseMode::Alias);
    assert!(empty.phrases_in_force().is_empty());
    empty.phrases.push(RefPhrase {
        form: String::new(),
        feature: "ignored".into(),
        mode: PhraseMode::Alias,
    });
    assert_eq!(query(&empty, "1999"), ["year:1999"]);
    assert_eq!(
        names(&title_views(&empty, "1999").positive_features),
        ["year:1999"]
    );
}

#[test]
fn phrase_forms_use_current_punctuation_regardless_of_declaration_order() {
    let before =
        RefVocab::default()
            .fold_punct('-')
            .phrase("a-b", "entity:ab", PhraseMode::Collapse);
    assert_eq!(query(&before, "a-b"), ["entity:ab"]);
    let after = RefVocab::default()
        .phrase("a-b", "entity:ab", PhraseMode::Collapse)
        .fold_punct('-');
    assert_eq!(query(&after, "a-b"), query(&before, "a-b"));
    assert_eq!(title_views(&after, "a-b"), title_views(&before, "a-b"));
    assert_eq!(quoted_clause(&after, "a-b"), quoted_clause(&before, "a-b"));
    let mut later = after;
    later.punct.set('-', PunctClass::Split);
    assert_eq!(query(&later, "a-b"), ["entity:ab"]);
    assert_eq!(title_views(&later, "a-b").positions, 2);
    assert_eq!(quoted_clause(&later, "a-b").arcs[0].end, 2);
}

#[test]
fn token_lists_are_matched_as_given_without_cleaning_them_again() {
    let tokens = clean_tokens("A b a-b", &RefVocab::default().punct);
    for declared in [vec!["A", "b"], vec!["a-b"], vec!["a b"], vec![""]] {
        let phrase = Phrase {
            tokens: declared.into_iter().map(str::to_owned).collect(),
            feature: "invalid".into(),
            mode: PhraseMode::Collapse,
        };
        assert!(analysis::all_phrase_arcs(&[phrase], &tokens).is_empty());
    }
    let vocab = RefVocab::default().fold_punct(' ');
    let phrase = Phrase {
        tokens: vec!["a".into(), "b".into()],
        feature: "pair".into(),
        mode: PhraseMode::Collapse,
    };
    let tokens = clean_tokens("a-b", &vocab.punct);
    assert_eq!(
        analysis::all_phrase_arcs(&[phrase], &tokens),
        [arc(Feature::raw("pair"), 0, 2)]
    );
}

#[test]
fn kept_non_ascii_uppercase_is_a_token_the_cleaner_can_produce() {
    let mut vocab = RefVocab::default().phrase("Ā x", "pair", PhraseMode::Alias);
    vocab.punct.set('Ā', PunctClass::Keep);
    assert_eq!(clean_tokens("Ā x", &vocab.punct), ["Ā", "x"]);
    assert_eq!(query(&vocab, "Ā x"), ["pair"]);
    assert!(has(
        &title_views(&vocab, "Ā x").canonical_arcs,
        "pair",
        0,
        2
    ));
}

#[test]
fn selection_is_leftmost_then_longest_and_never_overlaps() {
    let vocab = RefVocab::default()
        .phrase("a b", "short", PhraseMode::Collapse)
        .phrase("a b c", "long", PhraseMode::Collapse)
        .phrase("b c d", "later", PhraseMode::Collapse);
    assert_eq!(query(&vocab, "a b c d"), ["long", "term:d"]);
    let vocab = RefVocab::default()
        .phrase("a b", "first", PhraseMode::Additive)
        .phrase("b c d", "later", PhraseMode::Collapse)
        .phrase("c d", "next", PhraseMode::Collapse)
        .phrase("a b", "tied", PhraseMode::Collapse);
    assert_eq!(
        query(&vocab, "a b c d"),
        ["first", "next", "term:a", "term:b"]
    );
}

#[test]
fn modes_control_components_but_not_phrase_positions() {
    for (mode, query_keeps, title_keeps) in [
        (PhraseMode::Collapse, false, false),
        (PhraseMode::Additive, true, true),
        (PhraseMode::Alias, false, true),
    ] {
        let vocab = RefVocab::default().phrase("a b", "entity:ab", mode);
        let expected = if query_keeps {
            vec!["entity:ab", "term:a", "term:b"]
        } else {
            vec!["entity:ab"]
        };
        assert_eq!(query(&vocab, "a b"), expected);
        let views = title_views(&vocab, "a b a b");
        assert_eq!(views.positions, 4);
        assert!(has(&views.canonical_arcs, "entity:ab", 0, 2));
        assert!(has(&views.canonical_arcs, "entity:ab", 2, 4));
        assert_eq!(has(&views.canonical_arcs, "term:a", 0, 1), title_keeps);
        assert_eq!(views.canonical_arcs.len(), if title_keeps { 6 } else { 2 });
        assert_eq!(
            query_features(&vocab, "a b a b")
                .iter()
                .filter(|feature| feature.as_str() == "entity:ab")
                .count(),
            2
        );
    }
}

#[test]
fn numbers_precede_synonyms_and_use_exact_ascii_grammar() {
    let vocab = RefVocab::default()
        .synonym("1999", "wrong")
        .synonym("#", "wrong")
        .synonym("/", "wrong")
        .synonym("1.2.3", "not_a_number");
    for (text, feature) in [
        ("1899", "term:1899"),
        ("1900", "year:1900"),
        ("1999", "year:1999"),
        ("2099", "year:2099"),
        ("2100", "term:2100"),
        ("01999", "term:01999"),
        ("9.5", "term:9.5"),
        (".5", "term:.5"),
        ("5.", "term:5."),
        ("1999.", "term:1999."),
        (".", "term:."),
        ("1.2.3", "not_a_number"),
        (
            "999999999999999999999999999999",
            "term:999999999999999999999999999999",
        ),
    ] {
        assert_eq!(query(&vocab, text), [feature], "{text}");
    }
    assert!(query(&vocab, "# / １２３").is_empty());
    let mut punct = vocab;
    punct.punct.set('１', PunctClass::Keep);
    assert_eq!(query(&punct, "１999"), ["term:１999"]);
}

#[test]
fn number_context_is_by_original_neighbor_position() {
    let mut vocab = RefVocab::default();
    vocab.number_context.push("MoDeL".into());
    for text in ["#1999", "/1999", "1999/", "model 1999"] {
        assert!(query(&vocab, text).contains(&"term:1999".into()), "{text}");
        assert!(!query(&vocab, text).contains(&"year:1999".into()), "{text}");
    }
    for text in ["1999#", "model x 1999", "series 1999"] {
        assert!(query(&vocab, text).contains(&"year:1999".into()), "{text}");
    }
    for (context, text) in [("model", "model 1999"), ("#", "#1999"), ("/", "1999/")] {
        let consumed = vocab
            .clone()
            .phrase(context, "context", PhraseMode::Collapse);
        assert_eq!(query(&consumed, text), ["context", "term:1999"]);
    }
    assert_eq!(
        query(&RefVocab::default(), "model 1999"),
        ["term:model", "year:1999"]
    );
}

#[test]
fn synonym_fallback_and_custom_markers() {
    let mut vocab = RefVocab::default()
        .synonym("PKG", "unclean_key")
        .synonym("pkg", "term:package")
        .synonym("pkg", "second")
        .synonym("café", "unclean_key");
    vocab.punct.set('@', PunctClass::Marker);
    assert_eq!(
        query(&vocab, "PKG café @1999"),
        ["term:@", "term:cafe", "term:package", "year:1999"]
    );
    assert_eq!(
        query(&RefVocab::default(), "wireless mouse"),
        ["term:mouse", "term:wireless"]
    );
    assert_eq!(query(&vocab, "x x"), ["term:x"]);
}

#[test]
fn synonym_keys_are_not_lowercased_or_cleaned_at_declaration() {
    let vocab = RefVocab::default()
        .synonym("PKG", "uppercase")
        .synonym("café", "accent");
    for text in ["PKG", "pkg"] {
        assert_eq!(query(&vocab, text), ["term:pkg"]);
    }
    for text in ["café", "cafe"] {
        assert_eq!(query(&vocab, text), ["term:cafe"]);
    }
    let vocab = vocab.synonym("pkg", "first").synonym("pkg", "second");
    assert_eq!(query(&vocab, "PKG"), ["first"]);
}
