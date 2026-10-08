use super::*;

#[test]
fn alias_forms_are_cleaned_and_match_whole_consecutive_tokens() {
    let vocab = RefVocab::default().alias_form("Nórth, - stár");
    for text in ["north, star", "north - star", "north star"] {
        assert_eq!(query(&vocab, text), ["term:north_star"]);
        assert!(has(
            &title_views(&vocab, text).canonical_arcs,
            "term:north_star",
            0,
            2
        ));
    }
    for text in ["north starlight", "upnorth star", "north bright star"] {
        assert!(!query(&vocab, text).contains(&"term:north_star".into()));
    }
}

#[test]
fn empty_lists_and_alias_forms_shorter_than_two_tokens_are_ignored() {
    let vocab = RefVocab::default()
        .phrase_tokens(&[], "empty_list", PhraseMode::Alias)
        .alias_form("")
        .alias_form("!!!")
        .alias_form("NÝ")
        .alias_form("a");
    assert!(vocab.phrases_in_force().is_empty());
    assert!(!vocab.has_alias());
    assert_eq!(
        query(&vocab, "1999 ny a"),
        ["term:a", "term:ny", "year:1999"]
    );
    assert_eq!(
        names(&title_views(&vocab, "1999").positive_features),
        ["year:1999"]
    );
    assert_eq!(
        names(&quoted_clause(&vocab, "ny").arcs[0].alternatives),
        ["term:ny"]
    );
    let equivalent = vocab.equivalence(&["ny", "gotham"]);
    assert!(matches(&equivalent, "ny", "gotham"));
}

#[test]
fn alias_forms_use_final_punctuation_and_derive_names_from_the_result() {
    let before = RefVocab::default()
        .fold_punct('-')
        .alias_form("wi-fi router");
    let after = RefVocab::default()
        .alias_form("wi-fi router")
        .fold_punct('-');
    assert_eq!(query(&before, "wi-fi router"), ["term:wifi_router"]);
    assert_eq!(
        query(&after, "wi-fi router"),
        query(&before, "wi-fi router")
    );
    assert_eq!(
        title_views(&after, "wi-fi router"),
        title_views(&before, "wi-fi router")
    );
    assert_eq!(
        quoted_clause(&after, "wi-fi router"),
        quoted_clause(&before, "wi-fi router")
    );
    let mut later = after;
    later.punct.set('-', PunctClass::Split);
    assert_eq!(query(&later, "wi-fi router"), ["term:wi_fi_router"]);
    assert_eq!(title_views(&later, "wi-fi router").positions, 3);
    let graph = quoted_clause(&later, "wi-fi router");
    assert_eq!(graph.arcs[0].end, 3);
    assert_eq!(names(&graph.arcs[0].alternatives), ["term:wi_fi_router"]);
}

#[test]
fn public_token_lists_are_taken_as_given() {
    for tokens in [
        vec!["A", "b"],
        vec!["café", "shoe"],
        vec!["a-b"],
        vec!["a b"],
        vec![""],
    ] {
        let vocab = RefVocab::default().phrase_tokens(&tokens, "invalid", PhraseMode::Collapse);
        let text = "A b café shoe a-b";
        assert!(!query(&vocab, text).contains(&"invalid".into()));
        let views = title_views(&vocab, text);
        assert!(!names(&views.canonical_features).contains(&"invalid"));
        assert!(!names(&views.positive_features).contains(&"invalid"));
        assert!(!views
            .positive_arcs
            .iter()
            .any(|item| item.feature.as_str() == "invalid"));
        assert!(quoted_clause(&vocab, text)
            .arcs
            .iter()
            .all(|edge| !names(&edge.alternatives).contains(&"invalid")));
    }
    let vocab = RefVocab::default().fold_punct(' ').phrase_tokens(
        &["a", "b"],
        "pair",
        PhraseMode::Collapse,
    );
    assert_eq!(query(&vocab, "a-b"), ["pair"]);
    assert_eq!(
        title_views(&vocab, "a-b").canonical_arcs,
        [arc(Feature::raw("pair"), 0, 2)]
    );
    assert_eq!(quoted_clause(&vocab, "a-b").arcs[0].end, 2);
}

#[test]
fn phrase_convenience_preserves_tokens_in_every_mode() {
    for mode in [
        PhraseMode::Collapse,
        PhraseMode::Additive,
        PhraseMode::Alias,
    ] {
        let direct = RefVocab::default().phrase_tokens(&["North,", "STAR"], "invalid", mode);
        let convenience = RefVocab::default().phrase("North, STAR", "invalid", mode);
        assert_eq!(
            query(&convenience, "north star"),
            ["term:north", "term:star"]
        );
        assert_eq!(
            title_views(&convenience, "north star"),
            title_views(&direct, "north star")
        );
        let folded = RefVocab::default()
            .phrase("wi-fi router", "unchanged", mode)
            .fold_punct('-');
        assert_eq!(query(&folded, "wi-fi router"), ["term:router", "term:wifi"]);
    }
}

#[test]
fn kept_non_ascii_uppercase_can_occur_in_a_public_token_list() {
    let mut vocab = RefVocab::default().phrase_tokens(&["Ā", "x"], "pair", PhraseMode::Collapse);
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
fn alias_forms_promote_lists_in_either_declaration_order_and_keep_the_first_lists_feature() {
    for mode in [
        PhraseMode::Collapse,
        PhraseMode::Additive,
        PhraseMode::Alias,
    ] {
        for alias_first in [false, true] {
            let mut vocab = RefVocab::default();
            if alias_first {
                vocab = vocab.alias_form("North, STAR");
            }
            vocab = vocab
                .phrase_tokens(&["north", "star"], "brand:first", mode)
                .phrase_tokens(&["north", "star"], "brand:discarded", PhraseMode::Collapse);
            if !alias_first {
                vocab = vocab.alias_form("North, STAR");
            }
            assert_eq!(query(&vocab, "north star"), ["brand:first"]);
            assert_eq!(
                names(&title_views(&vocab, "north star").canonical_features),
                ["brand:first", "term:north", "term:star"]
            );
            let graph = quoted_clause(&vocab, "north star");
            assert_eq!(graph.arcs.len(), 1);
            assert_eq!(names(&graph.arcs[0].alternatives), ["brand:first"]);
            assert!(names(&title_views(&vocab, "star north").positive_features)
                .contains(&"brand:first"));
            assert!(vocab.has_alias());
        }
    }
}

#[test]
fn standalone_alias_names_are_derived_and_duplicate_forms_are_ignored() {
    let vocab = RefVocab::default()
        .alias_form("NÉW, York")
        .alias_form("new york");
    assert_eq!(vocab.phrases_in_force().len(), 1);
    assert_eq!(
        query_features(&vocab, "new york"),
        [Feature::term("new_york")]
    );
    assert_eq!(
        names(&title_views(&vocab, "new york").canonical_features),
        ["term:new", "term:new_york", "term:york"]
    );
    assert_eq!(
        names(&quoted_clause(&vocab, "new york").arcs[0].alternatives),
        ["term:new_york"]
    );
    assert!(names(&title_views(&vocab, "york new").positive_features).contains(&"term:new_york"));
    let equivalent = vocab.equivalence(&["new york", "ny"]);
    assert!(resolve_equivalences(&equivalent).contains_key(&Feature::term("new_york")));
}
