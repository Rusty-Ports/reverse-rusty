use super::*;

#[test]
fn feature_superset_requires_an_alias_but_graph_superset_does_not() {
    let vocab = RefVocab::default()
        .phrase("1999 pkg", "entity:pair", PhraseMode::Collapse)
        .synonym("pkg", "term:package");
    let plain = title_views(&vocab, "1999 pkg");
    assert_eq!(names(&plain.canonical_features), ["entity:pair"]);
    assert_eq!(plain.positive_features, plain.canonical_features);
    for (feature, start) in [
        ("year:1999", 0),
        ("term:1999", 0),
        ("term:package", 1),
        ("term:pkg", 1),
    ] {
        assert!(has(&plain.positive_arcs, feature, start, start + 1));
    }
    let aliases = vocab.phrase("unseen alias", "entity:unseen", PhraseMode::Alias);
    let views = title_views(&aliases, "1999 pkg");
    assert_eq!(views.canonical_features, plain.canonical_features);
    assert_eq!(
        names(&views.positive_features),
        [
            "entity:pair",
            "term:1999",
            "term:package",
            "term:pkg",
            "year:1999"
        ]
    );
    assert_eq!(views.positive_arcs, plain.positive_arcs);
}

#[test]
fn every_overlapping_phrase_mode_contributes_to_positive_views() {
    let vocab = RefVocab::default()
        .phrase("a b c", "long", PhraseMode::Collapse)
        .phrase("b c", "alias", PhraseMode::Alias)
        .phrase("b c d", "additive", PhraseMode::Additive)
        .phrase("a b", "short", PhraseMode::Collapse);
    let views = title_views(&vocab, "a b c d");
    assert_eq!(names(&views.canonical_features), ["long", "term:d"]);
    assert_eq!(
        names(&views.positive_features),
        ["additive", "alias", "long", "short", "term:a", "term:b", "term:c", "term:d"]
    );
    for (name, start, end) in [
        ("long", 0, 3),
        ("alias", 1, 3),
        ("additive", 1, 4),
        ("short", 0, 2),
    ] {
        assert!(has(&views.positive_arcs, name, start, end));
    }
}

#[test]
fn alias_pieces_can_be_single_token_analysis_or_shorter_phrases() {
    let vocab = RefVocab::default()
        .phrase("north star mouse", "entity:kit", PhraseMode::Alias)
        .phrase("north star", "brand:ns", PhraseMode::Collapse)
        .synonym("ns", "brand:ns")
        .equivalence(&["ns", "polaris"]);
    for text in ["ns mouse", "mouse polaris", "star mouse north"] {
        assert!(
            names(&title_views(&vocab, text).positive_features).contains(&"entity:kit"),
            "{text}"
        );
    }
    assert!(!names(&title_views(&vocab, "north mouse").positive_features).contains(&"entity:kit"));
    let single = RefVocab::default()
        .phrase("1999 pkg", "entity:kit", PhraseMode::Alias)
        .synonym("pkg", "term:package")
        .synonym("ninety", "year:1999");
    assert!(
        names(&title_views(&single, "package ninety").positive_features).contains(&"entity:kit")
    );
    let single = RefVocab::default()
        .phrase("ny mouse", "entity:kit", PhraseMode::Alias)
        .phrase("ny", "brand:ns", PhraseMode::Collapse)
        .synonym("ns", "brand:ns");
    assert!(names(&title_views(&single, "ns mouse").positive_features).contains(&"entity:kit"));
}

#[test]
fn alias_closure_reaches_a_fixed_point_without_inventing_position_arcs() {
    let vocab = RefVocab::default()
        .phrase("b tag", "entity:c", PhraseMode::Alias)
        .phrase("a box", "entity:b", PhraseMode::Alias)
        .phrase("red shoe", "entity:a", PhraseMode::Alias)
        .synonym("a", "entity:a")
        .synonym("b", "entity:b");
    let views = title_views(&vocab, "shoe red box tag");
    for feature in ["entity:a", "entity:b", "entity:c"] {
        assert!(names(&views.positive_features).contains(&feature));
        assert!(!names(&views.canonical_features).contains(&feature));
        assert!(!views
            .positive_arcs
            .iter()
            .any(|item| item.feature.as_str() == feature));
    }
    assert!(!matches(&vocab, "\"red shoe\"", "shoe red box tag"));
    assert!(!names(&title_views(&vocab, "box tag").positive_features).contains(&"entity:c"));
}

#[test]
fn partitions_need_every_piece_and_can_reuse_a_carried_feature() {
    let vocab = RefVocab::default()
        .phrase("red red", "twice", PhraseMode::Alias)
        .phrase("red blue", "collapse", PhraseMode::Collapse);
    assert!(names(&title_views(&vocab, "red").positive_features).contains(&"twice"));
    assert!(!names(&title_views(&vocab, "blue red").positive_features).contains(&"collapse"));
    assert!(!matches(&vocab, "\"red red\"", "red"));
}

#[test]
fn alias_partitions_consider_alternative_cuts_and_all_piece_modes() {
    for mode in [
        PhraseMode::Collapse,
        PhraseMode::Additive,
        PhraseMode::Alias,
    ] {
        let vocab = RefVocab::default()
            .phrase("a b c d", "whole", PhraseMode::Alias)
            .phrase("a b c", "long", mode)
            .phrase("a b", "left", mode)
            .phrase("c d", "right", mode)
            .synonym("x", "left")
            .synonym("y", "right")
            .synonym("z", "long");
        // The available longest piece leaves an unavailable d; the two shorter pieces work.
        assert!(names(&title_views(&vocab, "z y x").positive_features).contains(&"whole"));
        assert!(!names(&title_views(&vocab, "z x").positive_features).contains(&"whole"));
    }
}

#[test]
fn single_token_aliases_activate_the_superset() {
    let vocab = RefVocab::default().phrase("a", "first", PhraseMode::Alias);
    assert_eq!(
        names(&title_views(&vocab, "1999").positive_features),
        ["term:1999", "year:1999"]
    );
    let views = title_views(&vocab, "a");
    assert_eq!(names(&views.canonical_features), ["first", "term:a"]);
    assert_eq!(views.positive_features, views.canonical_features);
}

#[test]
fn duplicate_phrases_are_ignored_in_every_view_and_during_piece_inference() {
    for mode in [
        PhraseMode::Collapse,
        PhraseMode::Additive,
        PhraseMode::Alias,
    ] {
        let vocab = RefVocab::default()
            .phrase("north star", "first", mode)
            .phrase("NÓRTH, STAR", "discarded", PhraseMode::Alias)
            .phrase("unseen alias", "active", PhraseMode::Alias);
        assert!(!query(&vocab, "north star").contains(&"discarded".into()));
        assert!(quoted_clause(&vocab, "north star")
            .arcs
            .iter()
            .all(|edge| !names(&edge.alternatives).contains(&"discarded")));
        for text in ["north star", "star north"] {
            let views = title_views(&vocab, text);
            assert!(!names(&views.canonical_features).contains(&"discarded"));
            assert!(!names(&views.positive_features).contains(&"discarded"));
            assert!(!views
                .canonical_arcs
                .iter()
                .chain(&views.positive_arcs)
                .any(|item| item.feature.as_str() == "discarded"));
        }
        assert_eq!(
            names(&title_views(&vocab, "star north").positive_features).contains(&"first"),
            mode == PhraseMode::Alias
        );
    }
}

#[test]
fn ignored_aliases_do_not_activate_the_superset_and_current_punctuation_can_restore_them() {
    let vocab = RefVocab::default()
        .phrase("a-b", "first", PhraseMode::Collapse)
        .phrase("a b", "second", PhraseMode::Alias)
        .phrase("!!!", "empty", PhraseMode::Alias);
    assert!(!vocab.has_alias());
    assert_eq!(
        names(&title_views(&vocab, "1999").positive_features),
        ["year:1999"]
    );
    let changed = vocab.fold_punct('-');
    assert!(changed.has_alias());
    assert_eq!(
        names(&title_views(&changed, "1999").positive_features),
        ["term:1999", "year:1999"]
    );
    assert_eq!(query(&changed, "a-b"), ["first"]);
    assert_eq!(query(&changed, "a b"), ["second"]);
    let mut empty = RefVocab::default().phrase("@", "was_empty", PhraseMode::Alias);
    empty.punct.set('@', PunctClass::Keep);
    assert_eq!(query(&empty, "@"), ["was_empty"]);
    assert!(empty.has_alias());
}
