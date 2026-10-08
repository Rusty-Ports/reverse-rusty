use super::*;

#[test]
fn quoted_queries_and_title_graphs_clean_text_exactly_once() {
    let vocab = RefVocab::default().fold_punct(' ');
    let graph = quoted_clause(&vocab, "a-b");
    assert_eq!(graph.positions, 2);
    assert_eq!(graph.arcs.len(), 2);
    let views = title_views(&vocab, "a-b");
    assert_eq!(views.positions, 2);
    for arcs in [&views.canonical_arcs, &views.positive_arcs] {
        assert!(has(arcs, "term:a", 0, 1));
        assert!(has(arcs, "term:b", 1, 2));
        assert!(phrase_graph_matches(&graph, views.positions, arcs));
    }
    assert!(matches(&vocab, "\"a-b\"", "a-b"));
    assert!(!matches(&vocab, "\"a-b\"", "ab"));
    let alias = vocab.alias_form("a-b");
    let graph = quoted_clause(&alias, "a-b");
    assert_eq!(graph.positions, 2);
    assert_eq!(
        graph.arcs,
        [RefPhraseArc {
            start: 0,
            end: 2,
            alternatives: vec![Feature::raw("term:a_b")]
        }]
    );
    let views = title_views(&alias, "a-b");
    assert!(has(&views.canonical_arcs, "term:a_b", 0, 2));
    assert!(has(&views.canonical_arcs, "term:a", 0, 1));
    assert!(has(&views.canonical_arcs, "term:b", 1, 2));
}

#[test]
fn graph_fallback_fills_uncovered_positions_including_markers_only() {
    let vocab = RefVocab::default();
    let views = title_views(&vocab, "a # / b");
    assert_eq!(views.positions, 4);
    assert_eq!(names(&views.canonical_features), ["term:a", "term:b"]);
    assert!(has(&views.canonical_arcs, "term:#", 1, 2));
    assert!(has(&views.canonical_arcs, "term:/", 2, 3));
    assert!(query_features(&vocab, "# /").is_empty());
    assert_eq!(quoted_clause(&vocab, "# /").arcs.len(), 2);
    assert!(!matches(&vocab, "\"a b\"", "a # b"));
    let collapsed = vocab.phrase("a # b", "entity:ab", PhraseMode::Collapse);
    let views = title_views(&collapsed, "a # b");
    assert_eq!(views.canonical_arcs, [arc(Feature::raw("entity:ab"), 0, 3)]);
    assert!(!has(&views.positive_arcs, "term:#", 1, 2));
    assert_eq!(quoted_clause(&collapsed, "a # b").arcs.len(), 1);
}

#[test]
fn equal_endpoints_form_sorted_unique_alternatives() {
    for (canonical, expected) in [
        ("term:z", vec!["term:a", "term:z"]),
        ("term:a", vec!["term:a"]),
    ] {
        let vocab = RefVocab::default()
            .phrase("x", canonical, PhraseMode::Additive)
            .synonym("x", "term:a");
        let graph = quoted_clause(&vocab, "x");
        assert_eq!(graph.positions, 1);
        assert_eq!(graph.arcs.len(), 1);
        assert_eq!((graph.arcs[0].start, graph.arcs[0].end), (0, 1));
        assert_eq!(names(&graph.arcs[0].alternatives), expected);
    }
}

#[test]
fn quoted_paths_can_change_span_and_choose_an_additive_path() {
    let vocab = RefVocab::default()
        .alias_form("new york")
        .equivalence(&["new york", "ny"]);
    assert!(matches(&vocab, "\"new york\" inventory", "ny inventory"));
    assert!(matches(
        &vocab,
        "\"new york inventory\"",
        "old ny inventory item"
    ));
    assert!(!matches(
        &vocab,
        "\"new york inventory\"",
        "ny old inventory"
    ));
    assert!(!matches(
        &vocab,
        "\"new york\" inventory",
        "new vintage york inventory"
    ));
    assert!(matches(&vocab, "item -\"new york\"", "ny item"));
    assert!(!matches(&vocab, "item -\"new york\"", "new york item"));
    let additive = RefVocab::default()
        .phrase("red shoe", "entity:boot", PhraseMode::Additive)
        .synonym("boot", "entity:boot");
    assert!(matches(&additive, "\"red shoe\"", "boot"));
    assert!(matches(&additive, "\"red shoe\"", "red shoe"));
    assert!(!matches(&additive, "\"red shoe\"", "red leather shoe"));
    assert!(!matches(&additive, "\"red shoe\"", "red"));
}

#[test]
fn required_graphs_are_positive_and_forbidden_graphs_canonical() {
    let vocab = RefVocab::default().phrase("red shoe", "entity:boot", PhraseMode::Collapse);
    assert!(matches(&vocab, "\"red\"", "red shoe"));
    assert!(matches(&vocab, "item -\"red\"", "red shoe item"));
    assert!(!matches(&vocab, "item -\"red\"", "red item"));
    assert!(!matches(&vocab, "red", "red shoe"));
    assert!(!phrase_graph_matches(&RefPhraseGraph::default(), 1, &[]));
    assert!(!phrase_graph_matches(&quoted_clause(&vocab, "red"), 0, &[]));
}

#[test]
fn equivalence_forms_are_distinct_single_features_and_groups_merge() {
    let vocab = RefVocab::default()
        .equivalence(&["a", "b", "two terms", "!!!"])
        .equivalence(&["c", "d"])
        .equivalence(&["b", "c"])
        .equivalence(&["e e", "a"])
        .equivalence(&["alone", "alone", "!!!"]);
    let classes = resolve_equivalences(&vocab);
    for token in ["a", "b", "c", "d", "e"] {
        assert_eq!(
            names(&classes[&Feature::term(token)]),
            ["term:a", "term:b", "term:c", "term:d", "term:e"]
        );
    }
    assert!(!classes.contains_key(&Feature::term("alone")));
    assert!(!classes.contains_key(&Feature::term("two")));
    assert!(matches(&vocab, "\"a\"", "d"));
    assert!(matches(&vocab, "item -\"a\"", "d item"));
}
