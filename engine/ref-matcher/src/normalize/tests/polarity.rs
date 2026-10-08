use super::*;

#[test]
fn equivalences_widen_each_required_clause_kind_and_never_a_forbidden_clause() {
    let vocab = RefVocab::default().equivalence(&["red", "blue"]);
    for source in [
        "red marker",
        "(red shoe,boot) marker",
        "\"red shoe\" marker",
    ] {
        assert!(matches(&vocab, source, "blue shoe marker"), "{source}");
        assert!(!matches(&vocab, source, "green shoe marker"), "{source}");
    }
    for source in [
        "marker -red",
        "marker -(red shoe,boot)",
        "marker -\"red shoe\"",
    ] {
        assert!(matches(&vocab, source, "blue shoe marker"), "{source}");
        assert!(!matches(&vocab, source, "red shoe marker"), "{source}");
    }
    let views = title_views(&vocab, "blue");
    assert_eq!(names(&views.canonical_features), ["term:blue"]);
    assert_eq!(views.positive_features, views.canonical_features);
}

#[test]
fn every_query_piece_and_equivalence_form_uses_query_side_alias_consumption() {
    let vocab = RefVocab::default()
        .phrase_tokens(&["new", "york"], "brand:ny", PhraseMode::Collapse)
        .alias_form("New York")
        .synonym("ny", "brand:ny")
        .equivalence(&["new york", "gotham"]);
    assert_eq!(query(&vocab, "new york"), ["brand:ny"]);
    assert_eq!(
        names(&title_views(&vocab, "new york").canonical_features),
        ["brand:ny", "term:new", "term:york"]
    );
    let classes = resolve_equivalences(&vocab);
    assert_eq!(
        names(&classes[&Feature::raw("brand:ny")]),
        ["brand:ny", "term:gotham"]
    );
    for source in ["new york", "(new york,other)", "\"new york\""] {
        assert!(matches(&vocab, source, "ny"), "{source}");
        assert!(matches(&vocab, source, "gotham"), "{source}");
    }
    for source in [
        "item -new-york",
        "item -(new york,other)",
        "item -\"new york\"",
    ] {
        assert!(!matches(&vocab, source, "item ny"), "{source}");
        assert!(matches(&vocab, source, "item gotham"), "{source}");
    }
}

#[test]
fn alias_widening_of_numbers_is_positive_and_one_way() {
    let default = RefVocab::default();
    let wide = default.clone().alias_form("unseen alias");
    for (vocab, forward) in [(&default, false), (&wide, true)] {
        assert_eq!(matches(vocab, "#1999", "1999"), forward);
        assert_eq!(matches(vocab, "(#1999,other)", "1999"), forward);
        assert!(!matches(vocab, "1999", "#1999"));
        assert!(matches(vocab, "item -#1999", "item 1999"));
        assert!(matches(vocab, "item -1999", "item #1999"));
        assert!(!matches(vocab, "item -#1999", "item #1999"));
    }
}
