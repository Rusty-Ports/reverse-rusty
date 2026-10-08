//! Golden normalization cases — exact feature-*name* sets, authored by hand from
//! the spec (docs/design/normalization.md §2–§4, docs/reference/dsl.md), NOT
//! captured from `emit`. They exist because the differential oracle
//! (tests/oracle/) runs THIS normalizer on both its engine and its brute-force
//! ground truth, and only ever under the EMPTY `default_vocab` — so a
//! normalization-model bug is invisible there, and the entire vocab-driven path
//! (phrases/synonyms) is never exercised at all. These pins close that
//! gap with expectations a code bug cannot infect. See docs/DECISIONS.md ADR-050.
use super::*;
use crate::dict::Dict;

/// Sorted feature *names* for `text`. Uses the mutating compile path on purpose:
/// it interns every emitted feature, so `Dict::name` round-trips to a real name
/// (the read-only path would hash misses to a `"<oov>"` synthetic ID).
fn names(norm: &Normalizer, text: &str) -> Vec<String> {
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ids = norm.compile_features(text, &mut dict, &mut lc);
    let mut out: Vec<String> = ids.iter().map(|&id| dict.name(id).to_string()).collect();
    out.sort();
    out
}

fn s(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

/// A domain-neutral sample vocabulary, built explicitly so the expected canonical
/// names are themselves part of the contract.
fn sample_vocab() -> Normalizer {
    NormalizerBuilder::new()
        .phrase(&["acme", "labs"], "brand:acme_labs", FeatureKind::Brand)
        .phrase(
            &["wireless", "mouse"],
            "entity:wireless_mouse",
            FeatureKind::Entity,
        )
        .synonym("acme", "brand:acme_labs", FeatureKind::Brand)
        .synonym("refurb", "category:refurbished", FeatureKind::Category)
        .build()
        .expect("sample vocab automaton")
}

// ---- vocab-independent pipeline (the empty default_vocab still does this) ----

#[test]
fn diacritics_fold_to_ascii() {
    let n = Normalizer::default_vocab().unwrap();
    // normalization.md §2: Café->cafe, Český->cesky, Jalapeño->jalapeno (ñ no longer splits).
    assert_eq!(names(&n, "café"), s(&["term:cafe"]));
    assert_eq!(names(&n, "Český"), s(&["term:cesky"]));
    assert_eq!(
        names(&n, "Ronald Jalapeño"),
        s(&["term:jalapeno", "term:ronald"])
    );
}

#[test]
fn number_disambiguation_matrix() {
    let n = Normalizer::default_vocab().unwrap();
    // Structural markers keep identifier numbers generic.
    assert_eq!(names(&n, "#2 widget"), s(&["term:2", "term:widget"]));
    assert_eq!(names(&n, "/5"), s(&["term:5"])); // serial
    assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"])); // serial halves
    assert_eq!(names(&n, "1994"), s(&["year:1994"])); // year
    assert_eq!(names(&n, "count 1"), s(&["term:1", "term:count"]));
}

#[test]
fn generic_fallback_term() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "unknownword"), s(&["term:unknownword"]));
}

// ---- number-context words (ADR-069) ----

#[test]
fn number_context_is_empty_by_default() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "model 1995"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&n, "1995 model"), s(&["term:model", "year:1995"]));
}

#[test]
fn number_context_empty_list_is_position_insensitive() {
    // An EMPTY list keeps the default position-insensitive behavior.
    let p = NormalizerBuilder::new()
        .number_context_words(&[])
        .build()
        .unwrap();
    assert_eq!(names(&p, "model 1995"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&p, "1995 model"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&p, "model 7"), s(&["term:7", "term:model"]));
    // Marker-driven typing (`#`/`/`) is punctuation-table territory (ADR-058), not this knob.
    assert_eq!(names(&p, "#1995"), s(&["term:1995"]));
}

#[test]
fn number_context_is_caller_supplied() {
    let q = NormalizerBuilder::new()
        .number_context_words(&["model"])
        .build()
        .unwrap();
    assert_eq!(names(&q, "model 1995"), s(&["term:1995", "term:model"]));
    assert_eq!(names(&q, "series 1995"), s(&["term:series", "year:1995"]));
}

// ---- vocab-driven pipeline (spec vocab) — never reached by the oracle ----

#[test]
fn multiword_phrases_collapse_to_one_feature() {
    let n = sample_vocab();
    // normalization.md §1/§2: a multiword entity is ONE feature, not its tokens.
    assert_eq!(names(&n, "wireless mouse"), s(&["entity:wireless_mouse"]));
    assert_eq!(names(&n, "acme labs"), s(&["brand:acme_labs"]));
}

#[test]
fn separators_between_a_phrases_words_do_not_hide_it() {
    // ADR-218: a phrase is its words as consecutive tokens. Cleaning merges separators, so two
    // spaces, a comma and a space, or a hyphen between spaces are one token boundary, and the
    // phrase is found as it is across a single space. (Cleaning used to write a space for each
    // separator, and the phrase was looked for with exactly one: `wireless, mouse` gave the
    // two words and not the entity, on a title and in a query alike.)
    let n = sample_vocab();
    for text in [
        "wireless mouse",
        "wireless  mouse",
        "wireless, mouse",
        "wireless - mouse",
        "  wireless\t \tmouse  ",
        "Wireless-Mouse!",
    ] {
        assert_eq!(names(&n, text), s(&["entity:wireless_mouse"]), "{text:?}");
    }
    // A kept character is part of a token, and a token between the words is not a separator.
    assert_eq!(
        names(&n, "wireless . mouse"),
        s(&["term:.", "term:mouse", "term:wireless"])
    );
    assert_eq!(
        names(&n, "wireless optical mouse"),
        s(&["term:mouse", "term:optical", "term:wireless"])
    );
}

#[test]
fn cleaned_text_never_has_two_separators_in_a_row() {
    let punct = PunctTable::default();
    let mut lc = String::new();
    for (text, cleaned) in [
        ("a  b", "a b"),
        ("  a, b -- c  ", "a b c "),
        ("a #2 /3", "a # 2 / 3"),
        ("a#/b", "a # / b"),
        ("...", "..."),
        (", ,", ""),
    ] {
        super::core::clean_with(&punct, text, &mut lc);
        assert_eq!(lc, cleaned, "{text:?}");
        assert!(
            !lc.contains("  ") && !lc.starts_with(' '),
            "{text:?} -> {lc:?}"
        );
    }
}

#[test]
fn a_space_is_a_separator_whatever_class_a_vocabulary_gives_it() {
    // A vocabulary may give any character a class, the space included. Kept or marked, a
    // space still ends a token and is still not written twice, so no configuration brings
    // back runs of separators. (With the space kept, `north  star` cleaned to two spaces
    // again, and a quoted query for a synonym of the phrase stopped matching that title.)
    for class in [PunctClass::Keep, PunctClass::Marker, PunctClass::Split] {
        let mut punct = PunctTable::default();
        punct.set(' ', class);
        let mut lc = String::new();
        for (text, cleaned) in [("north  star", "north star"), (" a   b ", "a b ")] {
            super::core::clean_with(&punct, text, &mut lc);
            assert_eq!(lc, cleaned, "{class:?}: {text:?}");
        }

        let mut b = NormalizerBuilder::new();
        b.set_punct_class(' ', class);
        b.add_phrase(&["north", "star"], "brand:north_star", FeatureKind::Brand);
        b.add_synonym("ns", "brand:north_star", FeatureKind::Brand);
        let n = b.build().expect("normalizer");
        assert_eq!(
            names(&n, "north  star"),
            s(&["brand:north_star"]),
            "{class:?}"
        );
        assert_eq!(names(&n, "ns"), s(&["brand:north_star"]), "{class:?}");
    }
}

#[test]
fn a_run_inside_a_phrase_is_the_phrase_on_both_sides_and_in_both_views() {
    // The query side reduced runs only while a multi-word alias was active (ADR-061), and the
    // title's canonical view never did. Now neither needs to: there are no runs.
    let mut b = NormalizerBuilder::new();
    b.add_alias_form("new york");
    let n = b.build().expect("alias normalizer");

    assert_eq!(
        names(&n, "new  york catalog"),
        s(&["term:catalog", "term:new_york"]),
        "query side"
    );

    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("new york", &mut dict, &mut lc); // intern the entity dense
    let entity = dict.get_or_synthetic("term:new_york");
    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual(
        "new,  york catalog",
        &dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
    );
    assert!(neg.contains(&entity), "the title's canonical view");
    assert!(pos.contains(&entity), "the title's positive view");
}

#[test]
fn a_pattern_found_inside_a_word_hides_no_phrase() {
    // ADR-218: selection is leftmost-longest over the occurrences on token boundaries. The
    // leftmost-longest automaton takes a pattern that starts or ends inside a word, which is
    // no occurrence, and with it the valid phrase that pattern overlaps.
    let mut b = NormalizerBuilder::new();
    b.add_phrase(&["north", "star"], "brand:north_star", FeatureKind::Brand);
    b.add_phrase(&["star", "lamp"], "entity:star_lamp", FeatureKind::Entity);
    b.add_phrase(&["new", "york", "city"], "entity:nyc", FeatureKind::Entity);
    b.add_phrase(&["new", "york"], "entity:ny", FeatureKind::Entity);
    let n = b.build().expect("normalizer");
    assert_eq!(
        names(&n, "xnorth star lamp"),
        s(&["entity:star_lamp", "term:xnorth"])
    );
    assert_eq!(
        names(&n, "new york cityscape"),
        s(&["entity:ny", "term:cityscape"])
    );
    // Where the longer or the earlier phrase is an occurrence, it still wins.
    assert_eq!(names(&n, "new york city"), s(&["entity:nyc"]));
    assert_eq!(
        names(&n, "north star lamp"),
        s(&["brand:north_star", "term:lamp"])
    );
}

#[test]
fn boundary_invalid_match_cannot_suppress_a_valid_overlapping_alias() {
    // ADR-061 (codex R12, P1): the shared leftmost-longest automaton commits to a match BEFORE
    // the word-boundary check. With aliases `a b` and `b c`, the text `xa b c` contains `a b`
    // mid-token (inside `xa b`) — the legacy pass selects it, consumes its span (suppressing the
    // genuinely valid `b c`), and then drops it at the boundary post-filter: no phrase at all.
    // On the query side that compiles an alias query to component terms, so equivalence
    // expansion never reaches the group (an FN). With aliases active, selection runs over the
    // boundary-VALID candidates only, so `b c` collapses to its entity.
    let mut b = NormalizerBuilder::new();
    b.add_alias_form("a b");
    b.add_alias_form("b c");
    let n = b.build().expect("alias normalizer");
    assert_eq!(
        names(&n, "xa b c"),
        s(&["term:b_c", "term:xa"]),
        "the valid `b c` must be selected despite the mid-token `a b` candidate"
    );
    // No mid-token candidate: identical to the legacy leftmost-longest selection.
    assert_eq!(names(&n, "a b c"), s(&["term:a_b", "term:c"]));
}

#[test]
fn synonyms_converge_alternate_surface_forms() {
    let n = sample_vocab();
    // A synonym and its declared phrase land on the same feature.
    assert_eq!(names(&n, "acme"), s(&["brand:acme_labs"]));
    assert_eq!(names(&n, "acme labs"), s(&["brand:acme_labs"]));
    assert_eq!(names(&n, "refurb"), s(&["category:refurbished"]));
}

// ---- determinism (the §2 invariant; normalize∘normalize isn't typeable, so we
//      pin the two checkable properties it actually promises) ----

#[test]
fn fold_is_a_normalization_fixpoint() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "café"), names(&n, "cafe"));
    assert_eq!(names(&n, "Český"), names(&n, "cesky"));
}

#[test]
fn compile_does_not_drift_on_repeat() {
    let n = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let first = n.compile_features("wireless mouse model 10", &mut dict, &mut lc);
    let len_after_first = dict.len();
    let second = n.compile_features("wireless mouse model 10", &mut dict, &mut lc);
    assert_eq!(first, second, "same text -> same IDs");
    assert_eq!(
        dict.len(),
        len_after_first,
        "a repeat interns no new feature"
    );
}

// ---- punctuation-equivalence folding (ADR-058) ----

#[test]
fn default_punctuation_splits_apostrophe_and_hyphen() {
    // The historical default: `'` and `-` are word boundaries, so the punctuated
    // forms tokenize apart while the joined form is one token — the false-negative
    // gap (a query `obrien` misses an `O'Brien` title) that folding closes.
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "O'Brien"), s(&["term:brien", "term:o"]));
    assert_eq!(names(&n, "O-Brien"), s(&["term:brien", "term:o"]));
    assert_eq!(names(&n, "OBrien"), s(&["term:obrien"]));
}

#[test]
fn folding_collapses_punctuation_variants_to_one_token() {
    // Declaring apostrophe (ascii + curly U+2019) and mid-word hyphen as Fold makes
    // all four surface forms land on the SAME single token — so a query and a title
    // that differ only in punctuation now share a feature and match.
    let n = NormalizerBuilder::new()
        .punct('\'', PunctClass::Fold)
        .punct('\u{2019}', PunctClass::Fold)
        .punct('-', PunctClass::Fold)
        .build()
        .expect("folding normalizer");
    let expected = s(&["term:obrien"]);
    assert_eq!(names(&n, "O'Brien"), expected, "ascii apostrophe");
    assert_eq!(names(&n, "O\u{2019}Brien"), expected, "curly apostrophe");
    assert_eq!(names(&n, "O-Brien"), expected, "hyphen");
    assert_eq!(names(&n, "OBrien"), expected, "already joined");
}

#[test]
fn builder_batch_and_mut_fold_apis_fold() {
    // Exercise the `&mut` builder + batch helper (not just the fluent `.punct`).
    let mut b = NormalizerBuilder::new();
    b.fold_punctuation_chars(&['\'', '\u{2019}', '-']);
    let n = b.build().unwrap();
    assert_eq!(names(&n, "O-Brien"), s(&["term:obrien"]));
    assert_eq!(names(&n, "O\u{2019}Brien"), s(&["term:obrien"]));
}

#[test]
fn fold_merges_only_within_a_word_not_across_spaces() {
    // A folded character joins only ADJACENT alphanumerics; a hyphen flanked by
    // spaces still leaves two tokens (the surrounding spaces remain boundaries).
    let n = NormalizerBuilder::new()
        .punct('-', PunctClass::Fold)
        .build()
        .unwrap();
    assert_eq!(names(&n, "foo-bar"), s(&["term:foobar"]));
    assert_eq!(names(&n, "foo - bar"), s(&["term:bar", "term:foo"]));
}

#[test]
fn punct_class_keep_default_is_overridable_to_fold() {
    // `.` defaults to Keep; reclassifying it to Fold deletes it.
    let keep = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&keep, "a.b.c"), s(&["term:a.b.c"]));
    let fold = NormalizerBuilder::new()
        .punct('.', PunctClass::Fold)
        .build()
        .unwrap();
    assert_eq!(names(&fold, "a.b.c"), s(&["term:abc"]));
}

#[test]
fn marker_and_keep_defaults_are_unchanged_by_the_table() {
    // Regression guard: the default table reproduces the historical `#`/`/`/`.`
    // behaviors exactly (the same cases as `number_disambiguation_matrix`).
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "#2 widget"), s(&["term:2", "term:widget"]));
    assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"]));
}

// ---- ADR-061: multi-word alias dual title view ----

/// An alias phrase collapses to ONE entity on the query side (so ADR-054 expansion can
/// widen it), but on the title side it is additive AND the overlap superset adds nested
/// alias entities — while the canonical (negative) view stays leftmost-longest. This is the
/// load-bearing normalizer behavior behind Phase 2's two-view matcher.
#[test]
fn alias_phrase_collapses_on_query_overlaps_on_title() {
    let mut b = NormalizerBuilder::new();
    b.add_phrase_alias(&["new", "york"], "term:new_york", FeatureKind::Generic);
    b.add_phrase_alias(
        &["new", "york", "city"],
        "term:new_york_city",
        FeatureKind::Generic,
    );
    let norm = b.build().expect("alias automaton");

    // Intern the entities (mutating compile of each alias form) so ids are dense + stable.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = norm.compile_features("new york", &mut dict, &mut lc);
    let _ = norm.compile_features("new york city", &mut dict, &mut lc);
    let ny = dict.get_or_synthetic("term:new_york");
    let nyc = dict.get_or_synthetic("term:new_york_city");

    // Query side: a multi-word alias form collapses to its single entity feature.
    let q = norm.compile_features_readonly("new york", &dict, &mut lc);
    assert_eq!(q, vec![ny], "query-side alias must collapse to one entity");

    // Title side: dual view of "new york city inventory".
    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(
        "new york city inventory",
        &dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
    );

    // Negative (canonical) view: leftmost-longest reads "new york city", NOT the nested
    // "new york" — so a forbidden clause stays recall-correct.
    assert!(neg.contains(&nyc), "neg has the leftmost-longest entity");
    assert!(
        !neg.contains(&ny),
        "neg must be leftmost-longest: no nested new york"
    );
    // Positive (superset) view: the overlap pass adds the nested "new york".
    assert!(
        pos.contains(&nyc) && pos.contains(&ny),
        "pos is the superset"
    );
    // N(T) ⊆ P(T), and the title side is additive (keeps component tokens, not just entities).
    for f in &neg {
        assert!(pos.contains(f), "N(T) must be a subset of P(T)");
    }
    assert!(neg.len() > 2, "additive title keeps component tokens");
}

/// With no alias phrase registered, `match_features_dual` yields identical views and they
/// equal `match_features` — the default path is byte-identical (the no-overhead guarantee).
#[test]
fn positive_view_is_always_a_superset_of_negative() {
    // P(T) must union the canonical view with every additive/overlapping
    // entity and raw component; it can never replace N(T).
    let mut b = NormalizerBuilder::new();
    b.add_phrase(&["alpha", "beta"], "term:alpha_beta", FeatureKind::Generic);
    b.add_alias_form("new york"); // ⇒ the dual (P(T)/N(T)) path is active
    let n = b.build().expect("normalizer");
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("alpha beta 10", &mut dict, &mut lc);

    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual("alpha beta 10", &dict, &mut lc, &mut sc, &mut neg, &mut pos);
    let ten = dict.get_or_synthetic("term:10");
    assert!(
        neg.contains(&ten),
        "N(T) reads the trailing number as term:10"
    );
    for f in &neg {
        assert!(
            pos.contains(f),
            "P(T) must contain every N(T) feature (superset) — incl. {}",
            dict.name(*f)
        );
    }
}

#[test]
fn dual_view_equals_single_view_without_aliases() {
    let n = sample_vocab();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let title = "1994 acme labs wireless mouse model 10";
    // Seed the dict with a mutating compile so ids are dense.
    let _ = n.compile_features(title, &mut dict, &mut lc);

    let mut sc = super::NormScratch::new();
    let mut single = Vec::new();
    n.match_features(title, &dict, &mut lc, &mut sc, &mut single);
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual(title, &dict, &mut lc, &mut sc, &mut neg, &mut pos);
    assert_eq!(neg, single, "negative view == single view without aliases");
    assert_eq!(pos, single, "positive view == single view without aliases");
}
