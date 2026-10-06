//! A title that carries every word of a multi-word alias form carries the form's entity in
//! its positive view (ADR-205), and only there.

use super::{NormScratch, Normalizer, NormalizerBuilder};
use crate::dict::{Dict, FeatureId, FeatureKind};

fn build(configure: impl FnOnce(&mut NormalizerBuilder)) -> Normalizer {
    let mut builder = NormalizerBuilder::new();
    configure(&mut builder);
    builder.build().expect("normalizer")
}

/// `(canonical view, positive view)` of a title.
fn views(norm: &Normalizer, title: &str) -> (Vec<FeatureId>, Vec<FeatureId>) {
    let dict = Dict::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(
        title,
        &dict,
        &mut String::new(),
        &mut NormScratch::new(),
        &mut neg,
        &mut pos,
    );
    (neg, pos)
}

fn id(name: &str) -> FeatureId {
    Dict::new().get_or_synthetic(name)
}

fn positive_has(norm: &Normalizer, title: &str, feature: &str) -> bool {
    views(norm, title).1.contains(&id(feature))
}

fn canonical_has(norm: &Normalizer, title: &str, feature: &str) -> bool {
    views(norm, title).0.contains(&id(feature))
}

#[test]
fn the_words_apart_or_reordered_carry_the_form() {
    let norm = build(|b| b.add_alias_form("new york"));
    for title in [
        "new york inventory",
        "new seasonal york inventory",
        "york inventory new",
        "york, new",
    ] {
        assert!(positive_has(&norm, title, "term:new_york"), "{title:?}");
    }
    // One word is not the form.
    for title in ["new inventory", "york peppermint", "newyork", "inventory"] {
        assert!(!positive_has(&norm, title, "term:new_york"), "{title:?}");
    }
}

#[test]
fn the_canonical_view_keeps_the_adjacent_reading() {
    // Negation reads this view: a title with the words apart is not the form written out.
    let norm = build(|b| b.add_alias_form("new york"));
    assert!(canonical_has(&norm, "new york inventory", "term:new_york"));
    assert!(!canonical_has(
        &norm,
        "new seasonal york inventory",
        "term:new_york"
    ));
    assert!(!canonical_has(&norm, "york new", "term:new_york"));
    // And the positive view is still a superset of it.
    for title in ["new york inventory", "new seasonal york", "york new", "ny"] {
        let (neg, pos) = views(&norm, title);
        assert!(neg.iter().all(|feature| pos.contains(feature)), "{title:?}");
    }
}

#[test]
fn a_word_counts_under_what_it_compiles_to_on_its_own() {
    // `refurb` is a synonym of `refurbished`: a title that says either carries the word.
    let norm = build(|b| {
        b.add_synonym("refurb", "term:refurbished", FeatureKind::Generic);
        b.add_alias_form("refurb unit");
    });
    assert!(positive_has(
        &norm,
        "refurbished heavy unit",
        "term:refurb_unit"
    ));
    assert!(positive_has(&norm, "unit refurb", "term:refurb_unit"));
    assert!(!positive_has(
        &norm,
        "refurbished heavy",
        "term:refurb_unit"
    ));
}

#[test]
fn a_number_counts_however_its_context_types_it() {
    // Alone `1995` is a year; after a marker or a declared context word it is a plain
    // number. The title carries the word either way.
    let norm = build(|b| {
        b.set_number_context_words(&["lot"]);
        b.add_alias_form("1995 unit");
    });
    for title in ["1995 unit", "unit 1995", "unit #1995", "unit lot 1995"] {
        assert!(positive_has(&norm, title, "term:1995_unit"), "{title:?}");
    }
    assert!(!positive_has(&norm, "unit 1996", "term:1995_unit"));
}

#[test]
fn every_form_is_judged_on_its_own_words() {
    // Nested forms, and two forms that name one entity.
    let nested = build(|b| {
        b.add_alias_form("new york");
        b.add_alias_form("new york city");
    });
    assert!(positive_has(&nested, "city of york, new", "term:new_york"));
    assert!(positive_has(
        &nested,
        "city of york, new",
        "term:new_york_city"
    ));
    assert!(positive_has(&nested, "york new", "term:new_york"));
    assert!(!positive_has(&nested, "york new", "term:new_york_city"));

    let shared = build(|b| {
        b.add_phrase(&["new", "york"], "entity:place", FeatureKind::Entity);
        b.add_phrase(&["big", "apple"], "entity:place", FeatureKind::Entity);
        b.add_alias_form("new york");
        b.add_alias_form("big apple");
    });
    assert!(positive_has(&shared, "apple pie, big", "entity:place"));
    assert!(positive_has(&shared, "york new", "entity:place"));
    // A word of each form is no form at all.
    assert!(!positive_has(&shared, "new apple", "entity:place"));
}

#[test]
fn a_word_another_phrase_consumed_still_counts() {
    // The canonical view reads `york city` as one entity and drops its tokens. The positive
    // view keeps every token, so `york` is there for the form `new york`.
    let norm = build(|b| {
        b.add_phrase(&["york", "city"], "entity:york_city", FeatureKind::Entity);
        b.add_alias_form("new york");
    });
    assert!(canonical_has(&norm, "york city new", "entity:york_city"));
    assert!(!canonical_has(&norm, "york city new", "term:york"));
    assert!(positive_has(&norm, "york city new", "term:new_york"));
}

#[test]
fn a_word_restored_by_an_overlapping_phrase_still_counts() {
    // `new york` is a declared phrase whose entity is the word `ny`. With `new york city` an
    // alias form too, the longer form wins the parse and `new york` is found only by the
    // overlapping scan. Its entity is in the positive view all the same, so `ny` is carried.
    let before = build(|b| {
        b.add_phrase(&["new", "york"], "term:ny", FeatureKind::Generic);
        b.add_alias_form("ny catalog");
    });
    let after = build(|b| {
        b.add_phrase(&["new", "york"], "term:ny", FeatureKind::Generic);
        b.add_alias_form("ny catalog");
        b.add_alias_form("new york city");
    });
    for norm in [&before, &after] {
        assert!(positive_has(
            norm,
            "new york city catalog",
            "term:ny_catalog"
        ));
        assert!(positive_has(norm, "catalog of new york", "term:ny_catalog"));
    }
}

#[test]
fn a_word_is_not_carried_through_an_unrelated_phrase() {
    // `#` is kept as a token here and `# refurb` is a phrase of its own. Nothing about that
    // phrase makes a title that says `marked` carry the word `refurb`.
    let norm = build(|b| {
        b.set_punct_class('#', super::PunctClass::Keep);
        b.add_phrase(&["#", "refurb"], "term:marked", FeatureKind::Generic);
        b.add_alias_form("refurb unit");
    });
    assert!(positive_has(&norm, "unit refurb", "term:refurb_unit"));
    assert!(!positive_has(&norm, "marked unit", "term:refurb_unit"));
}

#[test]
fn forms_that_share_a_word_are_keyed_on_the_other_one() {
    // Thousands of forms `wireless <model>`: a title that says `wireless` must not have to
    // look at any of them, and one that names a model looks at that model's form.
    let norm = build(|b| {
        for model in 0..5_000 {
            b.add_alias_form(&format!("wireless zzmodel{model}"));
        }
    });
    let words = norm.alias_words.as_ref().expect("alias words");
    assert_eq!(words.forms_keyed_on("term:wireless"), 0);
    assert_eq!(words.forms_keyed_on("term:zzmodel7"), 1);
    assert!(positive_has(
        &norm,
        "zzmodel7 optical wireless",
        "term:wireless_zzmodel7"
    ));
    assert!(!positive_has(
        &norm,
        "zzmodel7 optical wireless",
        "term:wireless_zzmodel8"
    ));
    assert!(!positive_has(&norm, "wireless", "term:wireless_zzmodel7"));

    // A key word that titles carry under two names is keyed under both.
    let norm = build(|b| {
        b.add_synonym("refurb", "term:refurbished", FeatureKind::Generic);
        for other in ["refurb", "big", "small", "old"] {
            b.add_alias_form(&format!("{other} unit"));
        }
    });
    let words = norm.alias_words.as_ref().expect("alias words");
    assert_eq!(words.forms_keyed_on("term:unit"), 0);
    assert_eq!(words.forms_keyed_on("term:refurb"), 1);
    assert_eq!(words.forms_keyed_on("term:refurbished"), 1);
    assert!(positive_has(&norm, "unit, refurbished", "term:refurb_unit"));
}

#[test]
fn a_form_the_title_carries_can_be_a_word_of_another_form() {
    // `new york` is a phrase for the word `ny`, and an alias form. A title with its words
    // apart carries `ny`, and with `catalog` it therefore carries the form `ny catalog`.
    let norm = build(|b| {
        b.add_phrase(&["new", "york"], "term:ny", FeatureKind::Generic);
        b.add_alias_form("new york");
        b.add_alias_form("ny catalog");
        b.add_alias_form("ny_catalog sale");
    });
    assert!(positive_has(&norm, "york new catalog", "term:ny"));
    assert!(positive_has(&norm, "york new catalog", "term:ny_catalog"));
    assert!(!positive_has(&norm, "york catalog", "term:ny_catalog"));
    // And so on down the chain, in whatever order the forms are examined.
    assert!(positive_has(
        &norm,
        "sale catalog york new",
        "term:ny_catalog_sale"
    ));
    assert!(!canonical_has(&norm, "york new catalog", "term:ny_catalog"));
}

#[test]
fn a_title_that_repeats_a_word_carries_it_once() {
    // The names of a title are reduced to the distinct ones before any form is examined,
    // so a word repeated thousands of times costs one look, not one per occurrence.
    let words = super::core::AliasWords::new(vec![(
        "term:wireless_mouse".to_string(),
        vec![
            vec!["term:wireless".to_string()],
            vec!["term:mouse".to_string()],
        ],
    )])
    .expect("alias words");
    let wireless = super::core::name_hash("term:wireless");
    let mouse = super::core::name_hash("term:mouse");
    let dict = Dict::new();

    let mut carried = vec![wireless; 50_000];
    let mut out = Vec::new();
    words.complete_into(&mut carried, &dict, &mut out);
    assert_eq!(carried, vec![wireless]);
    assert!(out.is_empty());

    let mut carried = vec![mouse, wireless, mouse, wireless];
    words.complete_into(&mut carried, &dict, &mut out);
    assert_eq!(out, vec![id("term:wireless_mouse")]);
    assert_eq!(carried.len(), 3, "the two words and the entity");
}

#[test]
fn a_repeated_word_is_one_word() {
    let norm = build(|b| b.add_alias_form("tick tick boom"));
    assert!(positive_has(&norm, "boom tick", "term:tick_tick_boom"));
    assert!(!positive_has(&norm, "tick tick", "term:tick_tick_boom"));
}

#[test]
fn without_a_multiword_alias_nothing_changes() {
    let norm = build(|b| {
        b.add_phrase(&["north", "star"], "entity:north_star", FeatureKind::Entity);
        b.add_synonym("refurb", "term:refurbished", FeatureKind::Generic);
    });
    for title in ["star of the north", "north star refurb", "refurbished star"] {
        let (neg, pos) = views(&norm, title);
        assert_eq!(neg, pos, "{title:?}");
    }
    assert!(!positive_has(
        &norm,
        "star of the north",
        "entity:north_star"
    ));
}
