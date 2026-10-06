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

/// The positive view of a title under a vocabulary, with the vocabulary's equivalences
/// installed on the dictionary the way an engine installs them.
fn positive_under(vocab: &crate::vocab::Vocab, title: &str, feature: &str) -> bool {
    let norm = vocab.to_normalizer().expect("normalizer");
    let mut dict = Dict::new();
    let equivalences = vocab.resolve_equivalences(&norm, &dict);
    dict.set_equivalences(equivalences);
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(
        title,
        &dict,
        &mut String::new(),
        &mut NormScratch::new(),
        &mut neg,
        &mut pos,
    );
    pos.contains(&dict.get_or_synthetic(feature))
}

fn alias_vocab(solr: &str, declared: &[&[&str]]) -> crate::vocab::Vocab {
    let mut vocab = crate::vocab::Vocab::new();
    for group in declared {
        vocab.add_equivalence(group);
    }
    let norm = vocab.to_normalizer().expect("normalizer");
    vocab
        .import_solr_aliases(solr, &norm, &Dict::new())
        .expect("import");
    vocab
}

#[test]
fn a_piece_counts_under_what_a_query_takes_it_for() {
    // To a query, `pkg` is `pkg` or `package`, and `ny` is `ny` or the form `new york`.
    // A title carries the words of `pkg deal` and `ny catalog` under either name. The
    // equivalences are read from the dictionary, where the compiler reads them.
    let vocab = alias_vocab(
        "ny => new york\nnycat => ny catalog\npkg => package\nbargain => pkg deal",
        // Groups that share a member are one class.
        &[&["package", "parcel"]],
    );
    assert!(positive_under(&vocab, "deal package", "term:pkg_deal"));
    assert!(positive_under(&vocab, "parcel deal", "term:pkg_deal"));
    assert!(!positive_under(&vocab, "package", "term:pkg_deal"));
    // `york new` carries `new york`, which is what a query means by `ny`.
    assert!(positive_under(
        &vocab,
        "york new catalog",
        "term:ny_catalog"
    ));
    assert!(positive_under(
        &vocab,
        "catalog new york",
        "term:ny_catalog"
    ));
    assert!(!positive_under(&vocab, "york catalog", "term:ny_catalog"));

    // The same normalizer over a dictionary without the equivalences: a word is itself.
    let norm = vocab.to_normalizer().expect("normalizer");
    assert!(!positive_has(&norm, "deal package", "term:pkg_deal"));
    assert!(positive_has(&norm, "deal pkg", "term:pkg_deal"));
}

#[test]
fn a_phrase_inside_a_form_counts_as_a_piece() {
    // `new york catalog` was, to a query, the phrase `new york` and the word `catalog`,
    // and the phrase was `new york` or `big apple`. A title carries the longer form under
    // any of those: the words, the phrase, or what a query takes the phrase for.
    let vocab = alias_vocab("new york => big apple\nnyc => new york catalog", &[]);
    for title in [
        "big seasonal apple catalog",
        "apple big catalog",
        "catalog big apple",
        "new york catalog",
        "york catalog new",
    ] {
        assert!(
            positive_under(&vocab, title, "term:new_york_catalog"),
            "{title:?}"
        );
    }
    for title in ["big catalog", "apple catalog", "new catalog", "big apple"] {
        assert!(
            !positive_under(&vocab, title, "term:new_york_catalog"),
            "{title:?}"
        );
    }

    // When `catalog` is a word of many forms, the long form is keyed on its first words,
    // and a title that has those only as `big apple` still reaches it: the key holds the
    // phrase over that stretch as well as the words.
    let vocab = alias_vocab(
        "new york => big apple\nnyc => new york catalog\nzc1 => zza catalog\n\
         zc2 => zzb catalog\nzc3 => zzc catalog\nzc4 => zzd catalog",
        &[],
    );
    let norm = vocab.to_normalizer().expect("normalizer");
    let words = norm.alias_words.as_ref().expect("alias words");
    assert_eq!(words.forms_keyed_on("term:catalog"), 0);
    assert_eq!(words.forms_keyed_on("term:new_york"), 1);
    assert!(positive_under(
        &vocab,
        "big seasonal apple catalog",
        "term:new_york_catalog"
    ));
    assert!(!positive_under(
        &vocab,
        "big catalog",
        "term:new_york_catalog"
    ));

    // A phrase that is not an alias is a piece too, where it stands whole.
    let mut vocab = crate::vocab::Vocab::new();
    vocab.add_phrase(&["north", "star"], "entity:north_star", FeatureKind::Entity);
    vocab.add_equivalence(&["north star", "polaris"]);
    let norm = vocab.to_normalizer().expect("normalizer");
    vocab
        .import_solr_aliases("nslamp => north star lamp", &norm, &Dict::new())
        .expect("import");
    assert!(positive_under(
        &vocab,
        "lamp polaris",
        "term:north_star_lamp"
    ));
    assert!(positive_under(
        &vocab,
        "lamp, north star",
        "term:north_star_lamp"
    ));
    assert!(positive_under(
        &vocab,
        "star lamp north",
        "term:north_star_lamp"
    ));
    assert!(!positive_under(&vocab, "lamp", "term:north_star_lamp"));
}

#[test]
fn the_pieces_of_a_reading_join_end_to_end() {
    // Two phrases inside a form that overlap each other: `za zb` and `zb zc zd` in
    // `za zb zc zd`. A reading uses one or the other, with the words that are left. A title
    // that has both phrases, each under its other name, has no reading: the two do not
    // join, and it has none of the words.
    let vocab = alias_vocab("zp => za zb\nzq => zb zc zd\nzlong => za zb zc zd", &[]);
    let entity = "term:za_zb_zc_zd";
    assert!(positive_under(&vocab, "zp zc zd", entity));
    assert!(positive_under(&vocab, "zd zc zp", entity));
    assert!(positive_under(&vocab, "za zq", entity));
    assert!(positive_under(&vocab, "zd zc zb za", entity));
    assert!(!positive_under(&vocab, "zp zq", entity));
    assert!(!positive_under(&vocab, "zq zp zc", entity));
    assert!(!positive_under(&vocab, "zp zd", entity));
}

#[test]
fn a_very_long_form_is_built_and_carried_like_any_other() {
    // Nothing bounds the length of an alias form. Two thousand tokens are read in one scan
    // for the phrases inside them, and a title of which the form has little costs little.
    let tokens: Vec<String> = (0..2_000).map(|at| format!("zw{at}")).collect();
    let long = tokens.join(" ");
    let norm = build(|b| {
        b.add_phrase(&["zw10", "zw11"], "entity:pair", FeatureKind::Entity);
        b.add_alias_form(&long);
    });
    let entity = format!("term:{}", tokens.join("_"));
    let mut shuffled = tokens.clone();
    shuffled.reverse();
    assert!(positive_has(&norm, &shuffled.join(" "), &entity));
    let mut missing = shuffled.clone();
    missing.retain(|token| token != "zw1500");
    assert!(!positive_has(&norm, &missing.join(" "), &entity));
    assert!(!positive_has(&norm, "zw0 zw1 zw2", &entity));
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

mod completion;
