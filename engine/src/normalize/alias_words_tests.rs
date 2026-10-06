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
fn a_word_counts_under_what_a_query_takes_it_to_be_equivalent_to() {
    // To a query, `pkg` is `pkg` or `package`, and `ny` is `ny` or the form `new york`.
    // A title carries the words of `pkg deal` and `ny catalog` under either name.
    let norm = build(|b| {
        b.add_alias_form("new york");
        b.add_alias_form("ny catalog");
        b.add_alias_form("pkg deal");
        b.add_equivalent_forms(&["ny".to_string(), "new york".to_string()]);
        b.add_equivalent_forms(&["pkg".to_string(), "package".to_string()]);
        // Groups that share a member are one class.
        b.add_equivalent_forms(&["package".to_string(), "parcel".to_string()]);
        // A form that does not compile to one feature is no member of a group.
        b.add_equivalent_forms(&["deal".to_string(), "two words".to_string()]);
    });
    assert!(positive_has(&norm, "deal package", "term:pkg_deal"));
    assert!(positive_has(&norm, "parcel deal", "term:pkg_deal"));
    assert!(!positive_has(&norm, "two words pkg", "term:pkg_deal"));
    assert!(!positive_has(&norm, "package", "term:pkg_deal"));
    // `york new` carries `new york`, which is what a query means by `ny`.
    assert!(positive_has(&norm, "york new catalog", "term:ny_catalog"));
    assert!(positive_has(&norm, "catalog new york", "term:ny_catalog"));
    assert!(!positive_has(&norm, "york catalog", "term:ny_catalog"));
    assert!(!canonical_has(&norm, "deal package", "term:pkg_deal"));

    // Without the groups, a word is only itself.
    let norm = build(|b| b.add_alias_form("pkg deal"));
    assert!(!positive_has(&norm, "deal package", "term:pkg_deal"));
}

/// Run the completion on a title given by the names it carries. Returns the entities added,
/// distinct and in order, and how many times a form was examined.
fn complete(words: &super::core::AliasWords, carried: &mut Vec<u64>) -> (Vec<FeatureId>, usize) {
    let mut scratch = super::core::AliasScratch::default();
    let (out, examined) = complete_with(words, carried, &mut scratch);
    (out, examined)
}

fn complete_with(
    words: &super::core::AliasWords,
    carried: &mut Vec<u64>,
    scratch: &mut super::core::AliasScratch,
) -> (Vec<FeatureId>, usize) {
    let mut out = Vec::new();
    scratch.names.clear();
    scratch.names.append(carried);
    let examined = words.complete_into(scratch, &Dict::new(), &mut out);
    carried.clone_from(&scratch.names);
    // The view sorts what it is given, and a form can be reached twice.
    out.sort_unstable();
    out.dedup();
    (out, examined)
}

fn hashes(names: &[&str]) -> Vec<u64> {
    names
        .iter()
        .map(|name| super::core::name_hash(name))
        .collect()
}

fn ids(names: &[&str]) -> Vec<FeatureId> {
    let mut ids: Vec<FeatureId> = names.iter().map(|name| id(name)).collect();
    ids.sort_unstable();
    ids
}

fn form(entity: &str, words: &[&str]) -> (String, Vec<Vec<String>>) {
    (
        entity.to_string(),
        words.iter().map(|word| vec![(*word).to_string()]).collect(),
    )
}

#[test]
fn a_title_that_repeats_a_word_carries_it_once() {
    // The names of a title are reduced to the distinct ones before any form is examined,
    // so a word repeated thousands of times costs one look, not one per occurrence.
    let words = super::core::AliasWords::new(vec![form(
        "term:wireless_mouse",
        &["term:wireless", "term:mouse"],
    )])
    .expect("alias words");
    let wireless = super::core::name_hash("term:wireless");
    let mouse = super::core::name_hash("term:mouse");

    let mut carried = vec![wireless; 50_000];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(carried, vec![wireless]);
    assert!(out.is_empty());
    assert!(examined <= 1, "examined {examined} times");

    let mut carried = vec![mouse, wireless, mouse, wireless];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out, vec![id("term:wireless_mouse")]);
    assert_eq!(examined, 1);
}

#[test]
fn the_work_follows_the_forms_a_title_touches() {
    // Thousands of forms that share a word: a title with that word alone examines none.
    let shared: Vec<_> = (0..5_000)
        .map(|model| {
            form(
                &format!("term:wireless_m{model}"),
                &["term:wireless", &format!("term:m{model}")],
            )
        })
        .collect();
    let words = super::core::AliasWords::new(shared).expect("alias words");
    let (out, examined) = complete(&words, &mut vec![super::core::name_hash("term:wireless")]);
    assert!(out.is_empty());
    assert_eq!(examined, 0);

    // A chain two thousand forms long, each built on the entity of the one before. The
    // title carries the first word; every form completes, and each is examined once.
    let chain: Vec<_> = (0..2_000)
        .map(|link| {
            form(
                &format!("term:s{}", link + 1),
                &[&format!("term:s{link}"), "term:unit"],
            )
        })
        .collect();
    let words = super::core::AliasWords::new(chain).expect("alias words");
    let mut carried = vec![
        super::core::name_hash("term:unit"),
        super::core::name_hash("term:s0"),
    ];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out.len(), 2_000);
    assert!(out.contains(&id("term:s2000")));
    assert_eq!(examined, 2_000);

    // The chain entered in the middle completes from there on only.
    let mut carried = vec![
        super::core::name_hash("term:unit"),
        super::core::name_hash("term:s1500"),
    ];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out.len(), 500);
    assert_eq!(examined, 500);

    // An entity the title already carries has had its forms examined; completing the
    // form for it does not examine them again.
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:unit", "term:s0", "term:s1", "term:s2"]),
    );
    assert_eq!(out.len(), 2_000);
    assert_eq!(examined, 2_000);
}

#[test]
fn two_forms_for_one_entity_add_it_once() {
    let words = super::core::AliasWords::new(vec![
        form("entity:place", &["term:new", "term:york"]),
        form("entity:place", &["term:big", "term:apple"]),
        form("term:trip", &["entity:place", "term:tour"]),
    ])
    .expect("alias words");
    let mut carried: Vec<u64> = [
        "term:new",
        "term:york",
        "term:big",
        "term:apple",
        "term:tour",
    ]
    .iter()
    .map(|name| super::core::name_hash(name))
    .collect();
    let (out, _) = complete(&words, &mut carried);
    assert_eq!(out, ids(&["entity:place", "term:trip"]));
}

#[test]
fn an_entity_that_many_forms_share_as_a_word_wakes_none_of_them() {
    // `wire less` is a form for the word `wireless`, which ten thousand forms
    // `wireless <model>` share. They are keyed on their model, so a title that carries
    // `wireless` only through `wire less` examines the one form it touches.
    let mut forms = vec![form("term:wireless", &["term:wire", "term:less"])];
    forms.extend((0..10_000).map(|model| {
        form(
            &format!("term:wireless_m{model}"),
            &["term:wireless", &format!("term:m{model}")],
        )
    }));
    let words = super::core::AliasWords::new(forms).expect("alias words");
    assert_eq!(words.forms_keyed_on("term:wireless"), 0);

    let (out, examined) = complete(&words, &mut hashes(&["term:wire", "term:less"]));
    assert_eq!(out, ids(&["term:wireless"]));
    assert_eq!(examined, 1);

    // With a model as well, that model's form is the only other one looked at: once if
    // `wireless` was already in the view, twice if the form had to wait for it.
    let (out, examined) = complete(&words, &mut hashes(&["term:wire", "term:less", "term:m7"]));
    assert_eq!(out, ids(&["term:wireless", "term:wireless_m7"]));
    assert!((2..=3).contains(&examined), "examined {examined} times");
}

#[test]
fn a_form_waits_for_the_word_it_lacks() {
    // `zk s2` is keyed on `zk`, which the title carries, and lacks `s2`, which only the
    // completion can supply, two steps later. It is looked at once when its key is seen
    // and once when `s2` arrives, and not in between.
    let words = super::core::AliasWords::new(vec![
        form("term:s3", &["term:zk", "term:s2"]),
        form("term:s1", &["term:za", "term:zb"]),
        form("term:s2", &["term:s1", "term:zc"]),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:zk"), 1);
    assert_eq!(words.forms_keyed_on("term:s2"), 0);
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:za", "term:zb", "term:zc", "term:zk"]),
    );
    assert_eq!(out, ids(&["term:s1", "term:s2", "term:s3"]));
    assert_eq!(examined, 4);

    // Without the word the chain needs, the waiting form is never looked at again.
    let (out, examined) = complete(&words, &mut hashes(&["term:za", "term:zb", "term:zk"]));
    assert_eq!(out, ids(&["term:s1"]));
    assert_eq!(examined, 3, "each form once");
}

#[test]
fn a_form_is_noted_once_under_a_word_it_waits_on() {
    // The second word of `last` can arrive under two names, `a1` and `a2`, and both do,
    // one after the other, while its third word `b` is still two steps away. The second
    // arrival finds the form waiting on `b` as the first left it, and notes nothing: when
    // `b` arrives the form is looked at once, not once per earlier look.
    let twice = |entity: &str, word: &str| form(entity, &[word, word]);
    let words = super::core::AliasWords::new(vec![
        form("term:q1", &["term:x1", "term:x2"]),
        form("term:q2", &["term:y1", "term:y2"]),
        twice("term:a1", "term:q1"),
        twice("term:a2", "term:q2"),
        form("term:bb", &["term:a1", "term:a2"]),
        twice("term:b", "term:bb"),
        (
            "term:last".to_string(),
            vec![
                vec!["term:zkey".to_string()],
                vec!["term:a1".to_string(), "term:a2".to_string()],
                vec!["term:b".to_string()],
            ],
        ),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:zkey"), 1);
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:x1", "term:x2", "term:y1", "term:y2", "term:zkey"]),
    );
    assert_eq!(
        out,
        ids(&[
            "term:q1",
            "term:q2",
            "term:a1",
            "term:a2",
            "term:bb",
            "term:b",
            "term:last"
        ])
    );
    // Six forms once each, and `last` four times: at its key, when `a1` and `a2` arrive,
    // and when `b` does.
    assert_eq!(examined, 10);
}

#[test]
fn a_key_word_carried_under_two_names_completes_the_form_once() {
    // `unit` is a word of two forms, so the first is keyed on its other word, which has
    // two names.
    let words = super::core::AliasWords::new(vec![
        (
            "term:refurb_unit".to_string(),
            vec![
                vec!["term:refurb".to_string(), "term:refurbished".to_string()],
                vec!["term:unit".to_string()],
            ],
        ),
        form("term:unit_price", &["term:unit", "term:price"]),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:refurb"), 1);
    assert_eq!(words.forms_keyed_on("term:refurbished"), 1);
    let mut scratch = super::core::AliasScratch::default();
    let mut out = Vec::new();
    scratch.names = hashes(&["term:refurb", "term:refurbished", "term:unit"]);
    let examined = words.complete_into(&mut scratch, &Dict::new(), &mut out);
    assert_eq!(out, vec![id("term:refurb_unit")], "added once");
    assert_eq!(examined, 2, "once per name of the key word");
}

#[test]
fn a_title_that_touches_no_form_holds_no_completion_state() {
    // A scratch made for one title (cluster routing makes one per request) must not pay
    // for the size of the registry.
    let forms: Vec<_> = (0..20_000)
        .map(|model| {
            form(
                &format!("term:wireless_m{model}"),
                &["term:wireless", &format!("term:m{model}")],
            )
        })
        .collect();
    let words = super::core::AliasWords::new(forms).expect("alias words");
    let mut scratch = super::core::AliasScratch::default();
    let (out, examined) = complete_with(
        &words,
        &mut hashes(&["term:wireless", "term:lamp"]),
        &mut scratch,
    );
    assert!(out.is_empty());
    assert_eq!(examined, 0);
    assert_eq!(scratch.held(), 0);

    // Nor does a title that has a form's key word and lacks a word nothing can supply.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:m3"]), &mut scratch);
    assert!(out.is_empty());
    assert_eq!(examined, 1);
    assert_eq!(scratch.held(), 0);
}

#[test]
fn nothing_of_one_title_is_left_for_the_next() {
    // One scratch serves every title of a request. `s3` waits on `s2`; `s2` is keyed on
    // `zc` and needs `s1`.
    let words = super::core::AliasWords::new(vec![
        form("term:s1", &["term:za", "term:zb"]),
        form("term:s2", &["term:zc", "term:s1"]),
        form("term:s3", &["term:zk", "term:s2"]),
    ])
    .expect("alias words");
    let mut scratch = super::core::AliasScratch::default();
    let full = ["term:za", "term:zb", "term:zc", "term:zk"];
    let all = ids(&["term:s1", "term:s2", "term:s3"]);

    let (out, first) = complete_with(&words, &mut hashes(&full), &mut scratch);
    assert_eq!(out, all);
    // The same title again completes the same forms with the same work: a form that was
    // completed for the first title is not taken as done for the second.
    let (out, again) = complete_with(&words, &mut hashes(&full), &mut scratch);
    assert_eq!(out, all);
    assert_eq!(again, first);

    // `s1` was in the first title's view. A title without its words does not carry it,
    // so `s2` is examined and not completed, and `s3` waits on a word that never comes.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:zc", "term:zk"]), &mut scratch);
    assert!(out.is_empty());
    assert_eq!(examined, 2);

    // A title that left `s3` waiting leaves no one waiting for the next title.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:za", "term:zb"]), &mut scratch);
    assert_eq!(out, ids(&["term:s1"]));
    assert_eq!(examined, 1);
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
