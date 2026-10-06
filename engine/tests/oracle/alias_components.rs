//! Activating a multi-word alias keeps every match a stored query had (ADR-205).
//!
//! A query that spells an alias form out (`wireless mouse`) meant "both words" before the
//! alias existed. Activation reads the words as one entity so that the other forms of the
//! alias match too. The words themselves must stay a way to satisfy the query, or a title
//! that has them apart or in another order (`wireless optical mouse`, `mouse, wireless`)
//! stops matching a query that matched it before.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use std::collections::HashSet;

fn matched(eng: &mut Engine, s: &mut MatchScratch, title: &str) -> HashSet<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out.iter().copied().collect()
}

fn owned(queries: &[(u64, &str)]) -> Vec<(u64, String)> {
    queries
        .iter()
        .map(|&(id, dsl)| (id, dsl.to_string()))
        .collect()
}

fn matches_of(eng: &mut Engine, titles: &[&str]) -> Vec<HashSet<u64>> {
    let mut s = MatchScratch::new();
    titles
        .iter()
        .map(|title| matched(eng, &mut s, title))
        .collect()
}

fn assert_nothing_lost(titles: &[&str], before: &[HashSet<u64>], after: &[HashSet<u64>]) {
    for ((title, before), after) in titles.iter().zip(before).zip(after) {
        let lost: Vec<&u64> = before.difference(after).collect();
        assert!(
            lost.is_empty(),
            "activation removed {lost:?} from the matches of {title:?}"
        );
    }
}

const QUERIES: &[(u64, &str)] = &[
    (1, "wireless mouse"),
    (2, "new york inventory"),
    (3, "logitech wireless mouse"),
    (4, "(wireless mouse, trackball) usb"),
    (5, "york new"),
    (6, "mouse"),
    (7, "wireless mouse -optical"),
    (8, "inventory -(new york, boston)"),
    (9, "\"new york\" inventory"),
    (10, "(new york, boston) inventory"),
    (11, "(new york) inventory"),
    (12, "ny catalog"),
];

const TITLES: &[&str] = &[
    "wireless mouse",
    "wireless optical mouse",
    "mouse, wireless",
    "cordless mouse",
    "logitech wireless optical mouse",
    "logitech mouse wireless usb",
    "usb wireless gaming mouse",
    "usb trackball",
    "new york inventory",
    "new seasonal york inventory",
    "york inventory new",
    "ny inventory",
    "boston inventory",
    "optical mouse wireless",
    "inventory of new items",
    "york inventory",
    "new seasonal york catalog",
];

/// The property the finding is about: what matched before activation matches after it.
#[test]
fn activation_keeps_every_match_the_query_had() {
    let queries = owned(QUERIES);
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let before = matches_of(&mut eng, TITLES);
    assert!(
        before[1].contains(&1),
        "precondition: the words apart match"
    );
    assert!(
        before[2].contains(&1),
        "precondition: the words reordered match"
    );
    assert!(
        before[9].contains(&10),
        "precondition: a member's words apart match"
    );

    let report = eng
        .import_alias_synonyms("wireless mouse => cordless mouse\nny => new york")
        .expect("import");
    assert!(report.activated >= 2, "both groups activate: {report:?}");

    let after = matches_of(&mut eng, TITLES);
    assert_nothing_lost(TITLES, &before, &after);

    // The alias does what it is for.
    let mut s = MatchScratch::new();
    assert!(matched(&mut eng, &mut s, "cordless mouse").contains(&1));
    assert!(matched(&mut eng, &mut s, "ny inventory").contains(&2));
    assert!(matched(&mut eng, &mut s, "logitech cordless mouse").contains(&3));
    assert!(matched(&mut eng, &mut s, "usb cordless mouse").contains(&4));
    assert!(matched(&mut eng, &mut s, "ny inventory").contains(&10));
    assert!(matched(&mut eng, &mut s, "ny inventory").contains(&11));

    // The title with the words apart carries the form for every query, also one that
    // names only the alias's other form.
    assert!(matched(&mut eng, &mut s, "new seasonal york catalog").contains(&12));

    // One word of a form is not the form, alone or as a member of a group.
    for title in ["inventory of new items", "york inventory"] {
        for query in [2, 10, 11] {
            assert!(
                !matched(&mut eng, &mut s, title).contains(&query),
                "{title:?} matched query {query}"
            );
        }
    }
    // A quoted form asked for adjacency and keeps it.
    assert!(!matched(&mut eng, &mut s, "new seasonal york inventory").contains(&9));
    assert!(matched(&mut eng, &mut s, "new york inventory").contains(&9));
    // A negated form rejects the form written out, and not its alias or its words apart.
    assert!(!matched(&mut eng, &mut s, "new york inventory").contains(&8));
    assert!(matched(&mut eng, &mut s, "ny inventory").contains(&8));

    // Candidate retrieval is lossless for the new plans: the engine returns exactly what
    // evaluating every stored query against the title returns.
    let vocab = eng.vocab().expect("vocab installed").clone();
    let brute = Brute::build_with_vocab(&queries, &vocab);
    let (mut lc, mut feats) = (String::new(), Vec::new());
    for title in TITLES {
        assert_eq!(
            matched(&mut eng, &mut s, title),
            brute.matches(title, &mut lc, &mut feats),
            "{title:?}"
        );
    }
}

/// A corpus-learned additive phrase over the same words (ADR-053) had tightened the
/// spelled-out query to adjacency. As an alias it accepts the words apart as well.
#[test]
fn an_additive_phrase_turned_alias_accepts_its_words_apart() {
    let mut vocab = Vocab::new();
    vocab.add_phrase_additive(
        &["wireless", "mouse"],
        "term:wireless_mouse",
        FeatureKind::Generic,
    );
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "wireless mouse"), (2, "mouse")]));
    let titles = ["wireless mouse", "wireless optical mouse", "cordless mouse"];
    let before = matches_of(&mut eng, &titles);
    assert!(before[0].contains(&1));
    assert!(
        !before[1].contains(&1),
        "precondition: the additive phrase requires adjacency"
    );

    eng.import_alias_synonyms("wireless mouse => cordless mouse")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    assert!(after[1].contains(&1), "the words apart now match");
    assert!(after[2].contains(&1), "the alias matches");
    assert_eq!(after[1].contains(&2), before[1].contains(&2));
}

/// A declared collapse phrase over the same words meant the entity alone. The alias takes
/// the phrase over, and like every alias form it then accepts its words apart.
#[test]
fn a_collapse_phrase_turned_alias_accepts_its_words_apart() {
    let mut vocab = Vocab::new();
    vocab.add_phrase(
        &["wireless", "mouse"],
        "term:wireless_mouse",
        FeatureKind::Generic,
    );
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "wireless mouse")]));
    let titles = ["wireless mouse", "wireless optical mouse", "cordless mouse"];
    let before = matches_of(&mut eng, &titles);
    assert_eq!(
        before
            .iter()
            .map(|ids| ids.contains(&1))
            .collect::<Vec<_>>(),
        [true, false, false]
    );

    eng.import_alias_synonyms("wireless mouse => cordless mouse")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    assert_eq!(
        after.iter().map(|ids| ids.contains(&1)).collect::<Vec<_>>(),
        [true, true, true]
    );
}

/// A word of the form that is also a synonym for the alias's entity. That word alone names
/// the entity, as before the alias; the form's other word alone is not the form.
#[test]
fn a_word_that_names_the_entity_leaves_no_alternative() {
    let mut vocab = Vocab::new();
    vocab.add_synonym("wireless", "term:wireless_mouse", FeatureKind::Generic);
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "wireless mouse")]));
    let titles = ["wireless mouse", "mouse", "wireless", "cordless mouse"];
    let before = matches_of(&mut eng, &titles);

    eng.import_alias_synonyms("wireless mouse => cordless mouse")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    assert!(after[0].contains(&1) && after[3].contains(&1));
    assert!(!after[1].contains(&1), "`mouse` alone is not the form");
}

/// At scale, on generated queries that the aliases really rewrite. The generator writes the
/// brands `north star` and `blue peak` as two words in its queries. Titles are the generated
/// ones plus, for each such query, its own words in four spellings: as written, with the
/// brand's words apart, reversed, and glued into the alias's other form. Ground truth is the
/// brute-force matcher under the vocabulary with no alias; every match it finds must survive
/// activation.
#[test]
fn activation_keeps_every_match_at_scale() {
    const BRANDS: [(&str, &str, &str, &str); 2] = [
        ("north star", "north polar star", "star north", "northstar"),
        ("blue peak", "blue alpine peak", "peak blue", "bluepeak"),
    ];
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0A11_A5E2,
        num_entities: 500,
        num_collections: 250,
    };
    let data = generate(&cfg);

    let mut titles = data.titles.clone();
    let mut apart = HashSet::new();
    let mut rewritten = HashSet::new();
    for (id, dsl) in &data.queries {
        let Some(&(brand, split, reversed, glued)) =
            BRANDS.iter().find(|(brand, ..)| dsl.contains(brand))
        else {
            continue;
        };
        rewritten.insert(*id);
        if rewritten.len() % 4 != 0 {
            continue; // every fourth such query is enough titles
        }
        // The query's own positive words make a title it matches.
        let words: Vec<&str> = dsl
            .split_whitespace()
            .filter(|word| !word.starts_with('-'))
            .collect();
        let written = words.join(" ");
        titles.push(written.clone());
        for variant in [
            written.replace(brand, split),
            written.replace(brand, reversed),
        ] {
            apart.insert(titles.len());
            titles.push(variant);
        }
        titles.push(written.replace(brand, glued));
    }
    assert!(
        rewritten.len() > 100,
        "the aliases must rewrite stored queries: {}",
        rewritten.len()
    );

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&data.queries);
    let brute = Brute::build(&data.queries); // the vocabulary with no alias

    let report = eng
        .import_alias_synonyms("northstar => north star\nbluepeak => blue peak")
        .expect("import");
    assert!(report.activated >= 2, "{report:?}");

    let mut s = MatchScratch::new();
    let (mut lc, mut feats) = (String::new(), Vec::new());
    let (mut truth_apart, mut gained) = (0usize, 0usize);
    for (index, title) in titles.iter().enumerate() {
        let after = matched(&mut eng, &mut s, title);
        let truth = brute.matches(title, &mut lc, &mut feats);
        let lost: Vec<&u64> = truth.difference(&after).collect();
        assert!(
            lost.is_empty(),
            "activation removed {lost:?} from the matches of {title:?}"
        );
        if apart.contains(&index) {
            truth_apart += truth.intersection(&rewritten).count();
        }
        gained += after.difference(&truth).count();
    }
    assert!(
        truth_apart > 100,
        "rewritten queries that matched a title with their brand's words apart: {truth_apart}"
    );
    assert!(gained > 100, "matches the aliases added: {gained}");
}

/// Queries written after the alias is active keep the plan they have always had under an
/// alias: the entity and the alias's other forms. A query that is only a form of two very
/// common words, or has such a form in a group, is therefore default-visible, and the
/// title side supplies the entity for titles that have the words apart.
#[test]
fn a_form_of_very_common_words_stays_default_visible() {
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 1,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0A11_A5E2,
        num_entities: 500,
        num_collections: 250,
    };
    let mut queries = generate(&cfg).queries;
    let base = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let (form, plus, max, group) = (base, base + 1, base + 2, base + 3);
    queries.push((form, "plus max".to_string()));
    queries.push((plus, "plus".to_string()));
    queries.push((max, "max".to_string()));
    queries.push((group, "(plus max, zztrackball)".to_string()));

    let mut vocab = Vocab::new();
    vocab
        .import_solr_aliases(
            "plusmax => plus max",
            &Normalizer::default_vocab().expect("vocab"),
            &reverse_rusty::dict::Dict::new(),
        )
        .expect("aliases");
    let mut eng = Engine::with_vocab(vocab.clone(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut read = |eng: &mut Engine, title: &str, include_broad: bool| -> HashSet<u64> {
        let mut out = Vec::new();
        eng.match_title(title, &mut s, &mut out, include_broad);
        out.into_iter().collect()
    };
    // Precondition: each word is among the most common terms, so a query that is one of
    // them alone is opt-in.
    for (word, id) in [("plus", plus), ("max", max)] {
        assert!(
            !read(&mut eng, word, false).contains(&id),
            "{word} is opt-in"
        );
        assert!(read(&mut eng, word, true).contains(&id));
    }

    for title in ["plus max", "plus alpha max", "max and plus", "plusmax"] {
        assert!(
            read(&mut eng, title, false).contains(&form),
            "a default read of {title:?} lacks the form query"
        );
    }
    for title in ["plus alpha", "alpha max"] {
        assert!(!read(&mut eng, title, true).contains(&form), "{title:?}");
    }
    // A group with the form as one member is default-visible through its other members.
    for title in ["zztrackball", "plusmax", "max and plus"] {
        assert!(
            read(&mut eng, title, false).contains(&group),
            "a default read of {title:?} lacks the group query"
        );
    }

    // And retrieval is exact.
    let brute = Brute::build_with_vocab(&queries, &vocab);
    let (mut lc, mut feats) = (String::new(), Vec::new());
    for title in [
        "plus max",
        "plus alpha max",
        "max and plus",
        "plusmax",
        "plus alpha",
        "zztrackball",
    ] {
        assert_eq!(
            read(&mut eng, title, true),
            brute.matches(title, &mut lc, &mut feats),
            "{title:?}"
        );
    }
}

/// A word of the form that its context types differently. `#1995` is a plain number, and
/// `1995` alone is a year: the stored query asked for the plain number, and a title that
/// carries it must still match once `1995 unit` is an alias form.
#[test]
fn a_number_typed_by_its_context_still_matches() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&owned(&[(1, "#1995 unit"), (2, "1995 unit")]));
    let titles = ["unit #1995", "1995 unit", "unit 1995 edition", "#1995 unit"];
    let before = matches_of(&mut eng, &titles);
    assert!(before[0].contains(&1) && before[1].contains(&2));

    eng.import_alias_synonyms("unit1995 => 1995 unit")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    let mut s = MatchScratch::new();
    assert!(matched(&mut eng, &mut s, "unit1995").contains(&2));
}

/// Words that no stored query had carried as features: a declared collapse phrase consumed
/// them. The vocabulary change that replaces the phrase with an alias leaves the stored
/// query on the form's entity, and a title with the words apart carries it. A later insert
/// that interns one of the words must change nothing: the title-side rule reads feature
/// names, not ids.
#[test]
fn the_words_of_a_form_keep_one_identity_across_a_later_insert() {
    let mut phrased = Vocab::new();
    phrased.add_phrase(&["new", "york"], "term:new_york", FeatureKind::Generic);
    let mut eng = Engine::with_vocab(phrased, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "new york inventory")]));
    assert!(
        eng.dict().get("term:new").is_none(),
        "precondition: never stored"
    );

    let mut aliased = Vocab::new();
    aliased
        .import_solr_aliases(
            "ny => new york",
            &Normalizer::default_vocab().expect("vocab"),
            &reverse_rusty::dict::Dict::new(),
        )
        .expect("aliases");
    eng.set_vocab(aliased).expect("set_vocab");
    eng.recompile_stale_segments();

    let titles = [
        "new seasonal york inventory",
        "ny inventory",
        "new york inventory",
    ];
    let before = matches_of(&mut eng, &titles);
    assert!(before.iter().all(|ids| ids.contains(&1)), "{before:?}");

    eng.try_insert_live("new shoes", 2, 1).expect("insert");
    eng.try_insert_live("york minster", 3, 1).expect("insert");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
}

/// A later alias must not undo an earlier one. `new york` is a declared phrase for `ny`, and
/// `ny catalog` an alias form: a title that says `new york city catalog` carries it. Once
/// `new york city` is an alias form too it wins the title's parse, and the phrase is found
/// only by the overlapping scan; the words of `ny catalog` are still all in the positive
/// view.
#[test]
fn a_later_overlapping_alias_removes_no_match() {
    let mut vocab = Vocab::new();
    vocab.add_phrase(&["new", "york"], "term:ny", FeatureKind::Generic);
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "ny catalog"), (2, "new york catalog")]));
    let titles = [
        "new york city catalog",
        "ny catalog",
        "catalog of new york",
        "nycat",
    ];

    eng.import_alias_synonyms("nycat => ny catalog")
        .expect("import");
    let before = matches_of(&mut eng, &titles);
    assert!(before[0].contains(&1) && before[0].contains(&2));

    eng.import_alias_synonyms("nyc => new york city")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
}
