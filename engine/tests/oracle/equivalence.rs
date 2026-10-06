//! Equivalence learning via expansion-not-collapse (ADR-054) differential oracle.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Equivalence learning via expansion-not-collapse (ADR-054): declaring `pkg ≡ new` and
/// applying it must make a query phrased with one form match a title bearing the other —
/// while NEVER dropping a prior match (the match set only grows; FN-safe).
#[test]
fn equivalence_expansion_grows_matches_and_is_fn_safe() {
    use reverse_rusty::vocab::Vocab;

    // A corpus where "pkg" and "new" are distinct features (empty default vocab). Extra
    // queries ensure both tokens are interned in the dict.
    let mut queries: Vec<(u64, String)> = vec![
        (1, "1994 vertex pkg".into()), // requires pkg
        (2, "1994 vertex new".into()), // requires new
    ];
    for i in 0..20u64 {
        queries.push((100 + i, format!("pkg item{i}")));
        queries.push((200 + i, format!("new item{i}")));
    }
    let new_title = "1994 vertex new pro"; // has new, NOT pkg

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(new_title, &mut s, &mut out, true);
    let before: HashSet<u64> = out.iter().copied().collect();
    assert!(
        !before.contains(&1),
        "before the equivalence, the pkg-query must not match a new-only title"
    );

    // Declare pkg ≡ new and apply via expansion (set_vocab installs it; recompile expands).
    let mut v = Vocab::new();
    v.add_equivalence(&["pkg", "new"]);
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    eng.match_title(new_title, &mut s, &mut out, true);
    let after: HashSet<u64> = out.iter().copied().collect();
    assert!(
        after.contains(&1),
        "after pkg≡new, the pkg-query matches a new title (expansion grew the match set)"
    );
    assert!(
        before.is_subset(&after),
        "expansion must never drop a prior match (FN-safe / monotone)"
    );
}

/// The structural safety claim for expansion (ADR-054): even a WRONG (nonsense) equivalence
/// can only add false positives — it must NEVER drop a true match. We apply a garbage
/// equivalence and assert every match the ORIGINAL (unexpanded) queries had still survives,
/// in BOTH read modes: with the broad lane, and on the default read, where an expansion that
/// leaves a query only a top-64 anchor used to hide it (ADR-187).
#[test]
fn wrong_equivalence_never_causes_false_negatives() {
    use reverse_rusty::vocab::Vocab;

    let cfg = GenConfig {
        num_queries: 8_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0BAD_0E00,
        num_entities: 600,
        num_collections: 300,
    };
    let data = generate(&cfg);

    // Intern two unrelated nonsense tokens so the bogus equivalence resolves to real ids.
    let mut queries = data.queries.clone();
    for i in 0..20u64 {
        queries.push((9_000_000 + i, format!("wibble u{i}")));
        queries.push((9_100_000 + i, format!("wobble u{i}")));
    }
    // The shape the default read used to lose: anchored on the rare aliased term, with a
    // top-64 term (`standard`) as the only other requirement.
    let mixed = 9_200_000u64;
    queries.push((mixed, "wibble standard".to_string()));
    let mut titles = data.titles.clone();
    titles.push("wibble standard edition".to_string());

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    // Ground truth under the ORIGINAL semantics (no equivalence).
    let brute = Brute::build(&queries);
    let default_reads = |eng: &Engine| -> Vec<HashSet<u64>> {
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        titles
            .iter()
            .map(|title| {
                eng.match_title(title, &mut s, &mut out, false);
                out.iter().copied().collect()
            })
            .collect()
    };
    let default_before = default_reads(&eng);
    assert!(
        default_before
            .last()
            .is_some_and(|set| set.contains(&mixed)),
        "precondition: the mixed query is default-visible before the equivalence"
    );

    // Apply a nonsense equivalence and recompile.
    let mut v = Vocab::new();
    v.add_equivalence(&["wibble", "wobble"]);
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for title in &titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats); // original semantics
        total_truth += truth.len();
        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
    }
    assert_eq!(
        false_neg, 0,
        "expansion of a WRONG equivalence must never drop a true match (structural FN-safety)"
    );
    assert!(total_truth > 0, "degenerate test: no matches");

    let default_after = default_reads(&eng);
    for ((title, before), after) in titles.iter().zip(&default_before).zip(&default_after) {
        let hidden: Vec<_> = before.difference(after).collect();
        assert!(
            hidden.is_empty(),
            "the equivalence removed {hidden:?} from the default read of {title:?}"
        );
    }
}

/// The learned source end-to-end (ADR-054): `learn_and_apply_with` in expansion mode
/// turns the corpus's any-of groups into an equivalence applied via expansion, so a query
/// phrased with one form then matches a title bearing the other.
#[test]
fn learned_equivalence_via_expansion_matches_both_forms() {
    use reverse_rusty::vocab::CorpusLearnConfig;

    let mut queries: Vec<(u64, String)> = vec![(1, "1994 vertex pkg".into())];
    for i in 0..6u64 {
        queries.push((100 + i, "(pkg,new)".into())); // declare the any-of >= min_count
    }
    for i in 0..20u64 {
        queries.push((200 + i, format!("new u{i}")));
        queries.push((300 + i, format!("pkg u{i}")));
    }
    let new_title = "1994 vertex new pro";

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(new_title, &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "before learning, the pkg-query must not match a new title"
    );

    let cfg = CorpusLearnConfig {
        anyof_min_count: 2,
        anyof_mode: reverse_rusty::vocab::AnyOfLearnMode::Expansion,
        ..Default::default()
    };
    eng.learn_and_apply_with(&cfg)
        .expect("learn_and_apply equivalences");
    assert!(
        !eng.vocab().expect("vocab").equivalences().is_empty(),
        "an equivalence group must be learned from the any-of corpus"
    );

    eng.match_title(new_title, &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "after learning pkg≡new via expansion, the pkg-query matches a new title"
    );
}

/// Equivalences declared on the vocab BEFORE the initial build must be applied during
/// `build_from_queries` (not only via a later `set_vocab`). Regression for the gap where the
/// single-engine initial build skipped equivalence resolution.
#[test]
fn initial_build_applies_declared_equivalences() {
    use reverse_rusty::vocab::Vocab;
    use reverse_rusty::EngineConfig;

    let mut v = Vocab::new();
    v.add_equivalence(&["pkg", "new"]);
    let mut eng = Engine::with_vocab(v, EngineConfig::default()).expect("with_vocab");

    let mut queries: Vec<(u64, String)> = vec![(1, "1994 vertex pkg".into())];
    for i in 0..10u64 {
        queries.push((100 + i, format!("pkg u{i}")));
        queries.push((200 + i, format!("new u{i}")));
    }
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("1994 vertex new pro", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "initial build must apply declared equivalences: the pkg-query matches a new title"
    );
}
