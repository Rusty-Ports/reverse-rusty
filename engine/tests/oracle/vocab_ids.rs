//! A feature name has one id (ADR-204).
//!
//! A vocabulary change recompiles every stored query. A name the new vocabulary produces
//! for the first time (a synonym's canonical, a phrase's entity) must get the id that a
//! later insert of the same name gets, or titles would resolve it to one id while the
//! recompiled queries require another, and those queries would stop matching without any
//! error.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use std::collections::HashSet;

fn matched(eng: &mut Engine, title: &str) -> HashSet<u64> {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(title, &mut s, &mut out, true);
    out.into_iter().collect()
}

fn engine(queries: &[(u64, &str)]) -> Engine {
    let owned: Vec<(u64, String)> = queries
        .iter()
        .map(|&(id, dsl)| (id, dsl.to_string()))
        .collect();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&owned);
    eng
}

fn apply(eng: &mut Engine, vocab: Vocab) {
    eng.set_vocab(vocab).expect("set_vocab");
    eng.recompile_stale_segments();
}

/// The finding's first reproduction: a synonym whose canonical no stored query had used.
#[test]
fn an_insert_that_uses_a_new_synonym_canonical_changes_no_earlier_match() {
    let mut eng = engine(&[(1, "refurb widget")]);
    let mut vocab = Vocab::new();
    vocab.add_synonym("refurb", "term:refurbished", FeatureKind::Generic);
    apply(&mut eng, vocab);
    assert!(matched(&mut eng, "refurb widget").contains(&1));
    assert!(matched(&mut eng, "refurbished widget").contains(&1));

    eng.try_insert_live("refurbished gadget", 2, 1)
        .expect("insert");

    assert!(
        matched(&mut eng, "refurb widget").contains(&1),
        "the insert made an earlier query stop matching"
    );
    assert!(matched(&mut eng, "refurbished widget").contains(&1));
    assert!(matched(&mut eng, "refurb gadget").contains(&2));
}

/// The second: a phrase whose entity no stored query had used.
#[test]
fn an_insert_that_uses_a_new_phrase_entity_changes_no_earlier_match() {
    let mut eng = engine(&[(1, "north star lamp")]);
    let mut vocab = Vocab::new();
    vocab.add_phrase(&["north", "star"], "term:northstar", FeatureKind::Generic);
    apply(&mut eng, vocab);
    assert!(matched(&mut eng, "north star lamp").contains(&1));

    eng.try_insert_live("northstar shade", 2, 1)
        .expect("insert");

    assert!(
        matched(&mut eng, "north star lamp").contains(&1),
        "the insert made an earlier query stop matching"
    );
    assert!(matched(&mut eng, "north star shade").contains(&2));
}

/// The class of bug, not the two examples: on a generated corpus, change the vocabulary so
/// that many stored queries are rewritten to names nothing had interned, insert queries that
/// use those names, and compare the engine with a brute-force evaluation of every query.
#[test]
fn queries_inserted_after_a_vocabulary_change_leave_the_engine_exact() {
    let cfg = GenConfig {
        num_queries: 4_000,
        num_titles: 600,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1D5_0135,
        num_entities: 400,
        num_collections: 200,
    };
    let data = generate(&cfg);
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&data.queries);

    // Synonyms and phrases whose canonical names are new to the dictionary.
    let mut vocab = Vocab::new();
    for (token, canonical) in [
        ("wireless", "term:zzcordfree"),
        ("portable", "term:zzcarryable"),
        ("pro", "term:zzprofessional"),
        ("acme", "term:zzacmecorp"),
    ] {
        vocab.add_synonym(token, canonical, FeatureKind::Generic);
    }
    vocab.add_phrase(&["north", "star"], "term:zznorthstar", FeatureKind::Generic);
    vocab.add_phrase(&["blue", "peak"], "term:zzbluepeak", FeatureKind::Generic);
    for canonical in ["term:zzcordfree", "term:zznorthstar"] {
        assert!(
            eng.dict().get(canonical).is_none(),
            "precondition: {canonical} is new"
        );
    }
    apply(&mut eng, vocab.clone());

    // Later inserts that name the canonicals outright (each canonical is `term:<word>`).
    let mut queries = data.queries.clone();
    let base = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    for (offset, text) in [
        "zzcordfree zzlamp",
        "zzcarryable zzlamp",
        "zzprofessional zzlamp",
        "zzacmecorp zzlamp",
        "zznorthstar zzlamp",
        "zzbluepeak zzlamp",
    ]
    .into_iter()
    .enumerate()
    {
        let id = base + offset as u64;
        eng.try_insert_live(text, id, 1).expect("insert");
        queries.push((id, text.to_string()));
    }

    let brute = Brute::build_with_vocab(&queries, &vocab);
    let mut titles = data.titles.clone();
    titles.extend(
        [
            "wireless zzlamp",
            "portable zzlamp",
            "pro zzlamp",
            "acme zzlamp",
            "north star zzlamp",
            "blue peak zzlamp",
        ]
        .map(String::from),
    );
    let (mut lc, mut feats) = (String::new(), Vec::new());
    let mut truth_total = 0usize;
    for title in &titles {
        let want = brute.matches(title, &mut lc, &mut feats);
        truth_total += want.len();
        assert_eq!(matched(&mut eng, title), want, "{title:?}");
    }
    assert!(
        truth_total > 500,
        "the corpus matched too little: {truth_total}"
    );
}

/// What the dictionary already held is untouched: ids, frequencies and the frozen top-64
/// mask of every existing feature (ADR-188).
#[test]
fn a_vocabulary_change_leaves_existing_features_as_they_were() {
    let cfg = GenConfig {
        num_queries: 2_000,
        num_titles: 1,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 7,
        num_entities: 200,
        num_collections: 100,
    };
    let data = generate(&cfg);
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&data.queries);
    let before: Vec<(String, u32, u8)> = (0..eng.dict().len() as u32)
        .map(|id| {
            (
                eng.dict().name(id).to_string(),
                eng.dict().freq(id),
                eng.dict().mask_bit(id),
            )
        })
        .collect();

    let mut vocab = Vocab::new();
    vocab.add_synonym("wireless", "term:zzcordfree", FeatureKind::Generic);
    vocab.add_phrase(&["north", "star"], "term:zznorthstar", FeatureKind::Generic);
    apply(&mut eng, vocab);

    for (id, (name, freq, mask)) in before.iter().enumerate() {
        let id = id as u32;
        assert_eq!(eng.dict().name(id), name);
        assert_eq!(eng.dict().freq(id), *freq, "{name}");
        assert_eq!(eng.dict().mask_bit(id), *mask, "{name}");
    }
    // The names the change introduced are in the dictionary, with no mask bit.
    for canonical in ["term:zzcordfree", "term:zznorthstar"] {
        let id = eng.dict().get(canonical).expect("interned by the change");
        assert!(eng.dict().freq(id) > 0, "{canonical} counts its queries");
        assert!(
            !reverse_rusty::compile::is_hot(eng.dict(), id),
            "{canonical}"
        );
    }
}

/// A restart between the vocabulary change and the insert changes nothing: the recompiled
/// segment and the dictionary that names its features are committed together.
#[test]
fn the_ids_survive_a_reopen_between_the_change_and_the_insert() {
    let dir = std::env::temp_dir().join(format!("rr_adr205_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.add_synonym("refurb", "term:refurbished", FeatureKind::Generic);
    vocab.add_phrase(&["north", "star"], "term:northstar", FeatureKind::Generic);
    {
        let mut eng =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        eng.build_from_queries(&[
            (1, "refurb widget".to_string()),
            (2, "north star lamp".to_string()),
        ]);
        apply(&mut eng, vocab.clone());
        assert!(matched(&mut eng, "refurb widget").contains(&1));
    }

    let mut reopened = Engine::open_with_vocab(vocab, config).expect("reopen");
    reopened
        .try_insert_live("refurbished gadget", 3, 1)
        .expect("insert");
    reopened
        .try_insert_live("northstar shade", 4, 1)
        .expect("insert");
    assert!(matched(&mut reopened, "refurb widget").contains(&1));
    assert!(matched(&mut reopened, "north star lamp").contains(&2));
    assert!(matched(&mut reopened, "refurb gadget").contains(&3));
    assert!(matched(&mut reopened, "north star shade").contains(&4));
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}
