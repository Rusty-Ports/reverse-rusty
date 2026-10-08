//! Metamorphic set-identity: surface-only title edits must not move the match set.
//!
//! The identity perturbations (case, foldable diacritics, whitespace runs, Split-class
//! punctuation around tokens, end-appended junk) leave the title's feature set unchanged —
//! so the FULL corpus match set must be byte-identical, title by title. This is checked
//! under the default vocabulary and under one with phrases. Unlike the differential oracle, the
//! ground truth here is the engine's own answer on the clean twin: no shared-code
//! blindness, and a divergence in EITHER direction (lost match = FN, new match = FP)
//! fails loudly.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig, Rng};
use reverse_rusty::segment::MatchScratch;

#[test]
fn identity_perturbations_preserve_the_exact_match_set() {
    let cfg = GenConfig {
        num_queries: 25_000,
        num_titles: 2_500,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x3E7A_0001,
        num_entities: 2_000,
        num_collections: 800,
    };
    let data = generate(&cfg);
    let eng = engine_from(&data.queries);
    let mut s = MatchScratch::new();
    let mut rng = Rng::new(0x3E7A_0001);

    let mut total_matches = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        let baseline = matched(&eng, &mut s, title);
        total_matches += baseline.len();

        // Each op alone (rotating start point so all ops see all kinds of titles), then
        // every op composed.
        for op in [ti % IDENTITY_OPS, (ti + 2) % IDENTITY_OPS] {
            let p = identity_perturb(&mut rng, title, op);
            let out = matched(&eng, &mut s, &p);
            assert_eq!(
                out, baseline,
                "MATCH-SET DRIFT under identity op#{op}:\n  clean:     `{title}`\n  perturbed: `{p}`"
            );
        }
        let all = identity_perturb_all(&mut rng, title);
        let out = matched(&eng, &mut s, &all);
        assert_eq!(
            out, baseline,
            "MATCH-SET DRIFT under composed identity ops:\n  clean:     `{title}`\n  perturbed: `{all}`"
        );
    }
    assert!(
        total_matches > 1_000,
        "degenerate corpus: only {total_matches} baseline matches"
    );
}

/// The same property under a vocabulary with phrases (ADR-218). A phrase is its words as
/// consecutive tokens, so widening the gap between two words, or putting a comma or a
/// parenthesis in it, leaves the phrase where it was. It did not: the phrase was looked for
/// with exactly one space between its words, each separator had become a space of its own,
/// and `north,  star` carried the two words and not the brand, so every query for the brand
/// missed the title. The property above ran without phrases and could not see it, and the
/// differentials could not either, because their references read titles the same way.
#[test]
fn identity_perturbations_preserve_the_match_set_under_a_phrase_vocabulary() {
    let cfg = GenConfig {
        num_queries: 25_000,
        num_titles: 2_500,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x3E7A_0218,
        num_entities: 2_000,
        num_collections: 800,
    };
    let mut data = generate(&cfg);
    // The generated queries are long conjunctions, and few titles satisfy one, so on their
    // own they would hardly ever show whether a title still carries a phrase. These do: for
    // each multi-word brand, the brand as bare terms, as an any-of member, and negated.
    let first_phrase_query = 50_000_000u64;
    let mut next_id = first_phrase_query;
    for brand in reverse_rusty::gen::BRANDS
        .iter()
        .filter(|brand| brand.contains(' '))
    {
        let last_word = brand.rsplit(' ').next().expect("a word");
        for query in [
            (*brand).to_string(),
            format!("({brand},zznobrand)"),
            format!("{last_word} -zznothing"),
        ] {
            data.queries.push((next_id, query));
            next_id += 1;
        }
    }
    let eng = engine_with_phrases_from(&data.queries);
    let mut s = MatchScratch::new();
    let mut rng = Rng::new(0x3E7A_0218);

    let (mut total_matches, mut phrase_matches) = (0usize, 0usize);
    for (ti, title) in data.titles.iter().enumerate() {
        let baseline = matched(&eng, &mut s, title);
        total_matches += baseline.len();
        phrase_matches += baseline
            .iter()
            .filter(|id| **id >= first_phrase_query)
            .count();
        for op in [ti % IDENTITY_OPS, (ti + 2) % IDENTITY_OPS] {
            let p = identity_perturb(&mut rng, title, op);
            assert_eq!(
                matched(&eng, &mut s, &p),
                baseline,
                "MATCH-SET DRIFT under identity op#{op}:\n  clean:     `{title}`\n  perturbed: `{p}`"
            );
        }
        let all = identity_perturb_all(&mut rng, title);
        assert_eq!(
            matched(&eng, &mut s, &all),
            baseline,
            "MATCH-SET DRIFT under composed identity ops:\n  clean:     `{title}`\n  perturbed: `{all}`"
        );
    }
    assert!(
        total_matches > 1_000 && phrase_matches > 500,
        "degenerate corpus: {total_matches} baseline matches, {phrase_matches} of them on a \
         query for a phrase"
    );
}
