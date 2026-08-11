//! Filtered percolation (ADR-049) differential oracle.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

const CATEGORIES: [&str; 6] = ["items", "coins", "stamps", "comics", "toys", "art"];
const STATUSES: [&str; 3] = ["active", "inactive", "archived"];

/// Deterministic per-query tags, a pure function of the logical id so the engine and the
/// brute reference assign identical metadata with no shared state.
fn tags_for(logical: u64) -> Vec<(String, String)> {
    let cat = CATEGORIES[(logical % CATEGORIES.len() as u64) as usize];
    let status = STATUSES[((logical / 7) % STATUSES.len() as u64) as usize];
    vec![
        ("category".to_string(), cat.to_string()),
        ("status".to_string(), status.to_string()),
    ]
}

/// Reference filter semantics: AND across keys, OR within a key's value set.
fn passes_filter(qtags: &[(String, String)], filter: &[(String, Vec<String>)]) -> bool {
    filter.iter().all(|(k, vals)| {
        qtags
            .iter()
            .any(|(qk, qv)| qk == k && vals.iter().any(|v| v == qv))
    })
}

/// A small deterministic sweep of filters keyed off `i` — single category (the dominant
/// production pattern), a two-value category set, category+status, and a category value
/// that was never ingested (must return ∅).
fn filters_for(i: usize) -> Vec<Vec<(String, Vec<String>)>> {
    let c1 = CATEGORIES[i % CATEGORIES.len()].to_string();
    let c2 = CATEGORIES[(i + 1) % CATEGORIES.len()].to_string();
    let st = STATUSES[i % STATUSES.len()].to_string();
    vec![
        vec![("category".to_string(), vec![c1.clone()])],
        vec![("category".to_string(), vec![c1.clone(), c2])],
        vec![
            ("category".to_string(), vec![c1]),
            ("status".to_string(), vec![st]),
        ],
        vec![("category".to_string(), vec!["never-ingested".to_string()])],
    ]
}

#[test]
fn filtered_percolation_matches_oracle_and_only_removes() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0049_0049,
        num_entities: 2_500,
        num_collections: 1_000,
    };
    let data = generate(&cfg);

    // engine, built WITH per-query tags (parallel to data.queries)
    let tags: Vec<Vec<(String, String)>> = data.queries.iter().map(|(l, _)| tags_for(*l)).collect();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("default vocabulary"));
    eng.try_build_from_queries_with_tags(&data.queries, &tags)
        .expect("tagged build");
    let snap = eng.snapshot();

    let brute = Brute::build(&data.queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut checked = 0usize;
    let mut nonempty_filtered = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        // unfiltered baseline (engine + truth)
        let unfiltered: HashSet<u64> = {
            snap.match_title(title, &mut s, &mut out, true);
            out.iter().copied().collect()
        };
        let truth = brute.matches(title, &mut blc, &mut bfeats);

        for filter in filters_for(ti) {
            let pred = snap.compile_tag_predicate(&filter);
            snap.match_title_filtered(title, &mut s, &mut out, true, &pred);
            let engine_filtered: HashSet<u64> = out.iter().copied().collect();

            // reference = brute matches that also satisfy the tag filter
            let brute_filtered: HashSet<u64> = truth
                .iter()
                .copied()
                .filter(|l| passes_filter(&tags_for(*l), &filter))
                .collect();

            assert_eq!(
                engine_filtered, brute_filtered,
                "filtered set diverged from oracle (title {ti}, filter {filter:?})"
            );

            // monotonicity: filtering only ever REMOVES, never adds or drops a wanted
            // in-scope match. Every removed id must itself fail the filter.
            assert!(
                engine_filtered.is_subset(&unfiltered),
                "filter added a match not in the unfiltered set"
            );
            for removed in unfiltered.difference(&engine_filtered) {
                assert!(
                    !passes_filter(&tags_for(*removed), &filter),
                    "filter removed id {removed} that actually satisfies it (false negative)"
                );
            }
            checked += 1;
            if !engine_filtered.is_empty() {
                nonempty_filtered += 1;
            }
        }
    }
    eprintln!("filtered oracle: {checked} (title,filter) pairs, {nonempty_filtered} non-empty");
    assert!(
        nonempty_filtered > 0,
        "degenerate: no filter ever matched anything"
    );
}

#[test]
fn tag_segment_skip_is_result_equivalent_and_avoids_irrelevant_segments() {
    let config = EngineConfig {
        auto_compact_on_ingest: false,
        tag_segment_skipping: true,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("default vocabulary"),
        config,
    );
    let categories = ["items", "coins", "stamps"];
    for (segment, category) in categories.iter().enumerate() {
        let base = segment as u64 * 10;
        let queries = vec![
            (base + 1, "acme chrome".to_string()),
            (base + 2, "acme chrome".to_string()),
        ];
        let tags = vec![
            vec![("category".to_string(), (*category).to_string())],
            vec![("category".to_string(), (*category).to_string())],
        ];
        if segment == 0 {
            eng.try_build_from_queries_with_tags(&queries, &tags)
                .expect("first tagged segment");
        } else {
            eng.try_bulk_ingest_detailed_with_tags(&queries, &tags)
                .expect("additional tagged segment");
        }
    }

    let filter = vec![("category".to_string(), vec!["items".to_string()])];
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let on = eng.snapshot();
    let pred = on.compile_tag_predicate(&filter);
    let on_stats = on.match_title_filtered(
        "2020 acme chrome update",
        &mut scratch,
        &mut out,
        true,
        &pred,
    );
    let mut on_ids = out.clone();
    on_ids.sort_unstable();
    assert_eq!(on_ids, vec![1, 2]);
    assert_eq!(on_stats.tag_segments_skipped, 2);
    assert!(on_stats.postings_scanned > 0);
    assert!(on.metrics().tag_summary_bytes > 0);

    let unknown =
        on.compile_tag_predicate(&[("category".to_string(), vec!["never-ingested".to_string()])]);
    let unknown_stats = on.match_title_filtered(
        "2020 acme chrome update",
        &mut scratch,
        &mut out,
        true,
        &unknown,
    );
    assert!(out.is_empty());
    assert_eq!(unknown_stats.tag_segments_skipped, 3);
    assert_eq!(unknown_stats.postings_scanned, 0);
    assert_eq!(unknown_stats.unique_candidates, 0);

    let mut disabled = eng.config().clone();
    disabled.tag_segment_skipping = false;
    eng.set_config(disabled);
    let off = eng.snapshot();
    let pred = off.compile_tag_predicate(&filter);
    let off_stats = off.match_title_filtered(
        "2020 acme chrome update",
        &mut scratch,
        &mut out,
        true,
        &pred,
    );
    let mut off_ids = out.clone();
    off_ids.sort_unstable();
    assert_eq!(off_ids, on_ids, "the kill switch may change only work");
    assert_eq!(off_stats.tag_segments_skipped, 0);
    assert!(off_stats.postings_scanned > on_stats.postings_scanned);
    assert!(off_stats.unique_candidates > on_stats.unique_candidates);
}
