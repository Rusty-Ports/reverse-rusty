use super::*;

#[test]
fn batch_equals_scalar_under_tag_filter_including_materialized_pure_anchors() {
    // A high broad fraction so the columnar broad lane (and its pure-anchor
    // materialization fast path) is well exercised.
    let data = gen(0x00F1_17E5, 24_000, 2_500, 0.18);
    let eng = build_single_tagged(&data);

    let filters: [Vec<(String, Vec<String>)>; 3] = [
        vec![("category".to_string(), vec!["items".to_string()])],
        vec![(
            "category".to_string(),
            vec!["items".to_string(), "coins".to_string()],
        )],
        // a value never ingested ⇒ ∅ on both paths
        vec![("category".to_string(), vec!["nonexistent".to_string()])],
    ];

    let mut saw_nonempty = false;
    for filter in &filters {
        // `materialize` on AND off — `true` drives the pure-anchor fast path that the
        // Step-5 fix had to teach to honor the filter.
        for &materialize in &[true, false] {
            let scalar = scalar_filtered(&eng, &data.titles, filter);
            let batch = batch_filtered(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad: true,
                    broad_batch_size: 128,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: materialize,
                    broad_prefilter: true,
                },
                filter,
            );
            assert_eq!(
                scalar, batch,
                "batch ≠ scalar under filter {filter:?} (materialize={materialize})"
            );
            if scalar.iter().any(|r| !r.is_empty()) {
                saw_nonempty = true;
            }
        }
    }
    assert!(saw_nonempty, "degenerate: no filter matched anything");
}

#[test]
fn columnar_batch_tag_summary_skip_equals_disabled_path() {
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            auto_compact_on_ingest: false,
            tag_segment_skipping: true,
            ..EngineConfig::default()
        },
    );
    for segment in 0..3usize {
        let category = ["items", "coins", "stamps"][segment];
        let queries: Vec<_> = (0..64u64)
            .map(|row| ((segment as u64 * 100) + row + 1, "wireless".to_string()))
            .collect();
        let tags: Vec<_> = queries
            .iter()
            .map(|_| vec![("category".to_string(), category.to_string())])
            .collect();
        if segment == 0 {
            eng.try_build_from_queries_with_tags(&queries, &tags)
                .expect("first tagged broad segment");
        } else {
            eng.try_bulk_ingest_detailed_with_tags(&queries, &tags)
                .expect("additional tagged broad segment");
        }
    }
    let titles: Vec<_> = (0..65)
        .map(|i| format!("wireless item number {i}"))
        .collect();
    let filter = vec![("category".to_string(), vec!["items".to_string()])];
    let options = BatchMatchOptions {
        include_broad: true,
        broad_batch_size: 64,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: true,
    };

    let on = eng.snapshot();
    let pred = on.compile_tag_predicate(&filter);
    let (mut on_rows, on_stats) =
        on.match_titles_batch_with_stats_filtered(&titles, options, &pred);
    on_rows.sort_by_key(|(index, _)| *index);
    assert!(on_stats.tag_segments_skipped > 0);

    let mut disabled = eng.config().clone();
    disabled.tag_segment_skipping = false;
    eng.set_config(disabled);
    let off = eng.snapshot();
    let pred = off.compile_tag_predicate(&filter);
    let (mut off_rows, off_stats) =
        off.match_titles_batch_with_stats_filtered(&titles, options, &pred);
    off_rows.sort_by_key(|(index, _)| *index);
    assert_eq!(on_rows, off_rows);
    assert_eq!(off_stats.tag_segments_skipped, 0);
    assert!(off_stats.broad_postings_scanned > on_stats.broad_postings_scanned);
}
