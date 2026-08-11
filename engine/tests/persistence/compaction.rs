//! Compaction over mmap'd segments and the manual-flush → mmap-segment path.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};

#[test]
fn compaction_with_mmap_segments() {
    // Multiple flushes create mmap'd segments, then compact and verify.
    let dir = test_dir("compact_mmap");
    let norm = make_norm();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    let mut engine = Engine::with_config(norm, config);

    // Build base segment
    let q1: Vec<(u64, String)> = vec![
        (1, "wireless mouse 1986 vertex".into()),
        (2, "mechanical keyboard new".into()),
    ];
    engine.build_from_queries(&q1);

    // Bulk ingest a second segment
    let q2: Vec<(u64, String)> = vec![
        (3, "noise cancelling headphones pro".into()),
        (4, "air purifier 2011 acme update".into()),
    ];
    engine.bulk_ingest(&q2);

    assert_eq!(engine.num_segments(), 3); // 2 base + memtable

    // Record matches before compaction
    let title = "1986 Vertex Wireless Mouse New Item PRO";
    let before = match_ids(&engine, title);

    // Compact. Assert the result instead of discarding it: with two base segments
    // this merge is contract-guaranteed, so `None` means a durability write (the
    // merged segment or the manifest commit) failed and emitted a `DurabilityFailure`
    // event. Surfacing that here turns a swallowed I/O error into a clear failure
    // rather than the misleading "segment count" mismatch it used to cause under load.
    let report = engine.compact_all().expect(
        "compaction must merge the 2 base segments; None ⇒ a DurabilityFailure during \
         the segment/manifest write",
    );
    assert_eq!(report.segments_merged, 2);
    assert_eq!(engine.num_segments(), 2); // 1 base + memtable

    // Verify matches unchanged
    let after = match_ids(&engine, title);
    assert_eq!(before, after, "compaction changed match results");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flush_creates_mmap_segment() {
    let dir = test_dir("flush_mmap");
    let norm = make_norm();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };

    let mut engine = Engine::with_config(norm, config);
    engine.insert_live("wireless mouse 1986 vertex", 1, 1);
    engine.insert_live("mechanical keyboard new", 2, 1);

    // Manually flush
    engine.flush();
    assert_eq!(engine.num_segments(), 2); // 1 base (mmap'd) + memtable

    // Check that a .seg file exists
    let seg_dir = dir.join("segments");
    let seg_files: Vec<_> = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
        .collect();
    assert!(!seg_files.is_empty(), "no .seg file created after flush");

    // Verify matching still works
    let title = "1986 Vertex Wireless Mouse New Item";
    let ids = match_ids(&engine, title);
    assert!(!ids.is_empty(), "no matches after flush to mmap");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tag_summaries_rebuild_conservatively_across_compaction_and_reopen() {
    let dir = test_dir("tag_summary_compaction");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        tag_segment_skipping: true,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    let rows = |base: u64| {
        vec![
            (base + 1, "acme chrome".to_string()),
            (base + 2, "acme chrome".to_string()),
        ]
    };
    let tags = |category: &str| {
        vec![
            vec![("category".to_string(), category.to_string())],
            vec![("category".to_string(), category.to_string())],
        ]
    };
    engine
        .try_build_from_queries_with_tags(&rows(0), &tags("items"))
        .expect("items segment");
    engine
        .try_bulk_ingest_detailed_with_tags(&rows(10), &tags("coins"))
        .expect("coins segment");

    let title = "2020 acme chrome update";
    let filter = vec![("category".to_string(), vec!["items".to_string()])];
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let before = engine.snapshot();
    let pred = before.compile_tag_predicate(&filter);
    let before_stats = before.match_title_filtered(title, &mut scratch, &mut out, true, &pred);
    let mut expected = out.clone();
    expected.sort_unstable();
    assert_eq!(expected, vec![1, 2]);
    assert_eq!(before_stats.tag_segments_skipped, 1);
    drop(before);

    engine.compact_all().expect("compact tagged segments");
    let compacted = engine.snapshot();
    let pred = compacted.compile_tag_predicate(&filter);
    let compacted_stats =
        compacted.match_title_filtered(title, &mut scratch, &mut out, true, &pred);
    let mut got = out.clone();
    got.sort_unstable();
    assert_eq!(got, expected, "compaction changed filtered results");
    assert_eq!(
        compacted_stats.tag_segments_skipped, 0,
        "the merged union contains both categories and must fail open for items"
    );
    let unknown = compacted
        .compile_tag_predicate(&[("category".to_string(), vec!["never-ingested".to_string()])]);
    let unknown_stats =
        compacted.match_title_filtered(title, &mut scratch, &mut out, true, &unknown);
    assert!(out.is_empty());
    assert_eq!(unknown_stats.tag_segments_skipped, 1);
    drop(compacted);
    drop(engine);

    let reopened = Engine::open(make_norm(), config).expect("reopen compacted tagged segment");
    let snap = reopened.snapshot();
    let pred = snap.compile_tag_predicate(&filter);
    snap.match_title_filtered(title, &mut scratch, &mut out, true, &pred);
    let mut got = out.clone();
    got.sort_unstable();
    assert_eq!(got, expected);
    assert!(snap.metrics().tag_summary_bytes > 0);

    let _ = std::fs::remove_dir_all(&dir);
}
