use super::*;

#[test]
fn extracted_ingest_rejects_a_merged_tag_column_over_u16() {
    let mut engine =
        Engine::new(crate::normalize::Normalizer::default_vocab().expect("normalizer"));
    let seed = vec![(1, "1994 north star".to_string())];
    assert_eq!(engine.build_from_queries(&seed).ingested, 1);

    let ast = crate::dsl::parse("1994 north star").expect("parse");
    let mut lc = String::new();
    let ex = crate::compile::extract_readonly(&ast, &engine.norm, &engine.dict, &mut lc);
    let item = PlacedQuery {
        logical: 2,
        ex,
        dsl: "1994 north star".into(),
        version: 1,
        source_generation: None,
        tags: Vec::new(),
        // Nonempty carry-through bypasses the runtime max_tags check, but
        // the exact-store u16 count ceiling remains unconditional.
        tag_ids: (0..=u32::from(u16::MAX)).collect(),
        rank: crate::rank::RankValues::default(),
        placement: crate::ownership::QueryPlacement::standalone(),
    };
    let report = engine.ingest_extracted(&[item]);
    assert_eq!(report.ingested, 0);
    assert_eq!(report.rejected_parse, 1);
    assert!(
        !engine.snapshot().has_live_query(2),
        "a wrapping tag column must never reach the exact store"
    );
}

/// A replayed delete at or below the WAL watermark only tombstones memtable
/// copies, but it still drops the source text when that was the last live copy:
/// a deleted query must not leave its DSL behind in the source store.
#[test]
fn a_replayed_memtable_delete_drops_the_source_with_the_last_live_copy() {
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_replayed_delete_source_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let config = crate::config::EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..crate::config::EngineConfig::default()
    };
    let norm = || crate::normalize::Normalizer::default_vocab().expect("normalizer");
    {
        let mut engine = Engine::with_config(norm(), config.clone());
        engine.build_from_queries(&[(1, "usb hub silver".to_string())]);
        engine.bulk_ingest(&[(2, "smart speaker premium".to_string())]);
        engine
            .try_insert_live("desk lamp chrome", 10, 1)
            .expect("insert");
        assert_eq!(engine.delete_by_logical_id(10).expect("delete"), 1);
        engine.compact_all().expect("compaction ran"); // watermark passes both frames
    }
    let engine = Engine::open(norm(), config).expect("reopen");
    assert_eq!(engine.num_live_queries(), 2);
    assert!(engine.query_store.get_document(10).is_none());
    assert_eq!(engine.query_store.len(), 2);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
