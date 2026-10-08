//! An upsert's delete half replays even when a commit already holds the upsert's row
//! (ADR-221). Found by the single-node crash matrix.

use crate::config::EngineConfig;
use crate::fault::{Scope, Step};
use crate::normalize::Normalizer;
use crate::segment::{Engine, MatchScratch};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocabulary")
}

fn matches(engine: &Engine, title: &str) -> Vec<u64> {
    let (mut scratch, mut out) = (MatchScratch::new(), Vec::new());
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

/// A flush that cannot write its segment seals the memtable in memory and keeps the log. A
/// compaction then merges that segment, drops the rows that were replaced, and commits,
/// and the log still holds every record. A restart replays the record of a row the merge
/// dropped, because no segment holds it, and must take it away again when it reaches the
/// record that replaced it, although a segment holds that one.
#[test]
fn a_replaced_query_stays_replaced_when_a_merge_dropped_its_row_and_the_log_holds_both_records() {
    let dir = std::env::temp_dir().join(format!("rr_replayed_upsert_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let scope = Scope::open(&dir);
    let mut engine = Engine::with_config(norm(), config.clone());
    engine
        .try_bulk_ingest(&[(1, "package adapter".to_string())])
        .expect("bulk load");
    engine
        .try_insert_live("vintage lamp", 2, 1)
        .expect("insert");
    engine.flush();
    // An insert that an upsert replaces, and an upsert that an upsert replaces.
    engine
        .try_insert_live("copper kettle", 8, 1)
        .expect("insert");
    engine
        .try_upsert_live("silver spoon", 8, 2)
        .expect("upsert");
    engine
        .try_upsert_live("brass compass", 9, 1)
        .expect("upsert");
    engine
        .try_upsert_live("brass sextant", 9, 2)
        .expect("upsert");

    scope.fail(
        &Step {
            name: "create",
            path: "segments/seg_000003.seg.tmp".to_string(),
        },
        0,
    );
    engine.flush();
    assert!(scope.failed().is_some(), "the flush's segment write failed");
    engine.compact_all();
    let manifest = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_eq!(manifest.segment_files.len(), 1, "one merged segment");
    assert!(
        manifest.wal_seq_watermark >= 4,
        "the commit covers the four records"
    );
    let held = crate::wal::Wal::recover(&dir.join("wal.log")).expect("the log");
    assert_eq!(held.entries.len(), 4, "the log still holds them");
    let answers = |engine: &Engine, when: &str| {
        assert_eq!(matches(engine, "copper kettle"), [0u64; 0], "{when}");
        assert_eq!(matches(engine, "silver spoon"), [8], "{when}");
        assert_eq!(matches(engine, "brass compass"), [0u64; 0], "{when}");
        assert_eq!(matches(engine, "brass sextant"), [9], "{when}");
        assert_eq!(matches(engine, "vintage lamp"), [2], "{when}");
    };
    answers(&engine, "before the restart");
    drop(engine);

    let reopened = Engine::open(norm(), config).expect("reopen");
    answers(&reopened, "after the restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}
