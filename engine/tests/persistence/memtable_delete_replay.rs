//! A delete of a memtable row survives a manifest commit and a restart (ADR-066, amended).
//!
//! The memtable is rebuilt from the WAL tail alone. A manifest commit that does
//! not seal it (a compaction, a bulk ingest) still advances the WAL watermark,
//! and recovery used to skip every `DeleteByLogical` frame at or below it. The
//! insert frame replayed (its row is in no segment), the delete did not, and an
//! acknowledged delete came back.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use std::path::Path;

use crate::harness::{make_norm, match_ids, test_dir};

fn manual_cfg(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

/// An engine with two base segments, so `compact_all` has a merge to commit.
fn two_segments(dir: &Path) -> Engine {
    let mut engine = Engine::with_config(make_norm(), manual_cfg(dir));
    engine.build_from_queries(&[(1, "usb hub silver".to_string())]);
    engine.bulk_ingest(&[(2, "smart speaker premium".to_string())]);
    engine
}

/// The manifest commits that advance the watermark without sealing the memtable.
#[derive(Clone, Copy, Debug)]
enum Commit {
    Compaction,
    BulkIngest,
}

fn commit(engine: &mut Engine, kind: Commit) {
    match kind {
        Commit::Compaction => {
            engine.compact_all().expect("compaction ran");
        }
        Commit::BulkIngest => {
            let report = engine.bulk_ingest(&[(3, "air purifier compact".to_string())]);
            assert_eq!(report.ingested, 1);
        }
    }
}

const LAMP: &str = "desk lamp chrome";
const LAMP_TITLE: &str = "desk lamp chrome edition";

#[test]
fn a_memtable_delete_survives_a_commit_and_a_restart() {
    for kind in [Commit::Compaction, Commit::BulkIngest] {
        let dir = test_dir("memtable_delete_commit");
        {
            let mut engine = two_segments(&dir);
            engine.try_insert_live(LAMP, 10, 1).expect("insert"); // memtable only
            assert_eq!(engine.delete_by_logical_id(10).expect("delete"), 1);
            commit(&mut engine, kind); // watermark := last_seq, memtable not sealed
            assert!(!match_ids(&engine, LAMP_TITLE).contains(&10));
        }
        let engine = Engine::open(make_norm(), manual_cfg(&dir)).expect("reopen");
        assert!(
            !match_ids(&engine, LAMP_TITLE).contains(&10),
            "{kind:?}: an acknowledged delete came back after a restart"
        );
        assert_eq!(
            engine.snapshot().get_query_source(10),
            None,
            "{kind:?}: a deleted query has no source"
        );
        assert_eq!(
            match_ids(&engine, "usb hub silver cable"),
            vec![1],
            "{kind:?}"
        );
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// insert → delete → re-insert of one id, all in the memtable, then a commit and
/// a restart: exactly the re-inserted version is live.
#[test]
fn insert_delete_reinsert_keeps_only_the_newest_version() {
    for kind in [Commit::Compaction, Commit::BulkIngest] {
        let dir = test_dir("memtable_delete_reinsert");
        let live_before;
        {
            let mut engine = two_segments(&dir);
            engine.try_insert_live(LAMP, 10, 1).expect("insert");
            engine.delete_by_logical_id(10).expect("delete");
            engine
                .try_insert_live("desk lamp brass", 10, 2)
                .expect("re-insert");
            commit(&mut engine, kind);
            live_before = engine.num_live_queries();
        }
        let engine = Engine::open(make_norm(), manual_cfg(&dir)).expect("reopen");
        assert!(
            !match_ids(&engine, LAMP_TITLE).contains(&10),
            "{kind:?}: the deleted version came back"
        );
        assert_eq!(
            match_ids(&engine, "desk lamp brass shade"),
            vec![10],
            "{kind:?}"
        );
        assert_eq!(engine.num_live_queries(), live_before, "{kind:?}");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The delete lands AFTER the commit, so its frame is above the watermark: the
/// ordinary path, pinned next to the cases above.
#[test]
fn a_memtable_delete_after_the_commit_survives_a_restart() {
    let dir = test_dir("memtable_delete_after_commit");
    {
        let mut engine = two_segments(&dir);
        engine.try_insert_live(LAMP, 10, 1).expect("insert");
        commit(&mut engine, Commit::Compaction);
        assert_eq!(engine.delete_by_logical_id(10).expect("delete"), 1);
    }
    let engine = Engine::open(make_norm(), manual_cfg(&dir)).expect("reopen");
    assert!(!match_ids(&engine, LAMP_TITLE).contains(&10));
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A memtable row is deleted and the same id is then bulk-ingested with new
/// text. The bulk copy lives in a base segment and must survive (the delete
/// frame is older than it), while the memtable copy stays deleted.
#[test]
fn a_bulk_reinsert_after_a_memtable_delete_is_the_only_live_copy() {
    let dir = test_dir("memtable_delete_bulk_reinsert");
    let live_before;
    {
        let mut engine = two_segments(&dir);
        engine.try_insert_live(LAMP, 10, 1).expect("insert");
        engine.delete_by_logical_id(10).expect("delete");
        let report = engine.bulk_ingest(&[(10, "desk lamp brass".to_string())]);
        assert_eq!(report.ingested, 1);
        live_before = engine.num_live_queries();
    }
    let engine = Engine::open(make_norm(), manual_cfg(&dir)).expect("reopen");
    assert_eq!(match_ids(&engine, "desk lamp brass shade"), vec![10]);
    assert!(
        !match_ids(&engine, LAMP_TITLE).contains(&10),
        "the deleted memtable copy came back beside its bulk replacement"
    );
    assert_eq!(engine.num_live_queries(), live_before);
    assert_eq!(
        engine.snapshot().get_query_source(10).as_deref(),
        Some("desk lamp brass"),
        "the replayed delete must not take the surviving copy's source text with it"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An upsert whose previous version is memtable-only, across a commit and a
/// restart: one live copy, the new version.
#[test]
fn an_upsert_over_a_memtable_row_keeps_one_live_copy() {
    for kind in [Commit::Compaction, Commit::BulkIngest] {
        let dir = test_dir("memtable_upsert_commit");
        let live_before;
        {
            let mut engine = two_segments(&dir);
            engine.try_insert_live(LAMP, 10, 1).expect("insert");
            engine
                .try_upsert_live("desk lamp brass", 10, 2)
                .expect("upsert");
            commit(&mut engine, kind);
            live_before = engine.num_live_queries();
        }
        let engine = Engine::open(make_norm(), manual_cfg(&dir)).expect("reopen");
        assert!(!match_ids(&engine, LAMP_TITLE).contains(&10), "{kind:?}");
        assert_eq!(
            match_ids(&engine, "desk lamp brass shade"),
            vec![10],
            "{kind:?}"
        );
        assert_eq!(engine.num_live_queries(), live_before, "{kind:?}");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
