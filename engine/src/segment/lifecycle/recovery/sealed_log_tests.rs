//! ADR-223: a manifest says how far the log is sealed into its segments. Recovery skips the
//! records at or below that position whole and rebuilds the memtable from the rest in order,
//! so a position in the memtable names after a restart the row it named before.

use crate::config::EngineConfig;
use crate::fault::{Scope, Step};
use crate::normalize::Normalizer;
use crate::segment::{Engine, InsertOutcome, MatchScratch};
use std::path::{Path, PathBuf};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocabulary")
}

fn config(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        ..EngineConfig::default()
    }
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_sealed_log_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create");
    dir
}

fn matches(engine: &Engine, title: &str) -> Vec<u64> {
    let (mut scratch, mut out) = (MatchScratch::new(), Vec::new());
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

/// Insert and return the row's position in the memtable.
fn insert(engine: &mut Engine, text: &str, id: u64) -> u32 {
    match engine.try_insert_live(text, id, 1).expect("insert") {
        InsertOutcome::Inserted(stored) => stored.local,
        InsertOutcome::RejectedClassD => panic!("{text} was rejected"),
    }
}

fn manifest(dir: &Path) -> crate::storage::Manifest {
    crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest")
}

/// The sequence number of the last record in the log.
fn last_record(dir: &Path) -> u64 {
    let held = crate::wal::Wal::recover(&dir.join("wal.log")).expect("the log");
    held.entries.last().expect("a record").seq()
}

/// One segment on disk, and in the log an insert that an upsert replaced.
fn with_a_replaced_row_in_the_log(dir: &Path) -> Engine {
    let mut engine = Engine::with_config(norm(), config(dir));
    engine
        .try_bulk_ingest(&[(1, "package adapter".to_string())])
        .expect("bulk load");
    insert(&mut engine, "vintage lamp", 2);
    engine.flush();
    insert(&mut engine, "copper kettle", 8);
    engine
        .try_upsert_live("silver spoon", 8, 2)
        .expect("upsert");
    engine
}

/// The defect (RR-155). A flush that cannot write its segment seals the memtable in memory
/// and keeps the log; a merge then commits everything and drops the row that was replaced.
/// A delete by memtable position written after that named another row at replay, because
/// the dropped row's record was replayed ahead of the rows written since: a live query was
/// deleted and the deleted one came back.
#[test]
fn a_delete_by_memtable_position_names_the_same_row_after_a_restart() {
    let dir = scratch_dir("position");
    let scope = Scope::open(&dir);
    let mut engine = with_a_replaced_row_in_the_log(&dir);
    let sealed = last_record(&dir);
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
    let committed = manifest(&dir);
    assert_eq!(committed.segment_files.len(), 1, "one merged segment");
    assert_eq!(
        committed.wal_sealed_through,
        Some(sealed),
        "the flush sealed the log's records, and the merge's commit says so"
    );

    let kept = insert(&mut engine, "brass compass", 20);
    let deleted = insert(&mut engine, "brass sextant", 21);
    assert_eq!((kept, deleted), (0, 1), "a fresh memtable");
    engine.tombstone(deleted).expect("a delete by position");
    let answers = |engine: &Engine, when: &str| {
        assert_eq!(matches(engine, "brass compass"), [20], "{when}");
        assert_eq!(matches(engine, "brass sextant"), [0u64; 0], "{when}");
        assert_eq!(matches(engine, "silver spoon"), [8], "{when}");
        assert_eq!(matches(engine, "copper kettle"), [0u64; 0], "{when}");
    };
    answers(&engine, "before the restart");
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    answers(&reopened, "after the restart");
    assert_eq!(reopened.memtable.len(), 2, "the two rows written since");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crash between a flush's manifest rename and what it then does to the log leaves the
/// new manifest and the log as it was. Every record is at or below the position the
/// manifest sealed through, so none is replayed, a delete by memtable position among them
/// included, and the next record is numbered above it.
#[test]
fn records_at_or_below_the_sealed_position_are_skipped_whole() {
    let dir = scratch_dir("skipped");
    let mut engine = with_a_replaced_row_in_the_log(&dir);
    let gone = insert(&mut engine, "brass sextant", 21);
    engine.tombstone(gone).expect("a delete by position");
    let sealed = last_record(&dir);
    let log_before_the_flush = std::fs::read(dir.join("wal.log")).expect("the log");
    engine.flush();
    assert_eq!(manifest(&dir).wal_sealed_through, Some(sealed));
    drop(engine);
    std::fs::write(dir.join("wal.log"), &log_before_the_flush).expect("the crash");
    assert_eq!(
        last_record(&dir),
        sealed,
        "the log holds its four records again"
    );

    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(reopened.memtable.len(), 0, "nothing was replayed");
    assert_eq!(matches(&reopened, "silver spoon"), [8]);
    assert_eq!(matches(&reopened, "copper kettle"), [0u64; 0]);
    assert_eq!(matches(&reopened, "brass sextant"), [0u64; 0]);
    insert(&mut reopened, "brass compass", 20);
    drop(reopened);
    let again = Engine::open(norm(), config(&dir)).expect("reopen again");
    assert_eq!(
        matches(&again, "brass compass"),
        [20],
        "a record written after the restart is numbered above the sealed position"
    );
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A merge and a bulk load commit without sealing the memtable: the watermark moves on and
/// the sealed position stays. The records between the two rebuild the memtable, a delete by
/// position before the commit and one after it included.
#[test]
fn a_commit_that_does_not_seal_leaves_the_position_and_the_memtable_is_rebuilt() {
    let dir = scratch_dir("unsealed");
    let mut engine = with_a_replaced_row_in_the_log(&dir);
    let early = insert(&mut engine, "brass sextant", 21);
    engine.tombstone(early).expect("a delete by position");
    let sealed = manifest(&dir).wal_sealed_through;
    assert!(
        sealed.is_some_and(|sealed| sealed < last_record(&dir)),
        "the seed's flush sealed what came before these records: {sealed:?}"
    );

    engine.compact_all();
    engine
        .try_bulk_ingest(&[(30, "antique clock".to_string())])
        .expect("bulk load");
    let committed = manifest(&dir);
    assert_eq!(committed.wal_sealed_through, sealed, "no seal, no move");
    assert_eq!(
        committed.wal_seq_watermark,
        last_record(&dir),
        "the watermark moved on"
    );

    insert(&mut engine, "brass compass", 20);
    let late = insert(&mut engine, "copper kettle", 22);
    engine.tombstone(late).expect("a delete by position");
    let rows = engine.memtable.len();
    let answers = |engine: &Engine, when: &str| {
        assert_eq!(matches(engine, "silver spoon"), [8], "{when}");
        assert_eq!(matches(engine, "brass sextant"), [0u64; 0], "{when}");
        assert_eq!(matches(engine, "brass compass"), [20], "{when}");
        assert_eq!(matches(engine, "copper kettle"), [0u64; 0], "{when}");
        assert_eq!(matches(engine, "antique clock"), [30], "{when}");
        assert_eq!(matches(engine, "vintage lamp"), [2], "{when}");
    };
    answers(&engine, "before the restart");
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    answers(&reopened, "after the restart");
    assert_eq!(reopened.memtable.len(), rows, "row for row");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A manifest written before ADR-223 does not say how far it sealed. It is recovered by the
/// rule it was written under, commits that do not seal keep writing that layout, and the
/// first seal records the position.
#[test]
fn a_manifest_without_the_position_is_recovered_as_before_and_gains_it_at_the_first_seal() {
    let dir = scratch_dir("before");
    let engine = with_a_replaced_row_in_the_log(&dir);
    drop(engine);
    let mut earlier = manifest(&dir);
    assert!(earlier.wal_sealed_through.is_some());
    earlier.wal_sealed_through = None;
    crate::storage::write_manifest(&earlier, &dir.join("manifest.bin")).expect("an earlier layout");

    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(reopened.sealed_through, None);
    assert_eq!(matches(&reopened, "silver spoon"), [8]);
    assert_eq!(matches(&reopened, "copper kettle"), [0u64; 0]);
    assert_eq!(matches(&reopened, "vintage lamp"), [2]);
    reopened
        .try_bulk_ingest(&[(30, "antique clock".to_string())])
        .expect("bulk load");
    assert_eq!(
        manifest(&dir).wal_sealed_through,
        None,
        "a commit that does not seal cannot say how far the log is sealed"
    );

    let last = last_record(&dir);
    reopened.flush();
    assert_eq!(
        manifest(&dir).wal_sealed_through,
        Some(last),
        "the flush sealed every record the log held"
    );
    drop(reopened);
    let again = Engine::open(norm(), config(&dir)).expect("reopen again");
    assert_eq!(matches(&again, "silver spoon"), [8]);
    assert_eq!(matches(&again, "antique clock"), [30]);
    assert_eq!(matches(&again, "copper kettle"), [0u64; 0]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A deleted query has no source text after a restart, in a directory of either layout.
///
/// Found in review of the first fix for a replaced row that came back: with a manifest
/// that does not say how far it sealed, the insert that the upsert replaced is replayed,
/// source text and all. Taking its row away again left the text, and the delete that
/// follows found no row to delete and so did not remove it.
#[test]
fn a_deleted_query_has_no_source_text_after_a_restart() {
    for layout in ["sealed position recorded", "written before it"] {
        let dir = scratch_dir(if layout.starts_with("sealed") {
            "source_v9"
        } else {
            "source_v8"
        });
        let scope = Scope::open(&dir);
        let mut engine = with_a_replaced_row_in_the_log(&dir);
        scope.fail(
            &Step {
                name: "create",
                path: "segments/seg_000003.seg.tmp".to_string(),
            },
            0,
        );
        engine.flush();
        assert!(scope.failed().is_some(), "{layout}");
        engine.compact_all();
        engine.delete_by_logical_id(8).expect("delete");
        // A commit past the delete that leaves the deleted row in its segment: no merge
        // follows this bulk load.
        let mut no_merge = (*engine.config).clone();
        no_merge.auto_compact_on_ingest = false;
        engine.config = std::sync::Arc::new(no_merge);
        engine
            .try_bulk_ingest(&[(30, "antique clock".to_string())])
            .expect("bulk load");
        assert!(
            engine
                .segments
                .iter()
                .any(|segment| !segment.locals_for_logical(8).is_empty()),
            "{layout}: the deleted row is still in a segment"
        );
        assert_eq!(engine.get_query_source(8), None, "{layout}");
        drop(engine);
        if layout.starts_with("written") {
            let mut earlier = manifest(&dir);
            earlier.wal_sealed_through = None;
            crate::storage::write_manifest(&earlier, &dir.join("manifest.bin"))
                .expect("an earlier layout");
        }

        let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
        assert_eq!(matches(&reopened, "silver spoon"), [0u64; 0], "{layout}");
        assert_eq!(matches(&reopened, "copper kettle"), [0u64; 0], "{layout}");
        assert_eq!(matches(&reopened, "antique clock"), [30], "{layout}");
        assert_eq!(
            reopened.get_query_source(8),
            None,
            "{layout}: the source text of a deleted query"
        );
        drop(reopened);
        drop(scope);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A vocabulary rebuild compiles every row, the memtable's included, into one segment. That
/// is a seal: its commit says the log is sealed through the last record.
#[test]
fn a_vocabulary_rebuild_seals_the_memtable() {
    let dir = scratch_dir("rebuild");
    let mut engine = with_a_replaced_row_in_the_log(&dir);
    let sealed = last_record(&dir);
    let log_before_the_rebuild = std::fs::read(dir.join("wal.log")).expect("the log");
    engine
        .set_vocab(crate::vocab::Vocab::default())
        .expect("a vocabulary");
    assert!(engine.recompile_stale_segments() > 0, "the rebuild ran");
    assert_eq!(manifest(&dir).wal_sealed_through, Some(sealed));
    drop(engine);
    // A crash between the rebuild's manifest rename and what it then does to the log.
    std::fs::write(dir.join("wal.log"), &log_before_the_rebuild).expect("the crash");

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(reopened.memtable.len(), 0, "nothing was replayed");
    assert_eq!(matches(&reopened, "silver spoon"), [8]);
    assert_eq!(matches(&reopened, "copper kettle"), [0u64; 0]);
    assert_eq!(matches(&reopened, "vintage lamp"), [2]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The seal before a first bulk load puts the memtable back when its commit fails. The
/// position goes back with it: the rows are in the memtable again, and a later commit must
/// not say they are sealed.
#[test]
fn a_seal_that_is_undone_leaves_the_position_where_it_was() {
    let dir = scratch_dir("undone");
    let scope = Scope::open(&dir);
    let mut engine = Engine::with_config(norm(), config(&dir));
    insert(&mut engine, "copper kettle", 8);
    insert(&mut engine, "vintage lamp", 2);
    scope.fail(
        &Step {
            name: "rename",
            path: "manifest.bin".to_string(),
        },
        0,
    );
    assert!(engine
        .try_bulk_ingest(&[(1, "package adapter".to_string())])
        .is_err());
    assert!(scope.failed().is_some(), "the seal's commit failed");
    assert_eq!(
        engine.memtable.len(),
        2,
        "the rows are back in the memtable"
    );
    assert_eq!(engine.sealed_through, Some(0), "and nothing is sealed");
    drop(engine);

    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(matches(&reopened, "copper kettle"), [8]);
    assert_eq!(matches(&reopened, "vintage lamp"), [2]);
    let last = last_record(&dir);
    reopened.flush();
    assert_eq!(manifest(&dir).wal_sealed_through, Some(last));
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The watermark can be below the sealed position: a commit that keeps an older watermark
/// (a vocabulary-only commit) can follow a seal. When the log is then empty, the next record
/// is still numbered above the sealed position, or the next recovery would skip it.
#[test]
fn the_next_record_is_numbered_above_the_sealed_position_when_the_watermark_is_below_it() {
    let dir = scratch_dir("numbering");
    let mut engine = with_a_replaced_row_in_the_log(&dir);
    engine.flush();
    drop(engine);
    let mut below = manifest(&dir);
    let sealed = below.wal_sealed_through.expect("the flush sealed");
    assert!(sealed > 0);
    below.wal_seq_watermark = 0;
    crate::storage::write_manifest(&below, &dir.join("manifest.bin")).expect("manifest");

    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    insert(&mut reopened, "brass compass", 20);
    assert!(
        last_record(&dir) > sealed,
        "numbered above the sealed position"
    );
    drop(reopened);
    let again = Engine::open(norm(), config(&dir)).expect("reopen again");
    assert_eq!(matches(&again, "brass compass"), [20]);
    assert_eq!(matches(&again, "silver spoon"), [8]);
    let _ = std::fs::remove_dir_all(&dir);
}
