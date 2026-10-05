//! No commit leaves an in-memory base segment behind (ADR-190).
//!
//! A flush or vocabulary rebuild whose segment write fails keeps serving its
//! rows from an in-memory segment, with the WAL as their durable copy
//! (ADR-051). A later commit lists only on-disk segments and then retires the
//! WAL, so it must first put that segment on disk, or not happen.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::SegmentKind;
use reverse_rusty::segment::Engine;

fn manual_config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

/// How many base segments are served from memory, and how many from disk.
fn base_segments(engine: &Engine) -> (usize, usize) {
    let infos = engine.segment_infos();
    let count = |kind: SegmentKind| infos.iter().filter(|info| info.kind == kind).count();
    (count(SegmentKind::Memory), count(SegmentKind::Mmap))
}

/// An engine with query 1 committed and query 2 stranded in an in-memory
/// segment by a flush that could not write its file.
fn engine_with_a_failed_flush(config: &EngineConfig, dir: &std::path::Path) -> Engine {
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&[(1, "usb hub silver".into())]);
    engine.insert_live("mechanical keyboard blue", 2, 1);
    let original = StorageFailure::SegmentWrite.block(dir);
    engine.flush();
    StorageFailure::SegmentWrite.unblock(dir, original);
    assert_eq!(
        base_segments(&engine),
        (1, 1),
        "precondition: the failed flush left query 2 in an in-memory segment"
    );
    assert_eq!(match_ids(&engine, "mechanical keyboard blue"), vec![2]);
    engine
}

/// The reported loss: a flush fails, the next one succeeds, and the restart
/// used to come back without the first flush's rows. They were in no manifest,
/// and the second flush had reset the WAL.
#[test]
fn a_later_flush_commits_what_a_failed_flush_left_in_memory() {
    let dir = test_dir("fallback_then_flush");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        engine.insert_live("desk lamp chrome", 3, 1);
        engine.flush();
        assert_eq!(
            base_segments(&engine),
            (0, 3),
            "the commit put the stranded segment on disk"
        );
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(match_ids(&engine, "usb hub silver"), vec![1]);
    assert_eq!(
        match_ids(&engine, "mechanical keyboard blue"),
        vec![2],
        "an acknowledged write survives a failed flush, a later flush and a restart"
    );
    assert_eq!(match_ids(&engine, "desk lamp chrome"), vec![3]);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every stranded segment is written, not just the first one found.
#[test]
fn every_stranded_segment_is_committed() {
    let dir = test_dir("fallback_two");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        engine.insert_live("desk lamp chrome", 3, 1);
        let original = StorageFailure::SegmentWrite.block(&dir);
        engine.flush();
        StorageFailure::SegmentWrite.unblock(&dir, original);
        assert_eq!(base_segments(&engine), (2, 1), "two failed flushes");

        engine.insert_live("air purifier white", 4, 1);
        engine.flush();
        assert_eq!(base_segments(&engine), (0, 4));
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    for (title, id) in [
        ("usb hub silver", 1),
        ("mechanical keyboard blue", 2),
        ("desk lamp chrome", 3),
        ("air purifier white", 4),
    ] {
        assert_eq!(match_ids(&engine, title), vec![id], "{title}");
    }
    assert_eq!(base_segments(&engine), (0, 4));
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A flush with nothing new to seal still finishes what an earlier one could
/// not, so an operator can make the engine durable again without a restart.
#[test]
fn an_empty_flush_commits_a_stranded_segment() {
    let dir = test_dir("fallback_empty_flush");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        engine.flush();
        assert_eq!(base_segments(&engine), (0, 2));
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(match_ids(&engine, "mechanical keyboard blue"), vec![2]);
    assert_eq!(
        base_segments(&engine),
        (0, 2),
        "query 2 came back from its segment, not from a WAL replay"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// While the stranded segment still cannot be written, nothing is committed
/// and the WAL keeps its rows: a restart recovers them.
#[test]
fn nothing_is_committed_while_a_stranded_segment_cannot_be_written() {
    let dir = test_dir("fallback_still_failing");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        let original = StorageFailure::SegmentWrite.block(&dir);
        engine.flush(); // nothing new to seal; the stranded segment still cannot be written
        StorageFailure::SegmentWrite.unblock(&dir, original);
        assert_eq!(base_segments(&engine), (1, 1));
        assert_eq!(match_ids(&engine, "mechanical keyboard blue"), vec![2]);
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(match_ids(&engine, "usb hub silver"), vec![1]);
    assert_eq!(
        match_ids(&engine, "mechanical keyboard blue"),
        vec![2],
        "recovered from the WAL"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A delete of a stranded row is part of the segment when it reaches disk: the
/// row does not come back after the commit and a restart.
#[test]
fn a_delete_of_a_stranded_row_survives_the_commit() {
    let dir = test_dir("fallback_delete");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        assert_eq!(engine.delete_by_logical_id(2).expect("delete"), 1);
        engine.insert_live("desk lamp chrome", 3, 1);
        engine.flush();
        assert_eq!(base_segments(&engine), (0, 3));
        assert!(match_ids(&engine, "mechanical keyboard blue").is_empty());
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert!(
        match_ids(&engine, "mechanical keyboard blue").is_empty(),
        "the delete was acknowledged and must not be undone"
    );
    assert_eq!(match_ids(&engine, "desk lamp chrome"), vec![3]);
    assert_eq!(match_ids(&engine, "usb hub silver"), vec![1]);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A bulk batch commits through the same point, so it carries a stranded
/// segment to disk with it.
#[test]
fn a_bulk_commit_carries_a_stranded_segment_to_disk() {
    let dir = test_dir("fallback_bulk");
    let config = manual_config(&dir);
    {
        let mut engine = engine_with_a_failed_flush(&config, &dir);
        let report = engine
            .try_bulk_ingest(&[(3, "desk lamp chrome".into())])
            .expect("bulk");
        assert_eq!(report.ingested, 1);
        assert_eq!(base_segments(&engine).0, 0);
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(match_ids(&engine, "mechanical keyboard blue"), vec![2]);
    assert_eq!(match_ids(&engine, "desk lamp chrome"), vec![3]);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

fn ny_alias() -> reverse_rusty::Vocab {
    let mut vocab = reverse_rusty::Vocab::new();
    vocab
        .import_solr_aliases(
            "ny => new york",
            &make_norm(),
            &reverse_rusty::dict::Dict::new(),
        )
        .expect("valid alias fixture");
    vocab
}

/// The same hole with more in it: a vocabulary rebuild whose segment write
/// fails serves the whole corpus from one in-memory segment. The next flush
/// used to commit a manifest that listed only its own segment and reset the
/// WAL, so a restart came back with the new rows and nothing else.
#[test]
fn a_later_flush_commits_a_corpus_a_failed_rebuild_left_in_memory() {
    let dir = test_dir("fallback_rebuild");
    let config = manual_config(&dir);
    {
        let mut engine = Engine::open(make_norm(), config.clone()).expect("fresh engine");
        engine.build_from_queries(&[
            (1, "new york inventory".into()),
            (2, "usb hub silver".into()),
        ]);
        engine.set_vocab(ny_alias()).expect("runtime alias");
        let original = StorageFailure::SegmentWrite.block(&dir);
        engine.recompile_stale_segments();
        StorageFailure::SegmentWrite.unblock(&dir, original);
        assert_eq!(
            base_segments(&engine),
            (1, 0),
            "precondition: the rebuilt corpus is served from memory"
        );
        assert_eq!(match_ids(&engine, "ny inventory"), vec![1]);

        engine.insert_live("desk lamp chrome", 3, 1);
        engine.flush();
        assert_eq!(base_segments(&engine), (0, 2));
        // The segment that reached disk still counts as compiled under the current
        // vocabulary, so the engine keeps committing.
        engine.insert_live("air purifier white", 4, 1);
        engine.flush();
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(
        match_ids(&engine, "ny inventory"),
        vec![1],
        "the corpus and its vocabulary survive the restart"
    );
    assert_eq!(match_ids(&engine, "usb hub silver"), vec![2]);
    assert_eq!(match_ids(&engine, "desk lamp chrome"), vec![3]);
    assert_eq!(match_ids(&engine, "air purifier white"), vec![4]);
    assert_eq!(
        base_segments(&engine),
        (0, 3),
        "the later flush committed too: query 4 is in a segment, not a WAL replay"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
