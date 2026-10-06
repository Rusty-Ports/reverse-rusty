//! The top-64 mask is assigned once (ADR-188).
//!
//! Every compiled row keeps its required top-64 features as bits of that mask,
//! and its class depends on it. So the mask must never change under rows that
//! were compiled against it, and no row compiled before the first assignment
//! may be re-planned after it by a restart.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};

fn manual_config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

/// `(default read, include_broad read)`, each sorted.
fn reads(engine: &Engine, title: &str) -> (Vec<u64>, Vec<u64>) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, false);
    let mut default = out.clone();
    default.sort_unstable();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    (default, out)
}

/// `count` features named `{prefix}{k}`, each used `uses` times with a unique
/// companion, so they outrank everything rarer for the mask.
fn frequent_features(prefix: &str, count: u64, uses: u64, first_id: u64) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut id = first_id;
    for k in 0..count {
        for r in 0..uses {
            out.push((id, format!("{prefix}{k} {prefix}pad{k}x{r}")));
            id += 1;
        }
    }
    out
}

const WATCHED: u64 = 900_000;
const WATCHED_TITLE: &str = "aa1 aa2 widget extra";

/// The queries of the first build: 70 `aa` features own the mask, and the
/// watched query requires two of them, so it is stored as two mask bits.
fn first_build() -> Vec<(u64, String)> {
    let mut queries = frequent_features("aa", 70, 5, 0);
    queries.push((WATCHED, "aa1 aa2 widget".to_string()));
    queries
}

/// A second "initial build" on a populated engine must not re-rank the mask:
/// the rows already stored keep their required features as bits of the first
/// assignment, and a re-rank makes those bits mean other features.
#[test]
fn a_second_build_leaves_stored_queries_matching() {
    let dir = test_dir("mask_second_build");
    let config = manual_config(&dir);
    let second = frequent_features("bb", 70, 50, 2_000_000);
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&first_build());
        assert_eq!(
            reads(&engine, WATCHED_TITLE),
            (vec![WATCHED], vec![WATCHED])
        );
    }
    {
        let mut engine = Engine::open(make_norm(), config.clone()).expect("reopen");
        // Far heavier than the first build: a re-rank would hand every bit to `bb`.
        let report = engine.build_from_queries(&second);
        assert_eq!(report.ingested, second.len());
        assert_eq!(
            reads(&engine, WATCHED_TITLE),
            (vec![WATCHED], vec![WATCHED]),
            "a stored query stopped matching after a second build"
        );
        assert_eq!(
            reads(&engine, "aa1 widget extra").1,
            Vec::<u64>::new(),
            "and it still requires both features"
        );
        // The second build's own rows match too.
        assert_eq!(
            reads(&engine, "bb3 bbpad3x7").1,
            vec![2_000_000 + 3 * 50 + 7]
        );
    }
    let engine = Engine::open(make_norm(), config).expect("second reopen");
    assert_eq!(
        reads(&engine, WATCHED_TITLE),
        (vec![WATCHED], vec![WATCHED])
    );
    assert_eq!(
        reads(&engine, "bb3 bbpad3x7").1,
        vec![2_000_000 + 3 * 50 + 7]
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A query inserted before the mask exists is default-visible. If the first
/// batch then puts its only term in the top 64, a restart must not re-plan the
/// still-unflushed insert into the opt-in lane. Both batch entry points can be
/// the one that assigns the mask.
#[test]
fn a_restart_keeps_a_query_inserted_before_the_first_finalize_visible() {
    for first_batch_is_a_build in [false, true] {
        let ctx = format!("first_batch_is_a_build={first_batch_is_a_build}");
        let dir = test_dir("mask_restart_visibility");
        let config = manual_config(&dir);
        let expected = (vec![1], vec![1, 2]);
        {
            let mut engine = Engine::with_config(make_norm(), config.clone());
            engine.insert_live("rareword", 1, 1); // before the first finalize
                                                  // Assigns the mask with `rareword` in the top 64.
            let batch: Vec<(u64, String)> = (0..200u64)
                .map(|i| (1_000 + i, format!("rareword filler{i}")))
                .collect();
            let report = if first_batch_is_a_build {
                engine.build_from_queries(&batch)
            } else {
                engine.bulk_ingest(&batch)
            };
            assert_eq!(report.ingested, batch.len(), "{ctx}");
            engine.insert_live("rareword", 2, 1); // compiled now: opt-in, WAL tail only
            assert_eq!(reads(&engine, "rareword title"), expected, "{ctx}: before");
            // Dropped without an explicit flush.
        }
        let engine = Engine::open(make_norm(), config).expect("reopen");
        assert_eq!(reads(&engine, "rareword title"), expected, "{ctx}: after");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

fn first_batch() -> Vec<(u64, String)> {
    (0..200u64)
        .map(|i| (1_000 + i, format!("rareword filler{i}")))
        .collect()
}

/// If the memtable cannot be sealed, the batch must fail before the mask is
/// assigned, and the engine must be exactly as it was: the insert is still in
/// the memtable, so a retry seals it and nothing is lost.
#[test]
fn a_failed_seal_fails_the_batch_and_changes_nothing() {
    for failure in [StorageFailure::SegmentWrite, StorageFailure::Commit] {
        let dir = test_dir("mask_failed_seal");
        let config = manual_config(&dir);
        {
            let mut engine = Engine::with_config(make_norm(), config.clone());
            engine.insert_live("rareword", 1, 1);

            let original = failure.block(&dir);
            let result = engine.try_bulk_ingest(&first_batch());
            failure.unblock(&dir, original); // before asserting

            assert!(
                result.is_err(),
                "{failure:?}: the batch must not be ingested"
            );
            assert!(
                !engine.dict().is_finalized(),
                "{failure:?}: the mask must stay unassigned when the seal did not commit"
            );
            assert_eq!(engine.num_live_queries(), 1, "{failure:?}");
            assert_eq!(
                engine.num_segments(),
                1,
                "{failure:?}: the insert is still in the memtable"
            );
            assert_eq!(reads(&engine, "rareword title").0, vec![1], "{failure:?}");

            // The disk is writable again: the retry seals, assigns the mask and ingests.
            assert_eq!(
                engine.bulk_ingest(&first_batch()).ingested,
                200,
                "{failure:?}"
            );
            assert!(engine.dict().is_finalized(), "{failure:?}");
            assert_eq!(reads(&engine, "rareword title").0, vec![1], "{failure:?}");
        }
        let engine = Engine::open(make_norm(), config).expect("reopen");
        assert_eq!(
            reads(&engine, "rareword title").0,
            vec![1],
            "{failure:?}: still default-visible after the retry and a restart"
        );
        assert_eq!(engine.num_live_queries(), 201, "{failure:?}");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// After a failed seal the rows must not be left where a later flush would
/// retire their WAL frames without committing them: every acknowledged query
/// survives a later write, flush and restart.
#[test]
fn a_failed_seal_loses_no_acknowledged_query() {
    for failure in [StorageFailure::SegmentWrite, StorageFailure::Commit] {
        let dir = test_dir("mask_failed_seal_then_flush");
        let config = manual_config(&dir);
        {
            let mut engine = Engine::with_config(make_norm(), config.clone());
            engine.insert_live("rareword", 1, 1);
            let original = failure.block(&dir);
            assert!(
                engine.try_bulk_ingest(&first_batch()).is_err(),
                "{failure:?}"
            );
            failure.unblock(&dir, original);

            engine.insert_live("otherword", 2, 1);
            engine.flush();
        }
        let engine = Engine::open(make_norm(), config).expect("reopen");
        assert_eq!(reads(&engine, "rareword title").1, vec![1], "{failure:?}");
        assert_eq!(reads(&engine, "otherword title").1, vec![2], "{failure:?}");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The seal exists only because a WAL replays text. An in-memory engine never
/// replays, so its memtable is left alone when the mask is assigned.
#[test]
fn an_engine_without_a_wal_is_not_sealed() {
    let mut engine = Engine::with_config(
        make_norm(),
        EngineConfig {
            auto_compact_on_flush: false,
            auto_compact_on_ingest: false,
            ..EngineConfig::default()
        },
    );
    engine.insert_live("rareword", 1, 1);
    let batch: Vec<(u64, String)> = (0..200u64)
        .map(|i| (1_000 + i, format!("rareword filler{i}")))
        .collect();
    engine.bulk_ingest(&batch);
    // The bulk base segment plus the (unsealed) memtable.
    assert_eq!(engine.num_segments(), 2);
    assert_eq!(reads(&engine, "rareword title").0, vec![1]);
}

/// A flush that could not write its segment leaves rows in an in-memory
/// segment, compiled without a mask and durable only as WAL text, exactly like
/// memtable rows. The first batch must not assign the mask over them. While
/// that segment still cannot be written the batch is refused; once it can, the
/// batch commits it to disk first (ADR-190), so its rows keep the class they
/// were given and stay default-visible across a restart.
#[test]
fn the_first_batch_commits_what_a_failed_flush_left_in_memory() {
    let dir = test_dir("mask_after_failed_flush");
    let config = manual_config(&dir);
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.insert_live("rareword", 1, 1);
        let original = StorageFailure::SegmentWrite.block(&dir);
        engine.flush(); // fails: row 1 falls back to an in-memory segment
        let refused = engine.try_bulk_ingest(&first_batch());
        StorageFailure::SegmentWrite.unblock(&dir, original);
        assert!(
            refused.is_err(),
            "the mask must not be assigned over rows that are only in the WAL"
        );
        assert!(!engine.dict().is_finalized());
        assert_eq!(reads(&engine, "rareword title").0, vec![1]);

        engine.insert_live("otherword", 2, 1);
        assert_eq!(engine.bulk_ingest(&first_batch()).ingested, 200);
        assert!(engine.dict().is_finalized());
        assert_eq!(reads(&engine, "rareword title").0, vec![1]);
        assert_eq!(reads(&engine, "otherword title").0, vec![2]);
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert_eq!(
        reads(&engine, "rareword title").0,
        vec![1],
        "still default-visible after the mask was assigned and a restart"
    );
    assert_eq!(reads(&engine, "otherword title").0, vec![2]);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
