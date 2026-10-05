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

/// If the memtable cannot be sealed, the batch must fail before the mask is
/// assigned: assigning it anyway would leave the unflushed insert to be
/// re-planned on the next restart.
#[test]
fn a_failed_seal_fails_the_batch_and_leaves_the_mask_unassigned() {
    use std::os::unix::fs::PermissionsExt;

    let dir = test_dir("mask_failed_seal");
    let config = manual_config(&dir);
    let batch: Vec<(u64, String)> = (0..200u64)
        .map(|i| (1_000 + i, format!("rareword filler{i}")))
        .collect();
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.insert_live("rareword", 1, 1);

        // Make segments/ read-only so the seal's segment write fails.
        let seg_dir = dir.join("segments");
        std::fs::create_dir_all(&seg_dir).expect("segments dir");
        let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
        std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = engine.try_bulk_ingest(&batch);
        std::fs::set_permissions(&seg_dir, orig).unwrap(); // restore before asserting

        assert!(result.is_err(), "the batch must not be ingested");
        assert!(
            !engine.dict().is_finalized(),
            "the mask must stay unassigned when the seal did not commit"
        );
        assert_eq!(
            engine.num_live_queries(),
            1,
            "nothing from the batch is stored"
        );
        assert_eq!(reads(&engine, "rareword title").0, vec![1]);
    }
    // The insert is still in the WAL and replays under the same (absent) mask.
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert!(!engine.dict().is_finalized());
    assert_eq!(reads(&engine, "rareword title").0, vec![1]);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
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
