//! ADR-185: the per-shard atomic replace — one engine critical section, one translog
//! `Upsert` frame, one published snapshot.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cluster::clog::{ClusterMutation, LogPos};
use crate::cluster::shard::{LocalShard, PlacedWrite, ReplaceMode, ReplaceStatus, Shard};
use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::ownership::{PlacementGeneration, QueryPlacement};
use crate::tagdict::TagDict;

struct Fixture {
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    tags: Arc<TagDict>,
    queries: Vec<(&'static str, Extracted)>,
}

fn fixture(dsls: &[&'static str]) -> Fixture {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    let mut lc = String::new();
    let queries = dsls
        .iter()
        .map(|&dsl| {
            let ast = crate::dsl::parse(dsl).unwrap();
            (
                dsl,
                crate::compile::extract(&ast, &norm, &mut dict, &mut lc),
            )
        })
        .collect();
    dict.finalize_mask();
    let mut tags = TagDict::new();
    tags.mark_finalized();
    Fixture {
        norm,
        dict: Arc::new(dict),
        tags: Arc::new(tags),
        queries,
    }
}

impl Fixture {
    fn volatile(&self) -> LocalShard {
        LocalShard::new(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            Arc::clone(&self.tags),
            EngineConfig::default(),
        )
    }

    /// A durable shard (a volatile one has no translog to inspect) and its directory.
    fn durable(&self, name: &str) -> (LocalShard, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("rr_shard_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let shard = LocalShard::new_durable(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            Arc::clone(&self.tags),
            EngineConfig {
                data_dir: Some(dir.clone()),
                ..EngineConfig::default()
            },
        )
        .unwrap();
        (shard, dir)
    }

    fn write<'a>(
        &'a self,
        query: usize,
        logical: u64,
        version: u32,
        placement: &'a QueryPlacement,
    ) -> PlacedWrite<'a> {
        PlacedWrite {
            ex: &self.queries[query].1,
            logical,
            version,
            text: self.queries[query].0,
            tags: &[],
            placement,
        }
    }
}

fn selective(positions: &[u32]) -> QueryPlacement {
    QueryPlacement::selective(PlacementGeneration::INITIAL, 4, positions.to_vec())
        .expect("selective placement")
}

fn matches(shard: &LocalShard, title: &str) -> Vec<u64> {
    let mut ids = shard
        .percolate_filtered(title, true, &TagPredicate::empty())
        .expect("percolate")
        .0;
    ids.sort_unstable();
    ids
}

#[test]
fn replace_swaps_versions_and_logs_one_upsert_frame() {
    let fx = fixture(&["zzold zzshared", "zznew zzshared"]);
    let (shard, dir) = fx.durable("replace_frames");
    let placement = selective(&[1]);
    assert_eq!(
        shard
            .replace_placed(&fx.write(0, 7, 1, &placement), ReplaceMode::Unconditional)
            .unwrap(),
        ReplaceStatus::Inserted
    );
    assert_eq!(
        shard
            .replace_placed(&fx.write(1, 7, 2, &placement), ReplaceMode::Unconditional)
            .unwrap(),
        ReplaceStatus::Replaced { removed: 1 }
    );
    assert_eq!(matches(&shard, "zznew zzshared"), vec![7]);
    assert!(matches(&shard, "zzold zzshared").is_empty());

    let tail = shard.translog_tail(LogPos(0)).unwrap();
    assert_eq!(tail.len(), 2, "one frame per replace: {tail:?}");
    assert!(
        tail.iter()
            .all(|(_, m)| matches!(m, ClusterMutation::Upsert { logical: 7, .. })),
        "a replace is logged whole, never as Remove + Add: {tail:?}"
    );
    drop(shard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_conditional_replace_declines_without_changing_or_logging_anything() {
    let fx = fixture(&["zzold zzshared", "zznew zzshared"]);
    let (shard, dir) = fx.durable("replace_conditional");
    let held = selective(&[1]);
    let moved = selective(&[1, 2]);

    assert_eq!(
        shard
            .replace_placed(&fx.write(1, 7, 2, &held), ReplaceMode::IfSamePlacement)
            .unwrap(),
        ReplaceStatus::Absent
    );
    shard
        .replace_placed(&fx.write(0, 7, 1, &held), ReplaceMode::Unconditional)
        .unwrap();
    assert_eq!(
        shard
            .replace_placed(&fx.write(1, 7, 2, &moved), ReplaceMode::IfSamePlacement)
            .unwrap(),
        ReplaceStatus::PlacementMismatch
    );
    assert_eq!(
        matches(&shard, "zzold zzshared"),
        vec![7],
        "still the old version"
    );
    assert_eq!(
        shard.translog_tail(LogPos(0)).unwrap().len(),
        1,
        "a declined replace is not a mutation"
    );

    assert_eq!(
        shard
            .replace_placed(&fx.write(1, 7, 2, &held), ReplaceMode::IfSamePlacement)
            .unwrap(),
        ReplaceStatus::Replaced { removed: 1 }
    );
    assert_eq!(matches(&shard, "zznew zzshared"), vec![7]);
    drop(shard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A sealed prior copy (base segment) and a memtable copy are both tombstoned by one
/// replace, and a self-restart replays the single Upsert frame to the same live set.
#[test]
fn replace_tombstones_sealed_copies_and_replays_identically() {
    let fx = fixture(&["zzold zzshared", "zznew zzshared", "zzother"]);
    let dir = std::env::temp_dir().join(format!("rr_shard_replace_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let open = || {
        LocalShard::new_durable(
            Arc::clone(&fx.norm),
            Arc::clone(&fx.dict),
            Arc::clone(&fx.tags),
            config.clone(),
        )
        .unwrap()
    };
    let placement = selective(&[0]);
    let titles = ["zzold zzshared", "zznew zzshared", "zzother"];
    let live = {
        let shard = open();
        shard
            .replace_placed(&fx.write(0, 7, 1, &placement), ReplaceMode::Unconditional)
            .unwrap();
        shard
            .replace_placed(&fx.write(2, 8, 1, &placement), ReplaceMode::Unconditional)
            .unwrap();
        shard.flush().unwrap(); // the old version of 7 now lives in a base segment
        assert_eq!(
            shard
                .replace_placed(&fx.write(1, 7, 2, &placement), ReplaceMode::Unconditional)
                .unwrap(),
            ReplaceStatus::Replaced { removed: 1 }
        );
        titles.map(|title| matches(&shard, title))
    };
    assert_eq!(live, [vec![], vec![7], vec![8]]);
    let reopened = open();
    assert_eq!(
        titles.map(|title| matches(&reopened, title)),
        live,
        "self-restart replay of the Upsert frame equals the live apply"
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The reader-visible property itself: while one thread replaces an id over and over, a
/// reader of the same shard never finds it missing and never finds it twice. A replace
/// built from a delete and an insert with two publishes loses this within a few thousand
/// iterations.
#[test]
fn a_concurrent_reader_never_sees_a_replace_half_done() {
    let fx = fixture(&["zzkeep zzalpha", "zzkeep zzbeta"]);
    let shard = fx.volatile();
    let placement = selective(&[0]);
    shard
        .replace_placed(&fx.write(0, 7, 1, &placement), ReplaceMode::Unconditional)
        .unwrap();
    let stop = AtomicBool::new(false);
    let anomalies = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut anomalies = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let seen = matches(&shard, "zzkeep zzalpha zzbeta");
                if seen != vec![7] {
                    anomalies.push(seen);
                }
            }
            anomalies
        });
        for round in 0..4_000u32 {
            let query = (round % 2) as usize;
            let status = shard.replace_placed(
                &fx.write(query, 7, round + 2, &placement),
                ReplaceMode::IfSamePlacement,
            );
            if !matches!(status, Ok(ReplaceStatus::Replaced { removed: 1 })) {
                // Stop the reader before failing, so the scope can join it.
                stop.store(true, Ordering::Relaxed);
                panic!("round {round}: {status:?}");
            }
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().expect("reader")
    });
    assert!(
        anomalies.is_empty(),
        "the id went missing or doubled during a replace: {:?}",
        &anomalies[..anomalies.len().min(5)]
    );
}
