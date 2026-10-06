//! A commit that changes only the segment registry selects the source sidecar it already
//! has; it does not write the corpus out again (ADR-200).

use super::*;
use reverse_rusty::events::EngineEvent;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Counts the complete source-corpus writes an engine performs.
#[derive(Clone, Default)]
struct SourceWrites(Arc<AtomicUsize>);

impl SourceWrites {
    fn watch(engine: &mut Engine) -> Self {
        let writes = Self::default();
        let counter = Arc::clone(&writes.0);
        engine.set_observer(move |event: &EngineEvent| {
            if matches!(event, EngineEvent::SourceCommit { .. }) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        writes
    }

    /// The writes since the last call.
    fn take(&self) -> usize {
        self.0.swap(0, Ordering::SeqCst)
    }
}

fn durable(dir: &std::path::Path, retain_source: bool) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        retain_source,
        // The tests decide when a merge runs.
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

fn insert(engine: &mut Engine, id: u64, text: &str) {
    engine.try_insert_live(text, id, 1).expect("insert");
}

/// Sealed segments: `num_segments` also counts the memtable.
fn sealed_segments(engine: &Engine) -> usize {
    engine.num_segments() - 1
}

/// Every stored document the engine serves, by id.
fn sources_of(engine: &Engine, ids: impl IntoIterator<Item = u64>) -> BTreeMap<u64, String> {
    ids.into_iter()
        .filter_map(|id| Some((id, engine.get_query_source(id)?)))
        .collect()
}

/// Two flushed segments, each holding half of the sample queries.
fn two_segments(config: &EngineConfig) -> Engine {
    let mut engine = Engine::with_config(make_norm(), config.clone());
    let queries = sample_queries();
    let (first, second) = queries.split_at(queries.len() / 2);
    for (id, text) in first {
        insert(&mut engine, *id, text);
    }
    engine.flush();
    for (id, text) in second {
        insert(&mut engine, *id, text);
    }
    engine.flush();
    assert_eq!(sealed_segments(&engine), 2);
    engine
}

/// A merge moves rows between segments and changes no document. It used to write every
/// live document to a new sidecar all the same.
#[test]
fn a_merge_alone_keeps_the_selected_sidecar() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("merge_keeps_sidecar_{retain_source}"));
        let config = durable(&dir, retain_source);
        let mut engine = two_segments(&config);
        let writes = SourceWrites::watch(&mut engine);
        let selected = committed_source_path(&dir);

        engine.compact_all().expect("two segments merge");

        assert_eq!(sealed_segments(&engine), 1);
        assert_eq!(
            writes.take(),
            0,
            "retain_source={retain_source}: a merge wrote the source corpus"
        );
        assert_eq!(committed_source_path(&dir), selected);
        assert!(selected.exists(), "the selected sidecar is still on disk");

        let expected: BTreeMap<u64, String> = sample_queries().into_iter().collect();
        drop(engine);
        let reopened = Engine::open(make_norm(), config).unwrap();
        assert_eq!(
            sources_of(&reopened, 1..=10),
            expected,
            "retain_source={retain_source}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A flush writes the corpus once. When the flush also merges, the merge commit that
/// follows used to write it a second time.
#[test]
fn a_flush_that_merges_writes_the_sources_once() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("flush_merge_once_{retain_source}"));
        let config = EngineConfig {
            max_segments: 1,
            auto_compact_on_flush: true,
            ..durable(&dir, retain_source)
        };
        let mut engine = Engine::with_config(make_norm(), config.clone());
        for (id, text) in sample_queries().iter().take(5) {
            insert(&mut engine, *id, text);
        }
        engine.flush();
        let writes = SourceWrites::watch(&mut engine);
        for (id, text) in sample_queries().iter().skip(5) {
            insert(&mut engine, *id, text);
        }

        engine.flush();

        assert_eq!(sealed_segments(&engine), 1, "the flush merged");
        assert_eq!(
            writes.take(),
            1,
            "retain_source={retain_source}: one flush, one source write"
        );
        drop(engine);
        let reopened = Engine::open(make_norm(), config).unwrap();
        let expected: BTreeMap<u64, String> = sample_queries().into_iter().collect();
        assert_eq!(sources_of(&reopened, 1..=10), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The same for a bulk load of new ids whose commit is followed by a merge.
#[test]
fn a_bulk_load_that_merges_writes_the_sources_once() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("bulk_merge_once_{retain_source}"));
        let config = EngineConfig {
            max_segments: 1,
            auto_compact_on_ingest: true,
            ..durable(&dir, retain_source)
        };
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&sample_queries());
        let writes = SourceWrites::watch(&mut engine);

        let batch = vec![
            (100u64, "product omega preview".to_string()),
            (101u64, "portable monitor new".to_string()),
        ];
        engine.try_bulk_ingest(&batch).expect("bulk load");

        assert_eq!(sealed_segments(&engine), 1, "the bulk load merged");
        assert_eq!(
            writes.take(),
            1,
            "retain_source={retain_source}: one bulk load, one source write"
        );
        drop(engine);
        let reopened = Engine::open(make_norm(), config).unwrap();
        assert_eq!(
            reopened.get_query_source(101).as_deref(),
            Some("portable monitor new")
        );
        assert_eq!(
            reopened.get_query_source(1).as_deref(),
            Some("wireless mouse 1986 vertex")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A sidecar is kept only while it is exact. Any write to a document after it was selected
/// (a new one, a replacement, a removal) makes the next commit write the corpus, also when
/// that commit is only a merge.
#[test]
fn a_change_after_a_kept_sidecar_is_written_by_the_next_commit() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("change_after_kept_{retain_source}"));
        let config = durable(&dir, retain_source);
        let mut engine = two_segments(&config);
        let writes = SourceWrites::watch(&mut engine);
        engine.compact_all().expect("merge");
        assert_eq!(writes.take(), 0);
        let kept = committed_source_path(&dir);

        insert(&mut engine, 50, "product omega preview");
        engine
            .try_upsert_live("mechanical keyboard refurbished", 2, 2)
            .expect("upsert");
        engine.delete_by_logical_id(3).expect("delete");
        engine.flush();

        assert_eq!(writes.take(), 1, "retain_source={retain_source}");
        assert_ne!(committed_source_path(&dir), kept);
        drop(engine);
        let reopened = Engine::open(make_norm(), config).unwrap();
        assert_eq!(
            reopened.get_query_source(50).as_deref(),
            Some("product omega preview")
        );
        assert_eq!(
            reopened.get_query_source(2).as_deref(),
            Some("mechanical keyboard refurbished")
        );
        assert_eq!(reopened.get_query_source(3), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A reopened store knows its sidecar is exact when nothing was replayed into it: a merge
/// after a clean restart keeps the sidecar.
#[test]
fn a_store_reopened_with_a_clean_log_keeps_its_sidecar_through_a_merge() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("clean_reopen_merge_{retain_source}"));
        let config = durable(&dir, retain_source);
        drop(two_segments(&config));

        let mut engine = Engine::open(make_norm(), config.clone()).unwrap();
        let writes = SourceWrites::watch(&mut engine);
        let selected = committed_source_path(&dir);
        engine.compact_all().expect("two segments merge");

        assert_eq!(writes.take(), 0, "retain_source={retain_source}");
        assert_eq!(committed_source_path(&dir), selected);
        drop(engine);
        let reopened = Engine::open(make_norm(), config).unwrap();
        let expected: BTreeMap<u64, String> = sample_queries().into_iter().collect();
        assert_eq!(sources_of(&reopened, 1..=10), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// With writes replayed from the log, the store holds documents the selected sidecar does
/// not. The same merge then writes the corpus, and the replayed documents are in it.
#[test]
fn a_store_reopened_with_replayed_writes_does_not_keep_its_sidecar() {
    for retain_source in [true, false] {
        let dir = test_dir(&format!("replayed_reopen_merge_{retain_source}"));
        let config = durable(&dir, retain_source);
        let mut engine = two_segments(&config);
        insert(&mut engine, 60, "smart speaker refurbished");
        // Not flushed: the write is in the log only when the engine is dropped.
        drop(engine);

        let mut replayed = Engine::open(make_norm(), config.clone()).unwrap();
        let writes = SourceWrites::watch(&mut replayed);
        let selected = committed_source_path(&dir);
        replayed.compact_all().expect("two segments merge");

        assert_eq!(
            writes.take(),
            1,
            "retain_source={retain_source}: a replayed write is not in the old sidecar"
        );
        let rewritten = committed_source_path(&dir);
        assert_ne!(rewritten, selected);
        let stored = reverse_rusty::storage::SourceStore::open(&rewritten, true).unwrap();
        assert_eq!(
            stored.get(60).as_deref(),
            Some("smart speaker refurbished"),
            "the sidecar the merge wrote holds the replayed document"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A small deterministic generator, so a failing sequence can be replayed from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

/// Whatever mix of writes, flushes, merges and restarts runs, a restart serves exactly the
/// documents that were written. A sidecar kept when it should have been rewritten would
/// show here as a stale, missing or resurrected document.
#[test]
fn restarts_serve_the_written_documents_under_any_mix_of_commits() {
    const WORDS: [&str; 6] = ["mouse", "keyboard", "monitor", "speaker", "lamp", "camera"];
    for retain_source in [true, false] {
        for seed in 1..=6u64 {
            let dir = test_dir(&format!("source_model_{retain_source}_{seed}"));
            let config = EngineConfig {
                max_segments: 3,
                ..durable(&dir, retain_source)
            };
            let mut rng = Rng(seed);
            let mut model: BTreeMap<u64, String> = BTreeMap::new();
            let mut version: BTreeMap<u64, u32> = BTreeMap::new();
            let mut next_bulk_id = 1_000u64;
            let mut engine = Engine::with_config(make_norm(), config.clone());

            for step in 0..120 {
                let id = 1 + rng.next(12);
                let text = format!(
                    "wireless {} acme {}",
                    WORDS[rng.next(6) as usize],
                    rng.next(1_000)
                );
                match rng.next(10) {
                    0..=3 => {
                        let next = version.get(&id).copied().unwrap_or(0) + 1;
                        engine.try_upsert_live(&text, id, next).expect("upsert");
                        version.insert(id, next);
                        model.insert(id, text);
                    }
                    4 => {
                        engine.delete_by_logical_id(id).expect("delete");
                        model.remove(&id);
                    }
                    5 => {
                        let batch: Vec<(u64, String)> = (0..3)
                            .map(|n| (next_bulk_id + n, format!("{text} bulk {n}")))
                            .collect();
                        next_bulk_id += 3;
                        engine.try_bulk_ingest(&batch).expect("bulk load");
                        model.extend(batch);
                    }
                    6 => engine.flush(),
                    7 => {
                        engine.compact_all();
                    }
                    8 => {
                        engine.maybe_compact();
                    }
                    _ => {
                        drop(engine);
                        engine = Engine::open(make_norm(), config.clone()).unwrap();
                    }
                }
                if step % 10 == 9 {
                    drop(engine);
                    engine = Engine::open(make_norm(), config.clone()).unwrap();
                    let served = sources_of(&engine, (1..=12).chain(1_000..next_bulk_id));
                    assert_eq!(
                        served, model,
                        "retain_source={retain_source} seed={seed} step={step}"
                    );
                }
            }
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
