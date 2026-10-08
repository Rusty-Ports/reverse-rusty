//! A log that is gone is refused, not replaced with an empty one (ADR-213).

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

/// A data directory with a manifest once had a write-ahead log: the log is opened before the
/// first commit and is only ever replaced through a rename. If it is gone, every acknowledged
/// write that had not been flushed to a segment went with it. The engine used to put an empty
/// log in its place and start without them. It is refused, nothing is created, and nothing
/// else is changed: with the log back in place the same directory opens with every write.
#[test]
fn an_engine_with_a_manifest_and_no_log_is_refused() {
    let dir = test_dir("lost_log_single_node");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.insert_live("wireless mouse", 1, 1);
        engine.flush();
        assert!(
            engine.persistence_healthy(),
            "precondition: the flush committed"
        );
        // Acknowledged, and only in the log.
        engine.insert_live("mechanical keyboard", 2, 1);
    }
    let wal = dir.join("wal.log");
    let held = std::fs::read(&wal).expect("the log");
    std::fs::remove_file(&wal).expect("lose the log");

    for attempt in 1..=2 {
        match Engine::open(make_norm(), config.clone()) {
            Err(error) => {
                let reason = error.to_string();
                assert!(
                    reason.contains("is missing") && reason.contains("Restore"),
                    "refused for another reason: {reason}"
                );
            }
            Ok(engine) => panic!(
                "attempt {attempt}: opened without its log; the acknowledged write is {}",
                if match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2) {
                    "there"
                } else {
                    "gone"
                }
            ),
        }
        assert!(!wal.exists(), "a refused open created a log");
    }

    std::fs::write(&wal, &held).expect("put the log back");
    let engine = Engine::open(make_norm(), config).expect("with its log");
    assert!(match_ids(&engine, "1986 vertex wireless mouse new").contains(&1));
    assert!(
        match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2),
        "the refusals changed something: the write in the log did not come back"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A directory with no manifest is a store that has not committed yet. It may have no log
/// either, and it starts.
#[test]
fn an_engine_with_no_manifest_and_no_log_starts() {
    let dir = test_dir("no_manifest_no_log");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    std::fs::create_dir_all(&dir).expect("an empty data directory");
    drop(Engine::open(make_norm(), config).expect("a new store"));
    assert!(dir.join("wal.log").exists(), "a new store has a log");
    let _ = std::fs::remove_dir_all(&dir);
}
