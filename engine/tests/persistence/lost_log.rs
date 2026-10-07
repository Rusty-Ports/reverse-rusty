//! A log that is gone is refused, not replaced with an empty one (ADR-213).

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use reverse_rusty::segment::Engine;
use std::sync::{Arc, Mutex};

fn lost_log_events(engine: &mut Engine) -> Vec<String> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    engine.set_observer(move |event: &EngineEvent| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::LogLost,
            error,
            ..
        } = event
        {
            seen.lock().unwrap().push(error.clone());
        }
    });
    let seen = events.lock().unwrap().clone();
    seen
}

/// A data directory with a manifest once had a write-ahead log: the log is opened before the
/// first commit and is only ever replaced through a rename. If it is gone, the writes
/// acknowledged since the last flush went with it. The engine used to put an empty log in
/// its place and start without them. It is refused, and nothing is created.
///
/// With `accept_lost_log` it starts from the last flush, says what it lost, and from then on
/// opens as usual.
#[test]
fn an_engine_with_a_manifest_and_no_log_is_refused_unless_the_loss_is_accepted() {
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
    std::fs::remove_file(&wal).expect("lose the log");

    match Engine::open(make_norm(), config.clone()) {
        Err(error) => {
            let reason = error.to_string();
            assert!(
                reason.contains("is missing") && reason.contains("accept_lost_log"),
                "refused for another reason: {reason}"
            );
        }
        Ok(engine) => panic!(
            "opened without its log; the acknowledged write is {}",
            if match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2) {
                "there"
            } else {
                "gone"
            }
        ),
    }
    assert!(!wal.exists(), "a refused open created a log");

    let accepting = EngineConfig {
        accept_lost_log: true,
        ..config.clone()
    };
    let mut engine = Engine::open(make_norm(), accepting.clone()).expect("the loss was accepted");
    let lost = lost_log_events(&mut engine);
    assert_eq!(lost.len(), 1, "the loss is reported once: {lost:?}");
    assert!(match_ids(&engine, "1986 vertex wireless mouse new").contains(&1));
    assert!(
        !match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2),
        "the write that was only in the log"
    );
    engine.insert_live("product omega", 3, 1);
    drop(engine);

    // The log exists again: no flag is needed and nothing is reported, and the flag changes
    // nothing when the log is there.
    for config in [config, accepting] {
        let mut engine = Engine::open(make_norm(), config).expect("reopen");
        assert!(lost_log_events(&mut engine).is_empty());
        assert!(match_ids(&engine, "2021 acme product omega").contains(&3));
    }
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
    let mut engine = Engine::open(make_norm(), config).expect("a new store");
    assert!(lost_log_events(&mut engine).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}
