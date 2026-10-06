//! A crash while the WAL is being reset, or first created, must not stop the node (ADR-198).

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

/// The state an older binary could leave behind: every write flushed and committed, and
/// `wal.log` cut inside its eight-byte header by a crash in the reset that follows a flush.
/// The node used to refuse to start ("WAL too small") although nothing was missing.
#[test]
fn an_engine_starts_when_a_crash_cut_the_wal_inside_its_header() {
    for kept in [0usize, 3, 7] {
        let dir = test_dir(&format!("wal_reset_cut_header_{kept}"));
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        {
            let mut engine = Engine::with_config(make_norm(), config.clone());
            engine.insert_live("wireless mouse", 1, 1);
            engine.insert_live("mechanical keyboard", 2, 1);
            engine.flush();
            assert!(
                engine.persistence_healthy(),
                "precondition: the flush committed"
            );
        }
        let wal = dir.join("wal.log");
        let header = std::fs::read(&wal).expect("the reset log");
        assert_eq!(
            header.len(),
            8,
            "precondition: a flush leaves a header-only log"
        );
        std::fs::write(&wal, &header[..kept]).expect("cut the log inside its header");

        let mut engine = Engine::open(make_norm(), config.clone())
            .unwrap_or_else(|error| panic!("{kept} header bytes left: {error}"));
        assert!(match_ids(&engine, "1986 vertex wireless mouse new").contains(&1));
        assert!(match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2));
        // The log is whole again and takes writes that survive a crash.
        engine.insert_live("product omega", 3, 1);
        drop(engine);
        let engine = Engine::open(make_norm(), config).expect("reopen");
        assert!(match_ids(&engine, "2021 acme product omega").contains(&3));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A flush resets the log by replacing it. Whatever a crash leaves of that replacement
/// (here: the finished file that was never renamed into place) is not mistaken for the log,
/// and the next reset simply writes over it.
#[test]
fn a_leftover_replacement_file_is_harmless() {
    let dir = test_dir("wal_reset_leftover_replacement");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.insert_live("wireless mouse", 1, 1);
        // Not flushed: the write is only in the log. A crash now, with a replacement from
        // an interrupted reset lying next to it.
    }
    std::fs::write(dir.join("wal.log.tmp"), b"PWAL").expect("leftover");
    let mut engine = Engine::open(make_norm(), config.clone()).expect("reopen");
    assert!(
        match_ids(&engine, "1986 vertex wireless mouse new").contains(&1),
        "the log, not the leftover, is what is replayed"
    );
    engine.flush();
    assert!(
        engine.persistence_healthy(),
        "a reset writes over the leftover"
    );
    drop(engine);
    let engine = Engine::open(make_norm(), config).expect("reopen after the reset");
    assert!(match_ids(&engine, "1986 vertex wireless mouse new").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}
