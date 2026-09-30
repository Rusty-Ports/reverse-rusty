//! WAL recovery: tagged inserts replayed with their tags, live inserts restored
//! after a simulated crash, and corrupt-tail reporting.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

#[test]
fn repaired_tail_is_reported_once_and_new_writes_survive_the_next_restart() {
    use reverse_rusty::events::{DurabilityOp, EngineEvent};
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    for manifest in [false, true] {
        let dir = test_dir(&format!("wal_repair_observer_manifest_{manifest}"));
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            memtable_flush_threshold: usize::MAX,
            wal_sync_on_write: true,
            ..EngineConfig::default()
        };
        {
            let mut engine = Engine::with_config(make_norm(), config.clone());
            if manifest {
                engine.build_from_queries(&[(99, "placeholder seed".into())]);
            }
            engine.try_insert_live("wireless mouse", 1, 1).unwrap();
        }
        let path = dir.join("wal.log");
        let valid_len = std::fs::metadata(&path).unwrap().len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&[32, 0, 0, 0, 0xaa, 0xbb, 0xcc, 0xdd])
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut engine = Engine::open(make_norm(), config.clone()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
        let observer = |events: Arc<Mutex<Vec<String>>>| {
            move |event: &EngineEvent| {
                if let EngineEvent::DurabilityFailure {
                    op: DurabilityOp::WalTornTail,
                    error,
                    ..
                } = event
                {
                    events.lock().unwrap().push(error.clone());
                }
            }
        };
        engine.set_observer(observer(Arc::clone(&events)));
        engine.set_observer(observer(Arc::clone(&events)));
        assert_eq!(*events.lock().unwrap(), vec!["8 bytes"]);
        engine.try_insert_live("mechanical keyboard", 2, 1).unwrap();
        drop(engine);
        let mut engine = Engine::open(make_norm(), config).unwrap();
        engine.set_observer(observer(Arc::clone(&events)));
        assert_eq!(*events.lock().unwrap(), vec!["8 bytes"]);
        assert!(match_ids(&engine, "wireless mouse").contains(&1));
        assert!(match_ids(&engine, "mechanical keyboard").contains(&2));
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A crash BEFORE the first manifest commit (no flush/bulk/build yet) leaves
/// acknowledged writes only in `wal.log`. `Engine::open`'s fresh path used to
/// construct an empty engine WITHOUT replaying that tail — silently losing every
/// acknowledged write a start-empty-and-PUT server had taken (the WAL-first
/// contract of ADR-013 was void until the first flush). The fresh path now runs
/// the same replay loop as the manifest path.
#[test]
fn writes_before_first_manifest_survive_crash() {
    let dir = test_dir("fresh_path_wal_replay");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        // NO build/bulk/flush — every mutation lives only in the WAL.
        engine.insert_live("wireless mouse", 1, 1);
        engine.insert_live("mechanical keyboard", 2, 1);
        engine
            .try_upsert_live("noise cancelling headphones", 2, 2)
            .expect("upsert (replaces q2)");
        engine.insert_live("product omega", 3, 1);
        assert!(engine.delete_by_logical_id(3).expect("delete") >= 1);
        assert!(
            !dir.join("manifest.bin").exists(),
            "precondition: no manifest commit happened"
        );
        // crash
    }
    let engine = Engine::open(make_norm(), config).expect("reopen");
    assert!(
        match_ids(&engine, "1986 vertex wireless mouse new").contains(&1),
        "an acknowledged insert must survive a pre-first-manifest crash"
    );
    assert!(
        match_ids(&engine, "2008 acme noise cancelling headphones").contains(&2),
        "the upsert's insert half must survive"
    );
    assert!(
        !match_ids(&engine, "2003 acme mechanical keyboard new").contains(&2),
        "the upsert's tombstone half must survive"
    );
    assert!(
        !match_ids(&engine, "2021 acme product omega").contains(&3),
        "an acknowledged delete must survive"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_replay_preserves_queries_accepted_above_current_defaults() {
    let dir = test_dir("wal_recovery_structural_parse_limits");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        max_query_clauses: 300,
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };
    let query = (0..257)
        .map(|i| format!("term{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine
            .try_insert_live(&query, 1, 1)
            .expect("front door accepts configured limit");
        assert!(!dir.join("manifest.bin").exists());
    }

    let reopened = Engine::open(make_norm(), config).expect("recovery uses structural limits");
    assert!(
        match_ids(&reopened, &query).contains(&1),
        "an acknowledged query above today's default must survive WAL replay"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tagged_inserts_survive_wal_recovery() {
    // Tags ride the WAL (v2, ADR-049): a live tagged insert that has NOT been flushed is
    // replayed on reopen WITH its tags, so a filter still narrows correctly.
    let dir = test_dir("tagged_wal");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // keep live inserts in WAL + memtable
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        // A base build writes the manifest first; the manifest-less replay path has
        // its own pin below (`writes_before_first_manifest_survive_crash`). The seed
        // query is unrelated to the title/filters below.
        engine.build_from_queries(&[(99, "zzz placeholder seed".to_string())]);
        engine.insert_live_with_tags(
            "acme chrome",
            1,
            1,
            &[("category".to_string(), "items".to_string())],
        );
        engine.insert_live_with_tags(
            "acme chrome",
            2,
            1,
            &[("category".to_string(), "coins".to_string())],
        );
        // No flush — the tagged inserts live only in the WAL + memtable, so reopen must
        // replay them (with tags) to reconstruct the memtable.
        drop(engine);
    }
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let snap = engine2.snapshot();
    let title = "2020 acme chrome update";

    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();

    let items = snap.compile_tag_predicate(&[("category".to_string(), vec!["items".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &items);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1],
        "WAL-replayed tags narrow category=items to query 1"
    );

    let coins = snap.compile_tag_predicate(&[("category".to_string(), vec!["coins".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &coins);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![2],
        "WAL-replayed tags narrow category=coins to query 2"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_recovery_inserts() {
    // Insert via insert_live (goes through WAL), then simulate crash + recovery.
    let dir = test_dir("wal_recovery");
    let norm = make_norm();
    let queries = sample_queries();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // never auto-flush
        ..EngineConfig::default()
    };

    // 1) Build base segment, then add live inserts (not flushed)
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&queries);

    // These go to the memtable + WAL but are NOT flushed to segments
    engine.insert_live("product omega preview", 100, 1);
    engine.insert_live("portable monitor new", 101, 1);

    let title_omega = "Product Omega 2019 Summit Chrome Preview";
    let title_monitor = "Portable Monitor 2019 Acme Chrome New";
    let expected_omega = match_ids(&engine, title_omega);
    let expected_monitor = match_ids(&engine, title_monitor);

    drop(engine); // simulate crash

    // 2) Recover
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let actual_omega = match_ids(&engine2, title_omega);
    let actual_monitor = match_ids(&engine2, title_monitor);

    assert_eq!(
        expected_omega, actual_omega,
        "WAL recovery lost omega insert"
    );
    assert_eq!(
        expected_monitor, actual_monitor,
        "WAL recovery lost monitor insert"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A complete corrupt frame may contain an acknowledged write. Refuse the entire
/// recovery without resynchronizing, truncating, or silently returning a partial prefix.
#[test]
fn wal_recovery_refuses_a_mid_file_bitflip_without_modifying_the_log() {
    use reverse_rusty::wal::Wal;

    let dir = test_dir("wal_midfile_bitflip");
    let wal_path = dir.join("wal.log");

    {
        let mut wal = Wal::open(&wal_path, false).unwrap();
        wal.append_insert(1, 1, "first record aaa", &[]).unwrap();
        wal.append_insert(2, 1, "second record bbb", &[]).unwrap();
        wal.append_insert(3, 1, "third record ccc", &[]).unwrap();
    }

    // File layout: 8-byte header, then frames of [len:u32][crc:u32][body:len].
    let mut bytes = std::fs::read(&wal_path).unwrap();
    let frame1_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let frame2_start = 8 + 8 + frame1_len;
    let frame2_len =
        u32::from_le_bytes(bytes[frame2_start..frame2_start + 4].try_into().unwrap()) as usize;
    assert!(frame2_len > 12, "sanity: frame 2 has a body to corrupt");
    // Flip one bit in the middle of frame 2's BODY (its CRC no longer matches).
    let target = frame2_start + 8 + frame2_len / 2;
    bytes[target] ^= 0x01;
    std::fs::write(&wal_path, &bytes).unwrap();

    assert_eq!(
        Wal::recover(&wal_path).err().unwrap().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(
        Wal::open(&wal_path, true).err().unwrap().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(std::fs::read(&wal_path).unwrap(), bytes);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_recovery_reports_corrupt_tail() {
    use reverse_rusty::wal::Wal;
    use std::io::Write;

    let dir = test_dir("wal_corrupt_tail");
    let wal_path = dir.join("wal.log");

    // Write a valid WAL with two inserts
    {
        let mut wal = Wal::open(&wal_path, false).unwrap();
        wal.append_insert(1, 1, "wireless mouse item", &[]).unwrap();
        wal.append_insert(2, 1, "mechanical keyboard new", &[])
            .unwrap();
    }

    // Append garbage to simulate a torn write
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF])
            .unwrap();
    }

    // Recover and check that we get the valid entries + skipped bytes reported
    let recovery = Wal::recover(&wal_path).unwrap();
    assert_eq!(
        recovery.entries.len(),
        2,
        "should recover both valid entries"
    );
    assert!(
        recovery.skipped_bytes > 0,
        "should report skipped bytes from corrupt tail"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
