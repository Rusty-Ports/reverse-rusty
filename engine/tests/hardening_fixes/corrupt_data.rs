//! Fix 2: corrupt data graceful handling (no panics).

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

use crate::harness::{make_norm, match_ids, sample_queries, test_dir};

#[test]
fn incomplete_final_wal_write_recovers_without_losing_the_committed_corpus() {
    let dir = test_dir("corrupt_wal");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.insert_live("wireless mouse 1986 vertex", 1, 1);
    engine.insert_live("mechanical keyboard new", 2, 1);
    engine.flush();
    engine
        .try_insert_live("noise cancelling headphones", 3, 1)
        .unwrap();
    drop(engine);

    // Keep only the new frame header and four payload bytes: an actual
    // incomplete final write, rather than arbitrary corruption.
    let wal_path = dir.join("wal.log");
    let bytes = std::fs::read(&wal_path).unwrap();
    assert!(bytes.len() > 20);
    std::fs::write(&wal_path, &bytes[..20]).unwrap();

    let reopened = Engine::open(make_norm(), config).unwrap();
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 8);
    assert_eq!(match_ids(&reopened, "wireless mouse 1986 vertex"), vec![1]);
    assert_eq!(match_ids(&reopened, "mechanical keyboard new"), vec![2]);
    assert!(match_ids(&reopened, "noise cancelling headphones").is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ambiguous_wal_garbage_is_refused_without_changing_the_log() {
    let dir = test_dir("ambiguous_wal_tail");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::open(make_norm(), config.clone()).unwrap();
    engine.try_insert_live("wireless mouse", 1, 1).unwrap();
    drop(engine);
    let wal_path = dir.join("wal.log");
    let mut bytes = std::fs::read(&wal_path).unwrap();
    // The former synthetic "torn" fixture has a matching payload-prefix CRC:
    // CRC32([0xff; 4]) == 0xffffffff. Its damaged-length interpretation is
    // ambiguous, so it must be preserved and refused rather than truncated.
    bytes.extend_from_slice(&[0xff; 37]);
    std::fs::write(&wal_path, &bytes).unwrap();
    let error = Engine::open(make_norm(), config).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&wal_path).unwrap(), bytes);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn corrupt_segment_file_skipped_on_open() {
    let dir = test_dir("corrupt_seg");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&sample_queries()[..5]);
    engine.flush();
    drop(engine);

    // Corrupt a segment file
    let seg_dir = dir.join("segments");
    if let Ok(entries) = std::fs::read_dir(&seg_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|e| e == "seg") {
                // Overwrite the middle of the file with garbage
                let data = std::fs::read(entry.path()).unwrap();
                if data.len() > 20 {
                    let mut corrupted = data;
                    for b in &mut corrupted[10..20] {
                        *b = 0xDE;
                    }
                    std::fs::write(entry.path(), &corrupted).unwrap();
                }
                break;
            }
        }
    }

    // Reopen should succeed — corrupt segment is skipped
    let reopened = Engine::open(make_norm(), config);
    assert!(
        reopened.is_ok(),
        "engine should open despite corrupt segment"
    );
    let engine = reopened.unwrap();
    assert!(
        engine.skipped_segments > 0,
        "should report skipped segments"
    );
}
