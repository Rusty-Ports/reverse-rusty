use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::cluster::shard::{EventSink, LocalShard, Shard};
use crate::cluster::translog::TRANSLOG_FILE;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

#[test]
fn self_restart_repairs_tail_reports_it_outside_locks_and_preserves_new_appends() {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    let mut lc = String::new();
    let queries: Vec<_> = ["wireless mouse", "mechanical keyboard"]
        .into_iter()
        .map(|dsl| {
            let ast = crate::dsl::parse(dsl).unwrap();
            (
                dsl,
                crate::compile::extract(&ast, &norm, &mut dict, &mut lc),
            )
        })
        .collect();
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let tags = Arc::new(tags);
    let dir = std::env::temp_dir().join(format!("rr_shard_tail_repair_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        wal_sync_on_write: true,
        ..EngineConfig::default()
    };
    let open = || {
        Arc::new(
            LocalShard::new_durable(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tags),
                config.clone(),
            )
            .unwrap(),
        )
    };
    let shard = open();
    shard
        .insert_extracted_with_tags(&queries[0].1, 1, 1, queries[0].0, &[])
        .unwrap();
    drop(shard);
    let path = dir.join(TRANSLOG_FILE);
    let valid_len = std::fs::metadata(&path).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[32, 0, 0, 0, 0xaa, 0xbb, 0xcc, 0xdd])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let shard = open();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    let weak = Arc::downgrade(&shard);
    let sink: EventSink = Arc::new(move |event| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::WalTornTail,
            error,
            ..
        } = event
        {
            // Acquires the engine mutex: callback under that mutex would deadlock.
            assert_eq!(weak.upgrade().unwrap().live_sources().unwrap().len(), 1);
            seen.lock().unwrap().push(error.clone());
        }
    });
    // The shard hands its startup events back; whoever installed the sink delivers them.
    for event in shard.set_event_sink(Arc::clone(&sink)) {
        sink(&event);
    }
    assert_eq!(*events.lock().unwrap(), vec!["8 bytes"]);
    let again = shard.set_event_sink(Arc::new(|_| panic!("no event is expected after startup")));
    assert!(again.is_empty(), "startup event must drain exactly once");
    shard
        .insert_extracted_with_tags(&queries[1].1, 2, 1, queries[1].0, &[])
        .unwrap();
    drop(shard);
    let shard = open();
    let startup = shard.set_event_sink(Arc::new(|_| panic!("no event is expected")));
    assert!(
        startup.is_empty(),
        "clean restart must not report another torn tail"
    );
    let mut ids = shard.live_logical_ids().unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
    let _ = std::fs::remove_dir_all(dir);
}

/// A durable shard writes its checkpoint file after its translog exists whole. So when a
/// restarting shard finds that file and a translog shorter than its header, the translog has
/// lost its content, whatever position the checkpoint records, and the shard is refused. It
/// is not an interrupted creation (ADR-212): that leaves no checkpoint file, and the next
/// start is a fresh one that makes a new translog.
#[test]
fn a_restarting_shard_is_not_given_an_empty_translog() {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let tags = Arc::new(tags);
    let dir = std::env::temp_dir().join(format!("rr_shard_short_translog_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        wal_sync_on_write: true,
        ..EngineConfig::default()
    };
    let open = || {
        LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tags),
            config.clone(),
        )
    };
    // An interrupted first start: a translog with no header and no checkpoint file. The next
    // start is a fresh one, and makes a new translog.
    let path = dir.join(TRANSLOG_FILE);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&path, b"").unwrap();
    drop(open().expect("a fresh durable shard"));
    let header = std::fs::read(&path).unwrap();
    assert_eq!(header.len(), 8, "a fresh translog holds only its header");
    for held in [0usize, 4, 7] {
        std::fs::write(&path, &header[..held]).unwrap();
        assert!(
            open().is_err(),
            "{held} header bytes: a restarting shard was given an empty translog"
        );
        assert_eq!(std::fs::read(&path).unwrap(), &header[..held]);
    }
    // With the translog whole again the shard restarts.
    std::fs::write(&path, &header).unwrap();
    drop(open().expect("restart"));
    let _ = std::fs::remove_dir_all(dir);
}
