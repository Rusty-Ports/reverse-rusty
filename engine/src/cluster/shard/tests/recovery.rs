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

/// A restarting shard has a checkpoint file, written after its translog existed. A translog
/// that is not there has been lost with every write the shard acknowledged since that
/// checkpoint, and the shard is refused; before ADR-213 it made an empty one and served
/// without them. The refusal changes nothing: with the translog back the shard restarts with
/// its row.
#[test]
fn a_restarting_shard_whose_translog_is_gone_is_refused() {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = crate::dsl::parse("wireless mouse").unwrap();
    let extracted = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let tags = Arc::new(tags);
    let dir = std::env::temp_dir().join(format!("rr_shard_gone_translog_{}", std::process::id()));
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
    let shard = open().expect("a fresh durable shard");
    shard
        .insert_extracted_with_tags(&extracted, 1, 1, "wireless mouse", &[])
        .unwrap();
    drop(shard);
    let path = dir.join(TRANSLOG_FILE);
    let held = std::fs::read(&path).expect("the translog");
    std::fs::remove_file(&path).expect("lose the translog");

    for attempt in 1..=2 {
        match open() {
            Err(error) => {
                let reason = error.to_string();
                assert!(
                    reason.contains("is missing") && reason.contains("Recover the shard"),
                    "refused for another reason: {reason}"
                );
            }
            Ok(shard) => panic!(
                "attempt {attempt}: restarted without its translog; it holds {:?}",
                shard.live_logical_ids().unwrap()
            ),
        }
        assert!(!path.exists(), "a refused restart created a translog");
    }

    std::fs::write(&path, &held).expect("put the translog back");
    let shard = open().expect("with its translog");
    assert_eq!(
        shard.live_logical_ids().unwrap(),
        vec![1],
        "the refusals changed something: the row in the translog did not come back"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// The `.seg` files in a shard's directory.
fn segment_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join("segments"))
        .expect("the shard's segments")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
        })
        .collect();
    names.sort();
    names
}

/// Two rows in a sealed segment, then one of them deleted: the next seal rewrites that
/// segment, which replaces the file the shard's checkpoint file names.
fn a_shard_with_a_deletion_in_a_sealed_segment(
    tag: &str,
    owns_its_commit_record: bool,
) -> (
    Arc<LocalShard>,
    std::path::PathBuf,
    impl Fn() -> Result<LocalShard, crate::cluster::shard::ShardError>,
) {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    let mut lc = String::new();
    let rows: Vec<_> = ["wireless mouse", "mechanical keyboard"]
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
    let dir = std::env::temp_dir().join(format!("rr_shard_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        wal_sync_on_write: true,
        ..EngineConfig::default()
    };
    let open = move || {
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tags),
            config.clone(),
        )?;
        if owns_its_commit_record {
            shard.own_the_commit_record();
        }
        Ok(shard)
    };
    let shard = Arc::new(open().expect("a fresh durable shard"));
    for (id, (dsl, extracted)) in rows.iter().enumerate() {
        shard
            .insert_extracted_with_tags(extracted, id as u64 + 1, 1, dsl, &[])
            .unwrap();
    }
    shard.seal_for_checkpoint().expect("seal");
    assert_eq!(segment_files(&dir).len(), 1, "one sealed segment");
    shard.delete_by_logical_id(1).expect("delete a sealed row");
    (shard, dir, open)
}

/// A shard node's shard restarts from its own checkpoint file. A seal rewrites the segment
/// that holds the deletion and then writes the checkpoint file; a kill in between left a
/// checkpoint file that named a segment the rewrite had already removed, and the node could
/// not restart. The old file is kept until a checkpoint file that no longer names it is
/// written, and removed then.
#[test]
fn a_shard_node_restarts_after_a_kill_between_a_rewrite_and_its_checkpoint_file() {
    let (shard, dir, open) = a_shard_with_a_deletion_in_a_sealed_segment("node_kill_window", true);
    let named = segment_files(&dir);
    // The seal rewrites the segment and then cannot write its checkpoint file.
    let blocker = dir.join("shard.ckpt.tmp");
    std::fs::create_dir_all(&blocker).expect("block the checkpoint file");
    let failed = shard.seal_for_checkpoint().is_err();
    std::fs::remove_dir_all(&blocker).expect("unblock");
    assert!(
        failed,
        "precondition: the checkpoint file could not be written"
    );
    let now = segment_files(&dir);
    assert!(
        now.len() > named.len(),
        "precondition: the segment was rewritten: {now:?}"
    );
    assert!(
        named.iter().all(|name| now.contains(name)),
        "the shard removed a segment its checkpoint file still names"
    );
    drop(shard); // the kill

    let restarted = open().expect("the node restarts from its checkpoint file");
    assert_eq!(
        restarted.live_logical_ids().unwrap(),
        vec![2],
        "the deletion holds"
    );
    // A seal that does write its checkpoint file releases what was replaced.
    restarted.seal_for_checkpoint().expect("seal");
    let named_now = restarted.segment_filenames().expect("segment files");
    drop(restarted);
    let mut on_disk = segment_files(&dir);
    on_disk.retain(|name| named_now.contains(name));
    assert_eq!(
        on_disk.len(),
        named_now.len(),
        "the files the checkpoint file names are there"
    );
    let again = open().expect("restart");
    assert_eq!(again.live_logical_ids().unwrap(), vec![2]);
    let _ = std::fs::remove_dir_all(dir);
}

/// Who names a shard's files decides what it does with one it has replaced. Its own
/// checkpoint file (a shard node): it is removed once a checkpoint file that no longer names
/// it is written. A coordinator's manifest (the default): it is left where it is, for the
/// coordinator to remove after its own commit. Nothing (an in-process replica): it is removed
/// at once.
#[test]
fn who_names_a_shards_files_decides_what_it_does_with_a_replaced_one() {
    for named_by in [
        "its own checkpoint file",
        "a coordinator's manifest",
        "nothing",
    ] {
        let (shard, dir, _open) = a_shard_with_a_deletion_in_a_sealed_segment(
            &format!("named_by_{}", named_by.len()),
            named_by == "its own checkpoint file",
        );
        if named_by == "nothing" {
            shard.no_record_names_your_segment_files();
        }
        let named = segment_files(&dir);
        shard.seal_for_checkpoint().expect("seal");
        let now = segment_files(&dir);
        let old_is_there = named.iter().all(|name| now.contains(name));
        assert_eq!(
            old_is_there,
            named_by == "a coordinator's manifest",
            "named by {named_by}: before {named:?}, after the seal {now:?}"
        );
        assert_eq!(shard.live_logical_ids().unwrap(), vec![2]);
        drop(shard);
        let _ = std::fs::remove_dir_all(dir);
    }
}
