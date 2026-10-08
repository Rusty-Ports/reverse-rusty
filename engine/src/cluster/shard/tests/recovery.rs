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

/// A durable shard with rows 1 and 2 in a sealed segment and row 1 then deleted: the next
/// seal rewrites that segment, which replaces the file the shard's checkpoint file names.
struct ShardWithADeletion {
    shard: Arc<LocalShard>,
    dir: std::path::PathBuf,
    /// Opens the shard again from its directory, as a restart does.
    open: Box<dyn Fn() -> Result<LocalShard, crate::cluster::shard::ShardError>>,
    /// Rows 3 and 4, compiled and not inserted.
    spare: Vec<(&'static str, crate::compile::Extracted)>,
}

impl ShardWithADeletion {
    fn insert_spare(&self) {
        for (offset, (dsl, extracted)) in self.spare.iter().enumerate() {
            self.shard
                .insert_extracted_with_tags(extracted, offset as u64 + 3, 1, dsl, &[])
                .unwrap();
        }
    }
}

fn a_shard_with_a_deletion_in_a_sealed_segment(
    tag: &str,
    owns_its_commit_record: bool,
) -> ShardWithADeletion {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    let mut lc = String::new();
    let mut rows: Vec<_> = [
        "wireless mouse",
        "mechanical keyboard",
        "usb hub",
        "laptop stand",
    ]
    .into_iter()
    .map(|dsl| {
        let ast = crate::dsl::parse(dsl).unwrap();
        (
            dsl,
            crate::compile::extract(&ast, &norm, &mut dict, &mut lc),
        )
    })
    .collect();
    let spare = rows.split_off(2);
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
    ShardWithADeletion {
        shard,
        dir,
        open: Box::new(open),
        spare,
    }
}

/// A shard node's shard restarts from its own checkpoint file. A seal rewrites the segment
/// that holds the deletion and then writes the checkpoint file; a kill in between left a
/// checkpoint file that named a segment the rewrite had already removed, and the node could
/// not restart. The old file is kept while the checkpoint file on disk names it, whatever
/// asks for its removal.
#[test]
fn a_shard_node_restarts_after_a_kill_between_a_rewrite_and_its_checkpoint_file() {
    let ShardWithADeletion {
        shard, dir, open, ..
    } = a_shard_with_a_deletion_in_a_sealed_segment("node_kill_window", true);
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
    // The node's seal worker asks for the removal whatever the seal's outcome.
    shard.remove_replaced_files();
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
    restarted.seal_for_checkpoint().expect("seal");
    restarted.remove_replaced_files();
    let named_now = restarted.segment_filenames().expect("segment files");
    drop(restarted);
    let on_disk = segment_files(&dir);
    assert!(
        named_now.iter().all(|name| on_disk.contains(name)),
        "the files the checkpoint file names are there: {named_now:?} in {on_disk:?}"
    );
    let again = open().expect("restart");
    assert_eq!(again.live_logical_ids().unwrap(), vec![2]);
    let _ = std::fs::remove_dir_all(dir);
}

/// Who names a shard's files decides what it does with one it has replaced. Its own
/// checkpoint file (a shard node): it is kept through the seal, and removed when the node
/// asks once the checkpoint file no longer names it. A coordinator's manifest (the default):
/// it is left where it is, for the coordinator to remove after its own commit. Nothing (an
/// in-process replica): it is removed at once.
#[test]
fn who_names_a_shards_files_decides_what_it_does_with_a_replaced_one() {
    for named_by in [
        "its own checkpoint file",
        "a coordinator's manifest",
        "nothing",
    ] {
        let ShardWithADeletion { shard, dir, .. } = a_shard_with_a_deletion_in_a_sealed_segment(
            &format!("named_by_{}", named_by.len()),
            named_by == "its own checkpoint file",
        );
        if named_by == "nothing" {
            shard.no_record_names_your_segment_files();
        }
        let named = segment_files(&dir);
        let old_is_there = || {
            let now = segment_files(&dir);
            named.iter().all(|name| now.contains(name))
        };
        shard.seal_for_checkpoint().expect("seal");
        assert_eq!(
            old_is_there(),
            named_by != "nothing",
            "named by {named_by}: after the seal"
        );
        shard.remove_replaced_files();
        assert_eq!(
            old_is_there(),
            named_by == "a coordinator's manifest",
            "named by {named_by}: after the node asked for the removal"
        );
        assert_eq!(shard.live_logical_ids().unwrap(), vec![2]);
        drop(shard);
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A seal removes nothing that was kept. A shard is also sealed to serve as a recovery
/// source, outside the node's installation barrier, and at that moment a recovery into its
/// own slot can have written a received file under the name of a kept one. Only the removal
/// the node asks for under the barrier takes a kept file away.
#[test]
fn a_seal_alone_removes_no_kept_file() {
    let ShardWithADeletion { shard, dir, .. } =
        a_shard_with_a_deletion_in_a_sealed_segment("seal_alone", true);
    let kept = segment_files(&dir);
    shard.seal_for_checkpoint().expect("seal");
    assert!(
        !shard.segment_filenames().unwrap().contains(&kept[0]),
        "precondition: the checkpoint file no longer names the replaced file"
    );
    // A file arrives under the kept file's name, and the shard is sealed again.
    let received = dir.join("segments").join(&kept[0]);
    std::fs::write(&received, b"a segment received from a recovery source").unwrap();
    shard.seal_for_checkpoint().expect("seal");
    assert_eq!(
        std::fs::read(&received).ok().as_deref(),
        Some(&b"a segment received from a recovery source"[..]),
        "a seal removed a file by the name of one the shard had kept"
    );
    shard.remove_replaced_files();
    assert!(!received.exists(), "the node's removal takes it");
    drop(shard);
    let _ = std::fs::remove_dir_all(dir);
}

/// A replaced file that the checkpoint file never named is removed at once: nothing could
/// reopen from it. Otherwise a shard that is not sealed for a long time (a replica on a
/// node is never sealed by its coordinator) would keep every segment it ever compacted.
#[test]
fn a_shard_node_removes_at_once_a_replaced_file_its_checkpoint_file_never_named() {
    let with = a_shard_with_a_deletion_in_a_sealed_segment("never_named", true);
    with.shard.seal_for_checkpoint().expect("seal");
    with.shard.remove_replaced_files();
    let named = segment_files(&with.dir);
    assert_eq!(named.len(), 1, "one segment, named by the checkpoint file");

    // Rows 3 and 4 go into a new segment, which no checkpoint file names yet.
    with.insert_spare();
    with.shard.flush().expect("flush");
    let unnamed: Vec<String> = segment_files(&with.dir)
        .into_iter()
        .filter(|name| !named.contains(name))
        .collect();
    assert_eq!(unnamed.len(), 1, "one new segment: {unnamed:?}");
    // A deletion in it: the seal rewrites it, before it writes the checkpoint file.
    with.shard.delete_by_logical_id(3).expect("delete");
    with.shard.seal_for_checkpoint().expect("seal");
    let now = segment_files(&with.dir);
    assert!(
        !now.contains(&unnamed[0]),
        "a replaced file that no checkpoint file ever named was kept: {now:?}"
    );
    assert_eq!(with.shard.live_logical_ids().unwrap(), vec![2, 4]);
    let _ = std::fs::remove_dir_all(&with.dir);
}

/// When the checkpoint file cannot be read, nothing is removed on its account: the shard
/// keeps what it replaces, as if the file named everything.
#[test]
fn a_shard_node_keeps_a_replaced_file_when_its_checkpoint_file_cannot_be_read() {
    let ShardWithADeletion { shard, dir, .. } =
        a_shard_with_a_deletion_in_a_sealed_segment("unreadable_record", true);
    let named = segment_files(&dir);
    let checkpoint_file = dir.join("shard.ckpt");
    let good = std::fs::read(&checkpoint_file).expect("the checkpoint file");
    std::fs::write(&checkpoint_file, b"not a checkpoint file").unwrap();
    // The rewrite happens while the record is unreadable; the seal then cannot finish.
    let blocker = dir.join("shard.ckpt.tmp");
    std::fs::create_dir_all(&blocker).expect("block the checkpoint file");
    assert!(shard.seal_for_checkpoint().is_err());
    std::fs::remove_dir_all(&blocker).expect("unblock");
    shard.remove_replaced_files();
    let now = segment_files(&dir);
    assert!(
        now.len() > named.len() && named.iter().all(|name| now.contains(name)),
        "a file was removed on the strength of a checkpoint file nobody could read: {now:?}"
    );
    std::fs::write(&checkpoint_file, good).unwrap();
    drop(shard);
    let _ = std::fs::remove_dir_all(dir);
}

/// A coordinator's build creates its shards with `create_durable`, which refuses a directory
/// that holds a shard (ADR-215). The constructor a shard node uses restores that shard: right
/// for a node restarting over its own data, and how the rows of an unfinished build came
/// back into the build that followed it.
#[test]
fn creating_a_shard_refuses_a_directory_that_holds_one() {
    let norm = Arc::new(Normalizer::default_vocab().unwrap());
    let mut dict = Dict::new();
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let tags = Arc::new(tags);
    let dir = std::env::temp_dir().join(format!("rr_shard_create_only_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let create = || {
        LocalShard::create_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tags),
            config.clone(),
        )
    };
    drop(create().expect("a shard in an empty directory"));
    match create() {
        Err(crate::cluster::shard::ShardError::Config(message)) => {
            assert!(message.contains("already holds a shard"), "{message}");
        }
        Err(other) => panic!("refused with the wrong kind of error: {other:?}"),
        Ok(_) => panic!("a shard was created over the one the directory holds"),
    }
    // The node's constructor restores it.
    drop(
        LocalShard::new_durable(norm, dict, tags, config.clone())
            .expect("a node restarts over its own shard"),
    );
    let _ = std::fs::remove_dir_all(dir);
}
