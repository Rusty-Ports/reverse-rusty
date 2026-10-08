use super::*;

fn framed(body: &[u8]) -> Vec<u8> {
    let mut data = Vec::from(CLOG_MAGIC);
    data.extend_from_slice(&CLOG_VERSION.to_le_bytes());
    write_frame(&mut data, body).unwrap();
    data
}

#[test]
fn reopen_repairs_partial_frames_and_preserves_later_acknowledged_appends() {
    let path = scratch_path("repair_append_reopen");
    let prefix = framed(&FileClusterLog::encode_body(1, &add(1, "alpha")));
    let frame = framed(&FileClusterLog::encode_body(2, &add(99, "incomplete")));
    let frame = &frame[CLOG_HEADER_SIZE..];
    let tails = [1, 4, 7, 8, 12, frame.len() - 1]
        .into_iter()
        .map(|len| frame[..len].to_vec())
        .chain(std::iter::once(vec![0; 64]));
    for tail in tails {
        let len = tail.len();
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&tail);
        std::fs::write(&path, bytes).unwrap();
        let log = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), prefix);
        assert_eq!(log.replay(LogPos(0)).unwrap().skipped_bytes, len);
        assert_eq!(log.replay(LogPos(0)).unwrap().skipped_bytes, 0);
        assert_eq!(log.append(&add(2, "beta")).unwrap(), LogPos(2));
        drop(log);
        let log = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).unwrap();
        let replay = log.replay(LogPos(0)).unwrap();
        assert_eq!(
            replay.entries,
            vec![(LogPos(1), add(1, "alpha")), (LogPos(2), add(2, "beta"))]
        );
        assert_eq!(replay.skipped_bytes, 0);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn complete_unknown_or_malformed_payloads_refuse_open_replay_and_checkpoint() {
    let path = scratch_path("invalid_complete");
    let mutation = ClusterMutation::Add {
        logical: 1,
        version: 1,
        dsl: "alpha".into(),
        tags: Vec::new(),
        placement: crate::ownership::QueryPlacement::selective(
            crate::ownership::PlacementGeneration(9),
            3,
            vec![2],
        )
        .unwrap(),
    };
    let body = FileClusterLog::encode_body(1, &mutation);
    let mut cases = Vec::new();
    let mut unknown_op = body.clone();
    unknown_op[8] = 99;
    cases.push(unknown_op);
    cases.push(body[..9].to_vec());
    let mut short_remove = body[..9].to_vec();
    short_remove[8] = OP_REMOVE;
    cases.push(short_remove);
    let mut invalid_utf8 = body.clone();
    invalid_utf8[25] = 0xff;
    cases.push(invalid_utf8);
    let mut invalid_tags = body.clone();
    invalid_tags[30..34].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(invalid_tags);
    let mut invalid_generation = body.clone();
    invalid_generation[34..42].fill(0);
    cases.push(invalid_generation);
    let mut invalid_shards = body.clone();
    invalid_shards[42..46].fill(0);
    cases.push(invalid_shards);
    let mut invalid_mode = body.clone();
    invalid_mode[46] = 99;
    cases.push(invalid_mode);
    let mut invalid_positions = body.clone();
    invalid_positions[47..51].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(invalid_positions);
    let mut trailing = body.clone();
    trailing.push(0);
    cases.push(trailing);
    for (i, body) in cases.into_iter().enumerate() {
        let healthy = framed(&FileClusterLog::encode_body(1, &add(1, "alpha")));
        std::fs::write(&path, healthy).unwrap();
        let log = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).unwrap();
        let mut invalid = framed(&body);
        invalid.extend_from_slice(&[0xaa; 3]);
        std::fs::write(&path, &invalid).unwrap();
        assert_eq!(
            FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData,
            "case {i}"
        );
        assert!(log.replay(LogPos(0)).is_err(), "case {i}");
        assert!(log.checkpoint(LogPos(1)).is_err(), "case {i}");
        assert_eq!(std::fs::read(&path).unwrap(), invalid, "case {i}");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn complete_crc_failure_and_damaged_length_before_later_records_refuse_repair() {
    let path = scratch_path("middle_corruption");
    let prefix = framed(&FileClusterLog::encode_body(1, &add(1, "alpha")));
    let later = framed(&FileClusterLog::encode_body(2, &add(2, "beta")));
    for damage_length in [false, true] {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&later[CLOG_HEADER_SIZE..]);
        if damage_length {
            bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        } else {
            bytes[16] ^= 1;
        }
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    let _ = std::fs::remove_file(path);
}

/// The eight bytes of a current cluster log's header.
fn current_header() -> Vec<u8> {
    let mut header = CLOG_MAGIC.to_vec();
    header.extend_from_slice(&CLOG_VERSION.to_le_bytes());
    header
}

/// A log whose creation was interrupted is a file shorter than its header. Releases that
/// created the file and then wrote the header could leave one after a crash or a full disk.
/// It never held a record, so its owner can have it finished: it then opens as the empty
/// log it is, takes a write, and reads it back.
#[test]
fn a_log_whose_creation_was_interrupted_is_finished_as_an_empty_log() {
    let header = current_header();
    for held in 0..header.len() {
        let path = scratch_path(&format!("interrupted_{held}"));
        std::fs::write(&path, &header[..held]).unwrap();
        FileClusterLog::finish_interrupted_creation(&path)
            .unwrap_or_else(|error| panic!("{held} header bytes: {error}"));
        assert_eq!(std::fs::read(&path).unwrap(), header, "{held} header bytes");
        {
            let log =
                FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).expect("open");
            log.append(&add(7, "alpha beta")).expect("append");
        }
        let reopened =
            FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).expect("reopen");
        let held_records = reopened.replay(LogPos(0)).expect("replay");
        assert_eq!(
            held_records.entries,
            vec![(LogPos(1), add(7, "alpha beta"))],
            "{held} header bytes"
        );
        assert!(
            !crate::storage::framed_log::replacement_path(&path).exists(),
            "a replacement file was left beside the log"
        );
        let _ = std::fs::remove_file(&path);
    }
}

/// An older release's header, cut short, is the same thing.
#[test]
fn an_interrupted_header_of_an_older_format_is_finished() {
    let path = scratch_path("interrupted_older");
    let mut older = CLOG_MAGIC.to_vec();
    older.extend_from_slice(&(CLOG_VERSION - 1).to_le_bytes());
    std::fs::write(&path, &older[..6]).unwrap();
    FileClusterLog::finish_interrupted_creation(&path).expect("an interrupted older header");
    assert_eq!(std::fs::read(&path).unwrap(), current_header());
    let _ = std::fs::remove_file(&path);
}

/// Opening a log never decides that a short file is an interrupted creation. The file cannot
/// say whether it was interrupted or has lost its content; only the owner knows, and a
/// restarting data node, whose checkpoint file proves the log was once whole, must be
/// refused. So `open` refuses every file shorter than a header and leaves it as it found it.
#[test]
fn open_refuses_a_log_shorter_than_its_header() {
    let header = current_header();
    for floor in [LogPos(0), LogPos(12)] {
        for held in 0..header.len() {
            let path = scratch_path(&format!("short_{}_{held}", floor.0));
            std::fs::write(&path, &header[..held]).unwrap();
            assert!(
                FileClusterLog::open(&path, true, floor, IfMissing::Create).is_err(),
                "{held} header bytes, floor {}: opened",
                floor.0
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                &header[..held],
                "the file was changed"
            );
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// A short file that is not the start of a header is damage. It is not finished into an
/// empty log, and it is still refused.
#[test]
fn a_short_log_that_is_no_header_is_left_alone_and_refused() {
    let path = scratch_path("short_other");
    std::fs::write(&path, b"XY").unwrap();
    FileClusterLog::finish_interrupted_creation(&path).expect("nothing to finish");
    assert_eq!(std::fs::read(&path).unwrap(), b"XY", "the file was changed");
    assert!(FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).is_err());
    // A whole log, and no log at all, are not touched either.
    std::fs::write(&path, current_header()).unwrap();
    FileClusterLog::finish_interrupted_creation(&path).expect("a whole log");
    assert_eq!(std::fs::read(&path).unwrap(), current_header());
    std::fs::remove_file(&path).unwrap();
    FileClusterLog::finish_interrupted_creation(&path).expect("no log");
    assert!(!path.exists(), "a log was created where there was none");
}

/// A new log is written beside its path and renamed in, so a creation that fails part-way
/// leaves nothing at the path. Here the replacement cannot be written at all.
#[test]
fn a_creation_that_fails_leaves_no_file_at_the_path() {
    let path = scratch_path("blocked_creation");
    let blocker = crate::storage::framed_log::replacement_path(&path);
    let _ = std::fs::remove_dir_all(&blocker);
    std::fs::create_dir_all(&blocker).unwrap();
    let failed = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).is_err();
    let left_behind = path.exists();
    std::fs::remove_dir_all(&blocker).unwrap();
    assert!(failed, "the log was created without its replacement");
    assert!(
        !left_behind,
        "a failed creation left a file at the log's path"
    );
    // With the way clear it is created whole.
    FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).expect("create");
    assert_eq!(std::fs::read(&path).unwrap(), current_header());
    let _ = std::fs::remove_file(&path);
}

/// Every caller says what a missing file means (ADR-213). With `Refuse` nothing is created:
/// the caller's commit record says the log existed, so an empty one in its place would hide
/// the loss of everything that was in it. With `Create` a new log is made.
#[test]
fn a_missing_log_is_created_only_when_the_caller_says_so() {
    let path = scratch_path("missing");
    let _ = std::fs::remove_file(&path);
    let refused = FileClusterLog::open(&path, true, LogPos(5), IfMissing::Refuse)
        .err()
        .expect("a missing log was created for a caller that said it existed");
    assert_eq!(refused.kind(), std::io::ErrorKind::NotFound);
    assert!(!path.exists(), "a refused open created a log");

    let log = FileClusterLog::open(&path, true, LogPos(5), IfMissing::Create).expect("create");
    assert_eq!(log.last_pos().expect("position"), LogPos(5));
    drop(log);
    // An existing log opens the same either way.
    FileClusterLog::open(&path, true, LogPos(5), IfMissing::Refuse).expect("reopen");
    let _ = std::fs::remove_file(&path);
}

/// A checkpoint builds the log's replacement before it touches the log. One that cannot
/// build it fails and leaves the log as it was, still taking writes. Before, the append
/// handle was disabled first, so every later write was refused ("log append disabled") until
/// a restart, although nothing had happened to the log.
#[test]
fn a_checkpoint_that_cannot_build_its_replacement_leaves_the_log_taking_writes() {
    let path = scratch_path("checkpoint_blocked");
    let _ = std::fs::remove_file(&path);
    let log = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).expect("open");
    log.append(&add(1, "alpha")).expect("append");
    let blocker = path.with_extension("clog.tmp");
    let _ = std::fs::remove_dir_all(&blocker);
    std::fs::create_dir_all(&blocker).expect("block the replacement");
    let failed = ClusterLog::checkpoint(&log, LogPos(1)).is_err();
    let appended = log.append(&add(2, "beta"));
    std::fs::remove_dir_all(&blocker).expect("unblock");
    assert!(failed, "the checkpoint went ahead without its replacement");
    assert_eq!(
        appended.expect("the log still takes writes"),
        LogPos(2),
        "a failed checkpoint disabled the log"
    );
    let held = log.replay(LogPos(0)).expect("replay");
    assert_eq!(
        held.entries,
        vec![(LogPos(1), add(1, "alpha")), (LogPos(2), add(2, "beta"))],
        "the log is as it was, plus the new write"
    );
    // With the way clear the checkpoint goes through and keeps what is after its position.
    ClusterLog::checkpoint(&log, LogPos(1)).expect("checkpoint");
    assert_eq!(
        log.replay(LogPos(0)).expect("replay").entries,
        vec![(LogPos(2), add(2, "beta"))]
    );
    log.append(&add(3, "gamma"))
        .expect("append after the checkpoint");
    let _ = std::fs::remove_file(&path);
}

/// A checkpoint that fails at its rename has given up the handle it had: from the rename on
/// that handle may address a file that is no longer the log. The log says so, refuses
/// appends, and holds everything it held; reopened, it takes writes again.
#[test]
fn a_checkpoint_that_fails_at_its_rename_leaves_a_log_that_says_it_takes_no_writes() {
    let path = scratch_path("checkpoint_rename_refused");
    let _ = std::fs::remove_file(&path);
    let log = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Create).expect("open");
    log.append(&add(1, "alpha")).expect("append");
    assert!(!ClusterLog::appends_disabled(&log));

    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    FAIL_NEXT_CHECKPOINT_PUBLISH_OF.with(|named| *named.borrow_mut() = Some(name));
    assert!(ClusterLog::checkpoint(&log, LogPos(0)).is_err());
    assert!(
        ClusterLog::appends_disabled(&log),
        "the log does not say that it refuses appends"
    );
    assert!(log.append(&add(2, "beta")).is_err(), "an append was taken");
    drop(log);

    let reopened = FileClusterLog::open(&path, true, LogPos(0), IfMissing::Refuse).expect("reopen");
    assert!(!ClusterLog::appends_disabled(&reopened));
    assert_eq!(
        reopened.replay(LogPos(0)).expect("replay").entries,
        vec![(LogPos(1), add(1, "alpha"))]
    );
    reopened
        .append(&add(2, "beta"))
        .expect("append after the reopen");
    let _ = std::fs::remove_file(&path);
}
