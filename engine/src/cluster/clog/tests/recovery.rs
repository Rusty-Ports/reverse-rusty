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
    for len in [1, 4, 7, 8, 12, frame.len() - 1] {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&frame[..len]);
        std::fs::write(&path, bytes).unwrap();
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), prefix);
        assert_eq!(log.replay(LogPos(0)).unwrap().skipped_bytes, len);
        assert_eq!(log.replay(LogPos(0)).unwrap().skipped_bytes, 0);
        assert_eq!(log.append(&add(2, "beta")).unwrap(), LogPos(2));
        drop(log);
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
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
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
        let mut invalid = framed(&body);
        invalid.extend_from_slice(&[0xaa; 3]);
        std::fs::write(&path, &invalid).unwrap();
        assert_eq!(
            FileClusterLog::open(&path, true, LogPos(0))
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
            FileClusterLog::open(&path, true, LogPos(0))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    let _ = std::fs::remove_file(path);
}
