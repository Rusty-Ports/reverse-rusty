use super::*;
use crate::storage::framed_log::write_frame;

fn framed(body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::from(WAL_MAGIC);
    bytes.extend_from_slice(&WAL_VERSION.to_le_bytes());
    write_frame(&mut bytes, body).unwrap();
    bytes
}

#[test]
fn reopen_repairs_every_partial_frame_before_acknowledging_new_writes() {
    let path = scratch_path("repair_then_append");
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert(1, 1, "alpha", &[]).unwrap();
    let prefix = std::fs::read(&path).unwrap();
    wal.append_insert(99, 1, "incomplete", &[]).unwrap();
    drop(wal);
    let bytes = std::fs::read(&path).unwrap();
    let frame = &bytes[prefix.len()..];
    let tails = [1, 4, 7, 8, 12, frame.len() - 1]
        .into_iter()
        .map(|len| frame[..len].to_vec())
        .chain(std::iter::once(vec![0; 64]));
    for tail in tails {
        let len = tail.len();
        let mut damaged = prefix.clone();
        damaged.extend_from_slice(&tail);
        std::fs::write(&path, damaged).unwrap();
        assert_eq!(Wal::recover(&path).unwrap().skipped_bytes, len);
        let mut wal = Wal::open(&path, true).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), prefix);
        assert_eq!(wal.size_bytes(), prefix.len() as u64);
        assert_eq!(wal.pending_entries(), 1);
        assert_eq!(wal.take_repaired_tail_bytes(), len);
        assert_eq!(wal.take_repaired_tail_bytes(), 0);
        assert_eq!(wal.append_insert(2, 1, "beta", &[]).unwrap(), 2);
        drop(wal);
        let wal = Wal::open(&path, true).unwrap();
        assert_eq!(wal.pending_entries(), 2);
        let recovered = Wal::recover(&path).unwrap();
        assert_eq!(recovered.skipped_bytes, 0);
        let ids: Vec<_> = recovered
            .entries
            .iter()
            .map(|entry| match entry {
                WalEntry::Insert { logical, .. } => *logical,
                other => panic!("unexpected entry {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec![1, 2], "partial length {len}");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn complete_incompatible_frames_are_never_repaired_or_skipped() {
    let path = scratch_path("invalid_complete_payloads");
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert_with_source_generation(1, 1, "alpha", &[], None, 1, false)
        .unwrap();
    drop(wal);
    let bytes = std::fs::read(&path).unwrap();
    let body = bytes[16..].to_vec();
    let mut cases = Vec::new();
    let mut unknown_op = body.clone();
    unknown_op[8] = 99;
    cases.push(unknown_op);
    cases.push(body[..9].to_vec());
    let mut invalid_utf8 = body.clone();
    invalid_utf8[25] = 0xff;
    cases.push(invalid_utf8);
    let mut invalid_text_len = body.clone();
    invalid_text_len[21..25].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(invalid_text_len);
    let mut invalid_tag_count = body.clone();
    invalid_tag_count[30..32].copy_from_slice(&u16::MAX.to_le_bytes());
    cases.push(invalid_tag_count);
    let mut unknown_extension = body.clone();
    unknown_extension[32] = b'?';
    cases.push(unknown_extension);
    let mut zero_generation = body.clone();
    zero_generation[36..44].fill(0);
    cases.push(zero_generation);
    let mut invalid_flag = body.clone();
    invalid_flag[44] = 2;
    cases.push(invalid_flag);
    let mut missing_priority = body.clone();
    missing_priority[44] = 1;
    cases.push(missing_priority);
    let mut trailing_priority = body.clone();
    trailing_priority.extend_from_slice(&[0; 8]);
    cases.push(trailing_priority);
    for (i, body) in cases.into_iter().enumerate() {
        let mut invalid = framed(&body);
        invalid.extend_from_slice(&[0xaa; 3]); // must refuse before repairing even this tail
        std::fs::write(&path, &invalid).unwrap();
        assert_eq!(
            Wal::recover(&path).err().unwrap().kind(),
            std::io::ErrorKind::InvalidData,
            "case {i}"
        );
        assert_eq!(
            Wal::open(&path, true).err().unwrap().kind(),
            std::io::ErrorKind::InvalidData,
            "case {i}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), invalid, "case {i}");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn future_header_refuses_and_known_legacy_header_upgrades_before_append() {
    let path = scratch_path("header_fence");
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert(1, 1, "alpha", &[]).unwrap();
    drop(wal);
    let original = std::fs::read(&path).unwrap();
    for version in [0, WAL_VERSION + 1, u32::MAX] {
        let mut bytes = original.clone();
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&[0xaa; 3]);
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            Wal::open(&path, true).err().unwrap().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    for version in 1..WAL_VERSION {
        let mut bytes = original.clone();
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let mut wal = Wal::open(&path, true).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "header upgrade overwrites offset 4"
        );
        wal.append_insert(2, 1, "beta", &[]).unwrap();
        drop(wal);
        assert_eq!(Wal::recover(&path).unwrap().entries.len(), 2);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn legacy_insert_without_tags_remains_readable_after_header_upgrade() {
    let path = scratch_path("legacy_no_tag_section");
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert(1, 1, "alpha", &[]).unwrap();
    drop(wal);
    let bytes = std::fs::read(&path).unwrap();
    // V1 payload ends at the text, before the two-byte tag count.
    let mut legacy = framed(&bytes[16..bytes.len() - 2]);
    legacy[4..8].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, legacy).unwrap();
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert_ranked(2, 1, "beta", &[], 9).unwrap();
    drop(wal);
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    assert!(
        matches!(&recovered.entries[0], WalEntry::Insert {logical: 1, tags, priority: None, source_generation: None, ..} if tags.is_empty())
    );
    assert!(matches!(
        &recovered.entries[1],
        WalEntry::Insert {
            logical: 2,
            priority: Some(9),
            ..
        }
    ));
    let _ = std::fs::remove_file(path);
}

#[test]
fn oversized_tag_fields_return_errors_before_writing_any_frame() {
    let path = scratch_path("oversized_tags");
    let mut wal = Wal::open(&path, true).unwrap();
    let original = std::fs::read(&path).unwrap();
    for tags in [
        vec![("x".repeat(usize::from(u16::MAX) + 1), String::new())],
        vec![(String::new(), "x".repeat(usize::from(u16::MAX) + 1))],
        vec![(String::new(), String::new()); usize::from(u16::MAX) + 1],
    ] {
        assert_eq!(
            wal.append_insert(1, 1, "alpha", &tags).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
    wal.append_insert(2, 1, "beta", &[]).unwrap();
    drop(wal);
    assert_eq!(Wal::recover(&path).unwrap().entries.len(), 1);
    let _ = std::fs::remove_file(path);
}

#[test]
fn damaged_length_of_an_acknowledged_last_frame_refuses_repair() {
    let path = scratch_path("damaged_last_frame_length");
    let mut wal = Wal::open(&path, true).unwrap();
    wal.append_insert(1, 1, "alpha", &[]).unwrap();
    drop(wal);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(
        Wal::recover(&path).err().unwrap().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(
        Wal::open(&path, true).err().unwrap().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let _ = std::fs::remove_file(path);
}
