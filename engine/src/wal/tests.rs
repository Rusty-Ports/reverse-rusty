use super::*;

mod recovery;

fn scratch_path(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "reverse_rusty_wal_{}_{}.log",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn append_surfaces_write_errors_instead_of_swallowing() {
    let path = scratch_path("append_err");
    let mut wal = Wal::open(&path, false).unwrap();
    // A healthy append succeeds.
    assert!(wal.append_insert(1, 1, "wireless mouse", &[]).is_ok());
    // Once the file can no longer be written, the error is returned (not swallowed).
    wal.break_writes_for_test();
    assert!(wal.append_insert(2, 1, "mechanical keyboard", &[]).is_err());
    assert!(wal.append_tombstone(u32::MAX, 0).is_err());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fsync_each_write_round_trips_through_recovery() {
    let path = scratch_path("fsync_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 2, "product omega", &[]).unwrap();
        wal.append_tombstone(0, 3).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[0] {
        WalEntry::Insert {
            logical,
            version,
            text,
            ..
        } => {
            assert_eq!(*logical, 7);
            assert_eq!(*version, 2);
            assert_eq!(text, "product omega");
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn insert_tags_round_trip_through_recovery_and_untagged_reads_empty() {
    let path = scratch_path("tags_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        // A tagged insert (the ADR-049 case) and an untagged one.
        wal.append_insert(
            7,
            1,
            "1994 north star",
            &[
                ("category".to_string(), "items".to_string()),
                ("status".to_string(), "active".to_string()),
            ],
        )
        .unwrap();
        wal.append_insert(8, 1, "no tags here", &[]).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    match &recovered.entries[0] {
        WalEntry::Insert { logical, tags, .. } => {
            assert_eq!(*logical, 7);
            assert_eq!(
                tags,
                &vec![
                    ("category".to_string(), "items".to_string()),
                    ("status".to_string(), "active".to_string()),
                ]
            );
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Insert { logical, tags, .. } => {
            assert_eq!(*logical, 8);
            assert!(tags.is_empty(), "an untagged insert recovers empty tags");
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn typed_priority_extension_round_trips_without_new_opcode() {
    let path = scratch_path("priority_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert_ranked(
            7,
            3,
            "acme chrome",
            &[("priority".to_string(), "-55".to_string())],
            -55,
        )
        .unwrap();
        wal.append_upsert_ranked(
            7,
            4,
            "acme chrome premium",
            &[("priority".to_string(), "99".to_string())],
            99,
        )
        .unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    match &recovered.entries[0] {
        WalEntry::Insert { priority, .. } => assert_eq!(*priority, Some(-55)),
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Upsert { priority, .. } => assert_eq!(*priority, Some(99)),
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn source_generation_extension_round_trips_with_optional_priority() {
    let path = scratch_path("source_generation_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert_with_source_generation(7, 3, "acme chrome", &[], None, 41, false)
            .unwrap();
        wal.append_upsert_with_source_generation(
            7,
            4,
            "acme chrome premium",
            &[("priority".to_string(), "-55".to_string())],
            Some(-55),
            42,
            true,
        )
        .unwrap();
    }

    let recovered = Wal::recover(&path).unwrap();
    match &recovered.entries[0] {
        WalEntry::Insert {
            source_generation,
            priority,
            class_d_accepted,
            ..
        } => {
            assert_eq!(*source_generation, Some(41));
            assert_eq!(*priority, None);
            assert!(!class_d_accepted);
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Upsert {
            source_generation,
            priority,
            class_d_accepted,
            ..
        } => {
            assert_eq!(*source_generation, Some(42));
            assert_eq!(*priority, Some(-55));
            assert!(class_d_accepted);
        }
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn delete_by_logical_round_trips_through_recovery() {
    let path = scratch_path("delete_logical_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 1, "product omega", &[]).unwrap();
        wal.append_delete_logical(7).unwrap();
        // Old positional frames still coexist in the same file.
        wal.append_tombstone(u32::MAX, 3).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 3);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[1] {
        WalEntry::DeleteByLogical { logical, .. } => assert_eq!(*logical, 7),
        other => panic!("expected DeleteByLogical, got {other:?}"),
    }
    match &recovered.entries[2] {
        WalEntry::Tombstone {
            seg_idx, local_id, ..
        } => {
            assert_eq!(*seg_idx, u32::MAX);
            assert_eq!(*local_id, 3);
        }
        other => panic!("expected Tombstone, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn upsert_round_trips_with_tags_and_coexists_with_insert() {
    let path = scratch_path("upsert_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 1, "product omega", &[]).unwrap();
        wal.append_upsert(
            7,
            2,
            "product omega pro",
            &[("category".to_string(), "items".to_string())],
        )
        .unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[1] {
        WalEntry::Upsert {
            logical,
            version,
            text,
            tags,
            ..
        } => {
            assert_eq!(*logical, 7);
            assert_eq!(*version, 2);
            assert_eq!(text, "product omega pro");
            assert_eq!(tags, &vec![("category".to_string(), "items".to_string())]);
        }
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn last_seq_is_monotonic_across_reset() {
    let path = scratch_path("last_seq_monotonic");
    let mut wal = Wal::open(&path, false).unwrap();
    assert_eq!(wal.last_seq(), 0, "no entries yet");
    wal.append_insert(1, 1, "wireless mouse", &[]).unwrap();
    wal.append_delete_logical(1).unwrap();
    assert_eq!(wal.last_seq(), 2);
    wal.reset().unwrap();
    assert_eq!(wal.last_seq(), 2, "reset must not rewind the watermark");
    wal.append_insert(2, 1, "mechanical keyboard", &[]).unwrap();
    assert_eq!(wal.last_seq(), 3);
    let _ = std::fs::remove_file(&path);
}

/// Micro-benchmark: per-write fsync vs. checkpoint-only. Ignored by default
/// (it does real device flushes). Run with:
///   cargo test --release -p reverse-rusty --lib wal::tests::bench_fsync_cost -- --ignored --nocapture
#[test]
#[ignore = "benchmark: does real device flushes; run with --ignored"]
fn bench_fsync_cost() {
    use std::time::Instant;
    const N: u64 = 5_000;
    for &(label, fsync) in &[
        ("checkpoint-only (fsync=false)", false),
        ("per-write fsync=true", true),
    ] {
        let path = scratch_path(&format!("bench_{fsync}"));
        let mut wal = Wal::open(&path, fsync).unwrap();
        let t = Instant::now();
        for i in 0..N {
            wal.append_insert(i, 1, "1994 north star wireless mouse limited pro", &[])
                .unwrap();
        }
        let per = t.elapsed().as_secs_f64() / N as f64;
        println!(
            "{label:35}: {:.1} us/append   ({:.0} appends/sec)",
            per * 1e6,
            1.0 / per
        );
        let _ = std::fs::remove_file(&path);
    }
}

/// What `Wal::open` writes at the start of a log.
fn header_bytes() -> Vec<u8> {
    let mut header = WAL_MAGIC.to_vec();
    header.extend_from_slice(&WAL_VERSION.to_le_bytes());
    header
}

/// A reset replaces the log with a new file; it does not truncate the one in place. A
/// truncate leaves a moment at which the file holds no header, and a crash in that moment
/// left a log the next start refused. The new file takes appends after its header.
#[test]
fn reset_publishes_a_new_log_and_appends_follow_its_header() {
    let path = scratch_path("reset_replaces");
    let mut wal = Wal::open(&path, false).unwrap();
    wal.append_insert(1, 1, "wireless mouse", &[]).unwrap();
    wal.sync().unwrap();
    #[cfg(unix)]
    let before = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap());

    wal.reset().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), header_bytes());
    assert_eq!(wal.size_bytes(), WAL_HEADER_SIZE as u64);
    assert_eq!(wal.pending_entries(), 0);
    #[cfg(unix)]
    assert_ne!(
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap()),
        before,
        "the log must be replaced, not truncated in place"
    );

    // Twice, so the handle a reset leaves behind is itself reset.
    wal.reset().unwrap();
    wal.append_insert(2, 1, "mechanical keyboard", &[]).unwrap();
    wal.sync().unwrap();
    assert!(std::fs::read(&path).unwrap().starts_with(&header_bytes()));
    assert_eq!(wal.size_bytes(), std::fs::metadata(&path).unwrap().len());
    drop(wal);

    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(
        recovered.entries.len(),
        1,
        "only the record after the reset"
    );
    assert!(matches!(
        &recovered.entries[0],
        WalEntry::Insert { logical: 2, .. }
    ));
    let _ = std::fs::remove_file(&path);
}

/// A crash between the old truncate and the header write left `wal.log` with fewer than
/// eight bytes, and a first start could do the same. Such a file never held a record, so
/// it opens as an empty log instead of stopping the node.
#[test]
fn a_log_whose_header_was_interrupted_opens_empty() {
    let header = header_bytes();
    // The interrupted header may be an earlier release's: v3 is `PWAL\x03\0\0\0`.
    let earlier = Wal::header(3);
    let cases = (0..WAL_HEADER_SIZE)
        .map(|written| (written, &header[..written]))
        .chain((5..WAL_HEADER_SIZE).map(|written| (written + 100, &earlier[..written])));
    for (written, prefix) in cases {
        let path = scratch_path(&format!("interrupted_header_{written}"));
        std::fs::write(&path, prefix).unwrap();
        let recovered =
            Wal::recover(&path).unwrap_or_else(|error| panic!("{written} header bytes: {error}"));
        assert!(recovered.entries.is_empty());
        let mut wal =
            Wal::open(&path, false).unwrap_or_else(|error| panic!("{written} bytes: {error}"));
        assert_eq!(std::fs::read(&path).unwrap(), header, "{written} bytes");
        wal.append_insert(9, 1, "wireless mouse", &[]).unwrap();
        wal.sync().unwrap();
        drop(wal);
        assert_eq!(Wal::recover(&path).unwrap().entries.len(), 1);
        let _ = std::fs::remove_file(&path);
    }
}

/// Only a prefix of the header is read that way. Any other short file, and any file with a
/// full but wrong header, is still refused: it is not something this code wrote.
#[test]
fn a_short_file_that_is_not_a_header_prefix_is_refused() {
    let path = scratch_path("not_a_header");
    let refused: [&[u8]; 7] = [
        b"XYZ",
        b"PWAX",
        b"PXAL\x03\x00\x00\x00",
        // A header prefix of a format this reader does not support: a later one, version
        // zero, and version bytes no header has. The full header would be refused too.
        b"PWAL\x08\x00\x00",
        b"PWAL\x00",
        b"PWAL\x07\x01",
        b"PWAL\x07\x00\x00\x01",
    ];
    for content in refused {
        std::fs::write(&path, content).unwrap();
        assert!(Wal::recover(&path).is_err(), "{content:?}");
        assert!(Wal::open(&path, false).is_err(), "{content:?}");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            content,
            "a refused file is not rewritten"
        );
    }
    let _ = std::fs::remove_file(&path);
}

/// A reset that cannot build its replacement has not touched the log: every record is
/// still there and the WAL keeps taking writes.
#[test]
fn a_reset_that_cannot_build_its_replacement_leaves_the_log_in_use() {
    let path = scratch_path("reset_blocked");
    let blocker = Wal::replacement_path(&path);
    let _ = std::fs::remove_dir_all(&blocker);
    let mut wal = Wal::open(&path, false).unwrap();
    wal.append_insert(1, 1, "wireless mouse", &[]).unwrap();
    // A directory where the replacement would be written.
    std::fs::create_dir(&blocker).unwrap();
    wal.reset()
        .expect_err("the replacement cannot be created over a directory");
    wal.append_insert(2, 1, "mechanical keyboard", &[])
        .expect("the log is as it was, so it still takes writes");
    wal.sync().unwrap();
    drop(wal);
    assert_eq!(Wal::recover(&path).unwrap().entries.len(), 2);
    let _ = std::fs::remove_dir(&blocker);
    let _ = std::fs::remove_file(&path);
}
