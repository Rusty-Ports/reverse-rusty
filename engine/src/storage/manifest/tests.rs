use super::*;

/// The v3 engine manifest round-trips the WAL watermark + per-segment dead-locals
/// bitmaps (ADR-066) alongside every earlier field.
#[test]
fn engine_manifest_v3_round_trips_watermark_and_tombstones() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v3_rt_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");

    let manifest = Manifest {
        segment_files: vec!["seg_000001.seg".to_string(), "seg_000002.seg".to_string()],
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: false,
        hot_anchor_theta: 0,
        next_seg_id: 3,
        dict_data: vec![1, 2, 3],
        tag_dict_data: vec![4, 5],
        rejected_parse: 7,
        rejected_class_d: 9,
        wal_seq_watermark: 42,
        segment_tombstones: vec![("seg_000001.seg".to_string(), vec![10, 20, 30])],
        source_file_name: "sources.dat".to_string(),
        feature_model_fingerprint: None,
        vocab_data: Vec::new(),
        wal_sealed_through: None,
    };
    write_manifest(&manifest, &path).expect("write");
    let got = read_manifest(&path).expect("read");
    assert_eq!(got.segment_files, manifest.segment_files);
    assert_eq!(got.next_seg_id, manifest.next_seg_id);
    assert_eq!(got.dict_data, manifest.dict_data);
    assert_eq!(got.tag_dict_data, manifest.tag_dict_data);
    assert_eq!(got.rejected_parse, manifest.rejected_parse);
    assert_eq!(got.rejected_class_d, manifest.rejected_class_d);
    assert_eq!(got.wal_seq_watermark, 42);
    assert_eq!(got.segment_tombstones, manifest.segment_tombstones);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_manifest_v6_fences_source_generation_segments() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v6_source_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");
    let manifest = Manifest {
        segment_files: vec!["seg_000001.seg".to_string()],
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: true,
        hot_anchor_theta: 0,
        next_seg_id: 2,
        dict_data: Vec::new(),
        tag_dict_data: Vec::new(),
        rejected_parse: 0,
        rejected_class_d: 0,
        wal_seq_watermark: 0,
        segment_tombstones: Vec::new(),
        source_file_name: "sources.dat".to_string(),
        feature_model_fingerprint: None,
        vocab_data: Vec::new(),
        wal_sealed_through: None,
    };
    write_manifest(&manifest, &path).expect("write v6");
    let bytes = std::fs::read(&path).expect("read manifest");
    assert_eq!(
        read_u32_at(&bytes, 4).expect("version"),
        MANIFEST_VERSION_SOURCE_GENERATION
    );
    let got = read_manifest(&path).expect("read v6");
    assert!(got.source_generation_fence);
    assert!(!got.hot_fence);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_manifest_v7_selects_immutable_source_sidecar() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v7_source_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");
    let source_file_name = "sources_g00000000000000000007.dat".to_string();
    let manifest = Manifest {
        segment_files: vec!["seg_000001.seg".to_string()],
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: true,
        hot_anchor_theta: 0,
        next_seg_id: 2,
        dict_data: Vec::new(),
        tag_dict_data: Vec::new(),
        rejected_parse: 0,
        rejected_class_d: 0,
        wal_seq_watermark: 11,
        segment_tombstones: Vec::new(),
        source_file_name: source_file_name.clone(),
        feature_model_fingerprint: None,
        vocab_data: Vec::new(),
        wal_sealed_through: None,
    };
    write_manifest(&manifest, &path).expect("write v7");
    let bytes = std::fs::read(&path).expect("read manifest");
    assert_eq!(
        read_u32_at(&bytes, 4).expect("version"),
        MANIFEST_VERSION_SOURCE_COMMIT
    );
    let got = read_manifest(&path).expect("read v7");
    assert_eq!(got.source_file_name, source_file_name);
    assert_eq!(
        got.feature_model_fingerprint, None,
        "a pre-v8 manifest records no feature model"
    );
    assert!(got.vocab_data.is_empty());

    let mut unsafe_manifest = manifest;
    unsafe_manifest.source_file_name = "../outside.dat".to_string();
    assert_eq!(
        write_manifest(&unsafe_manifest, &path)
            .expect_err("source path traversal must fail")
            .kind(),
        io::ErrorKind::InvalidData
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-184: v8 records the feature model — fingerprint plus vocabulary blob — after the v7
/// fields, even for a legacy `sources.dat` selection with no other fence; a vocabulary without
/// a fingerprint is refused on write, and an unknown future version is refused on read.
#[test]
fn engine_manifest_v8_records_the_feature_model() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v8_model_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");
    let manifest = Manifest {
        segment_files: vec!["seg_000001.seg".to_string()],
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: false,
        hot_anchor_theta: 0,
        next_seg_id: 2,
        dict_data: vec![1, 2],
        tag_dict_data: vec![3],
        rejected_parse: 4,
        rejected_class_d: 5,
        wal_seq_watermark: 6,
        segment_tombstones: vec![("seg_000001.seg".to_string(), vec![7, 8])],
        source_file_name: "sources.dat".to_string(),
        feature_model_fingerprint: Some(0x0123_4567_89AB_CDEF),
        vocab_data: br#"{"synonyms":[]}"#.to_vec(),
        wal_sealed_through: None,
    };
    write_manifest(&manifest, &path).expect("write v8");
    let bytes = std::fs::read(&path).expect("read manifest");
    assert_eq!(
        read_u32_at(&bytes, 4).expect("version"),
        MANIFEST_VERSION_FEATURE_MODEL
    );
    let got = read_manifest(&path).expect("read v8");
    assert_eq!(
        got.feature_model_fingerprint,
        manifest.feature_model_fingerprint
    );
    assert_eq!(got.vocab_data, manifest.vocab_data);
    assert_eq!(got.source_file_name, "sources.dat");
    assert_eq!(got.segment_tombstones, manifest.segment_tombstones);
    assert_eq!(got.wal_seq_watermark, 6);
    assert_eq!(got.dict_data, manifest.dict_data);

    let mut bare = manifest.clone();
    bare.vocab_data.clear();
    write_manifest(&bare, &path).expect("a bare-normalizer model has no vocabulary");
    assert!(read_manifest(&path).expect("read").vocab_data.is_empty());

    let mut unstamped = manifest.clone();
    unstamped.feature_model_fingerprint = None;
    assert_eq!(
        write_manifest(&unstamped, &path)
            .expect_err("a vocabulary without a fingerprint must fail")
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let mut future = bytes;
    future[4..8].copy_from_slice(&(MANIFEST_VERSION_SEALED_LOG + 1).to_le_bytes());
    let body = future.len() - 4;
    let crc = crc32(&future[..body]);
    future[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &future).expect("forge future version");
    let Err(error) = read_manifest(&path) else {
        panic!("a future manifest version must fail loud");
    };
    assert!(
        error.to_string().contains("unsupported manifest version"),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-223: v9 records how far the log is sealed into the segments, after the v8 fields. A
/// manifest that does not record it is written as v8, byte for byte as before, and reads
/// back without it. It is independent of the watermark: a merge moves the watermark on and
/// leaves it, and a commit that keeps an older watermark can follow a seal.
#[test]
fn engine_manifest_v9_records_how_far_the_log_is_sealed() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v9_sealed_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");
    let unsealed = Manifest {
        segment_files: vec!["seg_000001.seg".to_string()],
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: false,
        hot_anchor_theta: 0,
        next_seg_id: 2,
        dict_data: vec![1, 2],
        tag_dict_data: vec![3],
        rejected_parse: 0,
        rejected_class_d: 0,
        wal_seq_watermark: 40,
        segment_tombstones: Vec::new(),
        source_file_name: "sources_g00000000000000000002.dat".to_string(),
        feature_model_fingerprint: Some(7),
        vocab_data: Vec::new(),
        wal_sealed_through: None,
    };
    write_manifest(&unsealed, &path).expect("write v8");
    let v8 = std::fs::read(&path).expect("read manifest");
    assert_eq!(
        read_u32_at(&v8, 4).expect("version"),
        MANIFEST_VERSION_FEATURE_MODEL
    );
    assert_eq!(
        read_manifest(&path).expect("read v8").wal_sealed_through,
        None
    );

    for sealed_through in [0, 12, 40, 55] {
        let mut sealed = unsealed.clone();
        sealed.wal_sealed_through = Some(sealed_through);
        write_manifest(&sealed, &path).expect("write v9");
        let v9 = std::fs::read(&path).expect("read manifest");
        assert_eq!(
            read_u32_at(&v9, 4).expect("version"),
            MANIFEST_VERSION_SEALED_LOG
        );
        assert_eq!(v9.len(), v8.len() + 8, "one position after the v8 fields");
        let got = read_manifest(&path).expect("read v9");
        assert_eq!(got.wal_sealed_through, Some(sealed_through));
        assert_eq!(got.wal_seq_watermark, 40);
        assert_eq!(got.source_file_name, unsealed.source_file_name);
        assert_eq!(got.feature_model_fingerprint, Some(7));
    }

    // The position is the last thing in the file: a v9 manifest cut short of it is refused.
    let mut sealed = unsealed.clone();
    sealed.wal_sealed_through = Some(12);
    write_manifest(&sealed, &path).expect("write v9");
    let whole = std::fs::read(&path).expect("read manifest");
    let mut cut = whole[..whole.len() - 12].to_vec();
    let crc = crc32(&cut);
    cut.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &cut).expect("forge a short v9");
    assert!(read_manifest(&path).is_err(), "a v9 manifest without it");

    let mut unstamped = sealed;
    unstamped.feature_model_fingerprint = None;
    assert_eq!(
        write_manifest(&unstamped, &path)
            .expect_err("the position without a feature model")
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A v2 manifest (written by a pre-ADR-066 binary) reads back with watermark 0 and
/// no tombstone bitmaps. Hand-rolled bytes so the pin is at the format level, not
/// against our own writer.
#[test]
fn engine_manifest_v2_reads_back_without_v3_section() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_v2_bc_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");

    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(b"PMAN");
    bytes.extend_from_slice(&2u32.to_le_bytes()); // version 2
    bytes.extend_from_slice(&5u64.to_le_bytes()); // next_seg_id
    bytes.extend_from_slice(&1u64.to_le_bytes()); // rejected_parse
    bytes.extend_from_slice(&2u64.to_le_bytes()); // rejected_class_d
    bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 segment file
    let name = b"seg_000001.seg";
    bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(&3u32.to_le_bytes()); // dict blob
    bytes.extend_from_slice(&[7, 8, 9]);
    bytes.extend_from_slice(&0u32.to_le_bytes()); // empty tag-dict blob
    let crc = crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).expect("write v2 bytes");

    let got = read_manifest(&path).expect("read v2");
    assert_eq!(got.segment_files, vec!["seg_000001.seg".to_string()]);
    assert_eq!(got.next_seg_id, 5);
    assert_eq!(got.dict_data, vec![7, 8, 9]);
    assert!(got.tag_dict_data.is_empty());
    assert_eq!(got.wal_seq_watermark, 0, "v2 has no watermark");
    assert!(got.segment_tombstones.is_empty(), "v2 has no bitmaps");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tier-D (defense-in-depth): a crafted segment-filename length prefix that overruns
/// the manifest buffer must fail loud with a typed `InvalidData` error, not panic on a
/// slice index — matching the dict/tag-dict/tombstone reads in `read_manifest`. Only
/// reachable via tampering that also recomputes the trailing CRC, which this forges.
#[test]
fn manifest_segment_filename_length_overrun_fails_loud() {
    let dir = std::env::temp_dir().join(format!("rr_manifest_fn_ovr_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("manifest.bin");

    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(b"PMAN");
    bytes.extend_from_slice(&2u32.to_le_bytes()); // version 2 (simplest layout)
    bytes.extend_from_slice(&5u64.to_le_bytes()); // next_seg_id
    bytes.extend_from_slice(&0u64.to_le_bytes()); // rejected_parse
    bytes.extend_from_slice(&0u64.to_le_bytes()); // rejected_class_d
    bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 segment file
                                                  // A length prefix far larger than any remaining bytes → would index out of bounds.
    bytes.extend_from_slice(&1_000_000u32.to_le_bytes());
    bytes.extend_from_slice(b"seg_000001.seg"); // only 14 bytes actually present
                                                // Re-seal the trailing whole-file CRC so the CRC gate passes and the structural
                                                // guard is what fires.
    let crc = crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).expect("write forged bytes");

    match read_manifest(&path) {
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData, "got: {e}"),
        Ok(_) => panic!("overrunning segment-filename length must fail loud"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The v8 cluster manifest's nested per-shard columns + compiler semantics + the
/// appended vocab and tag-dict blobs must round-trip byte-exactly (varied per-shard
/// file counts, including an empty shard). The hand-rolled length-prefixed encoding is
/// easy to get cursor-wrong, so pin it.
#[test]
fn cluster_manifest_v8_round_trips_registry_vocab_tagdict_and_generation() {
    let dir = std::env::temp_dir().join(format!("rr_cmanifest_rt_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("cluster_manifest.bin");

    let manifest = ClusterManifest {
        epoch: 7,
        snapshot_pos: 42,
        dict_fingerprint: 0xDEAD_BEEF_1234_5678,
        num_shards: 3,
        vnodes: 64,
        include_broad: true,
        broad_replicate_all: true,
        placement_generation: crate::ownership::PlacementGeneration(7),
        segment_registry: vec![
            vec!["seg_000001.seg".to_string(), "seg_000004.seg".to_string()],
            vec![], // an empty shard (no committed segments)
            vec!["seg_000002.seg".to_string()],
        ],
        next_seg_ids: vec![5, 1, 3],
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        source_files: vec![
            "sources_g00000000000000000007.dat".into(),
            "sources_g00000000000000000007.dat".into(),
            "sources_g00000000000000000007.dat".into(),
        ],
        dict_data: vec![1, 2, 3, 4, 5],
        vocab_data: vec![9, 8, 7, 6], // a non-empty (opaque) vocab blob — the v3 field
        tag_dict_data: vec![11, 22, 33], // a non-empty (opaque) tag-dict blob — the v4 field
        feature_model_fingerprint: Some(0xFEED),
    };
    write_cluster_manifest(&manifest, &path).expect("write");
    let got = read_cluster_manifest(&path).expect("read");

    assert_eq!(got.epoch, manifest.epoch);
    assert_eq!(got.snapshot_pos, manifest.snapshot_pos);
    assert_eq!(got.dict_fingerprint, manifest.dict_fingerprint);
    assert_eq!(got.num_shards, manifest.num_shards);
    assert_eq!(got.vnodes, manifest.vnodes);
    assert_eq!(got.include_broad, manifest.include_broad);
    assert_eq!(got.broad_replicate_all, manifest.broad_replicate_all);
    assert_eq!(got.placement_generation, manifest.placement_generation);
    let raw = std::fs::read(&path).expect("read raw for version");
    assert_eq!(
        read_u32_at(&raw, 4).unwrap(),
        8,
        "ADR-184 durable clusters always write manifest v8"
    );
    assert_eq!(got.feature_model_fingerprint, Some(0xFEED));
    assert_eq!(got.segment_registry, manifest.segment_registry);
    assert_eq!(got.next_seg_ids, manifest.next_seg_ids);
    assert_eq!(
        got.compiler_semantics_version,
        manifest.compiler_semantics_version
    );
    assert_eq!(got.source_files, manifest.source_files);
    assert_eq!(got.dict_data, manifest.dict_data);
    assert_eq!(got.vocab_data, manifest.vocab_data);
    assert_eq!(got.tag_dict_data, manifest.tag_dict_data);

    // A flipped byte in the body must fail the trailing-CRC check (fail loud).
    let mut bytes = std::fs::read(&path).expect("read raw");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("corrupt");
    assert!(
        read_cluster_manifest(&path).is_err(),
        "corrupt manifest must error"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cluster_manifest_rejects_mismatched_per_shard_columns() {
    const REGISTRY_COUNT_OFFSET: usize = 4 + 4 + 8 + 8 + 8 + 4 + 4 + 1;

    let dir = std::env::temp_dir().join(format!("rr_cmanifest_columns_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("cluster_manifest.bin");
    let manifest = ClusterManifest {
        epoch: 1,
        snapshot_pos: 0,
        dict_fingerprint: 7,
        num_shards: 1,
        vnodes: 64,
        include_broad: true,
        broad_replicate_all: true,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        segment_registry: vec![vec!["seg_000001.seg".into()]],
        next_seg_ids: vec![2],
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        source_files: vec!["sources.dat".into()],
        dict_data: Vec::new(),
        vocab_data: Vec::new(),
        tag_dict_data: Vec::new(),
        feature_model_fingerprint: Some(0xFEED),
    };
    write_cluster_manifest(&manifest, &path).expect("write valid manifest");

    // Insert a second, empty registry row while leaving num_shards and the
    // other two per-shard columns at one. Re-seal the CRC so structural
    // validation—not corruption detection—is responsible for the error.
    let mut bytes = std::fs::read(&path).expect("read manifest");
    let body_len = bytes.len() - 4;
    bytes.truncate(body_len);
    let original_registry_count =
        read_u32_at(&bytes, REGISTRY_COUNT_OFFSET).expect("registry count");
    assert_eq!(original_registry_count, 1);
    let mut cursor = REGISTRY_COUNT_OFFSET + 4;
    let file_count = read_u32_at(&bytes, cursor).expect("file count") as usize;
    cursor += 4;
    for _ in 0..file_count {
        let len = read_u32_at(&bytes, cursor).expect("filename length") as usize;
        cursor += 4 + len;
    }
    bytes.splice(cursor..cursor, 0u32.to_le_bytes());
    bytes[REGISTRY_COUNT_OFFSET..REGISTRY_COUNT_OFFSET + 4].copy_from_slice(&2u32.to_le_bytes());
    let crc = crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).expect("write malformed manifest");

    let Err(error) = read_cluster_manifest(&path) else {
        panic!("mismatched per-shard columns must fail in the reader");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("per-shard columns"),
        "unexpected error: {error}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The ADR-109 migration fence: v8 round-trips ownership generation, v5 is
/// rejected with an actionable rebuild error, and a future version is refused.
#[test]
fn cluster_manifest_v8_ownership_fences_v5_and_future_versions() {
    let dir = std::env::temp_dir().join(format!("rr_cmanifest_v5_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("cluster_manifest_v5.bin");

    let manifest = ClusterManifest {
        epoch: 3,
        snapshot_pos: 9,
        dict_fingerprint: 0xABCD,
        num_shards: 4,
        vnodes: 64,
        include_broad: true,
        broad_replicate_all: true, // an ADR-080 replicate-to-all cluster
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        segment_registry: vec![vec![], vec![], vec![], vec![]],
        next_seg_ids: vec![1, 1, 1, 1],
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        source_files: vec!["sources.dat".into(); 4],
        dict_data: vec![1, 2, 3],
        vocab_data: Vec::new(),
        tag_dict_data: Vec::new(),
        feature_model_fingerprint: Some(0xFEED),
    };
    write_cluster_manifest(&manifest, &path).expect("write");

    let raw = std::fs::read(&path).expect("read raw");
    assert_eq!(
        read_u32_at(&raw, 4).unwrap(),
        8,
        "feature-model metadata ⇒ cluster manifest v8"
    );
    let got = read_cluster_manifest(&path).expect("read");
    assert!(got.broad_replicate_all, "v8 retains replicate-to-all");
    assert_eq!(
        got.placement_generation,
        crate::ownership::PlacementGeneration::INITIAL
    );
    assert_eq!(got.segment_registry, manifest.segment_registry);

    // Forge legacy v5 + re-seal: the selected migration policy requires a rebuild.
    let mut bytes = raw.clone();
    bytes[4..8].copy_from_slice(&5u32.to_le_bytes());
    let body = bytes.len() - 4;
    let crc = crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).expect("rewrite");
    match read_cluster_manifest(&path) {
        Err(e) => assert!(
            e.to_string().contains("predates ADR-109") && e.to_string().contains("rebuild"),
            "got: {e}"
        ),
        Ok(_) => panic!("legacy v5 cluster manifest must fail loud"),
    }

    bytes[4..8].copy_from_slice(&9u32.to_le_bytes());
    let crc = crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).expect("rewrite future");
    match read_cluster_manifest(&path) {
        Err(e) => assert!(
            e.to_string()
                .contains("unsupported cluster manifest version"),
            "got: {e}"
        ),
        Ok(_) => panic!("future cluster manifest version must fail loud"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
