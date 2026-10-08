//! ADR-184: `Engine::open_seeded`, the server's `--vocab-file` policy. The file only seeds a
//! store; once a store has recorded its feature model, that model is served.

use super::feature_model::ny_alias;
use super::*;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::vocab::VocabSeedOutcome;

fn durable(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    }
}

/// A synonym file: under it, a title's `tee` normalizes to `term:shirt`, so a query compiled
/// under the stock normalizer (which requires the literal `tee`) would miss it.
fn tee_vocab() -> Vocab {
    let mut vocab = Vocab::new();
    vocab.add_synonym("tee", "term:shirt", FeatureKind::Generic);
    vocab
}

fn open(dir: &std::path::Path, seed: Option<Vocab>) -> (Engine, VocabSeedOutcome) {
    Engine::open_seeded(seed, durable(dir)).expect("open_seeded")
}

#[test]
fn a_fresh_directory_is_built_from_the_seed() {
    let dir = test_dir("rr003_seed_fresh");
    let (mut engine, outcome) = open(&dir, Some(ny_alias()));
    assert_eq!(outcome, VocabSeedOutcome::Fresh);
    engine.try_insert_live("ny catalog", 1, 1).expect("insert");
    assert!(match_ids(&engine, "new york catalog").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_recorded_vocabulary_wins_over_a_different_seed() {
    let dir = test_dir("rr003_seed_recorded");
    {
        let (mut engine, _) = open(&dir, Some(ny_alias()));
        engine.build_from_queries(&[(1, "new york inventory".into())]);
    }
    let (engine, outcome) = open(&dir, Some(tee_vocab()));
    assert_eq!(outcome, VocabSeedOutcome::Recorded { seed_ignored: true });
    assert!(
        match_ids(&engine, "new york inventory").contains(&1),
        "the recorded alias stays in effect"
    );
    assert!(engine.vocab().is_some_and(|v| v.synonyms().is_empty()));
    drop(engine);

    let (_, outcome) = open(&dir, Some(ny_alias()));
    assert_eq!(
        outcome,
        VocabSeedOutcome::Recorded {
            seed_ignored: false
        }
    );
    let (engine, outcome) = open(&dir, None);
    assert_eq!(
        outcome,
        VocabSeedOutcome::Recorded {
            seed_ignored: false
        }
    );
    assert!(match_ids(&engine, "new york inventory").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The single-node twin of the RR-003 cluster finding: a populated store built without a
/// vocabulary keeps serving the stock model it was compiled under; the file is not applied.
#[test]
fn a_populated_stock_store_does_not_apply_the_seed() {
    let dir = test_dir("rr003_seed_not_applied");
    {
        let (mut engine, outcome) = open(&dir, None);
        assert_eq!(outcome, VocabSeedOutcome::Fresh);
        engine.build_from_queries(&[(1, "black tee".into())]);
    }
    let (engine, outcome) = open(&dir, Some(tee_vocab()));
    assert_eq!(outcome, VocabSeedOutcome::SeedNotApplied);
    assert!(engine.vocab().is_none());
    assert!(
        match_ids(&engine, "black tee").contains(&1),
        "FN: the stock-compiled query must be served under the stock normalizer"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A store that recorded no vocabulary and holds no live queries takes the seed exactly as a
/// fresh store would, and records it — the next restart restores it without the file.
#[test]
fn an_empty_stock_store_activates_and_records_the_seed() {
    let dir = test_dir("rr003_seed_activated");
    {
        let (mut engine, _) = open(&dir, None);
        engine.build_from_queries(&[(1, "usb hub silver".into())]);
        engine.delete_by_logical_id(1).expect("delete");
        engine.flush();
        assert_eq!(engine.num_live_queries(), 0);
    }
    {
        let (mut engine, outcome) = open(&dir, Some(ny_alias()));
        assert_eq!(outcome, VocabSeedOutcome::SeedActivated);
        engine.try_insert_live("ny catalog", 2, 1).expect("insert");
    }
    let (engine, outcome) = open(&dir, None);
    assert_eq!(
        outcome,
        VocabSeedOutcome::Recorded {
            seed_ignored: false
        }
    );
    assert!(match_ids(&engine, "new york catalog").contains(&2));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Strip the ADR-184 suffix (fingerprint + vocabulary blob) and the ADR-223 one (how far
/// the log is sealed) from a manifest, leaving the exact v7 document an older binary wrote.
fn downgrade_manifest_to_v7(path: &std::path::Path) {
    let manifest = reverse_rusty::storage::read_manifest(path).expect("read the manifest");
    let mut bytes = std::fs::read(path).expect("read manifest bytes");
    let sealed = manifest.wal_sealed_through.map_or(0, |_| 8);
    let suffix = 8 + 4 + manifest.vocab_data.len() + sealed;
    let content = bytes.len() - 4 - suffix;
    bytes.truncate(content);
    bytes[4..8].copy_from_slice(&7u32.to_le_bytes());
    let crc = reverse_rusty::storage::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).expect("write v7 manifest");
}

#[test]
fn a_legacy_manifest_trusts_the_seed_and_records_it_at_the_next_commit() {
    let dir = test_dir("rr003_seed_legacy");
    {
        let (mut engine, _) = open(&dir, Some(ny_alias()));
        engine.build_from_queries(&[(1, "new york inventory".into())]);
    }
    let path = dir.join("manifest.bin");
    downgrade_manifest_to_v7(&path);
    assert_eq!(
        reverse_rusty::storage::read_manifest(&path)
            .expect("v7")
            .feature_model_fingerprint,
        None
    );

    {
        let (mut engine, outcome) = open(&dir, Some(ny_alias()));
        assert_eq!(outcome, VocabSeedOutcome::LegacyUnverified);
        assert!(match_ids(&engine, "new york inventory").contains(&1));
        engine.try_insert_live("ny catalog", 2, 1).expect("insert");
        engine.flush();
    }
    let manifest = reverse_rusty::storage::read_manifest(&path).expect("v8 again");
    assert_eq!(
        manifest.feature_model_fingerprint,
        Some(
            ny_alias()
                .to_normalizer()
                .expect("normalizer")
                .fingerprint()
        ),
        "the first commit after the upgrade records the trusted model"
    );
    assert!(!manifest.vocab_data.is_empty());
    let (engine, outcome) = open(&dir, None);
    assert_eq!(
        outcome,
        VocabSeedOutcome::Recorded {
            seed_ignored: false
        }
    );
    assert!(match_ids(&engine, "new york catalog").contains(&2));
    let _ = std::fs::remove_dir_all(&dir);
}
