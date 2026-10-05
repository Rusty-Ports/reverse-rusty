//! ADR-184 (RR-003), cluster side: the coordinator manifest records the feature model the
//! committed base and log tail were compiled under, and a reopen serves exactly that model.

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::vocab::{Vocab, VocabSeedOutcome};

fn durable_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rr-adr184-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.to_path_buf()),
        ..ClusterConfig::default()
    }
}

/// A synonym file: its normalizer rewrites a title's `tee` to `term:shirt`, so every query
/// compiled under the stock normalizer that requires the literal `tee` would miss.
fn tee_vocab() -> Vocab {
    let mut vocab = Vocab::new();
    vocab.add_synonym("tee", "term:shirt", FeatureKind::Generic);
    vocab
}

fn corpus() -> Vec<(u64, String)> {
    vec![
        (1, "black tee".into()),
        (2, "tee cotton".into()),
        (3, "graphic tee vintage".into()),
        (4, "usb hub silver".into()),
    ]
}

const TITLES: [&str; 5] = [
    "black tee",
    "cotton tee shirt",
    "vintage graphic tee",
    "usb hub silver",
    "black tee shirt",
];

fn matches(cluster: &ClusterEngine) -> Vec<Vec<u64>> {
    TITLES
        .iter()
        .map(|title| {
            let mut ids = cluster.percolate(title).expect("percolate");
            ids.sort_unstable();
            ids
        })
        .collect()
}

/// The RR-003 cluster finding: a populated cluster built without a vocabulary was reopened by
/// the server under the `--vocab-file` normalizer, so every stock-compiled `tee` query missed.
/// `open_seeded` serves the stock model it was built with and does not apply the file.
#[test]
fn a_populated_stock_cluster_keeps_its_model_under_a_vocab_file() {
    let dir = durable_dir("not-applied");
    let expected = {
        let cluster = ClusterEngine::build(vocab(), &cfg(&dir), &corpus()).expect("build");
        matches(&cluster)
    };
    assert!(expected[..3].iter().all(|ids| !ids.is_empty()), "fixture");

    let (cluster, outcome) =
        ClusterEngine::open_seeded(&dir, Some(tee_vocab()), Some(&cfg(&dir))).expect("reopen");
    assert_eq!(outcome, VocabSeedOutcome::SeedNotApplied);
    assert!(cluster.vocab().is_none());
    assert_eq!(
        matches(&cluster),
        expected,
        "FN: the reopen must match exactly what the stock-compiled cluster matched"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Library callers have no seed policy: reopening a bare-normalizer cluster under a different
/// normalizer is refused with both fingerprints, not served with silent misses.
#[test]
fn reopening_a_bare_cluster_under_another_normalizer_fails_loud() {
    let dir = durable_dir("mismatch");
    drop(ClusterEngine::build(vocab(), &cfg(&dir), &corpus()).expect("build"));

    let tee = tee_vocab().to_normalizer().expect("synonym normalizer");
    let tee_fingerprint = tee.fingerprint();
    match ClusterEngine::open(&dir, tee, Some(&cfg(&dir))) {
        Err(ShardError::FeatureModelMismatch(mismatch)) => {
            assert_eq!(mismatch.recorded, vocab().fingerprint());
            assert_eq!(mismatch.supplied, tee_fingerprint);
        }
        Err(other) => panic!("expected FeatureModelMismatch, got {other}"),
        Ok(_) => panic!("a mismatched normalizer must not open the cluster"),
    }
    let cluster = ClusterEngine::open(&dir, vocab(), Some(&cfg(&dir))).expect("stock reopen");
    assert!(cluster.percolate("black tee").expect("match").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A persisted vocabulary is authoritative: a different file is ignored (and reported).
#[test]
fn a_recorded_cluster_vocabulary_wins_over_a_different_file() {
    let dir = durable_dir("recorded");
    let expected = {
        let cluster =
            ClusterEngine::build_with_vocab(tee_vocab(), &cfg(&dir), &corpus()).expect("build");
        matches(&cluster)
    };
    let mut other = Vocab::new();
    other.set_number_context_words(&["model"]);
    let (cluster, outcome) =
        ClusterEngine::open_seeded(&dir, Some(other), Some(&cfg(&dir))).expect("reopen");
    assert_eq!(outcome, VocabSeedOutcome::Recorded { seed_ignored: true });
    assert_eq!(matches(&cluster), expected);
    drop(cluster);

    let (_, outcome) =
        ClusterEngine::open_seeded(&dir, Some(tee_vocab()), Some(&cfg(&dir))).expect("reopen");
    assert_eq!(
        outcome,
        VocabSeedOutcome::Recorded {
            seed_ignored: false
        }
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Strip the ADR-184 fingerprint from a v8 cluster manifest, leaving the v7 document an older
/// binary wrote.
fn downgrade_cluster_manifest_to_v7(dir: &std::path::Path) {
    let path = dir.join("cluster_manifest.bin");
    let mut bytes = std::fs::read(&path).expect("read v8 manifest");
    let content = bytes.len() - 4 - 8;
    bytes.truncate(content);
    bytes[4..8].copy_from_slice(&7u32.to_le_bytes());
    let crc = reverse_rusty::storage::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).expect("write v7 manifest");
}

/// Codex R2: a pre-ADR-184 cluster without a vocabulary may have been built from a custom bare
/// normalizer, which its manifest cannot distinguish from the stock one. With a seed whose
/// normalizer is not the stock one, neither choice can be verified, so the server path fails
/// loud instead of guessing (guessing stock silently missed every `tee` query).
#[test]
fn a_legacy_bare_cluster_with_a_non_stock_seed_fails_loud() {
    let dir = durable_dir("legacy-bare");
    let tee = tee_vocab().to_normalizer().expect("synonym normalizer");
    drop(ClusterEngine::build(tee, &cfg(&dir), &corpus()).expect("custom bare build"));
    downgrade_cluster_manifest_to_v7(&dir);

    match ClusterEngine::open_seeded(&dir, Some(tee_vocab()), Some(&cfg(&dir))) {
        Err(ShardError::Config(message)) => assert!(message.contains("ADR-184"), "{message}"),
        Err(other) => panic!("expected a loud configuration error, got {other}"),
        Ok(_) => panic!("an unverifiable legacy model must not be guessed"),
    }
    // The library path still trusts the caller for a legacy manifest, and its first
    // checkpoint records the model, after which the stock seed policy fails loud on it.
    let cluster = ClusterEngine::open(
        &dir,
        tee_vocab().to_normalizer().expect("synonym normalizer"),
        Some(&cfg(&dir)),
    )
    .expect("legacy reopen with the original normalizer");
    assert!(cluster.percolate("black tee").expect("match").contains(&1));
    cluster.checkpoint().expect("record the model");
    drop(cluster);
    assert!(matches!(
        ClusterEngine::open_seeded(&dir, None, Some(&cfg(&dir))),
        Err(ShardError::FeatureModelMismatch(_))
    ));
    let _ = std::fs::remove_dir_all(&dir);
}
