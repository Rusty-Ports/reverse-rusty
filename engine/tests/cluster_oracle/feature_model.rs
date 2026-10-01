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
