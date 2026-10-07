//! The server's startup-vocabulary policy for a durable in-process cluster (ADR-184), the
//! cluster twin of [`Engine::open_seeded`](crate::segment::Engine::open_seeded).

use std::path::PathBuf;

use crate::cluster::coordinator::{ClusterConfig, ClusterEngine, CLUSTER_MANIFEST_FILE};
use crate::cluster::shard::ShardError;
use crate::normalize::Normalizer;
use crate::vocab::{Vocab, VocabSeedOutcome};

impl ClusterEngine {
    /// Reopen a durable cluster with an operator's **startup vocabulary seed** (the server's
    /// `--vocab-file`). The manifest is authoritative: a persisted vocabulary is restored and
    /// a differing seed is ignored. A cluster that persisted no vocabulary was built from the
    /// stock normalizer (the server builds a vocabulary file through `build_with_vocab`, which
    /// persists it), so it is reopened under that normalizer — never the seed's, which the
    /// committed rows were not compiled with. The seed is then activated through the
    /// vocabulary rebuild (which persists it) only while the cluster holds no queries; a
    /// populated cluster keeps serving the stock model until an explicit `set_vocab`.
    ///
    /// A pre-ADR-184 manifest without a vocabulary records no fingerprint either, so a library
    /// caller's custom bare normalizer is indistinguishable from the stock one. Without a seed
    /// the stock normalizer is trusted unverified, as before; with a seed whose normalizer is
    /// not the stock one, the choice cannot be verified either way and the open fails loud,
    /// before anything is attached or migrated.
    pub fn open_seeded(
        data_dir: impl Into<PathBuf>,
        seed: Option<Vocab>,
        config: Option<&ClusterConfig>,
    ) -> Result<(Self, VocabSeedOutcome), ShardError> {
        let data_dir = data_dir.into();
        let stock = Normalizer::default_vocab()
            .map_err(|e| ShardError::Config(format!("building the stock normalizer: {e}")))?;
        let manifest_path = data_dir.join(CLUSTER_MANIFEST_FILE);
        let legacy_bare = manifest_path.exists() && {
            let manifest = crate::storage::read_cluster_manifest(&manifest_path)
                .map_err(|e| ShardError::Config(format!("reading cluster manifest: {e}")))?;
            manifest.feature_model_fingerprint.is_none() && manifest.vocab_data.is_empty()
        };
        if legacy_bare {
            if let Some(seed) = &seed {
                let seeded = seed.to_normalizer().map_err(|e| {
                    ShardError::Config(format!("building the --vocab-file normalizer: {e}"))
                })?;
                if seeded.fingerprint() != stock.fingerprint() {
                    return Err(ShardError::Config(format!(
                        "the cluster at {} predates feature-model recording (ADR-184) and \
                         stores no vocabulary, so whether its queries were compiled under the \
                         stock normalizer or this --vocab-file's cannot be verified. A cluster \
                         the server built without a vocabulary file uses the stock normalizer: \
                         restart without --vocab-file (its first checkpoint records the model), \
                         then apply the file with PUT /_vocab",
                        data_dir.display()
                    )));
                }
            }
        }
        let cluster = Self::open(&data_dir, stock, config)?;
        let Some(seed) = seed else {
            let outcome = if legacy_bare {
                VocabSeedOutcome::LegacyUnverified
            } else {
                VocabSeedOutcome::Recorded {
                    seed_ignored: false,
                }
            };
            return Ok((cluster, outcome));
        };
        if let Some(recorded) = cluster.vocab() {
            let seed_ignored = match (recorded.to_json(), seed.to_json()) {
                (Ok(recorded), Ok(seed)) => recorded != seed,
                _ => true,
            };
            return Ok((cluster, VocabSeedOutcome::Recorded { seed_ignored }));
        }
        if cluster.num_queries()? != 0 {
            return Ok((cluster, VocabSeedOutcome::SeedNotApplied));
        }
        cluster.set_vocab(seed)?;
        Ok((cluster, VocabSeedOutcome::SeedActivated))
    }
}
