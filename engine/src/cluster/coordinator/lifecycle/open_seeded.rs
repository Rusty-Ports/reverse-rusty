//! The server's startup-vocabulary policy for a durable in-process cluster (ADR-184), the
//! cluster twin of [`Engine::open_seeded`](crate::segment::Engine::open_seeded).

use std::path::PathBuf;

use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
use crate::cluster::shard::ShardError;
use crate::normalize::Normalizer;
use crate::vocab::{Vocab, VocabSeedOutcome};

impl ClusterEngine {
    /// Reopen a durable cluster with an operator's **startup vocabulary seed** (the server's
    /// `--vocab-file`). The manifest is authoritative: a persisted vocabulary is restored and
    /// a differing seed is ignored. A cluster that persisted no vocabulary was built from the
    /// stock normalizer, so it is reopened under that normalizer — never the seed's, which the
    /// committed rows were not compiled with. The seed is then activated through the
    /// vocabulary rebuild (which persists it) only while the cluster holds no queries; a
    /// populated cluster keeps serving the stock model until an explicit `set_vocab`.
    pub fn open_seeded(
        data_dir: impl Into<PathBuf>,
        seed: Option<Vocab>,
        config: Option<&ClusterConfig>,
    ) -> Result<(Self, VocabSeedOutcome), ShardError> {
        let stock = Normalizer::default_vocab()
            .map_err(|e| ShardError::Config(format!("building the stock normalizer: {e}")))?;
        let mut cluster = Self::open(data_dir, stock, config)?;
        let Some(seed) = seed else {
            return Ok((
                cluster,
                VocabSeedOutcome::Recorded {
                    seed_ignored: false,
                },
            ));
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
