//! ADR-184 feature-model recovery: choose the normalizer a reopened standalone engine serves,
//! and refuse a committed corpus under any other.
//!
//! A v8 manifest is authoritative. When it carries a vocabulary, the normalizer is rebuilt from
//! that vocabulary and the caller's normalizer/vocabulary are ignored (they only seed a fresh
//! directory). When it carries none, the corpus was built from a bare normalizer that no blob
//! can restore, so the caller's normalizer is used and the recorded fingerprint decides whether
//! it is the right one. A pre-v8 manifest recorded nothing: the caller's model is trusted, as
//! before, and the next commit records it.

use super::{Engine, Normalizer};
use crate::error::FeatureModelMismatch;
use crate::storage::Manifest;
use crate::vocab::{Vocab, VocabSeedOutcome};

/// Where the model a reopened engine serves came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveredModel {
    /// No manifest existed: a fresh engine built from the caller's model.
    Fresh,
    /// A v8 manifest recorded the model, and it was verified.
    Recorded,
    /// A pre-v8 manifest recorded no model; the caller's was trusted unverified.
    Legacy,
}

fn invalid_data(detail: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, detail.into())
}

/// Pick the normalizer and vocabulary to install from `manifest` (see the module docs).
pub(super) fn resolve(
    manifest: &Manifest,
    norm: Normalizer,
    vocab: Option<Vocab>,
) -> std::io::Result<(Normalizer, Option<Vocab>, RecoveredModel)> {
    if manifest.feature_model_fingerprint.is_none() {
        return Ok((norm, vocab, RecoveredModel::Legacy));
    }
    if manifest.vocab_data.is_empty() {
        // The committed model is a bare normalizer. A caller vocabulary is not part of it;
        // installing one would expand the WAL tail through equivalences the committed rows
        // never had. Apply a new vocabulary through `set_vocab`, which rebuilds from source.
        return Ok((norm, None, RecoveredModel::Recorded));
    }
    let json = std::str::from_utf8(&manifest.vocab_data)
        .map_err(|e| invalid_data(format!("manifest vocabulary is not UTF-8: {e}")))?;
    let recorded = Vocab::from_json(json)
        .map_err(|e| invalid_data(format!("cannot read the manifest vocabulary: {e}")))?;
    let norm = recorded
        .to_normalizer()
        .map_err(|e| invalid_data(format!("cannot rebuild the recorded normalizer: {e}")))?;
    Ok((norm, Some(recorded), RecoveredModel::Recorded))
}

/// Refuse to serve a committed corpus under a normalizer other than the one it was compiled
/// with. A pending compiler-semantics migration is exempt: it rebuilds every live row from
/// retained source under the current normalizer and commits the new fingerprint before the
/// engine is returned, so no row is served under the recorded model.
pub(super) fn verify(
    recorded: Option<u64>,
    live: &Normalizer,
    migration_pending: bool,
) -> std::io::Result<()> {
    match recorded {
        Some(recorded) if recorded != live.fingerprint() && !migration_pending => {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                FeatureModelMismatch {
                    recorded,
                    supplied: live.fingerprint(),
                },
            ))
        }
        _ => Ok(()),
    }
}

fn same_vocab(a: &Vocab, b: &Vocab) -> bool {
    match (a.to_json(), b.to_json()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

impl Engine {
    /// Open a durable data directory with an operator's **startup vocabulary seed** — the
    /// server's `--vocab-file` policy (ADR-184), shared with
    /// [`ClusterEngine::open_seeded`](crate::cluster::ClusterEngine::open_seeded):
    ///
    /// - a fresh directory is built from the seed (or the stock normalizer);
    /// - a committed store that records its feature model keeps it: a recorded vocabulary is
    ///   restored and a differing seed is ignored; a store recorded without a vocabulary
    ///   serves the stock normalizer it was built with, activating the seed only while it
    ///   holds no queries (a populated store needs an explicit `set_vocab` rebuild);
    /// - a pre-ADR-184 manifest records nothing, so the seed is trusted as before.
    ///
    /// The returned [`VocabSeedOutcome`] says which happened, for the caller to log.
    pub fn open_seeded(
        seed: Option<Vocab>,
        config: crate::config::EngineConfig,
    ) -> std::io::Result<(Self, VocabSeedOutcome)> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "data_dir required for open",
            )
        })?;
        let manifest_path = dir.join("manifest.bin");
        let records_model = manifest_path.exists()
            && crate::storage::read_manifest(&manifest_path)?
                .feature_model_fingerprint
                .is_some();
        if !records_model {
            let norm = match &seed {
                Some(v) => v.to_normalizer().map_err(|e| super::invalid_input(&e))?,
                None => Normalizer::default_vocab().map_err(|e| super::invalid_input(&e))?,
            };
            let (engine, model) = Self::open_inner(norm, config, seed)?;
            let outcome = if model == RecoveredModel::Legacy {
                VocabSeedOutcome::LegacyUnverified
            } else {
                VocabSeedOutcome::Fresh
            };
            return Ok((engine, outcome));
        }
        let stock = Normalizer::default_vocab().map_err(|e| super::invalid_input(&e))?;
        let (mut engine, _) = Self::open_inner(stock, config, None)?;
        let Some(seed) = seed else {
            return Ok((
                engine,
                VocabSeedOutcome::Recorded {
                    seed_ignored: false,
                },
            ));
        };
        if let Some(recorded) = engine.vocab() {
            let seed_ignored = !same_vocab(recorded, &seed);
            return Ok((engine, VocabSeedOutcome::Recorded { seed_ignored }));
        }
        if engine.num_live_queries() != 0 {
            return Ok((engine, VocabSeedOutcome::SeedNotApplied));
        }
        engine
            .set_vocab(seed)
            .map_err(|e| super::invalid_input(&e))?;
        engine.recompile_stale_segments();
        if engine.has_stale_segments() || !engine.persistence_healthy {
            return Err(std::io::Error::other(
                "the startup vocabulary could not be committed to the empty store",
            ));
        }
        Ok((engine, VocabSeedOutcome::SeedActivated))
    }
}
