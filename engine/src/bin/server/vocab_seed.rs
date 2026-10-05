//! Startup logging for the `--vocab-file` seed policy (ADR-184), shared by single-node and
//! in-process cluster mode so both describe the served feature model the same way.

use reverse_rusty::vocab::VocabSeedOutcome;
use tracing::{info, warn};

/// Log what a durable reopen did with the startup vocabulary. `seed_supplied` is whether
/// `--vocab-file` was given.
pub(crate) fn log_outcome(outcome: VocabSeedOutcome, seed_supplied: bool) {
    match outcome {
        VocabSeedOutcome::Fresh => {
            if seed_supplied {
                info!("new data directory: built from --vocab-file");
            }
        }
        VocabSeedOutcome::Recorded {
            seed_ignored: false,
        } => {
            info!("serving the vocabulary recorded in the manifest");
        }
        VocabSeedOutcome::Recorded { seed_ignored: true } => warn!(
            "--vocab-file NOT applied: it differs from the vocabulary recorded in the \
             manifest, which is authoritative for the committed queries and stays in effect. \
             Apply a new vocabulary with PUT /_vocab (it rebuilds every stored query)."
        ),
        VocabSeedOutcome::SeedActivated => {
            info!("activated --vocab-file on the empty store and recorded it in the manifest");
        }
        VocabSeedOutcome::SeedNotApplied => warn!(
            "--vocab-file NOT applied: the stored queries were compiled without a vocabulary, \
             so the stock vocabulary they were built with stays in effect. Apply the file \
             with PUT /_vocab (it rebuilds every stored query)."
        ),
        VocabSeedOutcome::LegacyUnverified => warn!(
            "this manifest predates feature-model recording (ADR-184): trusting the supplied \
             vocabulary (--vocab-file, or the stock vocabulary without one) unverified. It \
             must be the one the stored queries were built with; the next commit records it."
        ),
    }
}
