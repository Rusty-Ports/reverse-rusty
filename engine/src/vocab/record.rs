//! What a durable manifest may record as its vocabulary (ADR-184).

use super::Vocab;

impl Vocab {
    /// The `Vocab` JSON a manifest may record for a corpus served by `serving` (ADR-184), verified
    /// to **reopen as that normalizer**. Recovery reads the blob back, rebuilds the normalizer,
    /// demotes unexpressible aliases against the recorded dict, and refuses a fingerprint mismatch;
    /// this repeats exactly those steps. Every standalone commit and cluster checkpoint checks it,
    /// so a manifest that would not reopen is never written, and the standalone vocabulary install
    /// seams check it before mutating anything, so a bad vocabulary is refused rather than
    /// degrading persistence. It rejects a
    /// vocabulary paired with a normalizer it does not build (registry metadata on an engine built
    /// from a bare custom normalizer) and JSON that does not read back (a non-finite float
    /// serializes as `null`). Cost: one normalizer build, alongside the dict serialization every
    /// commit already performs.
    pub(crate) fn recordable_json(
        &self,
        serving: &crate::normalize::Normalizer,
        dict: &crate::dict::Dict,
    ) -> Result<String, String> {
        let json = self
            .to_json()
            .map_err(|e| format!("the vocabulary does not serialize: {e}"))?;
        let mut reread = Vocab::from_json(&json)
            .map_err(|e| format!("the serialized vocabulary does not read back: {e}"))?;
        let mut rebuilt = reread
            .to_normalizer()
            .map_err(|e| format!("the vocabulary does not build a normalizer: {e}"))?;
        if reread.aliases_mut().demote_unexpressible(&rebuilt, dict) > 0 {
            rebuilt = reread
                .to_normalizer()
                .map_err(|e| format!("the vocabulary does not build a normalizer: {e}"))?;
        }
        if rebuilt.fingerprint() != serving.fingerprint() {
            return Err(format!(
                "the vocabulary rebuilds normalizer {:#018x}, but the corpus is served by {:#018x}; \
                 a vocabulary can only be recorded on an engine whose normalizer it builds",
                rebuilt.fingerprint(),
                serving.fingerprint()
            ));
        }
        Ok(json)
    }
}
