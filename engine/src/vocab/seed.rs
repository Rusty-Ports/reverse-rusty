//! How a durable open treated the operator's startup vocabulary (ADR-184).

/// The outcome of [`Engine::open_seeded`](crate::segment::Engine::open_seeded) or
/// [`ClusterEngine::open_seeded`](crate::cluster::ClusterEngine::open_seeded). A startup
/// vocabulary (the server's `--vocab-file`) only seeds a store: once a store has committed a
/// feature model, the committed model is authoritative and is changed only through a
/// vocabulary rebuild (`PUT /_vocab`). Each variant names what was served so the caller can
/// log it accurately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VocabSeedOutcome {
    /// No committed store existed; it was built from the seed (or the stock normalizer).
    Fresh,
    /// The store recorded its feature model, which is served. `seed_ignored` is true when a
    /// seed was supplied that differs from the recorded vocabulary.
    Recorded {
        /// A supplied seed differed from the recorded vocabulary and was not applied.
        seed_ignored: bool,
    },
    /// The store recorded no vocabulary and held no queries, so the seed was applied and
    /// committed.
    SeedActivated,
    /// The store recorded no vocabulary but holds queries compiled under the stock
    /// normalizer: the seed was **not** applied, and the stock model is served.
    SeedNotApplied,
    /// A pre-ADR-184 manifest records no feature model — a single-node manifest, or a cluster
    /// manifest without a vocabulary — so the model was trusted unverified, as before: the seed
    /// (or the stock normalizer) for single-node, the stock normalizer for a cluster. The next
    /// commit records it.
    LegacyUnverified,
}
