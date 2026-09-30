//! The resize governor: the hysteresis layer that turns the advisory split signal into an accepted,
//! bounded resize operation (ADR-179).
//!
//! [`recommended_shard_count`](crate::cluster::recommended_shard_count) is a pure function of one
//! load snapshot. Applying it on every observation would let corpus-size noise, a transient ingest
//! burst, or an anchor that no amount of splitting relieves drive repeated `O(corpus)` rebuilds.
//! The governor separates a *recommendation* from an *accepted operation*:
//!
//! - **Hysteresis:** the recommendation must persist for `required_observations` consecutive
//!   observations of the same serving layout. A missing recommendation or any layout change (an
//!   operator resize, a vocabulary rebuild) resets the streak.
//! - **Cooldown:** no operation is accepted within `cooldown` of the last observed layout change.
//! - **Bounded steps:** one operation adds at most `max_step` shards and never exceeds
//!   `max_shards`. Growth is monotone because the recommendation never shrinks, so the governor
//!   cannot oscillate; shrinking remains an explicit operator decision.
//! - **Futility latch:** content-routed placement keeps every query for one hot anchor on one
//!   position, so adding shards may not relieve it. If the governor's previous accepted operation
//!   did not reduce the hottest shard's selective corpus by at least `min_relief_percent`, further
//!   automatic growth is held until the recommendation clears or the layout changes by some other
//!   means.
//!
//! The governor is clock-injected and allocation-free: the caller passes `now`, so the policy is
//! deterministic under test and the engine stays free of wall-clock state.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Upper bound on the shard count an automatic operation may request. It matches the public
/// resize API bound, which prevents one small decision from allocating an unbounded ring.
pub const MAX_GOVERNED_SHARDS: usize = 1_024;

/// Tunable governor knobs. [`Default`] is conservative; the governor only acts when a caller
/// constructs it with a non-zero `split_corpus_threshold` in the matching autoscale config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeGovernorConfig {
    /// Consecutive observations of the same layout that must recommend growth. At least 1.
    pub required_observations: u32,
    /// Minimum time since the last observed layout change before an operation is accepted.
    pub cooldown: Duration,
    /// Maximum shards one accepted operation may add. At least 1.
    pub max_step: usize,
    /// Hard ceiling for the resulting shard count, `1..=MAX_GOVERNED_SHARDS`.
    pub max_shards: usize,
    /// Minimum percentage reduction of the hottest shard's selective corpus that the previous
    /// accepted operation must have achieved before another automatic operation is accepted.
    /// `0` disables the futility latch.
    pub min_relief_percent: u8,
}

impl Default for ResizeGovernorConfig {
    fn default() -> Self {
        Self {
            required_observations: 3,
            cooldown: Duration::from_mins(15),
            max_step: 8,
            max_shards: 64,
            min_relief_percent: 10,
        }
    }
}

impl ResizeGovernorConfig {
    /// Validate the knobs, returning every problem (empty ⇒ valid).
    pub fn validate(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.required_observations == 0 {
            problems.push("required_observations must be >= 1".into());
        }
        if self.max_step == 0 {
            problems.push("max_step must be >= 1".into());
        }
        if self.max_shards == 0 || self.max_shards > MAX_GOVERNED_SHARDS {
            problems.push(format!("max_shards must be in 1..={MAX_GOVERNED_SHARDS}"));
        }
        if self.min_relief_percent > 100 {
            problems.push("min_relief_percent must be <= 100".into());
        }
        problems
    }
}

/// One observation of the serving layout and its load, collected by the driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResizeObservation {
    /// Serving shard count.
    pub num_shards: usize,
    /// Serving placement generation; any change marks a layout transition.
    pub placement_generation: u64,
    /// The advisory target from `recommended_shard_count`, if any.
    pub recommended: Option<usize>,
    /// Largest per-shard selective (non-replicated) corpus in this observation.
    pub max_selective_corpus: usize,
}

/// The governor's decision for one observation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum ResizeVerdict {
    /// No growth is recommended; the streak is reset.
    Idle,
    /// The layout already sits at `max_shards`. (The observed shard count is reported beside
    /// the verdict, so it is not repeated here.)
    AtCeiling { max_shards: usize },
    /// Growth is recommended but has not persisted long enough.
    Deferred {
        target: usize,
        observations: u32,
        required: u32,
    },
    /// Growth persisted, but the cooldown since the last layout change has not elapsed.
    CoolingDown { target: usize, remaining_ms: u64 },
    /// The previous governed operation did not relieve the hottest shard enough; automatic
    /// growth is held until the recommendation clears or the layout changes externally.
    Ineffective {
        before_max_selective: usize,
        after_max_selective: usize,
        min_relief_percent: u8,
    },
    /// Accept one resize from the observed layout to `to`. The driver must execute it
    /// conditionally on `if_placement_generation` and then report the outcome.
    Accept {
        from: usize,
        to: usize,
        if_placement_generation: u64,
    },
}

/// Evidence retained from the last governed operation for the futility latch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GovernedOperation {
    /// Serving placement generation the operation produced.
    placement_generation: u64,
    /// Hottest selective corpus observed when the operation was accepted.
    before_max_selective: usize,
}

/// Governor state carried between observations.
#[derive(Clone, Debug, Default)]
pub struct ResizeGovernor {
    layout: Option<(usize, u64)>,
    last_layout_change: Option<Instant>,
    streak: u32,
    /// Accepted but not yet reported.
    pending_accept: Option<GovernedOperation>,
    /// Completed, awaiting its first observation to measure the immediate relief.
    unmeasured: Option<GovernedOperation>,
    /// `(before, after)` hottest selective corpus of an operation that fell short of the
    /// required relief. Sticky until the recommendation clears or the layout changes by other
    /// means.
    ineffective: Option<(usize, usize)>,
}

impl ResizeGovernor {
    /// A fresh governor. The first observation establishes the layout without starting a
    /// cooldown, so a process restart does not add an artificial delay beyond the hysteresis.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consecutive growth recommendations for the current layout.
    #[must_use]
    pub fn streak(&self) -> u32 {
        self.streak
    }

    /// Evaluate one observation. Pure apart from the governor's own state.
    pub fn observe(
        &mut self,
        now: Instant,
        observation: ResizeObservation,
        config: &ResizeGovernorConfig,
    ) -> ResizeVerdict {
        let layout = (observation.num_shards, observation.placement_generation);
        match self.layout {
            None => self.layout = Some(layout),
            Some(previous) if previous != layout => {
                self.layout = Some(layout);
                self.last_layout_change = Some(now);
                self.streak = 0;
                let governed = self
                    .unmeasured
                    .is_some_and(|op| op.placement_generation == observation.placement_generation);
                if !governed {
                    // A layout the governor did not produce (an operator resize or a vocabulary
                    // rebuild) replaces the premises of any pending measurement or futility hold.
                    self.unmeasured = None;
                    self.ineffective = None;
                }
            }
            Some(_) => {}
        }

        // Measure a completed governed operation exactly once, on the first observation of the
        // layout it produced. Later organic growth of the same shard is not evidence of futility.
        if let Some(op) = self.unmeasured.take() {
            if op.placement_generation == observation.placement_generation
                && config.min_relief_percent > 0
            {
                let required_after = op
                    .before_max_selective
                    .saturating_mul(usize::from(100 - config.min_relief_percent))
                    / 100;
                if observation.max_selective_corpus > required_after {
                    self.ineffective =
                        Some((op.before_max_selective, observation.max_selective_corpus));
                }
            }
        }

        let Some(recommended) = observation
            .recommended
            .filter(|&target| target > observation.num_shards)
        else {
            self.streak = 0;
            self.ineffective = None;
            return ResizeVerdict::Idle;
        };

        if let Some((before, after)) = self.ineffective {
            self.streak = 0;
            return ResizeVerdict::Ineffective {
                before_max_selective: before,
                after_max_selective: after,
                min_relief_percent: config.min_relief_percent,
            };
        }

        let ceiling = config.max_shards.min(MAX_GOVERNED_SHARDS);
        let target = recommended
            .min(observation.num_shards.saturating_add(config.max_step))
            .min(ceiling);
        if target <= observation.num_shards {
            self.streak = 0;
            return ResizeVerdict::AtCeiling {
                max_shards: ceiling,
            };
        }

        self.streak = self.streak.saturating_add(1);
        if self.streak < config.required_observations {
            return ResizeVerdict::Deferred {
                target,
                observations: self.streak,
                required: config.required_observations,
            };
        }
        if let Some(changed) = self.last_layout_change {
            let elapsed = now.saturating_duration_since(changed);
            if elapsed < config.cooldown {
                let remaining = config.cooldown.saturating_sub(elapsed);
                return ResizeVerdict::CoolingDown {
                    target,
                    remaining_ms: u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX),
                };
            }
        }
        self.pending_accept = Some(GovernedOperation {
            placement_generation: observation.placement_generation,
            before_max_selective: observation.max_selective_corpus,
        });
        ResizeVerdict::Accept {
            from: observation.num_shards,
            to: target,
            if_placement_generation: observation.placement_generation,
        }
    }

    /// Report the outcome of the most recently accepted operation: the placement generation it
    /// produced, or `None` when it did not change the layout. Any outcome resets the streak, so a
    /// failed operation must re-earn acceptance through the full hysteresis.
    pub fn record_outcome(&mut self, now: Instant, produced_generation: Option<u64>) {
        self.streak = 0;
        let Some(accepted) = self.pending_accept.take() else {
            return;
        };
        if let Some(generation) = produced_generation {
            self.last_layout_change = Some(now);
            self.unmeasured = Some(GovernedOperation {
                placement_generation: generation,
                before_max_selective: accepted.before_max_selective,
            });
        }
    }
}

#[cfg(test)]
mod tests;
