use std::time::{Duration, Instant};

use super::{ResizeGovernor, ResizeGovernorConfig, ResizeObservation, ResizeVerdict};

fn config() -> ResizeGovernorConfig {
    ResizeGovernorConfig {
        required_observations: 3,
        cooldown: Duration::from_mins(1),
        max_step: 4,
        max_shards: 16,
        min_relief_percent: 10,
    }
}

fn obs(
    num_shards: usize,
    generation: u64,
    recommended: Option<usize>,
    hottest: usize,
) -> ResizeObservation {
    ResizeObservation {
        num_shards,
        placement_generation: generation,
        recommended,
        max_selective_corpus: hottest,
    }
}

fn at(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

#[test]
fn validate_rejects_degenerate_knobs() {
    let bad = ResizeGovernorConfig {
        required_observations: 0,
        cooldown: Duration::ZERO,
        max_step: 0,
        max_shards: 0,
        min_relief_percent: 101,
    };
    assert_eq!(bad.validate().len(), 4);
    assert!(ResizeGovernorConfig::default().validate().is_empty());
    let too_many = ResizeGovernorConfig {
        max_shards: super::MAX_GOVERNED_SHARDS + 1,
        ..ResizeGovernorConfig::default()
    };
    assert_eq!(too_many.validate().len(), 1);
}

#[test]
fn growth_is_accepted_only_after_the_required_streak() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    for i in 1..=2u32 {
        assert_eq!(
            governor.observe(at(base, u64::from(i)), obs(4, 7, Some(6), 900), &cfg),
            ResizeVerdict::Deferred {
                target: 6,
                observations: i,
                required: 3
            }
        );
    }
    assert_eq!(
        governor.observe(at(base, 3), obs(4, 7, Some(6), 900), &cfg),
        ResizeVerdict::Accept {
            from: 4,
            to: 6,
            if_placement_generation: 7
        }
    );
}

#[test]
fn an_interrupted_recommendation_restarts_the_streak() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    // Noise that alternates between recommending and not recommending never reaches acceptance.
    for i in 0..20u64 {
        let recommended = (i % 3 != 2).then_some(6);
        let verdict = governor.observe(at(base, i), obs(4, 7, recommended, 900), &cfg);
        assert!(
            !matches!(verdict, ResizeVerdict::Accept { .. }),
            "noisy observation {i} must not be accepted: {verdict:?}"
        );
    }
}

#[test]
fn a_recommendation_at_or_below_the_current_count_is_idle() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    assert_eq!(
        governor.observe(base, obs(8, 3, Some(8), 10), &cfg),
        ResizeVerdict::Idle
    );
    assert_eq!(
        governor.observe(base, obs(8, 3, Some(5), 10), &cfg),
        ResizeVerdict::Idle
    );
    assert_eq!(governor.streak(), 0);
}

#[test]
fn steps_are_bounded_and_the_ceiling_holds() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    let mut last = ResizeVerdict::Idle;
    for i in 0..3 {
        last = governor.observe(at(base, i), obs(4, 1, Some(40), 900), &cfg);
    }
    assert_eq!(
        last,
        ResizeVerdict::Accept {
            from: 4,
            to: 8,
            if_placement_generation: 1
        },
        "one operation adds at most max_step shards"
    );

    let mut at_limit = ResizeGovernor::new();
    assert_eq!(
        at_limit.observe(base, obs(16, 1, Some(20), 900), &cfg),
        ResizeVerdict::AtCeiling { max_shards: 16 }
    );
    let mut near_limit = ResizeGovernor::new();
    for i in 0..3 {
        last = near_limit.observe(at(base, i), obs(14, 1, Some(20), 900), &cfg);
    }
    assert_eq!(
        last,
        ResizeVerdict::Accept {
            from: 14,
            to: 16,
            if_placement_generation: 1
        }
    );
}

#[test]
fn a_layout_change_resets_the_streak_and_starts_the_cooldown() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    governor.observe(at(base, 0), obs(4, 1, Some(6), 900), &cfg);
    governor.observe(at(base, 1), obs(4, 1, Some(6), 900), &cfg);
    // An operator resize lands before the third observation.
    for i in 0..2u64 {
        let verdict = governor.observe(at(base, 2 + i), obs(5, 2, Some(7), 900), &cfg);
        assert!(
            matches!(verdict, ResizeVerdict::Deferred { .. }),
            "{verdict:?}"
        );
    }
    assert!(matches!(
        governor.observe(at(base, 4), obs(5, 2, Some(7), 900), &cfg),
        ResizeVerdict::CoolingDown { target: 7, .. }
    ));
    assert_eq!(
        governor.observe(at(base, 62), obs(5, 2, Some(7), 900), &cfg),
        ResizeVerdict::Accept {
            from: 5,
            to: 7,
            if_placement_generation: 2
        }
    );
}

#[test]
fn a_fresh_governor_does_not_invent_a_cooldown() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    let mut last = ResizeVerdict::Idle;
    for i in 0..3 {
        last = governor.observe(at(base, i), obs(4, 9, Some(5), 900), &cfg);
    }
    assert!(matches!(last, ResizeVerdict::Accept { .. }), "{last:?}");
}

#[test]
fn a_governed_operation_starts_the_cooldown_and_is_measured_once() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    for i in 0..3 {
        governor.observe(at(base, i), obs(4, 1, Some(6), 1000), &cfg);
    }
    governor.record_outcome(at(base, 3), Some(2));
    // Relieved from 1000 to 600: the latch stays open and the cooldown applies.
    for i in 0..3u64 {
        let verdict = governor.observe(at(base, 4 + i), obs(6, 2, Some(7), 600), &cfg);
        assert!(
            matches!(
                verdict,
                ResizeVerdict::Deferred { .. } | ResizeVerdict::CoolingDown { .. }
            ),
            "{verdict:?}"
        );
    }
    // Later organic growth of the same shard is not treated as futility.
    assert!(matches!(
        governor.observe(at(base, 64), obs(6, 2, Some(7), 990), &cfg),
        ResizeVerdict::Accept { from: 6, to: 7, .. }
    ));
}

#[test]
fn an_ineffective_operation_holds_further_growth() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    for i in 0..3 {
        governor.observe(at(base, i), obs(4, 1, Some(6), 1000), &cfg);
    }
    governor.record_outcome(at(base, 3), Some(2));
    // One hot anchor stays on one position: 1000 -> 950 is less than the 10% relief required.
    for i in 0..10u64 {
        assert_eq!(
            governor.observe(at(base, 100 + i), obs(6, 2, Some(7), 950), &cfg),
            ResizeVerdict::Ineffective {
                before_max_selective: 1000,
                after_max_selective: 950,
                min_relief_percent: 10
            }
        );
    }
    // Clearing the recommendation releases the latch.
    assert_eq!(
        governor.observe(at(base, 200), obs(6, 2, None, 100), &cfg),
        ResizeVerdict::Idle
    );
    assert!(matches!(
        governor.observe(at(base, 201), obs(6, 2, Some(7), 950), &cfg),
        ResizeVerdict::Deferred { .. }
    ));
}

#[test]
fn an_external_layout_change_releases_the_futility_hold() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    for i in 0..3 {
        governor.observe(at(base, i), obs(4, 1, Some(6), 1000), &cfg);
    }
    governor.record_outcome(at(base, 3), Some(2));
    assert!(matches!(
        governor.observe(at(base, 4), obs(6, 2, Some(7), 990), &cfg),
        ResizeVerdict::Ineffective { .. }
    ));
    // An operator resize to 9 replaces the premises of the hold.
    assert!(matches!(
        governor.observe(at(base, 5), obs(9, 3, Some(10), 990), &cfg),
        ResizeVerdict::Deferred { .. }
    ));
}

#[test]
fn a_disabled_latch_never_holds() {
    let base = Instant::now();
    let cfg = ResizeGovernorConfig {
        min_relief_percent: 0,
        ..config()
    };
    let mut governor = ResizeGovernor::new();
    for i in 0..3 {
        governor.observe(at(base, i), obs(4, 1, Some(6), 1000), &cfg);
    }
    governor.record_outcome(at(base, 3), Some(2));
    let verdict = governor.observe(at(base, 4), obs(6, 2, Some(7), 1000), &cfg);
    assert!(
        matches!(verdict, ResizeVerdict::Deferred { .. }),
        "{verdict:?}"
    );
}

#[test]
fn a_failed_operation_must_re_earn_the_streak() {
    let base = Instant::now();
    let cfg = config();
    let mut governor = ResizeGovernor::new();
    for i in 0..3 {
        governor.observe(at(base, i), obs(4, 1, Some(6), 1000), &cfg);
    }
    governor.record_outcome(at(base, 3), None);
    assert!(matches!(
        governor.observe(at(base, 4), obs(4, 1, Some(6), 1000), &cfg),
        ResizeVerdict::Deferred {
            observations: 1,
            ..
        }
    ));
}

#[test]
fn identical_inputs_give_identical_verdicts() {
    let base = Instant::now();
    let cfg = config();
    let script: Vec<ResizeObservation> = (0..12u64)
        .map(|i| {
            obs(
                4 + (i / 5) as usize,
                1 + i / 5,
                Some(9),
                800 - (i as usize) * 10,
            )
        })
        .collect();
    let run = || {
        let mut governor = ResizeGovernor::new();
        script
            .iter()
            .enumerate()
            .map(|(i, &o)| governor.observe(at(base, i as u64 * 30), o, &cfg))
            .collect::<Vec<_>>()
    };
    assert_eq!(run(), run());
}
