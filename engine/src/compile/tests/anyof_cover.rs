//! The any-of cover (ADR-187): a query whose only flat anchor would be a top-64
//! feature stays default-visible whenever one of its any-of groups has no top-64
//! member. Authored from the rule, not captured from `anchor_plan`.
use super::super::*;
use crate::dict::{Dict, FeatureKind};

const THETA: u32 = 100;

struct Fixture {
    dict: Dict,
    /// The 64 mask holders, most frequent first.
    top64: Vec<FeatureId>,
    /// Never top-64, frequency 1.
    rare: [FeatureId; 4],
    /// Never top-64, frequency ≥ θ: the hot tier's shape.
    theta_hot: FeatureId,
    /// Never top-64, yet more frequent than every mask holder: what frequency
    /// drift after the mask freezes produces.
    drifted: FeatureId,
}

fn fixture() -> Fixture {
    let mut dict = Dict::new();
    let intern = |dict: &mut Dict, name: &str, freq: u32| {
        let f = dict.intern(name, FeatureKind::Generic);
        for _ in 0..freq {
            dict.bump_freq(f);
        }
        f
    };
    let top64: Vec<FeatureId> = (0..64u32)
        .map(|i| intern(&mut dict, &format!("top{i}"), 1_000 - i))
        .collect();
    let rare = [
        intern(&mut dict, "rare0", 1),
        intern(&mut dict, "rare1", 1),
        intern(&mut dict, "rare2", 1),
        intern(&mut dict, "rare3", 1),
    ];
    let theta_hot = intern(&mut dict, "thetahot", THETA);
    let drifted = intern(&mut dict, "drifted", 1);
    dict.finalize_mask();
    for _ in 0..5_000 {
        dict.bump_freq(drifted);
    }
    assert!(top64.iter().all(|&f| is_hot(&dict, f)));
    assert!(!is_hot(&dict, drifted) && dict.freq(drifted) > dict.freq(top64[0]));
    Fixture {
        dict,
        top64,
        rare,
        theta_hot,
        drifted,
    }
}

fn body(required: &[FeatureId], anyof: &[&[FeatureId]]) -> Extracted {
    Extracted {
        required: required.to_vec(),
        anyof: anyof.iter().map(|group| group.to_vec()).collect(),
        ..Extracted::default()
    }
}

fn singles(features: &[FeatureId]) -> Vec<Vec<FeatureId>> {
    features.iter().map(|&f| vec![f]).collect()
}

#[test]
fn a_lone_top64_required_feature_anchors_on_a_selective_anyof_group() {
    let fx = fixture();
    let [a, b, ..] = fx.rare;
    // `new (acme, zenith)`: the required term is top-64, the group is not.
    let plan = anchor_plan(&body(&[fx.top64[0]], &[&[a, b]]), &fx.dict, 0);
    assert_eq!(plan.class, CostClass::B);
    assert_eq!(plan.main_anchors, singles(&[a, b]));
    assert!(plan.broad_anchors.is_empty() && plan.hot_anchors.is_empty());
    // Adding the required term must not make the query less visible than the
    // group alone, which was already class B.
    let alone = anchor_plan(&body(&[], &[&[a, b]]), &fx.dict, 0);
    assert_eq!(
        (alone.class, alone.main_anchors),
        (CostClass::B, singles(&[a, b]))
    );
}

#[test]
fn the_anyof_cover_goes_to_the_hot_tier_under_theta() {
    let fx = fixture();
    let group = [fx.rare[0], fx.theta_hot];
    let ex = body(&[fx.top64[0]], &[&group]);
    let off = anchor_plan(&ex, &fx.dict, 0);
    assert_eq!(
        (off.class, off.main_anchors),
        (CostClass::B, singles(&group))
    );
    let on = anchor_plan(&ex, &fx.dict, THETA);
    assert_eq!(on.class, CostClass::H, "θ moves cost, never visibility");
    assert_eq!(on.hot_anchors, singles(&group));
    assert!(on.main_anchors.is_empty() && on.broad_anchors.is_empty());
}

#[test]
fn only_a_top64_anchor_available_is_still_class_c() {
    let fx = fixture();
    let (hot, other_hot) = (fx.top64[0], fx.top64[1]);
    // No any-of group at all.
    let plan = anchor_plan(&body(&[hot], &[]), &fx.dict, 0);
    assert_eq!(
        (plan.class, plan.broad_anchors),
        (CostClass::C, singles(&[hot]))
    );
    // Every group has a top-64 member: nothing selective to anchor on.
    let plan = anchor_plan(&body(&[hot], &[&[fx.rare[0], other_hot]]), &fx.dict, 0);
    assert_eq!(
        (plan.class, plan.broad_anchors),
        (CostClass::C, singles(&[hot]))
    );
    // The same group with no required feature.
    let group = [fx.rare[0], other_hot];
    let plan = anchor_plan(&body(&[], &[&group]), &fx.dict, 0);
    assert_eq!(
        (plan.class, plan.broad_anchors),
        (CostClass::C, singles(&group))
    );
}

/// The cover is chosen by the frozen mask, not by frequency order: a group with
/// a top-64 member is never preferred over one without, however the live
/// frequencies have drifted. So two compiles of one body agree on visibility.
#[test]
fn the_anyof_cover_is_mask_keyed_not_frequency_ordered() {
    let fx = fixture();
    let masked = [fx.rare[0], fx.top64[63]];
    let clean = [fx.rare[1], fx.drifted];
    assert!(
        fx.dict.freq(fx.drifted) > fx.dict.freq(fx.top64[63]),
        "by frequency alone the masked group looks more selective"
    );
    for required in [vec![], vec![fx.top64[0]]] {
        let plan = anchor_plan(&body(&required, &[&masked, &clean]), &fx.dict, 0);
        assert_eq!(plan.class, CostClass::B, "required={required:?}");
        assert_eq!(plan.main_anchors, singles(&clean), "required={required:?}");
    }
}

#[test]
fn the_most_selective_clean_group_is_chosen() {
    let fx = fixture();
    let wide = [fx.rare[0], fx.theta_hot];
    let narrow = [fx.rare[1], fx.rare[2]];
    let plan = anchor_plan(&body(&[fx.top64[0]], &[&wide, &narrow]), &fx.dict, 0);
    assert_eq!(plan.main_anchors, singles(&narrow));
}

/// Adding a positive required term to a default-visible query never hides it.
#[test]
fn adding_a_required_term_never_makes_a_visible_query_opt_in() {
    let fx = fixture();
    let pool = [
        fx.top64[0],
        fx.top64[1],
        fx.rare[0],
        fx.rare[1],
        fx.theta_hot,
        fx.drifted,
    ];
    let groups: [&[FeatureId]; 4] = [
        &[fx.rare[2], fx.rare[3]],
        &[fx.rare[2], fx.top64[5]],
        &[fx.top64[6], fx.top64[7]],
        &[fx.drifted, fx.rare[3]],
    ];
    let mut checked = 0;
    for theta in [0, THETA] {
        for required_mask in 0u32..(1 << pool.len()) {
            let required: Vec<FeatureId> = (0..pool.len())
                .filter(|i| required_mask & (1 << i) != 0)
                .map(|i| pool[i])
                .collect();
            for group_mask in 0u32..(1 << groups.len()) {
                let anyof: Vec<&[FeatureId]> = (0..groups.len())
                    .filter(|i| group_mask & (1 << i) != 0)
                    .map(|i| groups[i])
                    .collect();
                let before = anchor_plan(&body(&required, &anyof), &fx.dict, theta).class;
                if before.is_opt_in() {
                    continue;
                }
                for &extra in pool.iter().filter(|f| !required.contains(f)) {
                    let mut wider = required.clone();
                    wider.push(extra);
                    let after = anchor_plan(&body(&wider, &anyof), &fx.dict, theta).class;
                    assert!(
                        !after.is_opt_in(),
                        "adding {extra:?} to required={required:?} anyof={anyof:?} \
                         (θ={theta}) moved {before:?} to {after:?}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(
        checked > 1_000,
        "the sweep must cover real cases, got {checked}"
    );
}

/// A required phrase still supplies the cover first: its candidate-only labels
/// are the anchor, not the any-of group, so `uses_required_phrase_proxy` (which
/// cluster placement keys on) keeps describing the plan.
#[test]
fn a_required_phrase_proxy_takes_precedence_over_the_anyof_cover() {
    use crate::normalize::Normalizer;

    let norm = Normalizer::default_vocab().expect("normalizer");
    let mut dict = Dict::new();
    for i in 0..63u32 {
        let filler = dict.intern(&format!("term:filler{i}"), FeatureKind::Generic);
        for _ in 0..500 {
            dict.bump_freq(filler);
        }
    }
    let mut lc = String::new();
    let parse = |text: &str| crate::dsl::parse(text).expect("parse");
    // Make `hword` the 64th mask holder.
    for _ in 0..400 {
        extract(&parse("hword"), &norm, &mut dict, &mut lc);
    }
    let ex = extract(
        &parse("\"red shoe\" hword (acme, zenith)"),
        &norm,
        &mut dict,
        &mut lc,
    );
    dict.finalize_mask();
    assert_eq!(ex.required.len(), 1, "one flat required feature: {ex:?}");
    assert!(is_hot(&dict, ex.required[0]) && !ex.required_phrases.is_empty());
    assert!(uses_required_phrase_proxy(&ex, &dict));

    let plan = anchor_plan(&ex, &dict, 0);
    let group: Vec<Vec<FeatureId>> = ex.anyof[0].iter().map(|&f| vec![f]).collect();
    assert_eq!(plan.class, CostClass::B);
    assert!(!plan.main_anchors.is_empty());
    assert!(
        plan.main_anchors
            .iter()
            .all(|anchor| !group.contains(anchor)),
        "the phrase labels anchor the query, not the any-of group: {:?}",
        plan.main_anchors
    );
}
