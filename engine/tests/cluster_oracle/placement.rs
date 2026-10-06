//! Placement by cost class, multi-shard any-of placement, content-routed fan-out (not scatter),
//! and the `ingest()`-on-a-populated-cluster guard.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::compile::CostClass;
use reverse_rusty::gen::{generate, GenConfig};

#[test]
fn placement_by_cost_class() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut id = 9_000_000u64;
    let mut next = || {
        id += 1;
        id
    };

    // class A: a rare anchor -> exactly one selective shard.
    match cluster
        .add_query(next(), "1994 north star rareentity0")
        .unwrap()
    {
        AddOutcome::Placed { shards, .. } => {
            assert_eq!(shards.len(), 1, "class A should hit exactly one shard");
            assert!(shards[0] < 8);
        }
        other => panic!("class A expected Placed, got {other:?}"),
    }

    // class B arity-2: all-hot required, no rare anchor -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "1994 north star").unwrap(),
        AddOutcome::Replicated {
            class: CostClass::B
        },
        "all-hot {{year}} {{brand}} should be class-B arity-2 -> replicated lane"
    );

    // class C: a single hot anchor (broad) -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "standard").unwrap(),
        AddOutcome::Replicated {
            class: CostClass::C
        },
        "broad single-hot anchor should be replicated"
    );

    // class B any-of: pure any-of of two rare entities -> selective (1..=2 shards).
    match cluster
        .add_query(next(), "(rareentity0,rareentity1000)")
        .unwrap()
    {
        AddOutcome::Placed { shards, .. } => {
            assert!(
                (1..=2).contains(&shards.len()),
                "any-of of two members places on 1..=2 shards, got {shards:?}"
            );
        }
        other => panic!("any-of expected Placed, got {other:?}"),
    }

    // a malformed query is surfaced, not silently dropped.
    assert!(
        matches!(
            cluster.add_query(next(), "(((").unwrap(),
            AddOutcome::RejectedParse(_)
        ),
        "malformed DSL should be RejectedParse"
    );
}

/// ADR-187: a query whose only required term is top-64 anchors on its selective
/// any-of group, so it ring-places like the group alone instead of joining the
/// opt-in replicated lane, and a default read returns it on any shard count.
#[test]
fn a_top64_required_term_with_a_selective_group_places_selectively() {
    let (queries, _titles) = build_corpus();
    let mixed = "standard (rareentity0,rareentity1000)";
    for num_shards in [1usize, 8] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: false,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        // Precondition: on its own the required term is opt-in.
        assert_eq!(
            cluster.add_query(9_100_000, "standard").unwrap(),
            AddOutcome::Replicated {
                class: CostClass::C
            }
        );
        match cluster.add_query(9_100_001, mixed).unwrap() {
            AddOutcome::Placed { shards, .. } => assert!(
                (1..=2).contains(&shards.len()),
                "K={num_shards}: one shard per group member, got {shards:?}"
            ),
            other => panic!("K={num_shards}: expected Placed, got {other:?}"),
        }
        for title in ["standard rareentity0", "rareentity1000 extra standard"] {
            let default = cluster.percolate_with_broad(title, false).expect("read");
            assert!(
                default.contains(&9_100_001) && !default.contains(&9_100_000),
                "K={num_shards}: {title:?} default read returned {default:?}"
            );
            let broad = cluster.percolate_with_broad(title, true).expect("read");
            assert!(broad.contains(&9_100_001) && broad.contains(&9_100_000));
        }
        // The required term is still required.
        let without = cluster
            .percolate_with_broad("rareentity0 alone", true)
            .expect("read");
        assert!(!without.contains(&9_100_001), "K={num_shards}: {without:?}");
    }
}

#[test]
fn anyof_query_can_place_on_multiple_shards() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 16,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    // Over many distinct rare-entity pairs on a 16-shard ring, at least one
    // any-of query must straddle two shards — the multi-shard placement case.
    let mut id = 8_000_000u64;
    let mut saw_two = false;
    for i in 0..150u64 {
        id += 1;
        if let AddOutcome::Placed { shards, .. } = cluster
            .add_query(id, &format!("(rareentity{i},rareentity{})", i + 1000))
            .unwrap()
        {
            if shards.len() == 2 {
                saw_two = true;
                break;
            }
        }
    }
    assert!(
        saw_two,
        "expected at least one any-of query to place on two distinct shards"
    );
}

#[test]
fn fan_out_is_content_routed_not_scatter() {
    let (queries, titles) = build_corpus();
    let k = 16usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let mut max_fanout = 0usize;
    let mut saw_multi = false;
    for title in &titles {
        let f = cluster.shard_fanout(title).len();
        max_fanout = max_fanout.max(f);
        if f >= 2 {
            saw_multi = true;
        }
    }
    // Content routing, not scatter-gather: even on 16 shards a title touches only
    // a handful (its rare features + the replicated lane), never all N.
    assert!(saw_multi, "expected some title to fan out to >1 shard");
    assert!(
        max_fanout <= 8,
        "fan-out {max_fanout} on {k} shards is too high — routing is not content-routed"
    );
}

/// `ingest()` must refuse a non-empty cluster: it re-indexes from scratch, so calling it
/// on an already-populated cluster would silently duplicate entries (the ADR-029 footgun).
/// It returns `ShardError::Config` instead. (The happy path — ingest into a freshly
/// connected empty cluster — is covered by `cluster_grpc_oracle.rs`.)
#[test]
fn ingest_on_a_populated_cluster_is_rejected() {
    let data = generate(&GenConfig {
        num_queries: 500,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1234_5678,
        num_entities: 200,
        num_collections: 100,
    });
    let cfg = ClusterConfig {
        num_shards: 3,
        ..ClusterConfig::default()
    };
    // build() loads the corpus, so the cluster is already populated.
    let cluster = ClusterEngine::build(vocab(), &cfg, &data.queries).expect("build cluster");
    assert!(
        cluster.num_queries().unwrap() > 0,
        "corpus should populate the cluster"
    );
    assert!(
        matches!(
            cluster.ingest(&data.queries),
            Err(reverse_rusty::cluster::ShardError::Config(_))
        ),
        "ingest() on a populated cluster must error, not silently duplicate"
    );
}

/// An accepted write reports the class the coordinator planned it under, and that is the
/// class every shard stored its copy under: a selective row on the shards it was placed on,
/// a replicated one on all of them.
#[test]
fn a_write_reports_the_class_every_copy_was_stored_under() {
    let (queries, _titles) = build_corpus();
    for &num_shards in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: true,
            per_shard: reverse_rusty::config::EngineConfig {
                accept_class_d: true,
                ..reverse_rusty::config::EngineConfig::default()
            },
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        let slot = |class: CostClass| match class {
            CostClass::A => 0,
            CostClass::B => 1,
            CostClass::C => 2,
            CostClass::D => 3,
            CostClass::H => 4,
        };
        for (at, (dsl, class)) in [
            ("1994 north star rareentity0", CostClass::A),
            ("(rareentity0,rareentity1000)", CostClass::B),
            ("1994 north star", CostClass::B),
            ("standard", CostClass::C),
            ("-standard", CostClass::D),
        ]
        .into_iter()
        .enumerate()
        {
            let before = cluster.class_counts().expect("class counts");
            let outcome = cluster.add_query(9_200_000 + at as u64, dsl).expect("add");
            assert_eq!(outcome.class(), Some(class), "K={num_shards} {dsl:?}");
            let copies = match &outcome {
                AddOutcome::Placed { shards, .. } => shards.len() as u64,
                AddOutcome::Replicated { .. } => num_shards as u64,
                other => panic!("K={num_shards} {dsl:?}: rejected as {other:?}"),
            };
            let mut want = before;
            want[slot(class)] += copies;
            assert_eq!(
                cluster.class_counts().expect("class counts"),
                want,
                "K={num_shards} {dsl:?}: every stored copy is counted under the reported class"
            );
            // Default visibility follows from the class alone.
            let visible = cluster
                .percolate_with_broad(&dsl.replace(['(', ')', ',', '-'], " "), false)
                .expect("percolate")
                .contains(&(9_200_000 + at as u64));
            if class != CostClass::D {
                assert_eq!(visible, !class.is_opt_in(), "K={num_shards} {dsl:?}");
            }
        }
        // A rejected write reports none.
        let rejected = cluster.add_query(9_300_000, "(((").expect("add");
        assert_eq!(rejected.class(), None);
    }
}
