//! A cluster keeps every match across a multi-word alias activation (ADR-205).
//!
//! A stored query that spells a form out is compiled to the form's entity, by the rebuild
//! and by a live write alike. The coordinator routes a title by its positive view and each
//! shard verifies by its own, so both must hold the entity for a title that has the form's
//! words apart, at every shard count.

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

fn reads(cluster: &ClusterEngine, titles: &[&str], include_broad: bool) -> Vec<HashSet<u64>> {
    titles
        .iter()
        .map(|title| {
            cluster
                .percolate_with_broad(title, include_broad)
                .expect("percolate")
                .into_iter()
                .collect()
        })
        .collect()
}

#[test]
fn a_cluster_keeps_every_match_across_a_multiword_alias() {
    let queries: Vec<(u64, String)> = vec![
        (1, "wireless mouse".into()),
        (2, "new york inventory".into()),
        (3, "(new york, boston) inventory".into()),
        (4, "york".into()),
        (5, "inventory -(new york, boston)".into()),
    ];
    let titles = [
        "wireless mouse",
        "wireless optical mouse",
        "mouse, wireless",
        "cordless mouse",
        "new york inventory",
        "new seasonal york inventory",
        "york inventory new",
        "ny inventory",
        "boston inventory",
        "new york warehouse",
        "new seasonal york warehouse",
        "ny warehouse",
        "york and new",
        "ny",
    ];
    for &num_shards in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
        let before = [
            reads(&cluster, &titles, true),
            reads(&cluster, &titles, false),
        ];
        assert!(before[0][1].contains(&1) && before[0][5].contains(&2));

        cluster
            .import_alias_synonyms("wireless mouse => cordless mouse\nny => new york")
            .expect("import");

        for (scope, before) in [true, false].into_iter().zip(&before) {
            let after = reads(&cluster, &titles, scope);
            for ((title, before), after) in titles.iter().zip(before).zip(&after) {
                let lost: Vec<&u64> = before.difference(after).collect();
                assert!(
                    lost.is_empty(),
                    "K={num_shards} broad={scope}: activation removed {lost:?} from {title:?}"
                );
            }
        }
        assert!(cluster.percolate("cordless mouse").unwrap().contains(&1));
        assert!(cluster.percolate("ny inventory").unwrap().contains(&2));

        // Live writes after activation take the read-only compile path.
        let live = [
            (100u64, "new york warehouse"),
            (101, "new york"),
            (102, "(new york, boston) warehouse"),
        ];
        for (id, dsl) in live {
            cluster.add_query(id, dsl).expect("add");
        }
        for (title, ids) in [
            ("new seasonal york warehouse", vec![100, 101, 102]),
            ("ny warehouse", vec![100, 101, 102]),
            ("york and new", vec![101]),
            ("ny", vec![101]),
            ("new york warehouse", vec![100, 101, 102]),
        ] {
            let got = cluster.percolate(title).expect("percolate");
            for id in ids {
                assert!(got.contains(&id), "K={num_shards}: {title:?} lacks {id}");
            }
        }

        // The cluster answers what one engine holding the same queries under the same
        // vocabulary answers.
        let mut all = queries.clone();
        all.extend(live.iter().map(|&(id, dsl)| (id, dsl.to_string())));
        let installed = cluster.vocab().expect("vocab installed").clone();
        let mut single = Engine::with_vocab(installed, EngineConfig::default()).expect("engine");
        single.build_from_queries(&all);
        let mut s = MatchScratch::new();
        for title in titles {
            let mut want = Vec::new();
            single.match_title(title, &mut s, &mut want, true);
            let want: HashSet<u64> = want.into_iter().collect();
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(got, want, "K={num_shards}: {title:?}");
        }
    }
}

#[test]
fn a_cluster_keeps_every_match_across_a_chain_of_imports() {
    // The second import makes `ny catalog` a form whose word `ny` a title carries only
    // through the first import's equivalence. The title has to be routed to the shard
    // that holds the recompiled query, and matched there.
    let queries: Vec<(u64, String)> = vec![
        (1, "ny catalog".into()),
        (2, "new york catalog".into()),
        (3, "nycat".into()),
        (4, "pkg deal".into()),
        (5, "bargain".into()),
    ];
    let titles = [
        "york new catalog",
        "new york catalog",
        "ny catalog",
        "catalog york",
        "nycat",
        "deal package",
        "pkg deal",
        "package",
    ];
    for &num_shards in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
        cluster
            .import_alias_synonyms("ny => new york\npkg => package")
            .expect("import");
        let before = [
            reads(&cluster, &titles, true),
            reads(&cluster, &titles, false),
        ];
        assert!(before[0][0].contains(&1) && before[0][5].contains(&4));

        cluster
            .import_alias_synonyms("nycat => ny catalog\nbargain => pkg deal")
            .expect("import");
        for (scope, before) in [true, false].into_iter().zip(&before) {
            let after = reads(&cluster, &titles, scope);
            for ((title, before), after) in titles.iter().zip(before).zip(&after) {
                let lost: Vec<&u64> = before.difference(after).collect();
                assert!(
                    lost.is_empty(),
                    "K={num_shards} broad={scope}: the second import removed {lost:?} from {title:?}"
                );
            }
        }
        let got = cluster.percolate("york new catalog").expect("percolate");
        assert!(
            got.contains(&3),
            "K={num_shards}: the title carries the form"
        );
        let got = cluster.percolate("deal package").expect("percolate");
        assert!(
            got.contains(&5),
            "K={num_shards}: the title carries the form"
        );

        let installed = cluster.vocab().expect("vocab installed").clone();
        let mut single = Engine::with_vocab(installed, EngineConfig::default()).expect("engine");
        single.build_from_queries(&queries);
        let mut s = MatchScratch::new();
        for title in titles {
            let mut want = Vec::new();
            single.match_title(title, &mut s, &mut want, true);
            let want: HashSet<u64> = want.into_iter().collect();
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(got, want, "K={num_shards}: {title:?}");
        }
    }
}
