//! A cluster rebuild never removes a query from default reads (ADR-203).
//!
//! Class C is opt-in: a read with the broad lane off does not return it. A vocabulary
//! change or a resize rebuilds the cluster by re-planning every stored query, and a query
//! that default reads were returning could be re-planned into class C: an alias moves its
//! rare term into an any-of group and leaves only a very common anchor, or the rebuilt
//! dictionary ranks its anchor among the most common terms. The single-node rebuild has
//! refused that since ADR-187. The cluster rebuild did not.

use crate::harness::*;
use reverse_rusty::cluster::{AutoscaleConfig, ClusterConfig, ClusterEngine};
use reverse_rusty::vocab::Vocab;
use std::collections::HashSet;

/// What a default read (broad lane off) returns for each title.
fn default_reads(cluster: &ClusterEngine, titles: &[String]) -> Vec<HashSet<u64>> {
    titles
        .iter()
        .map(|title| {
            cluster
                .percolate_with_broad(title, false)
                .expect("percolate")
                .into_iter()
                .collect()
        })
        .collect()
}

fn assert_nothing_hidden(
    what: &str,
    titles: &[String],
    before: &[HashSet<u64>],
    after: &[HashSet<u64>],
) {
    for ((title, before), after) in titles.iter().zip(before).zip(after) {
        let hidden: Vec<&u64> = before.difference(after).collect();
        assert!(
            hidden.is_empty(),
            "{what}: default reads of {title:?} lost {hidden:?}"
        );
    }
}

/// The smallest case. Three queries; an equivalence `pkg` ≡ `package` turns `widget pkg`
/// into `widget (pkg,package)`, whose anchors are all among the most common terms of so
/// small a corpus. Before the rebuild a default read of `widget pkg` returns it.
#[test]
fn an_alias_rebuild_keeps_a_visible_query_visible() {
    let queries = vec![
        (1u64, "widget pkg".to_string()),
        (2, "(pkg,package) widget".to_string()),
        (3, "(pkg,package) gadget".to_string()),
    ];
    let titles = vec!["widget pkg".to_string(), "gadget package".to_string()];
    for &(num_shards, replication_factor) in &[(1usize, 1usize), (3, 1), (8, 1), (3, 2)] {
        let cfg = ClusterConfig {
            num_shards,
            replication_factor,
            include_broad: false,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
        let before = default_reads(&cluster, &titles);
        assert!(before[0].contains(&1), "precondition: query 1 is visible");

        let mut aliased = Vocab::new();
        aliased.add_equivalence(&["pkg", "package"]);
        cluster.set_vocab(aliased).expect("set_vocab");

        let what = format!("K={num_shards} RF={replication_factor} alias");
        let after = default_reads(&cluster, &titles);
        assert_nothing_hidden(&what, &titles, &before, &after);
        // The row is returned once, and a read that asks for broad returns it too.
        let with_broad = cluster
            .percolate_with_broad(&titles[0], true)
            .expect("percolate");
        assert_eq!(
            with_broad.iter().filter(|&&id| id == 1).count(),
            1,
            "{what}: a broad read returns the kept row exactly once"
        );
    }
}

/// The same on a realistic corpus, for a declared equivalence and then a resize: every
/// query a default read returned before is still returned after each rebuild.
#[test]
fn rebuilds_never_hide_a_query_on_a_generated_corpus() {
    let (mut queries, mut titles) = build_corpus();
    // Queries anchored on a rare term that an equivalence will move into an any-of group,
    // beside `standard`, which this corpus makes common enough to be in the top-64 mask.
    for i in 0..40u64 {
        queries.push((8_500_000 + i, format!("zzrare{i} standard")));
        titles.push(format!("zzrare{i} standard item"));
    }
    for &num_shards in &[3usize, 8] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: false,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
        let before = default_reads(&cluster, &titles);

        let planted = titles.len() - 40;
        assert!(
            (0..40).all(|i| before[planted + i].contains(&(8_500_000 + i as u64))),
            "precondition: the planted queries are visible"
        );

        // Each rare term becomes interchangeable with `pro`, also in the top-64 mask, so
        // every anchor the planted queries have left is a very common one.
        let mut aliased = Vocab::new();
        for i in 0..40u64 {
            aliased.add_equivalence(&[&format!("zzrare{i}"), "pro"]);
        }
        cluster.set_vocab(aliased).expect("set_vocab");
        // The equivalence took effect: with the broad lane on, a title that says `pro`
        // where the query says `zzrare0` now matches.
        assert!(
            cluster
                .percolate_with_broad("pro standard item", true)
                .expect("percolate")
                .contains(&8_500_000),
            "the planted queries were rewritten by the equivalence"
        );
        let after_alias = default_reads(&cluster, &titles);
        assert_nothing_hidden(
            &format!("K={num_shards} alias"),
            &titles,
            &before,
            &after_alias,
        );

        cluster.resize(num_shards + 2).expect("resize");
        let after_resize = default_reads(&cluster, &titles);
        assert_nothing_hidden(
            &format!("K={num_shards} resize"),
            &titles,
            &after_alias,
            &after_resize,
        );
    }
}

/// The rule keeps what was visible and nothing more. A resize reuses the dictionary, so
/// every row's plan is the one it had: default reads and broad reads are both unchanged,
/// and the rows that were opt-in are still opt-in.
#[test]
fn a_resize_changes_no_read_and_promotes_no_opt_in_row() {
    let (queries, titles) = build_corpus();
    let titles: Vec<String> = titles.into_iter().take(400).collect();
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: false,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
    let broad_reads = |cluster: &ClusterEngine| -> Vec<HashSet<u64>> {
        titles
            .iter()
            .map(|title| {
                cluster
                    .percolate_with_broad(title, true)
                    .expect("percolate")
                    .into_iter()
                    .collect()
            })
            .collect()
    };
    let default_before = default_reads(&cluster, &titles);
    let broad_before = broad_reads(&cluster);
    assert!(
        default_before
            .iter()
            .zip(&broad_before)
            .any(|(default, broad)| default.len() < broad.len()),
        "precondition: some matching query is opt-in"
    );
    let classes_before = cluster.class_counts().expect("class counts");
    let autoscale = AutoscaleConfig::default();
    let replicated_before = cluster
        .collect_load(&autoscale)
        .expect("collect_load")
        .replicated_corpus;
    assert!(
        replicated_before as u64 >= (classes_before[2] + classes_before[3]) / 3,
        "the broad lane is part of the replicated share"
    );

    cluster.resize(5).expect("resize");
    assert_eq!(
        cluster
            .collect_load(&autoscale)
            .expect("collect_load")
            .replicated_corpus,
        replicated_before,
        "the same rows are replicated at any shard count"
    );

    assert_eq!(default_reads(&cluster, &titles), default_before);
    assert_eq!(broad_reads(&cluster), broad_before);
    // Replicated rows are counted once per shard, so compare what share of them is class C.
    let classes_after = cluster.class_counts().expect("class counts");
    assert_eq!(
        classes_after[2] * 3,
        classes_before[2] * 5,
        "class C is replicated to every shard and none of it moved lanes"
    );
    // A class-A row has one anchor and lives on that anchor's shard alone, at any shard
    // count: no selective row was replicated.
    assert_eq!(classes_after[0], classes_before[0]);
}

/// A kept row behaves like any other stored row afterwards: reads with the broad lane on
/// are exact against an independent oracle, a delete removes it everywhere, and a later
/// rebuild neither loses the rows that remain nor brings the deleted one back.
#[test]
fn kept_rows_stay_exact_and_can_be_deleted() {
    let (mut queries, mut titles) = build_corpus();
    titles.truncate(300);
    for i in 0..40u64 {
        queries.push((8_500_000 + i, format!("zzrare{i} standard")));
        titles.push(format!("zzrare{i} standard item"));
    }
    let mut aliased = Vocab::new();
    for i in 0..40u64 {
        aliased.add_equivalence(&[&format!("zzrare{i}"), "pro"]);
    }
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: false,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
    cluster.set_vocab(aliased.clone()).expect("set_vocab");

    let brute = Brute::build_with_equiv(&queries, vocab(), &aliased);
    let mut lc = String::new();
    let mut feats = Vec::new();
    for title in &titles {
        let got: HashSet<u64> = cluster
            .percolate_with_broad(title, true)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, brute.matches(title, &mut lc, &mut feats), "{title:?}");
    }

    let gone = 8_500_007u64;
    let title = "zzrare7 standard item";
    assert!(cluster
        .percolate_with_broad(title, false)
        .expect("percolate")
        .contains(&gone));
    assert!(cluster.remove_query(gone).expect("remove") > 0);
    for include_broad in [false, true] {
        assert!(!cluster
            .percolate_with_broad(title, include_broad)
            .expect("percolate")
            .contains(&gone));
    }

    let before = default_reads(&cluster, &titles);
    cluster.resize(6).expect("resize");
    let after = default_reads(&cluster, &titles);
    assert_nothing_hidden("resize after a delete", &titles, &before, &after);
    assert!(!cluster
        .percolate_with_broad(title, true)
        .expect("percolate")
        .contains(&gone));
}

/// The third rebuild: a durable cluster whose stored rows predate the current compiler is
/// rebuilt on open, before it serves. A row an earlier rebuild kept is kept again.
#[test]
fn a_compiler_migration_on_reopen_keeps_a_kept_row() {
    let dir = std::env::temp_dir().join(format!("rr-adr203-migration-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let queries = vec![
        (1u64, "widget pkg".to_string()),
        (2, "(pkg,package) widget".to_string()),
        (3, "(pkg,package) gadget".to_string()),
    ];
    let titles = vec!["widget pkg".to_string(), "gadget package".to_string()];
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: false,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    let before = {
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
        let mut aliased = Vocab::new();
        aliased.add_equivalence(&["pkg", "package"]);
        cluster.set_vocab(aliased).expect("set_vocab");
        let before = default_reads(&cluster, &titles);
        assert!(before[0].contains(&1), "precondition: query 1 was kept");
        before
    };

    let manifest_path = dir.join("cluster_manifest.bin");
    let manifest = reverse_rusty::storage::read_cluster_manifest(&manifest_path).expect("manifest");
    crate::vocab_reopen::downgrade_cluster_manifest_to_v6(&manifest_path, &manifest);

    let reopened = ClusterEngine::open(&dir, vocab(), Some(&cfg)).expect("migrating reopen");
    assert_eq!(
        reopened.placement_generation().0,
        manifest.placement_generation.0 + 1,
        "the reopen rebuilt the cluster"
    );
    let after = default_reads(&reopened, &titles);
    assert_nothing_hidden("compiler migration", &titles, &before, &after);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A kept row is on every position, so adding shards does not shrink it and it is not load
/// a split relieves. Sixty single-term queries written to an empty cluster are selective.
/// A vocabulary rebuild ranks every one of their terms into the top-64 mask, so all sixty
/// are kept and replicated; counted as splittable, they would recommend a larger cluster
/// after every resize.
#[test]
fn kept_rows_do_not_drive_a_resize() {
    for replication_factor in [1usize, 2] {
        let cfg = ClusterConfig {
            num_shards: 3,
            replication_factor,
            include_broad: false,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("build");
        for i in 0..60u64 {
            cluster
                .add_query(i + 1, &format!("zzsolo{i}"))
                .expect("add");
        }
        let titles: Vec<String> = (0..60).map(|i| format!("zzsolo{i} item")).collect();
        let before = default_reads(&cluster, &titles);
        assert!(
            (0..60).all(|i| before[i].contains(&(i as u64 + 1))),
            "precondition: every query is visible"
        );
        let autoscale = AutoscaleConfig {
            enabled: true,
            target_replication_factor: replication_factor,
            max_node_load_skew: 0.0,
            split_corpus_threshold: 25,
        };
        assert_eq!(
            cluster
                .collect_load(&autoscale)
                .expect("collect_load")
                .replicated_corpus,
            0,
            "precondition: every query is selective"
        );

        cluster.set_vocab(Vocab::new()).expect("set_vocab");
        let after = default_reads(&cluster, &titles);
        assert_nothing_hidden("rebuild", &titles, &before, &after);

        let load = cluster.collect_load(&autoscale).expect("collect_load");
        assert_eq!(
            load.shard_corpus,
            vec![60, 60, 60],
            "every query was kept, so every shard holds all of them"
        );
        assert_eq!(load.replicated_corpus, 60);
        assert_eq!(
            cluster
                .resize_to_recommended(&autoscale)
                .expect("resize_to_recommended"),
            None,
            "replicated rows are not a reason to add shards"
        );
        assert_eq!(cluster.num_shards(), 3);

        // A new write of a term the rebuilt mask ranks as very common is class C: opt-in,
        // on every shard, and counted with the replicated rows while it is still unsealed.
        cluster.add_query(1_000, "zzsolo0").expect("add");
        assert!(!cluster
            .percolate_with_broad("zzsolo0 item", false)
            .expect("percolate")
            .contains(&1_000));
        assert_eq!(
            cluster
                .collect_load(&autoscale)
                .expect("collect_load")
                .replicated_corpus,
            61
        );
    }
}
