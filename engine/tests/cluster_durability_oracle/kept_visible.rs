//! A row a rebuild kept in default reads (ADR-203) is still there after a restart.
//!
//! The rebuild stores the row replicated always-visible, in each shard's main lane. Both
//! facts live in the sealed segments, so a reopen must find the row where the rebuild left
//! it, and a rebuild after the reopen must read the kept placement back and keep it again.

use crate::harness::*;

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

/// The part of each shard's corpus that is stored on every shard.
fn replicated_corpus(cluster: &ClusterEngine) -> usize {
    cluster
        .collect_load(&reverse_rusty::cluster::AutoscaleConfig::default())
        .expect("collect_load")
        .replicated_corpus
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

#[test]
fn a_kept_row_survives_reopen_and_a_rebuild_after_it() {
    let (mut queries, mut titles) = build_corpus();
    titles.truncate(200);
    // Anchored on a rare term beside `standard`, which this corpus makes very common. The
    // equivalence below moves the rare term into an any-of group with `pro`, also very
    // common, so the rebuilt plan has no anchor outside the top-64 mask.
    for i in 0..40u64 {
        queries.push((8_500_000 + i, format!("zzrare{i} standard")));
        titles.push(format!("zzrare{i} standard item"));
    }
    let aliased = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        for i in 0..40u64 {
            v.add_equivalence(&[&format!("zzrare{i}"), "pro"]);
        }
        v
    };

    for &k in &[1usize, 3] {
        let dir = unique_dir(&format!("kept_visible_k{k}"));
        let (before, replicated) = {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            let before = default_reads(&cluster, &titles);
            cluster.set_vocab(aliased()).expect("set_vocab");
            let after = default_reads(&cluster, &titles);
            assert_nothing_hidden(&format!("k={k} alias"), &titles, &before, &after);
            (before, replicated_corpus(&cluster))
        };
        assert!(
            replicated > 0,
            "k={k}: the rebuild kept rows on every shard"
        );

        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        let after_reopen = default_reads(&reopened, &titles);
        assert_nothing_hidden(&format!("k={k} reopen"), &titles, &before, &after_reopen);
        // Sealed segments report the same replicated share, so a restart does not turn kept
        // rows into load a split would seem to relieve.
        assert_eq!(replicated_corpus(&reopened), replicated, "k={k}");

        // The rebuild reads each row's kept placement back from the reopened shards.
        reopened.resize(k + 2).expect("resize");
        let after_resize = default_reads(&reopened, &titles);
        assert_nothing_hidden(&format!("k={k} resize"), &titles, &before, &after_resize);
        drop(reopened);

        let again = ClusterEngine::open(dir.clone(), vocab(), None).expect("second reopen");
        let after_second = default_reads(&again, &titles);
        assert_nothing_hidden(
            &format!("k={k} second reopen"),
            &titles,
            &before,
            &after_second,
        );
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
