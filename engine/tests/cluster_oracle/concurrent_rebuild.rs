//! A resize or a vocabulary change runs beside reads and holds only writes out (ADR-209).
//!
//! Every operation runs on the layout that was published when it started. So a read that
//! overlaps a rebuild returns either what the old layout returns or what the new one does,
//! never a mix of the two, and a write is either refused or lands in the layout that reads use
//! afterwards.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::normalize::Normalizer;

/// One query per id, each with a term of its own, so the title of id `n` matches `[n]` and
/// nothing else under any shard count.
fn corpus(count: u64) -> Vec<(u64, String)> {
    (1..=count)
        .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 7)))
        .collect()
}

fn title_of(id: u64) -> String {
    format!("zzitem{id} zzgroup{}", id % 7)
}

fn cluster(shards: usize, queries: &[(u64, String)]) -> ClusterEngine {
    let config = ClusterConfig {
        num_shards: shards,
        include_broad: true,
        ..Default::default()
    };
    ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        queries,
    )
    .expect("cluster")
}

#[test]
fn reads_answer_through_resizes_and_never_mix_two_layouts() {
    const QUERIES: u64 = 4_000;
    let cluster = cluster(3, &corpus(QUERIES));
    let done = AtomicBool::new(false);
    let resizing = AtomicBool::new(false);
    let during = AtomicUsize::new(0);
    let wrong = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for reader in 0..3u64 {
            let (cluster, done, resizing, during, wrong) =
                (&cluster, &done, &resizing, &during, &wrong);
            scope.spawn(move || {
                let mut id = reader * 97 % QUERIES + 1;
                while !done.load(Ordering::SeqCst) {
                    let overlapped = resizing.load(Ordering::SeqCst);
                    match cluster.percolate_with_broad(&title_of(id), true) {
                        Ok(matched) if matched == vec![id] => {}
                        other => wrong
                            .lock()
                            .expect("wrong")
                            .push((id, format!("{other:?}"))),
                    }
                    if overlapped && resizing.load(Ordering::SeqCst) {
                        during.fetch_add(1, Ordering::SeqCst);
                    }
                    id = id % QUERIES + 1;
                }
            });
        }
        for shards in [5, 2, 8, 3, 6, 4, 9, 3] {
            resizing.store(true, Ordering::SeqCst);
            cluster.resize(shards).expect("resize");
            resizing.store(false, Ordering::SeqCst);
            assert_eq!(cluster.num_shards(), shards);
        }
        done.store(true, Ordering::SeqCst);
    });
    let wrong = wrong.into_inner().expect("wrong");
    assert!(
        wrong.is_empty(),
        "reads across a resize: {:?}",
        &wrong[..wrong.len().min(5)]
    );
    assert!(
        during.load(Ordering::SeqCst) > 0,
        "no read ran while a resize was in progress; the test proved nothing"
    );
}

#[test]
fn a_read_across_a_vocabulary_change_sees_the_old_vocabulary_or_the_new_one() {
    let mut queries = corpus(3_000);
    queries.push((900_001, "zzwidget pkg".into()));
    let cluster = cluster(3, &queries);
    // Before the change the title does not match; after it, it does.
    assert!(cluster
        .percolate_with_broad("zzwidget package", true)
        .expect("before")
        .is_empty());
    let done = AtomicBool::new(false);
    let seen = Mutex::new(BTreeSet::new());
    let went_back = AtomicBool::new(false);
    std::thread::scope(|scope| {
        for _ in 0..3 {
            let (cluster, done, seen, went_back) = (&cluster, &done, &seen, &went_back);
            scope.spawn(move || {
                let mut after = false;
                while !done.load(Ordering::SeqCst) {
                    let matched = cluster
                        .percolate_with_broad("zzwidget package", true)
                        .expect("read");
                    // The swap is one step: a reader that has seen the new vocabulary never
                    // sees the old one again.
                    if after && matched.is_empty() {
                        went_back.store(true, Ordering::SeqCst);
                    }
                    after |= matched == vec![900_001];
                    seen.lock().expect("seen").insert(matched);
                    // An unrelated title matches the same throughout.
                    assert_eq!(
                        cluster
                            .percolate_with_broad(&title_of(17), true)
                            .expect("read"),
                        vec![17]
                    );
                }
            });
        }
        let report = cluster
            .import_alias_synonyms("package, pkg")
            .expect("alias");
        assert!(report.applied);
        done.store(true, Ordering::SeqCst);
    });
    let seen = seen.into_inner().expect("seen");
    assert!(
        seen.iter()
            .all(|matched| matched.is_empty() || *matched == vec![900_001]),
        "a read saw neither the old result nor the new one: {seen:?}"
    );
    assert!(
        !went_back.load(Ordering::SeqCst),
        "a reader saw the old vocabulary after the new"
    );
    assert_eq!(
        cluster
            .percolate_with_broad("zzwidget package", true)
            .expect("after"),
        vec![900_001]
    );
}

#[test]
fn a_write_that_races_a_resize_is_refused_or_kept_never_lost() {
    const SEEDED: u64 = 3_000;
    let cluster = cluster(3, &corpus(SEEDED));
    let done = AtomicBool::new(false);
    let next = AtomicUsize::new(SEEDED as usize + 1);
    let acknowledged = Mutex::new(Vec::new());
    let refused = AtomicUsize::new(0);
    let unexpected = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..3 {
            let (cluster, done, next, acknowledged, refused, unexpected) =
                (&cluster, &done, &next, &acknowledged, &refused, &unexpected);
            scope.spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    let id = next.fetch_add(1, Ordering::SeqCst) as u64;
                    match cluster.add_query(id, &title_of(id)) {
                        Ok(AddOutcome::Placed { .. } | AddOutcome::Replicated { .. }) => {
                            acknowledged.lock().expect("acknowledged").push(id);
                        }
                        // Refused while a rebuild holds writes out: the caller retries.
                        Err(error) if error.to_string().contains("writes are paused") => {
                            refused.fetch_add(1, Ordering::SeqCst);
                        }
                        other => unexpected
                            .lock()
                            .expect("unexpected")
                            .push(format!("{id}: {other:?}")),
                    }
                }
            });
        }
        for shards in [5, 2, 7, 3, 6, 4, 8, 3] {
            cluster.resize(shards).expect("resize");
        }
        done.store(true, Ordering::SeqCst);
    });
    let unexpected = unexpected.into_inner().expect("unexpected");
    assert!(
        unexpected.is_empty(),
        "{:?}",
        &unexpected[..unexpected.len().min(5)]
    );
    let acknowledged = acknowledged.into_inner().expect("acknowledged");
    assert!(!acknowledged.is_empty(), "no write was acknowledged");
    let lost: Vec<u64> = acknowledged
        .iter()
        .copied()
        .filter(|&id| {
            cluster
                .percolate_with_broad(&title_of(id), true)
                .expect("read")
                != vec![id]
        })
        .collect();
    assert!(
        lost.is_empty(),
        "{} of {} acknowledged writes are not matched after the resizes ({} were refused): {:?}",
        lost.len(),
        acknowledged.len(),
        refused.load(Ordering::SeqCst),
        &lost[..lost.len().min(8)]
    );
    // The seeded corpus is intact too.
    for id in 1..=SEEDED {
        assert_eq!(
            cluster
                .percolate_with_broad(&title_of(id), true)
                .expect("read"),
            vec![id]
        );
    }
}
