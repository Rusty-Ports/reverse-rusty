//! A layout change builds from a corpus that is still, and what changes with the layout
//! changes at the same moment (ADR-209). These tests stop a write, or a rebuild, half-way.

use super::*;

#[test]
fn a_layout_change_waits_for_a_write_that_has_not_reached_its_shard() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let seeded: Vec<(u64, String)> = (1..=100u64)
        .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 7)))
        .collect();
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seeded).expect("cluster");
    let gate = Arc::new(FirstAppendGate::default());
    let once = Arc::new(AtomicBool::new(true));
    instrument(&mut cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |_position, call| {
            if matches!(call, WriteCall::Insert(9_003)) && once.swap(false, Ordering::SeqCst) {
                pause(&gate);
            }
            Ok(())
        })
    });
    let resized_beside_it = std::thread::scope(|scope| {
        let writer = scope.spawn(|| cluster.add_query(9_003, "zzmid zzflight"));
        gate.wait_until_entered();
        let resizer = scope.spawn(|| cluster.resize(5));
        std::thread::sleep(Duration::from_millis(300));
        let resized_beside_it = resizer.is_finished();
        gate.release_first();
        writer
            .join()
            .expect("writer")
            .expect("the write was admitted before the fence");
        resizer.join().expect("resizer").expect("resize");
        resized_beside_it
    });
    assert_eq!(
        cluster
            .percolate_with_broad("zzmid zzflight", true)
            .expect("read"),
        vec![9_003],
        "an acknowledged write is missing from the layout that replaced the one it was written to"
    );
    assert!(
        !resized_beside_it,
        "the resize did not wait for the write in flight"
    );
    assert_eq!(cluster.num_shards(), 5);
}

/// A frozen read view takes several steps (match, then fetch sources) and must take them all
/// on one layout. It
/// holds the exclusive side of the mutation barrier, and a layout is published on that side
/// too, so a view opened while a rebuild is building keeps the swap waiting until it is done.
#[test]
fn a_layout_is_not_published_under_a_frozen_read_view() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let seeded: Vec<(u64, String)> = (1..=100u64)
        .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 7)))
        .collect();
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seeded).expect("cluster");
    let gate = Arc::new(FirstAppendGate::default());
    let once = Arc::new(AtomicBool::new(true));
    instrument(&mut cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |_position, call| {
            if matches!(call, WriteCall::Gather) && once.swap(false, Ordering::SeqCst) {
                pause(&gate);
            }
            Ok(())
        })
    });
    let (published_under_it, matched, again) = std::thread::scope(|scope| {
        let resizer = scope.spawn(|| cluster.resize(5));
        // The rebuild has fenced writes and is reading the corpus: the barrier is free.
        gate.wait_until_entered();
        let view = cluster.consistent_read_view();
        gate.release_first();
        std::thread::sleep(Duration::from_millis(300));
        let published_under_it = resizer.is_finished() || cluster.num_shards() != 3;
        let matched = view
            .percolate_filtered_with_stats("zzitem9 zzgroup2", &[], true)
            .map(|(matched, _)| matched);
        let again = view
            .percolate_filtered_with_stats("zzitem40 zzgroup5", &[], true)
            .map(|(matched, _)| matched);
        // Released before anything can fail: the resize is waiting for it.
        drop(view);
        resizer.join().expect("resizer").expect("resize");
        (published_under_it, matched, again)
    });
    assert!(
        !published_under_it,
        "a layout was published while a frozen read view was open"
    );
    assert_eq!(matched.expect("match under the view"), vec![9]);
    assert_eq!(again.expect("second match under the view"), vec![40]);
    assert_eq!(cluster.num_shards(), 5);
}

fn exhaustive(cluster: &ClusterEngine, title: &str) -> Result<Vec<u64>, ShardError> {
    let mut sink = RecordingExhaustiveSink::default();
    cluster.try_percolate_filtered_all(
        title,
        &[],
        crate::result::QueryScope::Standard,
        None,
        16,
        None,
        &mut sink,
    )?;
    let mut ids: Vec<u64> = sink
        .chunks
        .iter()
        .flat_map(|chunk| chunk.matches.iter().map(|matched| matched.logical_id))
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// A queued repair says the old layout's shards disagree about an id, and an exhaustive read
/// refuses to certify a result while one is queued. A resize drops the queue, because the
/// layout it builds has no such disagreement. It has to drop it when that layout is
/// published and not before: until then exhaustive reads still run on the old shards.
#[test]
fn a_queued_repair_stays_until_the_new_layout_is_published() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    cluster.upsert_query(999, "zzredrive", 1).expect("seed");
    let failing = Arc::new(AtomicBool::new(true));
    let gate = Arc::new(FirstAppendGate::default());
    let once = Arc::new(AtomicBool::new(true));
    instrument(&mut cluster, {
        let (failing, gate) = (Arc::clone(&failing), Arc::clone(&gate));
        Arc::new(move |_position, call| match call {
            WriteCall::Insert(999) | WriteCall::Delete(999) | WriteCall::Replace(999, _)
                if failing.load(Ordering::SeqCst) =>
            {
                Err(ShardError::Remote("injected shard failure".into()))
            }
            WriteCall::Gather if once.swap(false, Ordering::SeqCst) => {
                pause(&gate);
                Ok(())
            }
            _ => Ok(()),
        })
    });
    assert!(matches!(
        cluster.upsert_query(999, "zzredrive", 2),
        Err(ShardError::PartiallyApplied { .. })
    ));
    failing.store(false, Ordering::SeqCst);
    assert_eq!(cluster.pending_repairs(), 1);

    let (queued_while_building, read_beside_it, read) = std::thread::scope(|scope| {
        let resizer = scope.spawn(|| cluster.resize(6));
        gate.wait_until_entered();
        let queued = cluster.pending_repairs();
        // An exhaustive read is not a search: it certifies a result, so it waits.
        let reader = scope.spawn(|| exhaustive(&cluster, "zzredrive"));
        std::thread::sleep(Duration::from_millis(200));
        let read_beside_it = reader.is_finished();
        gate.release_first();
        resizer.join().expect("resizer").expect("resize");
        (queued, read_beside_it, reader.join().expect("reader"))
    });
    assert_eq!(
        queued_while_building, 1,
        "the repair queue was emptied while the old layout was still serving"
    );
    assert!(
        !read_beside_it,
        "an exhaustive read ran on the old layout while it was being replaced"
    );
    assert_eq!(cluster.pending_repairs(), 0);
    assert_eq!(
        read.expect("exhaustive read on the rebuilt layout"),
        vec![999]
    );
}

/// Measuring the load and resizing to what it recommends are one layout change. Measured
/// first and applied later, a recommendation for three shards would be applied to the twelve
/// another resize had just built, and shrink them.
#[test]
fn a_recommended_resize_is_decided_on_the_layout_it_replaces() {
    use crate::cluster::autoscale::AutoscaleConfig;

    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let seeded: Vec<(u64, String)> = (1..=600u64)
        .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 7)))
        .collect();
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seeded).expect("cluster");
    // About 200 queries a shard at three shards, about 50 at twelve.
    let autoscale = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: 0.0,
        split_corpus_threshold: 120,
    };
    let at_three = cluster.collect_load(&autoscale).expect("load");
    assert!(
        crate::cluster::recommended_shard_count(&at_three, &autoscale).is_some(),
        "three shards of this corpus are over the threshold"
    );
    let gate = Arc::new(FirstAppendGate::default());
    let once = Arc::new(AtomicBool::new(true));
    instrument(&mut cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |_position, call| {
            if matches!(call, WriteCall::Gather) && once.swap(false, Ordering::SeqCst) {
                pause(&gate);
            }
            Ok(())
        })
    });
    let recommended = std::thread::scope(|scope| {
        let resizer = scope.spawn(|| cluster.resize(12));
        gate.wait_until_entered();
        let recommender = scope.spawn(|| cluster.resize_to_recommended(&autoscale));
        std::thread::sleep(Duration::from_millis(200));
        gate.release_first();
        resizer.join().expect("resizer").expect("resize");
        recommender.join().expect("recommender")
    });
    assert_eq!(
        cluster.num_shards(),
        12,
        "a recommendation measured before another resize was applied after it"
    );
    assert_eq!(recommended.expect("recommended resize"), None);
}
