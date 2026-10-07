//! A layout change builds from a corpus that is still (ADR-209).
//!
//! A write that passed the fence check before the fence went up is still on its way to a
//! shard. A rebuild that gathered the corpus now would build the new layout without it, and
//! the write would then land in shards nothing reads any more. So raising the fence waits,
//! once, for every such write.

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
