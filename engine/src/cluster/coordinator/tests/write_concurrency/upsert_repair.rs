//! ADR-185 on the partial-failure paths (ADR-047): a repair re-drives an upsert as the
//! same atomic per-shard replace; a failed install never lets the old copies be removed
//! first, and never strands them either; and a stale copy a failed tombstone left behind
//! is removed by the next upsert even when that upsert's own placement is unchanged.

use super::*;

type Calls = Arc<std::sync::Mutex<Vec<(usize, WriteCall)>>>;

/// Record every shard write for `logical`, failing the ones `fail` selects.
fn record_writes(
    cluster: &mut ClusterEngine,
    logical: u64,
    fail: impl Fn(usize, WriteCall) -> bool + Send + Sync + 'static,
) -> Calls {
    let calls: Calls = Arc::default();
    instrument(cluster, {
        let calls = Arc::clone(&calls);
        Arc::new(move |position, call| {
            let ours = matches!(
                call,
                WriteCall::Insert(id) | WriteCall::Delete(id) | WriteCall::Replace(id, _)
                    if id == logical
            );
            if ours {
                calls.lock().expect("calls").push((position, call));
                if fail(position, call) {
                    return Err(ShardError::Remote("injected shard failure".into()));
                }
            }
            Ok(())
        })
    });
    calls
}

fn shard_of(cluster: &ClusterEngine, dsl: &str) -> usize {
    let ast = crate::dsl::parse(dsl).expect("dsl");
    let mut lc = String::new();
    let ex = crate::compile::extract_readonly(&ast, &cluster.norm, &cluster.dict, &mut lc);
    match placement_of(
        &cluster.dict,
        &cluster.ring,
        &ex,
        true,
        cluster.per_shard.hot_anchor_threshold,
    ) {
        Target::Selective(shards) => shards[0],
        _ => panic!("{dsl} is not a selective query"),
    }
}

/// Two one-token bodies that live on different shards.
fn two_homes(cluster: &ClusterEngine) -> (String, String) {
    let tokens: Vec<String> = (0..64).map(|i| format!("zzfix{i}")).collect();
    let old = tokens[0].clone();
    let new = tokens
        .iter()
        .find(|token| shard_of(cluster, token) != shard_of(cluster, &old))
        .expect("a token on another shard")
        .clone();
    (old, new)
}

#[test]
fn resync_redrives_an_upsert_as_one_atomic_replace() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let body = "zzredrive";
    let home = shard_of(&cluster, body);
    cluster.upsert_query(999, body, 1).expect("seed");
    let failing = Arc::new(AtomicBool::new(true));
    let calls = record_writes(&mut cluster, 999, {
        let failing = Arc::clone(&failing);
        move |_, _| failing.load(Ordering::SeqCst)
    });

    assert!(matches!(
        cluster.upsert_query(999, body, 2),
        Err(ShardError::PartiallyApplied { .. })
    ));
    assert_eq!(
        cluster.percolate(body).expect("read"),
        vec![999],
        "a failed replace leaves the old version serving"
    );
    failing.store(false, Ordering::SeqCst);
    calls.lock().expect("calls").clear();

    let report = cluster.resync();
    assert_eq!((report.repaired, report.still_pending), (1, 0));
    let repair = calls.lock().expect("calls").clone();
    assert_eq!(
        repair
            .iter()
            .filter(|(shard, _)| *shard == home)
            .collect::<Vec<_>>(),
        vec![&(
            home,
            WriteCall::Replace(999, crate::cluster::shard::ReplaceMode::Unconditional)
        )],
        "the repair is one atomic replace on the failed shard, never a delete then an insert"
    );
    // The failed probe never said whether this was a move, so the repair also clears the
    // id from the shards outside the placement, after the install.
    assert_eq!(repair.first().map(|(shard, _)| *shard), Some(home));
    assert!(
        repair[1..]
            .iter()
            .all(|(_, call)| matches!(call, WriteCall::Delete(999))),
        "{repair:?}"
    );
    assert_eq!(cluster.percolate(body).expect("read"), vec![999]);
}

#[test]
fn the_next_upsert_sweeps_a_stale_copy_a_failed_move_left_behind() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let (old, new) = two_homes(&cluster);
    let old_home = shard_of(&cluster, &old);
    cluster.upsert_query(999, &old, 1).expect("seed");
    // The move's tombstone on the old shard fails: the new version lands, the old stays.
    let failing = Arc::new(AtomicBool::new(true));
    let calls = record_writes(&mut cluster, 999, {
        let failing = Arc::clone(&failing);
        move |position, call| {
            failing.load(Ordering::SeqCst)
                && position == old_home
                && matches!(call, WriteCall::Delete(_))
        }
    });
    assert!(matches!(
        cluster.upsert_query(999, &new, 2),
        Err(ShardError::PartiallyApplied { .. })
    ));
    assert_eq!(cluster.pending_repairs(), 1);
    assert_eq!(
        cluster.percolate(&old).expect("read"),
        vec![999],
        "precondition: the stale old version is still on its shard"
    );

    // A re-put that keeps the NEW placement. Its fast path replaces only the placement
    // shard, so without the sweep the stale copy would outlive the cleared repair entry.
    failing.store(false, Ordering::SeqCst);
    calls.lock().expect("calls").clear();
    cluster.upsert_query(999, &new, 3).expect("re-put");
    assert_eq!(cluster.pending_repairs(), 0);
    assert_eq!(
        cluster.percolate(&old).expect("read"),
        Vec::<u64>::new(),
        "the stale copy was swept"
    );
    assert_eq!(cluster.percolate(&new).expect("read"), vec![999]);
    assert_eq!(
        cluster
            .percolate(&format!("{old} {new}"))
            .expect("a title matching both versions"),
        vec![999]
    );
    assert!(
        calls
            .lock()
            .expect("calls")
            .contains(&(old_home, WriteCall::Delete(999))),
        "the sweep reached the old shard"
    );
}

/// A cluster with query 999 seeded as `old`, about to move to `new` on another shard.
/// Returns `(cluster, old, new, new_home)`.
fn seeded_for_a_move() -> (ClusterEngine, String, String, usize) {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let (old, new) = two_homes(&cluster);
    let new_home = shard_of(&cluster, &new);
    cluster.upsert_query(999, &old, 1).expect("seed");
    (cluster, old, new, new_home)
}

fn deletes(calls: &Calls) -> usize {
    calls
        .lock()
        .expect("calls")
        .iter()
        .filter(|(_, call)| matches!(call, WriteCall::Delete(_)))
        .count()
}

/// The move is finished: only the new version exists, exactly once.
fn assert_moved(cluster: &ClusterEngine, old: &str, new: &str) {
    assert_eq!(
        cluster.percolate(old).expect("read"),
        Vec::<u64>::new(),
        "the old version is gone from its shard"
    );
    assert_eq!(cluster.percolate(new).expect("read"), vec![999]);
    assert_eq!(
        cluster
            .percolate(&format!("{old} {new}"))
            .expect("a title matching both versions"),
        vec![999]
    );
}

/// The conditional probe on the new shard errors, so the coordinator never learns that
/// this upsert is a move. The old copy on the other shard must still be queued for
/// removal: a repair that only replaced on the failed shard would report success and
/// leave the old body matchable for good.
#[test]
fn a_failed_placement_probe_does_not_strand_the_old_copy() {
    let (mut cluster, old, new, new_home) = seeded_for_a_move();
    let failing = Arc::new(AtomicBool::new(true));
    let calls = record_writes(&mut cluster, 999, {
        let failing = Arc::clone(&failing);
        move |position, _| failing.load(Ordering::SeqCst) && position == new_home
    });

    match cluster.upsert_query(999, &new, 2) {
        Err(ShardError::PartiallyApplied { failed, .. }) => {
            assert_eq!(
                failed,
                vec![new_home],
                "only the shard that failed is reported"
            );
        }
        other => panic!("expected a partial apply, got {other:?}"),
    }
    assert_eq!(deletes(&calls), 0, "nothing is removed before the install");
    assert_eq!(
        cluster
            .percolate(&format!("{old} {new}"))
            .expect("a title matching both versions"),
        vec![999],
        "the old version keeps serving until the repair"
    );

    failing.store(false, Ordering::SeqCst);
    calls.lock().expect("calls").clear();
    let report = cluster.resync();
    assert_eq!((report.repaired, report.still_pending), (1, 0));
    let repair = calls.lock().expect("calls").clone();
    assert_eq!(
        repair.first(),
        Some(&(
            new_home,
            WriteCall::Replace(999, crate::cluster::shard::ReplaceMode::Unconditional)
        )),
        "the repair installs the new version before it removes anything: {repair:?}"
    );
    assert_eq!(
        repair.len(),
        4,
        "then one tombstone per other shard: {repair:?}"
    );
    assert_moved(&cluster, &old, &new);
}

/// The conditional probe declines (a move), and the install inside the fence fails.
/// Removing the old copy now would leave a title that matches both versions with
/// neither, so the tombstones wait, across a repair pass that still cannot install.
#[test]
fn a_move_whose_install_fails_keeps_the_old_version_until_repair() {
    let (mut cluster, old, new, new_home) = seeded_for_a_move();
    let failing = Arc::new(AtomicBool::new(true));
    let calls = record_writes(&mut cluster, 999, {
        let failing = Arc::clone(&failing);
        move |position, call| {
            failing.load(Ordering::SeqCst)
                && position == new_home
                && call
                    == WriteCall::Replace(999, crate::cluster::shard::ReplaceMode::Unconditional)
        }
    });
    let both = format!("{old} {new}");

    assert!(matches!(
        cluster.upsert_query(999, &new, 2),
        Err(ShardError::PartiallyApplied { .. })
    ));
    assert_eq!(deletes(&calls), 0, "nothing is removed before the install");
    assert_eq!(cluster.percolate(&both).expect("read"), vec![999]);

    // The shard is still down: the repair cannot install, so it must not remove either.
    let report = cluster.resync();
    assert_eq!((report.repaired, report.still_pending), (0, 1));
    assert_eq!(
        deletes(&calls),
        0,
        "a repair that cannot install removes nothing"
    );
    assert_eq!(cluster.percolate(&both).expect("read"), vec![999]);

    failing.store(false, Ordering::SeqCst);
    let report = cluster.resync();
    assert_eq!((report.repaired, report.still_pending), (1, 0));
    assert_eq!(
        deletes(&calls),
        3,
        "the tombstones ran once the install landed"
    );
    assert_moved(&cluster, &old, &new);
}
