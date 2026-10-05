//! ADR-185 on the partial-failure paths (ADR-047): a repair re-drives an upsert as the
//! same atomic per-shard replace, and a stale copy a failed move left behind is removed by
//! the next upsert even when that upsert's own placement is unchanged.

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
    assert_eq!(
        *calls.lock().expect("calls"),
        vec![(
            home,
            WriteCall::Replace(999, crate::cluster::shard::ReplaceMode::Unconditional)
        )],
        "the repair is one atomic replace on the failed shard, never a delete then an insert"
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
