//! A cluster write that not every shard took is a failure its caller retries (ADR-194).
//!
//! The repair queue is the coordinator's memory, so nothing may tell a caller the write is
//! safe, and the retry the caller is asked for has to converge: for an upsert, for a create,
//! and when the shard had in fact applied the write before its reply was lost.

use super::*;

/// A three-shard `from_parts` cluster over fault-injecting shards.
struct Faulty {
    cluster: ClusterEngine,
    /// While set, every shard write fails without being applied.
    fail: Arc<AtomicBool>,
    /// While set, a placed insert is applied and then reported as failed.
    lose_acks: Arc<AtomicBool>,
}

fn faulty_cluster() -> Faulty {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    // A throwaway build gives a frozen norm + dict to share.
    let seed = vec![(100u64, "1994 acme appliance".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let (norm, dict, tag_dict) = (
        Arc::clone(&real.layout().norm),
        Arc::clone(&real.layout().dict),
        Arc::clone(&real.tag_dict),
    );
    let fail = Arc::new(AtomicBool::new(false));
    let lose_acks = Arc::new(AtomicBool::new(false));
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|_| {
            let local = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(
                ToggleFailShard::new(local, Arc::clone(&fail))
                    .losing_insert_acks(Arc::clone(&lose_acks)),
            ) as Box<dyn Shard>
        })
        .collect();
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
    let durable = ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    let cluster = ClusterEngine::from_parts(
        norm,
        dict,
        tag_dict,
        ring,
        shards,
        cfg.include_broad,
        1,
        cfg.per_shard.clone(),
        durable,
    )
    .expect("from_parts cluster");
    Faulty {
        cluster,
        fail,
        lose_acks,
    }
}

/// One out-of-dictionary required term: class A, so it is placed on a single shard.
const FIRST: &str = "zzretryfirst";
const SECOND: &str = "zzretrysecond";

fn matches(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    cluster.percolate(title).expect("percolate")
}

/// Every live row's logical id, shard by shard. A shard refuses to enumerate when it holds
/// two live rows for one id, so this also proves there is no such pair.
fn live_rows(cluster: &ClusterEngine) -> Vec<u64> {
    let mut rows = Vec::new();
    for shard in cluster.layout().shards.iter() {
        rows.extend(
            shard
                .live_logical_ids()
                .expect("a shard enumerates its ids"),
        );
    }
    rows.sort_unstable();
    rows
}

fn create(cluster: &ClusterEngine, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
    cluster.create_query_with_tags(id, dsl, 1, &[])
}

#[test]
fn a_retried_create_converges_its_own_earlier_attempt() {
    let Faulty { cluster, fail, .. } = faulty_cluster();
    fail.store(true, Ordering::Release);
    let first = create(&cluster, 7, FIRST).expect_err("the shard refuses the insert");
    assert!(
        matches!(&first, ShardError::PartiallyApplied { applied, .. } if applied.is_empty()),
        "{first:?}"
    );
    assert_eq!(cluster.pending_repair_ids(), vec![7]);

    // Still failing: the retry is a retryable failure too. "Already exists" would tell the
    // caller its document is stored, and no shard holds it.
    let retry = create(&cluster, 7, FIRST).expect_err("the shard still refuses");
    assert!(
        matches!(
            retry,
            ShardError::EarlierWriteUnconverged { logical: 7, .. }
        ),
        "{retry:?}"
    );
    assert_eq!(cluster.pending_repair_ids(), vec![7]);
    assert!(matches(&cluster, FIRST).is_empty());

    // The shard is back: the retry delivers the earlier attempt, and only then is the id a
    // true conflict.
    fail.store(false, Ordering::Release);
    let healed = create(&cluster, 7, FIRST).expect_err("the document now exists");
    assert!(
        matches!(healed, ShardError::DuplicateLogicalId(7)),
        "{healed:?}"
    );
    assert!(cluster.pending_repair_ids().is_empty());
    assert_eq!(matches(&cluster, FIRST), vec![7]);
    assert_eq!(live_rows(&cluster), vec![7]);
}

/// A cluster whose document 7 has a delete that no shard took yet, and the create that was
/// refused because of it.
fn half_deleted_then_blocked() -> (Faulty, ShardError) {
    let faulty = faulty_cluster();
    create(&faulty.cluster, 7, FIRST).expect("create");
    faulty.fail.store(true, Ordering::Release);
    let removed = faulty
        .cluster
        .remove_query(7)
        .expect_err("the shards refuse");
    assert!(
        matches!(removed, ShardError::PartiallyApplied { .. }),
        "{removed:?}"
    );
    assert_eq!(
        matches(&faulty.cluster, FIRST),
        vec![7],
        "nothing was deleted yet"
    );
    let blocked = create(&faulty.cluster, 7, SECOND).expect_err("the delete is not converged");
    (faulty, blocked)
}

#[test]
fn a_create_after_a_half_applied_delete_finishes_the_delete_first() {
    let (Faulty { cluster, fail, .. }, blocked) = half_deleted_then_blocked();
    // While the delete cannot finish, the id is neither free nor simply taken.
    assert!(
        matches!(
            blocked,
            ShardError::EarlierWriteUnconverged { logical: 7, .. }
        ),
        "{blocked:?}"
    );

    fail.store(false, Ordering::Release);
    create(&cluster, 7, SECOND).expect("the delete finishes, then the id is free");
    assert!(cluster.pending_repair_ids().is_empty());
    assert!(matches(&cluster, FIRST).is_empty());
    assert_eq!(matches(&cluster, SECOND), vec![7]);
    assert_eq!(live_rows(&cluster), vec![7]);
}

/// The refusal is about the EARLIER write. The create itself was not applied and is not
/// queued, so it must not read as a partial apply of its own: a resync finishes the delete
/// and nothing else, and the caller has to send the create again.
#[test]
fn converging_the_earlier_write_does_not_perform_the_blocked_create() {
    let (Faulty { cluster, fail, .. }, blocked) = half_deleted_then_blocked();
    let ShardError::EarlierWriteUnconverged {
        logical, pending, ..
    } = &blocked
    else {
        panic!("{blocked:?}");
    };
    assert_eq!((*logical, pending.len()), (7, 3));
    assert!(blocked.to_string().contains("not applied or queued"));
    assert_eq!(
        cluster.pending_repair_ids(),
        vec![7],
        "only the delete is queued"
    );

    fail.store(false, Ordering::Release);
    assert_eq!(cluster.resync().repaired, 1);
    assert!(cluster.pending_repair_ids().is_empty());
    assert!(matches(&cluster, FIRST).is_empty(), "the delete finished");
    assert!(
        matches(&cluster, SECOND).is_empty(),
        "nothing performed the create"
    );
    create(&cluster, 7, SECOND).expect("sent again, it is stored");
    assert_eq!(matches(&cluster, SECOND), vec![7]);
}

/// A re-drive is a shard write, and a resize copy refuses shard writes so that the layout it
/// exports cannot change under it. A create that would re-drive gets the answer every write
/// gets during the copy, and the repair stays queued.
#[test]
fn a_create_does_not_redrive_a_repair_during_a_resize_copy() {
    let Faulty { cluster, fail, .. } = faulty_cluster();
    fail.store(true, Ordering::Release);
    create(&cluster, 7, FIRST).expect_err("the shard refuses the insert");
    fail.store(false, Ordering::Release);

    cluster.resize_write_fence.store(true, Ordering::Release);
    let paused = create(&cluster, 7, FIRST).expect_err("writes are paused");
    assert!(matches!(paused, ShardError::ControlPlane(_)), "{paused:?}");
    assert_eq!(
        cluster.pending_repair_ids(),
        vec![7],
        "the repair was not re-driven"
    );
    assert!(live_rows(&cluster).is_empty(), "no shard was written");

    cluster.resize_write_fence.store(false, Ordering::Release);
    let healed = create(&cluster, 7, FIRST).expect_err("the document now exists");
    assert!(
        matches!(healed, ShardError::DuplicateLogicalId(7)),
        "{healed:?}"
    );
    assert_eq!(live_rows(&cluster), vec![7]);
}

/// A failed shard write is ambiguous. Here the shard applied the insert and only its reply
/// was lost, so the repair meets a row that is already there.
#[test]
fn repairing_an_insert_the_shard_had_applied_leaves_one_row() {
    let Faulty {
        cluster, lose_acks, ..
    } = faulty_cluster();
    lose_acks.store(true, Ordering::Release);
    let lost = create(&cluster, 7, FIRST).expect_err("the acknowledgement is lost");
    assert!(
        matches!(lost, ShardError::PartiallyApplied { .. }),
        "{lost:?}"
    );
    assert_eq!(live_rows(&cluster), vec![7], "the shard holds it");
    lose_acks.store(false, Ordering::Release);

    let report = cluster.resync();
    assert_eq!((report.repaired, report.still_pending), (1, 0));
    assert_eq!(matches(&cluster, FIRST), vec![7]);
    assert_eq!(
        live_rows(&cluster),
        vec![7],
        "the repair must not store a second live row for the id"
    );
}

#[test]
fn the_ids_awaiting_repair_are_named_until_they_converge() {
    let Faulty { cluster, fail, .. } = faulty_cluster();
    assert!(cluster.pending_repair_ids().is_empty());
    fail.store(true, Ordering::Release);
    for id in [9, 5] {
        cluster
            .upsert_query(id, FIRST, 1)
            .expect_err("the shard refuses");
    }
    assert_eq!(cluster.pending_repair_ids(), vec![5, 9]);
    assert_eq!(cluster.pending_repairs(), 2);

    // A retried upsert is the repair for its own id.
    fail.store(false, Ordering::Release);
    cluster.upsert_query(9, FIRST, 1).expect("retry");
    assert_eq!(cluster.pending_repair_ids(), vec![5]);
    assert_eq!(cluster.resync().repaired, 1);
    assert!(cluster.pending_repair_ids().is_empty());
    assert_eq!(matches(&cluster, FIRST), vec![5, 9]);
}
