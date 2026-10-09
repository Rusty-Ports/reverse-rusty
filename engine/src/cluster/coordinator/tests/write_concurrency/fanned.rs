//! ADR-224: one step of a mutation is under way on every shard it touches before any
//! answer is waited for, and nothing else about the mutation changes: a step is finished on
//! every shard before the next begins, answers are read in the order of the shards, and a
//! shard in this process still applies the write when it is asked to start it.

use super::upsert_repair::{shard_of, two_homes};
use super::*;
use crate::cluster::shard::{Applied, FannedWrite, ReplaceMode};

type Calls = Arc<std::sync::Mutex<Vec<(usize, WriteCall)>>>;

const SHARDS: usize = 4;

fn cluster() -> ClusterEngine {
    let cfg = ClusterConfig {
        num_shards: SHARDS,
        ..Default::default()
    };
    ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster")
}

/// Shards that answer a started write when they are waited for, recording every write for
/// `logical` and failing the ones `fail` selects with an error that names the shard.
fn deferred(
    cluster: &mut ClusterEngine,
    logical: u64,
    fail: impl Fn(usize, WriteCall) -> bool + Send + Sync + 'static,
) -> Calls {
    let calls: Calls = Arc::default();
    instrument_deferred(cluster, {
        let calls = Arc::clone(&calls);
        Arc::new(move |position, call| {
            let ours = matches!(
                call,
                WriteCall::Insert(id)
                    | WriteCall::Delete(id)
                    | WriteCall::Replace(id, _)
                    | WriteCall::Sent(id)
                    if id == logical
            );
            if ours {
                calls.lock().expect("calls").push((position, call));
                if fail(position, call) {
                    return Err(ShardError::Remote(format!("shard {position} is down")));
                }
            }
            Ok(())
        })
    });
    calls
}

fn taken(calls: &Calls) -> Vec<(usize, WriteCall)> {
    std::mem::take(&mut *calls.lock().expect("calls"))
}

/// A delete goes to every shard. All of them have been sent it before the first answer is
/// read, and the answers are read in the order of the shards.
#[test]
fn a_delete_is_under_way_on_every_shard_before_an_answer_is_read() {
    let mut cluster = cluster();
    cluster.upsert_query(7, "zzfanned", 1).expect("seed");
    let calls = deferred(&mut cluster, 7, |_, _| false);

    assert_eq!(cluster.remove_query(7).expect("remove"), 1);
    let sent = (0..SHARDS).map(|shard| (shard, WriteCall::Sent(7)));
    let answered = (0..SHARDS).map(|shard| (shard, WriteCall::Delete(7)));
    assert_eq!(taken(&calls), sent.chain(answered).collect::<Vec<_>>());
}

/// An upsert that moves a query installs it on its new shard and only then removes the
/// copies elsewhere. Each of those two steps is under way on all of its shards at once, and
/// the second does not begin until the first has been answered everywhere.
#[test]
fn a_step_is_answered_on_every_shard_before_the_next_step_is_sent() {
    let mut cluster = cluster();
    let (old, new) = two_homes(&cluster);
    cluster.upsert_query(7, &old, 1).expect("seed");
    let home = shard_of(&cluster, &new);
    let calls = deferred(&mut cluster, 7, |_, _| false);

    cluster.upsert_query(7, &new, 2).expect("move");
    let elsewhere: Vec<usize> = (0..SHARDS).filter(|&shard| shard != home).collect();
    let mut expected = vec![
        // The new home is asked whether it holds this placement, and declines.
        (home, WriteCall::Sent(7)),
        (home, WriteCall::Replace(7, ReplaceMode::IfSamePlacement)),
        // So the move is fenced and the new version installed there.
        (home, WriteCall::Sent(7)),
        (home, WriteCall::Replace(7, ReplaceMode::Unconditional)),
    ];
    // Only now are the copies elsewhere removed: every shard is sent the delete, and then
    // every answer is read.
    expected.extend(elsewhere.iter().map(|&shard| (shard, WriteCall::Sent(7))));
    expected.extend(elsewhere.iter().map(|&shard| (shard, WriteCall::Delete(7))));
    assert_eq!(taken(&calls), expected);
    assert_eq!(cluster.percolate(&new).expect("match"), vec![7]);
    assert!(cluster.percolate(&old).expect("match").is_empty());
}

/// When more than one shard fails, the error reported is that of the lowest shard, as it
/// was when the shards were called one after the other, and every shard was still tried.
#[test]
fn the_error_reported_is_that_of_the_lowest_shard_that_failed() {
    let mut cluster = cluster();
    cluster.upsert_query(7, "zzfanned", 1).expect("seed");
    let calls = deferred(&mut cluster, 7, |position, call| {
        matches!(call, WriteCall::Delete(_)) && (position == 3 || position == 1)
    });

    let Err(ShardError::PartiallyApplied {
        applied,
        failed,
        detail,
        ..
    }) = cluster.remove_query(7)
    else {
        panic!("a remove that two shards refused");
    };
    assert_eq!((applied, failed), (vec![0, 2], vec![1, 3]));
    assert!(detail.contains("shard 1 is down"), "{detail}");
    assert_eq!(
        taken(&calls)
            .iter()
            .filter(|(_, call)| matches!(call, WriteCall::Delete(_)))
            .count(),
        SHARDS,
        "every shard was asked"
    );
}

/// A shard that cannot even be sent the write has failed like any other, and the rest of
/// the step goes on.
#[test]
fn a_shard_that_refuses_the_send_fails_and_the_others_are_still_asked() {
    let mut cluster = cluster();
    cluster.upsert_query(7, "zzfanned", 1).expect("seed");
    let calls = deferred(&mut cluster, 7, |position, call| {
        matches!(call, WriteCall::Sent(_)) && position == 2
    });

    let Err(ShardError::PartiallyApplied { failed, .. }) = cluster.remove_query(7) else {
        panic!("a remove that one shard refused");
    };
    assert_eq!(failed, vec![2]);
    let answered: Vec<usize> = taken(&calls)
        .iter()
        .filter(|(_, call)| matches!(call, WriteCall::Delete(_)))
        .map(|(shard, _)| *shard)
        .collect();
    assert_eq!(answered, [0, 1, 3]);
}

/// Writes are started in ascending shard position whatever order the shards are named in,
/// and the answers come back in the order they were named.
#[test]
fn writes_are_started_in_shard_order_and_answered_in_the_order_asked() {
    let mut cluster = cluster();
    let calls = deferred(&mut cluster, 7, |_, _| false);
    let layout = cluster.layout();
    let asked = [2, 0, 3];
    let answers = crate::cluster::coordinator::ingest::fanout::on_each(
        &layout.shards,
        &asked,
        &FannedWrite::Delete { logical: 7 },
    );
    assert_eq!(
        answers
            .iter()
            .map(|(shard, answer)| (*shard, answer.as_ref().ok().copied()))
            .collect::<Vec<_>>(),
        asked.map(|shard| (shard, Some(Applied::Deleted(0)))),
    );
    let sent = [0, 2, 3].map(|shard| (shard, WriteCall::Sent(7)));
    let answered = asked.map(|shard| (shard, WriteCall::Delete(7)));
    assert_eq!(taken(&calls), [sent, answered].concat());
}

/// A shard in this process applies a write when it is asked to start it: for an in-process
/// cluster each shard is written before the next one is asked, as before.
#[test]
fn a_shard_in_this_process_has_applied_the_write_when_it_returns() {
    let mut cluster = cluster();
    cluster.upsert_query(7, "zzfanned", 1).expect("seed");
    let calls: Calls = Arc::default();
    instrument(&mut cluster, {
        let calls = Arc::clone(&calls);
        Arc::new(move |position, call| {
            calls.lock().expect("calls").push((position, call));
            Ok(())
        })
    });
    assert_eq!(cluster.remove_query(7).expect("remove"), 1);
    assert_eq!(
        taken(&calls),
        (0..SHARDS)
            .map(|shard| (shard, WriteCall::Delete(7)))
            .collect::<Vec<_>>()
    );
}

/// A replicated shard sends its primary the write when it is asked to start it and holds
/// its lock until the answer is waited for; only then are the replicas written. Another
/// write to that shard waits for the lock, so a replica never sees the two out of order.
#[test]
fn a_replicated_shard_holds_its_lock_from_the_send_until_the_answer() {
    let cfg = ClusterConfig {
        num_shards: 1,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let calls: Calls = Arc::default();
    let hook: harness::WriteHook = {
        let calls = Arc::clone(&calls);
        Arc::new(move |position, call| {
            calls.lock().expect("calls").push((position, call));
            Ok(())
        })
    };
    // Position 0 is the primary (deferred) and position 1 its replica, in one composite.
    let mut replica = ClusterEngine::build(vocab(), &cfg, &[]).expect("replica");
    let mut taken_out = None;
    replica.replace_shards(|mut shards| {
        taken_out = shards.pop();
        shards
    });
    let replica_shard = taken_out.expect("one shard");
    cluster.replace_shards(|shards| {
        let primary = shards.into_iter().next().expect("one shard");
        vec![Box::new(crate::cluster::replica::ReplicatedShard::new(
            observed(primary, 0, Arc::clone(&hook), true),
            vec![observed(replica_shard, 1, Arc::clone(&hook), false)],
        )) as Box<dyn Shard>]
    });
    let layout = cluster.layout();
    let composite = layout.shards[0].as_ref();

    let started = composite.start_write(FannedWrite::Delete { logical: 7 });
    assert!(!started.is_done(), "the primary has only been sent it");
    assert_eq!(taken(&calls), [(0, WriteCall::Sent(7))], "no replica yet");

    std::thread::scope(|scope| {
        let (done, waited) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let answer = composite.delete_by_logical_id(8);
            let _ = done.send(answer.is_ok());
        });
        // The other write cannot begin: the composite's lock is held. Whatever happens
        // below, `started` is dropped when this closure ends, which releases it.
        let blocked = waited.recv_timeout(Duration::from_millis(200));
        let answer = started.wait();
        let finished = waited.recv_timeout(Duration::from_secs(10));
        assert!(blocked.is_err(), "a second write ran under the first");
        assert_eq!(answer.expect("delete"), Applied::Deleted(0));
        assert_eq!(finished, Ok(true), "and ran once the first was answered");
    });
    assert_eq!(
        taken(&calls),
        [
            (0, WriteCall::Delete(7)),
            (1, WriteCall::Delete(7)),
            (0, WriteCall::Delete(8)),
            (1, WriteCall::Delete(8)),
        ],
        "the replica is written after its primary answered, one write after the other"
    );
}

/// A shard whose backing can be exchanged (a handoff) starts a write on the backing in
/// place then, and that backing answers it even when it has been exchanged meanwhile, as a
/// blocking call finished on the backing it began on. The next write goes to the new one.
#[cfg(feature = "distributed")]
#[test]
fn a_started_write_is_answered_by_the_backing_it_was_sent_to() {
    let cfg = ClusterConfig {
        num_shards: 1,
        ..Default::default()
    };
    let calls: Calls = Arc::default();
    let hook: harness::WriteHook = {
        let calls = Arc::clone(&calls);
        Arc::new(move |position, call| {
            calls.lock().expect("calls").push((position, call));
            Ok(())
        })
    };
    // Two backings, told apart by the position they report: 0 before the exchange, 1 after.
    let mut backings = (0..2).map(|position| {
        let mut source = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
        let mut taken_out = None;
        source.replace_shards(|mut shards| {
            taken_out = shards.pop();
            shards
        });
        observed(
            taken_out.expect("one shard"),
            position,
            Arc::clone(&hook),
            true,
        )
    });
    let (wrapper, handle) =
        crate::cluster::handoff::wrap_handoff(backings.next().expect("first"), 0);
    let shards = vec![wrapper];

    let target = shards[0]
        .write_target()
        .expect("a wrapper names its backing");
    let started = target.start_write(FannedWrite::Delete { logical: 7 });
    handle.swap_backing(backings.next().expect("second"), 1);
    assert_eq!(started.wait().expect("delete"), Applied::Deleted(0));
    assert_eq!(
        taken(&calls),
        [(0, WriteCall::Sent(7)), (0, WriteCall::Delete(7))],
        "sent to the first backing and answered by it"
    );

    let sent = FannedWrite::Delete { logical: 7 };
    let answers = crate::cluster::coordinator::ingest::fanout::on_each(&shards, &[0], &sent);
    assert!(matches!(answers[..], [(0, Ok(Applied::Deleted(0)))]));
    assert_eq!(
        taken(&calls),
        [(1, WriteCall::Sent(7)), (1, WriteCall::Delete(7))],
        "the next write goes to the backing now in place"
    );
}

/// A replica is sent what its primary did, not what it was asked: a conditional replace
/// that the primary declined is not sent to the replica at all, and one the primary applied
/// is sent without the condition. Whether the write was started or called makes no
/// difference.
#[test]
fn a_replica_is_sent_what_its_primary_did_and_not_what_it_declined() {
    for deferred_primaries in [false, true] {
        let cfg = ClusterConfig {
            num_shards: 2,
            ..Default::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
        let (old, new) = two_homes(&cluster);
        cluster.upsert_query(7, &old, 1).expect("seed");
        let home = shard_of(&cluster, &new);
        let other = 1 - home;

        let calls: Calls = Arc::default();
        let hook: harness::WriteHook = {
            let calls = Arc::clone(&calls);
            Arc::new(move |position, call| {
                if !matches!(call, WriteCall::Sent(_)) {
                    calls.lock().expect("calls").push((position, call));
                }
                Ok(())
            })
        };
        // Each position becomes a primary with one replica; a replica reports as its
        // primary's position plus two. The replicas start with what the primaries hold.
        let mut replicas = ClusterEngine::build(vocab(), &cfg, &[]).expect("replicas");
        replicas.upsert_query(7, &old, 1).expect("seed");
        let mut taken_out = Vec::new();
        replicas.replace_shards(|shards| {
            taken_out = shards;
            Vec::new()
        });
        let mut replica_of = taken_out.into_iter();
        cluster.replace_shards(|shards| {
            shards
                .into_iter()
                .enumerate()
                .map(|(position, primary)| {
                    let replica = replica_of.next().expect("a replica for each");
                    Box::new(crate::cluster::replica::ReplicatedShard::new(
                        observed(primary, position, Arc::clone(&hook), deferred_primaries),
                        vec![observed(replica, position + 2, Arc::clone(&hook), false)],
                    )) as Box<dyn Shard>
                })
                .collect()
        });

        cluster.upsert_query(7, &new, 2).expect("move");
        assert_eq!(
            taken(&calls),
            [
                // The new home declines the condition, and its replica is not asked.
                (home, WriteCall::Replace(7, ReplaceMode::IfSamePlacement)),
                // It installs the new version, and its replica is sent that.
                (home, WriteCall::Replace(7, ReplaceMode::Unconditional)),
                (home + 2, WriteCall::Replace(7, ReplaceMode::Unconditional)),
                // The old home drops its copy, and so does its replica.
                (other, WriteCall::Delete(7)),
                (other + 2, WriteCall::Delete(7)),
            ],
            "deferred primaries: {deferred_primaries}"
        );
    }
}
