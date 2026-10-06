//! A replica is trusted for failover only when its composite was given a proof that it holds
//! what the primary holds (ADR-195). Without one it is treated exactly like a replica that
//! missed a write: never read, never written, and reported.

use std::sync::Mutex;

use crate::exact::TagPredicate;

use super::super::test_support::*;
use super::super::*;

fn seeded_replica(corpus: &CompiledCorpus) -> LocalShard {
    let (norm, dict, tag_dict, queries) = corpus;
    let replica = LocalShard::new(
        Arc::clone(norm),
        Arc::clone(dict),
        Arc::clone(tag_dict),
        EngineConfig::default(),
    );
    seed(&replica, queries);
    replica
}

#[test]
fn reads_never_fail_over_to_a_replica_without_a_proof() {
    let corpus = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let unproven = ReplicatedShard::with_proofs(
        Box::new(FailingShard::reads_remote()) as Box<dyn Shard>,
        vec![(
            Box::new(seeded_replica(&corpus)) as Box<dyn Shard>,
            Err("the copies differ".to_string()),
        )],
    );
    assert_eq!(unproven.out_of_sync_replicas(), 1);
    // The replica holds the match, and may be missing others: the read fails loudly.
    assert!(
        matches!(
            unproven.percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty()),
            Err(ShardError::Remote(_))
        ),
        "a replica that was not proven equal must not answer for its primary"
    );
    assert!(matches!(
        unproven.live_logical_ids(),
        Err(ShardError::Remote(_))
    ));

    // The same replica with a proof is served, as before.
    let proven = ReplicatedShard::with_proofs(
        Box::new(FailingShard::reads_remote()) as Box<dyn Shard>,
        vec![(Box::new(seeded_replica(&corpus)) as Box<dyn Shard>, Ok(()))],
    );
    assert_eq!(proven.out_of_sync_replicas(), 0);
    let (ids, _) = proven
        .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
        .expect("failover read");
    assert_eq!(ids, vec![1]);
}

#[test]
fn only_the_proven_replica_of_several_serves() {
    let corpus = compile_corpus(&[(1, "alpha bravo")]);
    // The unproven copy is first in the failover order and holds nothing.
    let empty = LocalShard::new(
        Arc::clone(&corpus.0),
        Arc::clone(&corpus.1),
        Arc::clone(&corpus.2),
        EngineConfig::default(),
    );
    let rs = ReplicatedShard::with_proofs(
        Box::new(FailingShard::reads_remote()) as Box<dyn Shard>,
        vec![
            (
                Box::new(empty) as Box<dyn Shard>,
                Err("an empty volume".to_string()),
            ),
            (Box::new(seeded_replica(&corpus)) as Box<dyn Shard>, Ok(())),
        ],
    );
    assert_eq!(rs.out_of_sync_replicas(), 1);
    let (ids, _) = rs
        .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
        .expect("the proven replica answers");
    assert_eq!(ids, vec![1], "the empty copy must be skipped, not served");
}

#[test]
fn a_replica_without_a_proof_is_reported_once_the_observer_is_installed() {
    let corpus = compile_corpus(&[(1, "alpha bravo")]);
    let rs = ReplicatedShard::with_proofs(
        Box::new(seeded_replica(&corpus)) as Box<dyn Shard>,
        vec![
            (Box::new(seeded_replica(&corpus)) as Box<dyn Shard>, Ok(())),
            (
                Box::new(seeded_replica(&corpus)) as Box<dyn Shard>,
                Err("the replica holds 0 live queries".to_string()),
            ),
        ],
    );
    let seen: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    rs.set_event_sink(Arc::new(move |event: &EngineEvent| {
        sink.lock().expect("events").push(event.clone());
    }));
    let seen = seen.lock().expect("events");
    assert_eq!(seen.len(), 1, "one replica started without a proof");
    let EngineEvent::DurabilityFailure { op, detail, error } = &seen[0] else {
        panic!("unexpected event {:?}", seen[0]);
    };
    assert_eq!(*op, DurabilityOp::ReplicaDesync);
    assert!(detail.contains("replica 1"), "{detail}");
    assert!(error.contains("0 live queries"), "{error}");
}

#[test]
fn writes_are_not_fanned_to_a_replica_without_a_proof() {
    let corpus = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let (norm, dict, tag_dict, queries) = &corpus;
    let fresh = || {
        LocalShard::new(
            Arc::clone(norm),
            Arc::clone(dict),
            Arc::clone(tag_dict),
            EngineConfig::default(),
        )
    };
    // The unproven replica refuses every write: were the composite to fan one to it, the
    // failure would be reported as a second desync event.
    let rs = ReplicatedShard::with_proofs(
        Box::new(fresh()) as Box<dyn Shard>,
        vec![(
            Box::new(FailingShard::writes_fail()) as Box<dyn Shard>,
            Err("not proven".to_string()),
        )],
    );
    let seen: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    rs.set_event_sink(Arc::new(move |event: &EngineEvent| {
        sink.lock().expect("events").push(event.clone());
    }));
    seed(&rs, queries);
    assert_eq!(rs.num_queries().expect("count"), 2, "the primary took them");
    assert_eq!(
        seen.lock().expect("events").len(),
        1,
        "only the start-up report: no write reached the unproven replica"
    );
}
