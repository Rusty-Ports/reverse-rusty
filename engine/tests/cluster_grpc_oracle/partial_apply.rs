//! Partial-apply DETECTION over the real wire (ADR-047): a fan-out write whose target
//! shard server is fenced must surface as `ShardError::PartiallyApplied`, emit a
//! `ClusterPartialApply` durability event, and queue the failed shard for repair. And the
//! reason that error is a failure the caller retries (ADR-194): the queue does not outlive
//! the coordinator.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine, ShardError, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// Partial-apply DETECTION over the real wire (ADR-047): when a selective add's target shard
/// server is down, the fan-out write must surface as [`ShardError::PartiallyApplied`] (NOT a
/// swallowed error or a silent half-write), emit a `ClusterPartialApply` durability event, and
/// queue the failed shard for repair. (Convergence — `resync` re-driving once the shard is back —
/// is proven deterministically by the in-process `partial_apply_is_detected_then_resync_converges`
/// unit test; reconnect-to-a-restarted-server is out of scope for this wire-level detection test.)
#[test]
fn grpc_partial_apply_is_detected_and_queued() {
    let (queries, _titles) = build_corpus();

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Capture durability events so we can assert the partial-apply event fires over the wire.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    // A single out-of-dict required term ⇒ a synthetic (freq-0, never-hot) feature ⇒ class A ⇒
    // selective placement on exactly one shard. Discover that shard via a healthy add, then free
    // the id so the failing case re-uses the same placement.
    let dsl = "zznovelaterm";
    let target = match cluster
        .add_query(900_001, dsl)
        .expect("healthy selective add over gRPC")
    {
        AddOutcome::Placed { shards, .. } => {
            assert_eq!(
                shards.len(),
                1,
                "a synthetic single-term query must be single-shard selective: {shards:?}"
            );
            shards[0]
        }
        other => panic!("expected selective Placed, got {other:?}"),
    };
    cluster.remove_query(900_001).expect("remove probe query");

    // FENCE the target shard's server so it REJECTS writes (`failed_precondition`) while staying
    // connected — a deterministic transient write failure. (Aborting the serve task would NOT do
    // it: tonic's per-connection handler tasks outlive the accept loop, so the cluster's existing
    // HTTP/2 connection keeps serving.) A separate client flips the server-side fence flag, which
    // every client to that server then observes.
    let fencer = reverse_rusty::cluster::RemoteShard::connect(
        &endpoints[target],
        rt.handle().clone(),
        dict.fingerprint(),
        empty_tag_dict().fingerprint(),
        // 1:1 deployment: endpoint `target` hosts the slot at shard-id `target` (ADR-093).
        target as u32,
    )
    .expect("connect fencer to target server");
    fencer.fence(1).expect("fence target server");

    match cluster.add_query(900_002, dsl) {
        Err(ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            ..
        }) => {
            assert_eq!(logical, 900_002);
            assert_eq!(
                failed,
                vec![target],
                "the downed shard must be the one reported failed"
            );
            assert!(
                applied.is_empty(),
                "a single-target add applies nowhere when its shard is down: {applied:?}"
            );
        }
        other => {
            panic!("expected PartiallyApplied after the target shard went down, got {other:?}")
        }
    }
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "the failed mutation must be queued for repair"
    );
    assert!(
        events.lock().unwrap().iter().any(|e| matches!(
            e,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                ..
            }
        )),
        "a ClusterPartialApply durability event must be emitted over the wire too"
    );
}

/// Three shard servers over one frozen dictionary, and their endpoints.
fn start_servers(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<reverse_rusty::normalize::Normalizer>,
    dict: &Arc<reverse_rusty::dict::Dict>,
) -> Vec<String> {
    let mut addrs: Vec<SocketAddr> = Vec::new();
    {
        let _enter = rt.enter();
        for _ in 0..3 {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server =
                ShardServer::new(Arc::clone(norm), Arc::clone(dict), EngineConfig::default());
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    addrs.iter().map(|a| format!("http://{a}")).collect()
}

/// A remote coordinator keeps no log, so the repair it queues for a write a shard refused
/// lives in that process only. A coordinator that replaces it knows nothing about the write:
/// it reports no pending repair, and the query is not there. That is why the write is answered
/// as a failure, and the retry it asks for is what stores it.
#[test]
fn grpc_a_partial_write_is_lost_with_its_coordinator_and_a_retry_stores_it() {
    let (queries, _titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoints = start_servers(&rt, &norm, &dict);
    let connect = || {
        ClusterEngine::connect_remote(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &cfg,
            &endpoints,
            rt.handle(),
        )
        .expect("connect remote cluster")
    };
    let first = connect();
    first.ingest(&queries).expect("ingest corpus over gRPC");

    // One out-of-dictionary required term: placed on exactly one shard. Find which.
    let dsl = "zzlostrepairterm";
    let (_, outcome) = first.upsert_query(900_010, dsl, 1).expect("healthy upsert");
    let AddOutcome::Placed { shards, .. } = outcome else {
        panic!("expected a selective placement, got {outcome:?}");
    };
    let target = shards[0];
    first.remove_query(900_010).expect("remove the probe");

    let fencer = reverse_rusty::cluster::RemoteShard::connect(
        &endpoints[target],
        rt.handle().clone(),
        dict.fingerprint(),
        empty_tag_dict().fingerprint(),
        target as u32,
    )
    .expect("connect fencer to the target server");
    fencer.fence(1).expect("fence the target server");
    let refused = first
        .upsert_query(900_010, dsl, 1)
        .expect_err("the fenced shard refuses the write");
    assert!(
        matches!(refused, ShardError::PartiallyApplied { .. }),
        "{refused:?}"
    );
    assert_eq!(first.pending_repairs(), 1);
    assert_eq!(first.pending_repair_ids(), vec![900_010]);

    // The coordinator is replaced before anyone resyncs, and the shard recovers.
    drop(first);
    fencer.unfence(1).expect("unfence the target server");
    let second = connect();
    assert_eq!(
        second.pending_repairs(),
        0,
        "a new coordinator has no record of the repair"
    );
    assert_eq!(second.resync().repaired, 0);
    assert!(
        !second.percolate(dsl).expect("percolate").contains(&900_010),
        "nothing re-delivers the write the first coordinator queued"
    );

    // The retry the caller was asked for is what stores it.
    second
        .upsert_query(900_010, dsl, 1)
        .expect("retried upsert");
    assert_eq!(second.percolate(dsl).expect("percolate"), vec![900_010]);
}
