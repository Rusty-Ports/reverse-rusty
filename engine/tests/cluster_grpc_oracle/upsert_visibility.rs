//! ADR-185 (RR-004) over the real wire: while a remote cluster upserts a query, a concurrent
//! unfenced reader always finds it exactly once — for a placement-preserving re-put (one
//! `ReplaceExtracted` per placement shard) and for an upsert that moves the query between
//! shard servers (rewritten inside the coordinator's move fence).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::rank::RankProgramSpec;
use reverse_rusty::result::TopKOptions;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

fn remote_cluster(rt: &tokio::runtime::Runtime, k: usize) -> ClusterEngine {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
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
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    ClusterEngine::connect_remote(norm, dict, empty_tag_dict(), &cfg, &endpoints, rt.handle())
        .expect("connect remote cluster")
}

/// The single shard a one-token query lands on.
fn home_shard(cluster: &ClusterEngine, probe_id: u64, dsl: &str) -> usize {
    let shard = match cluster.add_query(probe_id, dsl).expect("probe add") {
        AddOutcome::Placed { shards, .. } => shards[0],
        other => panic!("expected a selective placement, got {other:?}"),
    };
    cluster.remove_query(probe_id).expect("remove probe");
    shard
}

#[test]
fn grpc_upsert_churn_never_hides_or_doubles_the_query() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let cluster = remote_cluster(&rt, 3);
    // Two one-token bodies on different shard servers, and a title matching both.
    let tokens: Vec<String> = (0..64).map(|i| format!("zzwire{i}")).collect();
    let home = home_shard(&cluster, 900_000, &tokens[0]);
    let away = tokens
        .iter()
        .enumerate()
        .skip(1)
        .find(|(i, token)| home_shard(&cluster, 900_000 + *i as u64, token) != home)
        .map(|(_, token)| token.clone())
        .expect("a token routed to another shard server");
    let bodies = [tokens[0].clone(), away];
    let title = format!("{} {}", bodies[0], bodies[1]);
    cluster.upsert_query(999, &bodies[0], 1).expect("seed");

    let program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("rank program");
    let stop = AtomicBool::new(false);
    let (anomalies, reads) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut anomalies: Vec<String> = Vec::new();
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                reads += 1;
                match cluster.percolate(&title) {
                    Ok(ids) if ids == vec![999] => {}
                    other => anomalies.push(format!("percolate: {other:?}")),
                }
                match cluster.try_percolate_filtered_top_k(
                    &title,
                    &[],
                    TopKOptions::default(),
                    &program,
                    None,
                ) {
                    Ok(found) if found.hits.len() == 1 && found.hits[0].logical_id == 999 => {}
                    Ok(found) => anomalies.push(format!(
                        "top-k hits: {:?}",
                        found.hits.iter().map(|h| h.logical_id).collect::<Vec<_>>()
                    )),
                    Err(error) => anomalies.push(format!("top-k: {error}")),
                }
            }
            (anomalies, reads)
        });
        let mut failure = None;
        for round in 0..300u32 {
            // Rounds 0,1 re-put the same body (same placement); every third round moves it.
            let body = &bodies[((round / 3) % 2) as usize];
            if let Err(error) = cluster.upsert_query(999, body, round + 2) {
                failure = Some(format!("round {round}: {error}"));
                break;
            }
        }
        // Stop the reader before any assertion, so the scope can join it.
        stop.store(true, Ordering::Relaxed);
        let observed = reader.join().expect("reader thread");
        assert!(failure.is_none(), "{failure:?}");
        observed
    });
    assert!(reads > 0, "the reader overlapped the churn");
    assert!(
        anomalies.is_empty(),
        "{} of {reads} reads saw the query missing, doubled, or failing: {:?}",
        anomalies.len(),
        &anomalies[..anomalies.len().min(5)]
    );
}
