//! A shard node's engine configuration applies to every slot on it, including one that a
//! peer recovery fills (ADR-192). The coordinator ships a dictionary, never engine
//! configuration, so the node's own flags are the only source.

use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardMetricsSource, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use tokio::runtime::Runtime;

use crate::harness::*;

fn connect(rt: &Runtime, norm: &Arc<Normalizer>, dict: &Arc<Dict>, ep: &str) -> ClusterEngine {
    ClusterEngine::connect_remote(
        Arc::clone(norm),
        Arc::clone(dict),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &[ep.to_string()],
        rt.handle(),
    )
    .expect("connect coordinator")
}

/// What the node's `/_metrics` says about slot 0's translog: 1 when every write is fsynced.
fn sync_gauge(metrics: &ShardMetricsSource) -> Option<u64> {
    metrics.render().lines().find_map(|line| {
        line.strip_prefix("reverse_rusty_shard_translog_sync_on_write{shard=\"0\"} ")?
            .trim()
            .parse()
            .ok()
    })
}

/// A node started with fsync-per-write, serving on an ephemeral port.
fn start_syncing_node(rt: &Runtime, norm: &Arc<Normalizer>) -> (String, ShardMetricsSource) {
    let server = ShardServer::pending_durable(
        Arc::clone(norm),
        EngineConfig {
            wal_sync_on_write: true,
            ..EngineConfig::default()
        },
        server_dir("engine_config_target"),
    );
    let metrics = server.metrics_source();
    let _enter = rt.enter();
    let incoming =
        tonic::transport::server::TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = incoming.local_addr().expect("address");
    rt.spawn(server.serve_with_incoming(incoming));
    wait_until_listening(addr);
    (format!("http://{addr}"), metrics)
}

/// The source runs the default policy and the target fsyncs every write. The slot on the
/// target keeps the target's policy when it is adopted and when recovery replaces its shard
/// with the source's segments.
#[test]
fn grpc_a_recovered_slot_uses_the_target_nodes_sync_policy() {
    let queries = vec![
        (11, "+nike +shoe".to_string()),
        (12, "+sony +tv".to_string()),
    ];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = Runtime::new().expect("runtime");
    let source = RestartableNode::start(&rt, &norm, "engine_config_source");
    let (target, metrics) = start_syncing_node(&rt, &norm);

    let cluster = connect(&rt, &norm, &dict, &source.endpoint);
    cluster.ingest(&queries).expect("source corpus");
    let on_target = connect(&rt, &norm, &dict, &target);
    assert_eq!(sync_gauge(&metrics), Some(1), "adopted slot");

    cluster
        .peer_recover_replica(0, &source.endpoint, &target, rt.handle())
        .expect("recover target");
    assert_eq!(
        sync_gauge(&metrics),
        Some(1),
        "the recovered slot is still the target node's slot"
    );
    let hits = on_target.percolate("nike shoe").expect("percolate");
    assert_eq!(hits, vec![11], "and it serves the recovered corpus");
}
