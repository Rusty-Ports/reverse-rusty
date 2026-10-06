//! Requests larger than one default gRPC message (ADR-193).
//!
//! tonic refuses an inbound message over 4 MiB unless the server raises the limit. Two
//! coordinator paths each sent one message of unbounded size: a shard's whole bulk bucket,
//! and the whole dictionary at every connect. Both must work past that size.

use std::sync::Arc;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ShardError, ShardGroup, ShardMetricsSource, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::{Dict, FeatureKind};
use reverse_rusty::normalize::Normalizer;
use tokio::runtime::Runtime;

use crate::harness::*;

/// tonic's default inbound limit, which a default server still applies.
const DEFAULT_MESSAGE_LIMIT: usize = 4 * 1024 * 1024;

/// A pending in-memory shard node. `request_limit` sets its inbound limit; `None` leaves the
/// node's default.
fn start_node(rt: &Runtime, norm: &Arc<Normalizer>, request_limit: Option<usize>) -> String {
    let mut server = ShardServer::pending(Arc::clone(norm), EngineConfig::default());
    if let Some(limit) = request_limit {
        server = server
            .with_max_grpc_request_bytes(limit)
            .expect("request limit");
    }
    serve(rt, server)
}

fn serve(rt: &Runtime, server: ShardServer) -> String {
    let _enter = rt.enter();
    let incoming =
        tonic::transport::server::TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = incoming.local_addr().expect("address");
    rt.spawn(server.serve_with_incoming(incoming));
    wait_until_listening(addr);
    format!("http://{addr}")
}

/// How many `ingest` calls this coordinator has made: one per bulk bucket.
fn ingest_calls(cluster: &ClusterEngine) -> u64 {
    cluster
        .transport_metrics()
        .methods
        .iter()
        .find(|row| row.method == "ingest")
        .map_or(0, |row| row.calls)
}

/// What the node's `/_metrics` reports as slot 0's sealed segments.
fn base_segments(metrics: &ShardMetricsSource) -> Option<u64> {
    metrics.render().lines().find_map(|line| {
        line.strip_prefix("reverse_rusty_base_segments{shard=\"0\"} ")?
            .trim()
            .parse()
            .ok()
    })
}

fn single_shard() -> ClusterConfig {
    ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    }
}

fn connect(rt: &Runtime, norm: &Arc<Normalizer>, dict: &Arc<Dict>, ep: &str) -> ClusterEngine {
    ClusterEngine::connect_remote(
        Arc::clone(norm),
        Arc::clone(dict),
        empty_tag_dict(),
        &single_shard(),
        &[ep.to_string()],
        rt.handle(),
    )
    .expect("connect coordinator")
}

/// Queries whose one bucket encodes to well over one default message. Every query carries
/// the same padding terms, so the corpus is large while its dictionary stays small.
fn padded_corpus(count: u64) -> Vec<(u64, String)> {
    let padding = (0..24)
        .map(|k| format!("+padding{k:02}"))
        .collect::<Vec<_>>()
        .join(" ");
    (0..count)
        .map(|id| (id, format!("+item{id:06} {padding}")))
        .collect()
}

fn padded_title(id: u64) -> String {
    format!(
        "item{id:06} {}",
        (0..24)
            .map(|k| format!("padding{k:02}"))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn padded_bucket() -> Vec<(u64, String)> {
    let queries = padded_corpus(30_000);
    let encoded: usize = queries.iter().map(|(_, dsl)| dsl.len()).sum();
    assert!(
        encoded > DEFAULT_MESSAGE_LIMIT,
        "precondition: the bucket's DSL alone is {encoded} bytes"
    );
    queries
}

/// The remote bootstrap sends each shard its whole bucket. With one shard and this corpus
/// that was a single request about twice the default limit. It is one stream of bounded
/// messages now, so it loads on a node with the raised default and on one held at tonic's
/// 4 MiB, and the loaded cluster answers like an in-process one.
#[test]
fn grpc_bulk_ingest_larger_than_one_default_message() {
    let queries = padded_bucket();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let reference = ClusterEngine::build(vocab(), &single_shard(), &queries).expect("in-process");
    for request_limit in [None, Some(DEFAULT_MESSAGE_LIMIT)] {
        let rt = Runtime::new().expect("runtime");
        let endpoint = start_node(&rt, &norm, request_limit);
        let cluster = connect(&rt, &norm, &dict, &endpoint);
        cluster
            .ingest(&queries)
            .unwrap_or_else(|error| panic!("bulk ingest with limit {request_limit:?}: {error}"));
        assert_eq!(cluster.num_queries().expect("count"), queries.len());
        assert_eq!(ingest_calls(&cluster), 1, "a bucket is one call");
        for id in (0..30_000).step_by(1_499) {
            let title = padded_title(id);
            assert_eq!(
                cluster.percolate(&title).expect("percolate"),
                reference.percolate(&title).expect("reference"),
                "limit {request_limit:?}, {title}"
            );
            assert_eq!(cluster.percolate(&title).expect("percolate"), vec![id]);
        }
    }
}

/// However many messages a bucket needs, the node ends with the segments its own policy
/// allows. Sending the bucket as separate requests left one segment per request, each of
/// which also rewrote the node's whole source store, and nothing compacted them: a node that
/// only ever bulk-loads never flushes a memtable, which is where compaction otherwise runs.
#[test]
fn grpc_a_bulk_load_ends_within_the_nodes_segment_policy() {
    let queries = padded_bucket();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = Runtime::new().expect("runtime");
    let policy = EngineConfig {
        memtable_flush_threshold: 2_000,
        max_segments: 2,
        ..EngineConfig::default()
    };
    let server = ShardServer::pending_durable(
        Arc::clone(&norm),
        policy,
        server_dir("large_payload_segment_policy"),
    );
    let metrics = server.metrics_source();
    let endpoint = serve(&rt, server);
    let cluster = connect(&rt, &norm, &dict, &endpoint);
    cluster.ingest(&queries).expect("bulk ingest");

    assert_eq!(ingest_calls(&cluster), 1, "a bucket is one call");
    let segments = base_segments(&metrics).expect("the node reports its segments");
    assert!(
        (1..=2).contains(&segments),
        "{segments} segments after a bulk load, with a policy of 2"
    );
    assert_eq!(cluster.num_queries().expect("count"), queries.len());
    for id in [0, 1_999, 2_000, 17_531, 29_999] {
        assert_eq!(
            cluster.percolate(&padded_title(id)).expect("percolate"),
            vec![id]
        );
    }
}

/// A replica receives the same bucket through the same client, so it is sent the same
/// bounded messages and ends up holding every query.
#[test]
fn grpc_replicated_bulk_ingest_larger_than_one_default_message() {
    let queries = padded_bucket();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = Runtime::new().expect("runtime");
    let primary = start_node(&rt, &norm, None);
    let replica = start_node(&rt, &norm, None);
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &single_shard(),
        &[ShardGroup {
            primary: primary.clone(),
            replicas: vec![replica.clone()],
        }],
        rt.handle(),
    )
    .expect("connect replicated cluster");
    cluster.ingest(&queries).expect("replicated bulk ingest");

    // Ask each copy directly.
    for (copy, endpoint) in [("primary", &primary), ("replica", &replica)] {
        let direct = connect(&rt, &norm, &dict, endpoint);
        assert_eq!(
            direct.num_queries().expect("count"),
            queries.len(),
            "the {copy} holds the whole bucket"
        );
        assert_eq!(
            direct.percolate(&padded_title(29_999)).expect("percolate"),
            vec![29_999],
            "{copy}"
        );
    }
}

fn large_dict() -> Dict {
    let mut dict = Dict::new();
    for feature in 0..400_000u32 {
        dict.intern(&format!("term:feature{feature:08}"), FeatureKind::Generic);
    }
    dict.finalize_mask();
    let serialized = reverse_rusty::storage::serialize_dict(&dict).len();
    assert!(
        serialized > DEFAULT_MESSAGE_LIMIT,
        "precondition: the dictionary serializes to {serialized} bytes"
    );
    dict
}

/// The dictionary is shipped whole at every connect, including each coordinator restart.
#[test]
fn grpc_adopt_ships_a_dict_larger_than_one_default_message() {
    let norm = Arc::new(vocab());
    let dict = Arc::new(large_dict());
    let rt = Runtime::new().expect("runtime");
    let endpoint = start_node(&rt, &norm, None);

    let first = connect(&rt, &norm, &dict, &endpoint);
    first
        .add_query(7, "+feature00000007 +feature00000008")
        .expect("write through the adopted dictionary");
    drop(first);
    // A coordinator restart re-ships the same dictionary to the populated node.
    let again = connect(&rt, &norm, &dict, &endpoint);
    assert_eq!(
        again
            .percolate("feature00000007 feature00000008")
            .expect("percolate"),
        vec![7]
    );
}

/// A dictionary above the node's limit is refused with an error that says which setting to
/// raise, not with a bare transport status.
#[test]
fn grpc_a_dict_over_the_nodes_limit_names_the_setting() {
    let norm = Arc::new(vocab());
    let dict = Arc::new(large_dict());
    let rt = Runtime::new().expect("runtime");
    let endpoint = start_node(&rt, &norm, Some(1024 * 1024));
    let refused = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &single_shard(),
        std::slice::from_ref(&endpoint),
        rt.handle(),
    )
    .err()
    .expect("a dictionary over the limit cannot be adopted");
    assert!(
        matches!(refused, ShardError::Config(_)),
        "a configuration error, not a transport one: {refused:?}"
    );
    let message = refused.to_string();
    assert!(
        message.contains("--max-grpc-request-bytes") && message.contains("1048576"),
        "{message}"
    );
}
