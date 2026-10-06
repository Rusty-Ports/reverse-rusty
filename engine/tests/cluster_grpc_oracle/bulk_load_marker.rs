//! A bulk load that stops part-way is remembered by the shard nodes (ADR-196).
//!
//! A remote coordinator loads a corpus one shard after another, and skips the load when the
//! cluster already holds queries. A load that stopped after the first shard left a cluster
//! that "already holds queries", so the restarted coordinator served that part as the whole.
//! The shards now carry a mark from before the first bucket until after the last, and a
//! coordinator that finds one refuses to serve.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, RemoteShard, ShardError, ShardGroup, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty_shard_proto as raw;
use tokio::runtime::Runtime;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A durable shard node on its own runtime, so it can be stopped and started again on the
/// same address and directory, as a process would be.
struct Node {
    runtime: Option<Runtime>,
    addr: SocketAddr,
    dir: PathBuf,
}

impl Node {
    fn start(norm: &Arc<Normalizer>, tag: &str) -> Self {
        let dir = server_dir(&format!("bulk_load_marker_{tag}"));
        let runtime = Runtime::new().expect("node runtime");
        let addr = {
            let _enter = runtime.enter();
            let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
            let addr = incoming.local_addr().expect("address");
            let server = ShardServer::pending_durable(
                Arc::clone(norm),
                EngineConfig::default(),
                dir.clone(),
            );
            runtime.spawn(server.serve_with_incoming(incoming));
            addr
        };
        wait_until_listening(addr);
        Node {
            runtime: Some(runtime),
            addr,
            dir,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn stop(&mut self) {
        drop(self.runtime.take());
        wait_until_not_listening(self.addr);
    }

    fn restart(&mut self, norm: &Arc<Normalizer>) {
        if self.runtime.is_some() {
            self.stop();
        }
        let runtime = Runtime::new().expect("node runtime");
        {
            let _enter = runtime.enter();
            let incoming = TcpIncoming::bind(self.addr).expect("rebind");
            let server = ShardServer::open_durable(
                Arc::clone(norm),
                EngineConfig::default(),
                self.dir.clone(),
            )
            .expect("reopen the node from its directory");
            runtime.spawn(server.serve_with_incoming(incoming));
        }
        wait_until_listening(self.addr);
        self.runtime = Some(runtime);
    }
}

struct Fixture {
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    queries: Vec<(u64, String)>,
    client: Runtime,
    nodes: Vec<Node>,
}

impl Fixture {
    fn start(tag: &str) -> Self {
        let (queries, _titles) = build_corpus();
        let norm = Arc::new(vocab());
        let dict = frozen_dict_over(&queries, &norm);
        let nodes = (0..2)
            .map(|i| Node::start(&norm, &format!("{tag}_{i}")))
            .collect();
        Fixture {
            norm,
            dict,
            queries,
            client: Runtime::new().expect("client runtime"),
            nodes,
        }
    }

    fn endpoints(&self) -> Vec<String> {
        self.nodes.iter().map(Node::endpoint).collect()
    }

    fn connect(&self) -> Result<ClusterEngine, ShardError> {
        ClusterEngine::connect_remote(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            empty_tag_dict(),
            &ClusterConfig {
                num_shards: 2,
                include_broad: true,
                ..ClusterConfig::default()
            },
            &self.endpoints(),
            self.client.handle(),
        )
    }

    /// Make shard 1 refuse writes while it stays connected, as a handoff fence does.
    fn fence_shard_one(&self) -> RemoteShard {
        let fencer = RemoteShard::connect(
            &self.nodes[1].endpoint(),
            self.client.handle().clone(),
            self.dict.fingerprint(),
            empty_tag_dict().fingerprint(),
            1,
        )
        .expect("connect a fencer to shard 1");
        fencer.fence(1).expect("fence shard 1");
        fencer
    }
}

fn refusal(connected: Result<ClusterEngine, ShardError>) -> String {
    match connected {
        Err(ShardError::Config(message)) => message,
        Err(other) => panic!("expected a configuration refusal, got {other:?}"),
        Ok(_) => panic!("a cluster holding part of a corpus was handed out"),
    }
}

/// The sequence from the finding: the load reaches shard 0, fails on shard 1, the coordinator
/// exits, and a new one connects. It used to find a populated cluster and serve it.
#[test]
fn grpc_a_bulk_load_that_stops_part_way_is_refused_by_the_next_coordinator() {
    let mut fixture = Fixture::start("partial");
    let first = fixture.connect().expect("connect the empty cluster");
    let fencer = fixture.fence_shard_one();
    first
        .ingest(&fixture.queries)
        .expect_err("shard 1 refuses its bucket");
    assert!(
        first.num_queries().expect("count") > 0,
        "precondition: shard 0 took its bucket, so the cluster is not empty"
    );
    assert_eq!(first.unfinished_bulk_load().expect("marks"), Some(0));
    drop(first);
    fencer.unfence(1).expect("the shard is healthy again");

    let message = refusal(fixture.connect());
    assert!(message.contains("did not complete"), "{message}");
    assert!(message.contains("shard position 0"), "{message}");

    // The replicated builder asks the same question of every copy.
    let groups: Vec<ShardGroup> = fixture
        .endpoints()
        .into_iter()
        .map(|primary| ShardGroup {
            primary,
            replicas: Vec::new(),
        })
        .collect();
    let message = refusal(ClusterEngine::connect_replicated(
        Arc::clone(&fixture.norm),
        Arc::clone(&fixture.dict),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 2,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &groups,
        fixture.client.handle(),
    ));
    assert!(message.contains("did not complete"), "{message}");

    // The nodes remember across their own restart, too.
    for node in &mut fixture.nodes {
        node.restart(&fixture.norm);
    }
    let message = refusal(fixture.connect());
    assert!(message.contains("did not complete"), "{message}");
}

/// A load that completes leaves no mark, and the next coordinator is served the whole corpus.
#[test]
fn grpc_a_completed_bulk_load_leaves_no_mark() {
    let fixture = Fixture::start("complete");
    let first = fixture.connect().expect("connect the empty cluster");
    first.ingest(&fixture.queries).expect("bulk load");
    assert_eq!(first.unfinished_bulk_load().expect("marks"), None);
    let loaded = first.num_queries().expect("count");
    drop(first);

    let second = fixture.connect().expect("a completed load is served");
    assert_eq!(second.num_queries().expect("count"), loaded);
    assert_eq!(second.unfinished_bulk_load().expect("marks"), None);
}

/// A load into a cluster with an unreachable shard does not start, and the cluster can be
/// loaded once the shard is back.
#[test]
fn grpc_a_load_with_a_shard_down_does_not_start() {
    let mut fixture = Fixture::start("down");
    let first = fixture.connect().expect("connect the empty cluster");
    fixture.nodes[1].stop();
    first.ingest(&fixture.queries).expect_err("shard 1 is down");
    drop(first);
    fixture.nodes[1].restart(&fixture.norm);

    let second = fixture.connect().expect("no mark was left behind");
    assert_eq!(
        second.num_queries().expect("count"),
        0,
        "nothing was loaded"
    );
    second
        .ingest(&fixture.queries)
        .expect("the load runs once every shard is reachable");
    assert_eq!(second.unfinished_bulk_load().expect("marks"), None);
}

/// When one shard cannot be marked, nothing has been loaded yet: the marks already set on the
/// others are taken back, so they do not make every later coordinator refuse an empty cluster.
/// Shard 0 is a real node, and shard 1 a node that predates the mark.
#[test]
fn grpc_a_load_that_cannot_mark_every_shard_takes_its_marks_back() {
    let fixture = Fixture::start("takeback");
    let old_node = crate::legacy_layout::spawn_legacy_peer(
        &fixture.client,
        fixture.dict.fingerprint(),
        2,
        Some(0),
    );
    let endpoints = vec![fixture.nodes[0].endpoint(), old_node];
    let connect = || {
        ClusterEngine::connect_remote(
            Arc::clone(&fixture.norm),
            Arc::clone(&fixture.dict),
            empty_tag_dict(),
            &ClusterConfig {
                num_shards: 2,
                include_broad: true,
                ..ClusterConfig::default()
            },
            &endpoints,
            fixture.client.handle(),
        )
    };
    let first = connect().expect("connect");
    let refused = first
        .ingest(&fixture.queries)
        .expect_err("shard 1 cannot be marked");
    assert!(matches!(refused, ShardError::Config(_)), "{refused:?}");
    assert_eq!(
        first.unfinished_bulk_load().expect("marks"),
        None,
        "shard 0's mark was taken back"
    );
    assert_eq!(first.num_queries().expect("count"), 0, "nothing was loaded");
    // Nor are the ids of the corpus it never loaded reserved: a create for one of them is
    // not turned away as a duplicate. (One routed to the old node fails for its own reason.)
    let created: Vec<_> = fixture
        .queries
        .iter()
        .take(40)
        .map(|(id, dsl)| first.add_query(*id, dsl))
        .collect();
    assert!(
        !created
            .iter()
            .any(|result| matches!(result, Err(ShardError::DuplicateLogicalId(_)))),
        "an id of the unloaded corpus was still reserved"
    );
    assert!(
        created.iter().any(Result::is_ok),
        "precondition: some of them route to the real node and are stored"
    );
}

/// One mark anywhere is enough: here only the last shard carries one, as when a coordinator
/// stops while it is clearing them.
#[test]
fn grpc_a_mark_on_any_shard_refuses_the_cluster() {
    let fixture = Fixture::start("anyshard");
    let first = fixture.connect().expect("connect the empty cluster");
    first.ingest(&fixture.queries).expect("bulk load");
    drop(first);

    let endpoint = fixture.nodes[1].endpoint();
    fixture.client.block_on(async {
        let mut node = raw::shard_service_client::ShardServiceClient::connect(endpoint)
            .await
            .expect("connect to shard 1");
        node.set_bulk_load_state(raw::SetBulkLoadStateRequest {
            shard_id: 1,
            incomplete: true,
        })
        .await
        .expect("mark shard 1");
    });
    let message = refusal(fixture.connect());
    assert!(message.contains("shard position 1"), "{message}");
}

/// A load refused before any shard is written leaves no mark. Here the corpus names one id
/// twice, which is found while the buckets are built: correcting the file must be enough,
/// without resetting shards that were never touched.
#[test]
fn grpc_a_load_refused_before_any_shard_write_leaves_no_mark() {
    let fixture = Fixture::start("badfile");
    let first = fixture.connect().expect("connect the empty cluster");
    let mut twice = fixture.queries.clone();
    twice.push(twice[0].clone());
    first.ingest(&twice).expect_err("one id is named twice");
    assert_eq!(first.unfinished_bulk_load().expect("marks"), None);
    drop(first);

    let second = fixture
        .connect()
        .expect("an untouched cluster is not refused");
    second
        .ingest(&fixture.queries)
        .expect("the corrected corpus loads");
    assert_eq!(second.unfinished_bulk_load().expect("marks"), None);
}

/// A replica that refuses its bucket is dropped from the in-sync set and the load goes on,
/// so the primary ends up complete and the replica does not. The load is then not complete
/// on every copy: it fails, the marks stay, and the next coordinator refuses the cluster
/// where it would otherwise presume the replica held what its primary holds.
#[test]
fn grpc_a_load_a_replica_missed_is_not_complete() {
    let fixture = Fixture::start("replica");
    let groups = vec![ShardGroup {
        primary: fixture.nodes[0].endpoint(),
        replicas: vec![fixture.nodes[1].endpoint()],
    }];
    let connect = || {
        ClusterEngine::connect_replicated(
            Arc::clone(&fixture.norm),
            Arc::clone(&fixture.dict),
            empty_tag_dict(),
            &ClusterConfig {
                num_shards: 1,
                include_broad: true,
                ..ClusterConfig::default()
            },
            &groups,
            fixture.client.handle(),
        )
    };
    let first = connect().expect("connect the empty replicated position");
    // The replica hosts shard 0 of this layout; fence that slot so it refuses its bucket.
    let fencer = RemoteShard::connect(
        &fixture.nodes[1].endpoint(),
        fixture.client.handle().clone(),
        fixture.dict.fingerprint(),
        empty_tag_dict().fingerprint(),
        0,
    )
    .expect("connect a fencer to the replica");
    fencer.fence(1).expect("fence the replica");
    first
        .ingest(&fixture.queries)
        .expect_err("the replica did not take the load");
    assert!(
        first.num_queries().expect("count") > 0,
        "precondition: the primary took the load"
    );
    assert_eq!(first.unfinished_bulk_load().expect("marks"), Some(0));
    drop(first);
    fencer.unfence(1).expect("the replica is healthy again");

    let message = refusal(connect());
    assert!(message.contains("did not complete"), "{message}");
}

/// A node that predates the mark cannot record a load in progress, so it must not be loaded
/// in bulk: if that load stopped part-way, nothing would remember it. Such a node can still
/// be attached (it cannot hold a mark, so its answer reads as "none").
#[test]
fn grpc_a_node_that_cannot_record_a_bulk_load_is_not_loaded_in_bulk() {
    let rt = Runtime::new().expect("runtime");
    // The node reports itself empty, as an old node does: only the mark is beyond it.
    let cluster = crate::legacy_layout::cluster_on_a_legacy_peer(&rt, Some(0));
    assert_eq!(cluster.unfinished_bulk_load().expect("marks"), None);
    let refused = cluster
        .ingest(&[(1, "freshneedle".to_string())])
        .expect_err("the node cannot be marked");
    let ShardError::Config(message) = refused else {
        panic!("expected a configuration error, got {refused:?}");
    };
    assert!(message.contains("upgrade the shard nodes"), "{message}");
}
