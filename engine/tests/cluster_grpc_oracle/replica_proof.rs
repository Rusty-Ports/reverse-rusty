//! A coordinator trusts a replica for failover only after proving it against its primary
//! (ADR-195).
//!
//! Whether a replica may answer for its primary is a flag in the coordinator's memory. A
//! coordinator that has just connected cannot know what a replica missed under an earlier
//! coordinator, or that it came back on an empty volume. It used to presume every replica in
//! sync, and a later primary failure then answered from a copy that lacked queries.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError, ShardGroup, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use tokio::runtime::Runtime;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A durable shard node on its own runtime, so it can be stopped like a process and started
/// again on the same address and directory.
struct Node {
    runtime: Option<Runtime>,
    addr: SocketAddr,
    dir: PathBuf,
}

impl Node {
    fn start(norm: &Arc<Normalizer>, tag: &str) -> Self {
        let dir = server_dir(&format!("replica_proof_{tag}"));
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

    /// Stop the node as a crash does: its accept loop and every open connection end.
    fn stop(&mut self) {
        drop(self.runtime.take());
        wait_until_not_listening(self.addr);
    }

    /// Start it again on the same address, from its own directory.
    fn restart(&mut self, norm: &Arc<Normalizer>) {
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

const STORED: &str = "zzproofstored";
const MISSED: &str = "zzproofmissed";
/// Stored on both copies, then deleted while the replica is down: the replica keeps a row its
/// primary no longer holds.
const DELETED: &str = "zzproofdeleted";

struct Pair {
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    client: Runtime,
    primary: Node,
    replica: Node,
}

impl Pair {
    fn start(tag: &str) -> Self {
        let norm = Arc::new(vocab());
        let dict = frozen_dict_over(
            &[
                (1, STORED.to_string()),
                (2, MISSED.to_string()),
                (3, DELETED.to_string()),
            ],
            &norm,
        );
        Pair {
            client: Runtime::new().expect("client runtime"),
            primary: Node::start(&norm, &format!("{tag}_primary")),
            replica: Node::start(&norm, &format!("{tag}_replica")),
            norm,
            dict,
        }
    }

    /// One position: the primary with the replica behind it.
    fn connect(&self, recover_divergent_replicas: bool) -> ClusterEngine {
        ClusterEngine::connect_replicated(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            empty_tag_dict(),
            &ClusterConfig {
                num_shards: 1,
                include_broad: true,
                recover_divergent_replicas,
                ..ClusterConfig::default()
            },
            &[ShardGroup {
                primary: self.primary.endpoint(),
                replicas: vec![self.replica.endpoint()],
            }],
            self.client.handle(),
        )
        .expect("connect the replicated position")
    }

    /// Store two queries on both copies. Then, while the replica is down, store a third and
    /// delete one of the two, and bring the replica back on its own volume. The first
    /// coordinator is gone afterwards, and with it the only record that the replica missed
    /// writes.
    fn replica_misses_a_write(&mut self) {
        let first = self.connect(false);
        first
            .upsert_query(1, STORED, 1)
            .expect("both copies take it");
        first
            .upsert_query(3, DELETED, 1)
            .expect("both copies take it");
        first.flush().expect("flush");
        self.replica.stop();
        first
            .upsert_query(2, MISSED, 1)
            .expect("the primary takes it; the replica is down");
        assert_eq!(first.remove_query(3).expect("the primary deletes it"), 1);
        assert_eq!(
            first.out_of_sync_replicas(),
            1,
            "the first coordinator knows"
        );
        first.flush().expect("flush the primary");
        drop(first);
        self.replica.restart(&self.norm);
    }
}

/// The failure the finding describes: replica down during a write, replica back, coordinator
/// restart, primary lost. The failover read used to answer `Ok` without the missed query.
#[test]
fn grpc_a_replica_that_missed_a_write_is_not_trusted_by_the_next_coordinator() {
    let mut pair = Pair::start("missed");
    pair.replica_misses_a_write();

    let second = pair.connect(false);
    assert_eq!(
        second.out_of_sync_replicas(),
        1,
        "the copies differ, so the replica is not proven"
    );
    assert_eq!(second.percolate(MISSED).expect("primary"), vec![2]);
    // The replica was left as it is, so the row it still holds keeps its id reserved.
    assert!(matches!(
        second.add_query(3, DELETED),
        Err(ShardError::DuplicateLogicalId(3))
    ));

    pair.primary.stop();
    let failover = second.percolate(MISSED);
    assert!(
        matches!(failover, Err(ShardError::Remote(_))),
        "a read that cannot reach the primary must fail, not answer from the stale replica: \
         {failover:?}"
    );
}

/// An operator who knows the primaries are authoritative asks for divergent replicas to be
/// rebuilt at connect. The replica then holds the missed write and serves on failover.
#[test]
fn grpc_a_divergent_replica_is_recovered_at_connect_when_asked() {
    let mut pair = Pair::start("recovered");
    pair.replica_misses_a_write();

    let second = pair.connect(true);
    assert_eq!(
        second.out_of_sync_replicas(),
        0,
        "the replica was recovered from its primary and proven"
    );
    // The row the replica held and its primary had deleted went with the recovery, so its id
    // is free: only ids a copy still holds are reserved.
    second
        .add_query(3, DELETED)
        .expect("the deleted id can be created again");
    second.remove_query(3).expect("and removed");

    pair.primary.stop();
    assert_eq!(second.percolate(MISSED).expect("failover"), vec![2]);
    assert_eq!(second.percolate(STORED).expect("failover"), vec![1]);
    assert!(second.percolate(DELETED).expect("failover").is_empty());
}

/// Replicas that hold what their primary holds stay trusted across a coordinator restart.
#[test]
fn grpc_equal_replicas_are_trusted_by_the_next_coordinator() {
    let mut pair = Pair::start("equal");
    let first = pair.connect(false);
    first.upsert_query(1, STORED, 1).expect("upsert");
    first.upsert_query(2, MISSED, 1).expect("upsert");
    drop(first);

    let second = pair.connect(false);
    assert_eq!(second.out_of_sync_replicas(), 0);
    pair.primary.stop();
    assert_eq!(second.percolate(STORED).expect("failover"), vec![1]);
    assert_eq!(second.percolate(MISSED).expect("failover"), vec![2]);
}

/// A replica listed on a fresh, empty volume next to a populated primary. The runbook warned
/// that it was assembled as in sync and could be served empty.
#[test]
fn grpc_an_empty_replica_beside_a_populated_primary_is_not_trusted() {
    let mut pair = Pair::start("empty");
    // Load the primary alone, as a single-copy cluster.
    let alone = ClusterEngine::connect_remote(
        Arc::clone(&pair.norm),
        Arc::clone(&pair.dict),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &[pair.primary.endpoint()],
        pair.client.handle(),
    )
    .expect("connect the primary alone");
    alone.upsert_query(1, STORED, 1).expect("upsert");
    drop(alone);

    let replicated = pair.connect(false);
    assert_eq!(replicated.out_of_sync_replicas(), 1);
    assert_eq!(replicated.percolate(STORED).expect("primary"), vec![1]);
    pair.primary.stop();
    assert!(
        matches!(replicated.percolate(STORED), Err(ShardError::Remote(_))),
        "the empty replica must not answer for the primary"
    );
}
