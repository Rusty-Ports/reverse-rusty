//! Remote admission reconstruction, including co-location and physical restart.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError, ShardGroup, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::delivery::{ChunkSink, ChunkSinkError, MatchChunk};
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

fn spawn(rt: &tokio::runtime::Runtime, server: ShardServer) -> (String, JoinHandle<()>) {
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
    let address: SocketAddr = incoming.local_addr().expect("bound address");
    let task = rt.spawn(async move {
        server.serve_with_incoming(incoming).await.expect("serve");
    });
    (format!("http://{address}"), task)
}

fn corpus() -> Vec<(u64, String)> {
    let mut rows: Vec<_> = (0..100)
        .map(|id| (id, format!("directoryneedle{id}")))
        .collect();
    rows.push((u64::MAX, "1994 acme".into()));
    rows
}

#[derive(Default)]
struct Sink(usize);
impl ChunkSink for Sink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.0 += chunk.matches.len();
        Ok(())
    }
}

#[test]
fn grpc_logical_ids_reattach_restores_creates_but_not_unproven_exhaustive_state() {
    let rows = corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&rows, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints: Vec<_> = (0..3)
        .map(|_| {
            spawn(
                &rt,
                ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
            )
            .0
        })
        .collect();
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let connect = || {
        ClusterEngine::connect_remote_exclusive(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &config,
            &endpoints,
            rt.handle(),
            717,
        )
        .expect("connect")
    };
    let first = connect();
    first.ingest(&rows).expect("ingest");
    first.remove_query(7).expect("delete");
    first
        .upsert_query(9, "replacementneedle", 2)
        .expect("upsert");
    let probes = [
        "directoryneedle0",
        "directoryneedle7",
        "directoryneedle9",
        "replacementneedle",
        "1994 acme",
    ];
    let before: Vec<_> = probes
        .iter()
        .map(|title| first.percolate(title).expect("before"))
        .collect();
    drop(first);

    let reattached = connect();
    for id in [0, 9, u64::MAX] {
        assert!(
            matches!(reattached.create_query_with_tags(id, "replacement", 3, &[]),
            Err(ShardError::DuplicateLogicalId(conflict)) if conflict == id)
        );
    }
    for (title, expected) in probes.iter().zip(before) {
        assert_eq!(reattached.percolate(title).expect("after"), expected);
    }
    reattached
        .add_query(7, "reusedneedle")
        .expect("deleted id reusable");
    reattached
        .add_query(1000, "freshneedle")
        .expect("fresh create");
    assert_eq!(
        reattached.percolate("freshneedle").expect("fresh match"),
        vec![1000]
    );
    let mut sink = Sink::default();
    let error = reattached
        .try_percolate_filtered_all(
            "freshneedle",
            &[],
            reverse_rusty::QueryScope::WithBroad,
            None,
            8,
            None,
            &mut sink,
        )
        .expect_err("ID union is not historical convergence evidence");
    assert!(matches!(error, ShardError::Protocol(ref message) if message.contains("convergence")));
    assert_eq!(sink.0, 0);
    assert_eq!(
        reattached
            .transport_metrics()
            .methods
            .iter()
            .find(|row| row.method == "live_logical_ids")
            .expect("metric")
            .calls,
        3
    );
}

#[test]
fn grpc_logical_ids_reconstruct_from_colocated_mmap_and_translog_after_restart() {
    let rows = corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&rows, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let directory = server_dir("logical_ids_reopen");
    let node_rt = tokio::runtime::Runtime::new().expect("node runtime");
    let (endpoint, task) = spawn(
        &node_rt,
        ShardServer::open_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            directory.clone(),
        )
        .expect("durable node"),
    );
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..Default::default()
    };
    let connect = |endpoint: &str| {
        ClusterEngine::connect_remote(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &config,
            &[endpoint.to_string(), endpoint.to_string()],
            rt.handle(),
        )
        .expect("co-located connect")
    };
    let first = connect(&endpoint);
    first.ingest(&rows).expect("ingest");
    first.flush().expect("mmap base");
    first.remove_query(7).expect("tail tombstone");
    first
        .upsert_query(9, "replacementneedle", 2)
        .expect("tail upsert");
    first.add_query(1000, "freshneedle").expect("tail add");
    drop(first);
    task.abort();
    assert!(node_rt.block_on(task).expect_err("aborted").is_cancelled());
    // Drop connection tasks as well as the listener before reopening its data.
    drop(node_rt);

    let node_rt = tokio::runtime::Runtime::new().expect("reopened node runtime");
    let (endpoint, task) = spawn(
        &node_rt,
        ShardServer::open_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            directory.clone(),
        )
        .expect("reopened node"),
    );
    let reopened = connect(&endpoint);
    for id in [0, 9, 1000, u64::MAX] {
        assert!(matches!(reopened.add_query(id, "replacement"),
            Err(ShardError::DuplicateLogicalId(conflict)) if conflict == id));
    }
    reopened
        .add_query(7, "reusedneedle")
        .expect("tombstone survives restart");
    assert_eq!(
        reopened
            .percolate("replacementneedle")
            .expect("upsert survives"),
        vec![9]
    );
    assert_eq!(
        reopened.percolate("freshneedle").expect("add survives"),
        vec![1000]
    );
    drop(reopened);
    task.abort();
    let _ = node_rt.block_on(task);
    drop(node_rt);
    std::fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn grpc_logical_ids_restore_after_replica_failover_and_coordinator_reattach() {
    let rows = corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&rows, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let node_a_rt = tokio::runtime::Runtime::new().expect("node A runtime");
    let (a, task_a) = spawn(
        &node_a_rt,
        ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
    );
    let (b, _) = spawn(
        &rt,
        ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
    );
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..Default::default()
    };
    let groups = [
        ShardGroup {
            primary: a.clone(),
            replicas: vec![b.clone()],
        },
        ShardGroup {
            primary: b.clone(),
            replicas: vec![a],
        },
    ];
    let connect = || {
        ClusterEngine::connect_replicated(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &config,
            &groups,
            rt.handle(),
        )
        .expect("replicated connect")
    };
    let first = connect();
    first.ingest(&rows).expect("replicated ingest");
    drop(first);
    let reattached = connect();
    assert!(matches!(
        reattached.add_query(0, "replacement"),
        Err(ShardError::DuplicateLogicalId(0))
    ));
    reattached
        .add_query(1000, "freshneedle")
        .expect("replicated fresh create");
    let baseline: Vec<_> = rows
        .iter()
        .take(32)
        .map(|(_, title)| {
            (
                title.clone(),
                reattached.percolate(title).expect("healthy result"),
            )
        })
        .collect();
    task_a.abort();
    let _ = node_a_rt.block_on(task_a);
    // Aborting only the listener leaves established tonic connections alive.
    drop(node_a_rt);
    for (title, expected) in baseline {
        assert_eq!(
            reattached.percolate(&title).expect("surviving replica"),
            expected
        );
    }
    assert!(
        reattached.transport_metrics().total_errors() > 0,
        "the probes must exercise a failed primary, not only the surviving primary"
    );
    assert_eq!(
        reattached
            .percolate("freshneedle")
            .expect("in-sync read failover"),
        vec![1000]
    );
    drop(reattached);
    let survivor = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &[b.clone(), b],
        rt.handle(),
    )
    .expect("reattach to surviving slots");
    assert!(matches!(
        survivor.add_query(1000, "replacement"),
        Err(ShardError::DuplicateLogicalId(1000))
    ));
    survivor
        .add_query(1001, "survivorneedle")
        .expect("new id after failover");
    assert_eq!(
        survivor
            .percolate("survivorneedle")
            .expect("new survivor match"),
        vec![1001]
    );
}

#[test]
fn grpc_logical_ids_one_failed_position_keeps_the_whole_directory_unavailable() {
    let rows = vec![(0, "1994 acme".to_string())];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&rows, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints: Vec<_> = [4096, 1]
        .into_iter()
        .map(|cap| {
            spawn(
                &rt,
                ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
                    .with_max_grpc_result_bytes(cap)
                    .expect("cap"),
            )
            .0
        })
        .collect();
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..Default::default()
    };
    let connect = || {
        ClusterEngine::connect_remote(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &config,
            &endpoints,
            rt.handle(),
        )
        .expect("attach stays available")
    };
    let first = connect();
    first.ingest(&rows).expect("replicated broad ingest");
    assert!(first
        .shard_query_counts()
        .expect("counts")
        .iter()
        .all(|count| *count > 0));
    drop(first);
    let reattached = connect();
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    reattached.set_observer(Arc::new(move |event| {
        seen.lock().expect("events").push(event.clone());
    }));
    assert!(events.lock().expect("events").iter().any(|event| matches!(
        event,
        EngineEvent::DurabilityFailure {
            op: DurabilityOp::LogicalIdDirectory,
            ..
        }
    )));
    assert!(matches!(
        reattached.add_query(1000, "freshneedle"),
        Err(ShardError::Config(_))
    ));
    reattached
        .upsert_query(1000, "freshneedle", 1)
        .expect("explicit upsert remains usable");
}

#[test]
fn grpc_logical_ids_reserve_stale_replica_ids_even_with_an_empty_primary() {
    // A replica can retain an ID after missing a delete. Reattachment reconstructs
    // replica bookkeeping, so admission must inspect every copy that will receive writes.
    // Include a failed replica enumeration behind an empty primary: primary counts
    // must not bypass that failure and authorize an empty directory.
    for (populated_primary, broken_replica) in [(false, false), (true, false), (false, true)] {
        let rows = vec![
            (7, "stalereservedneedle".to_string()),
            (8, "keptneedle".into()),
        ];
        let norm = Arc::new(vocab());
        let dict = frozen_dict_over(&rows, &norm);
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let primary_rt = tokio::runtime::Runtime::new().expect("primary runtime");
        let (primary, _) = spawn(
            &primary_rt,
            ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
        );
        let (replica, _) = spawn(
            &rt,
            ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
                .with_max_grpc_result_bytes(if broken_replica { 1 } else { 4096 })
                .expect("replica cap"),
        );
        let config = ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..Default::default()
        };
        let seed = |endpoint: &str, rows: &[(u64, String)]| {
            let cluster = ClusterEngine::connect_remote(
                Arc::clone(&norm),
                Arc::clone(&dict),
                empty_tag_dict(),
                &config,
                &[endpoint.to_string()],
                rt.handle(),
            )
            .expect("seed endpoint");
            cluster.ingest(rows).expect("seed rows");
        };
        seed(&replica, &rows[..1]);
        if populated_primary {
            seed(&primary, &rows[1..]);
        }
        let reattached = ClusterEngine::connect_replicated_exclusive(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &config,
            &[ShardGroup {
                primary,
                replicas: vec![replica],
            }],
            rt.handle(),
            843,
        )
        .expect("reattach with stale replica");
        if broken_replica {
            assert!(matches!(
                reattached.add_query(9, "freshneedle"),
                Err(ShardError::Config(_))
            ));
        } else {
            assert!(matches!(
                reattached.add_query(7, "replacementneedle"),
                Err(ShardError::DuplicateLogicalId(7))
            ));
            reattached.add_query(9, "freshneedle").expect("fresh ID");
        }
        let mut sink = Sink::default();
        assert!(matches!(
            reattached.try_percolate_filtered_all(
                "freshneedle",
                &[],
                reverse_rusty::QueryScope::WithBroad,
                None,
                8,
                None,
                &mut sink,
            ),
            Err(ShardError::Protocol(ref message)) if message.contains("convergence")
        ));
        assert_eq!(sink.0, 0);
        reattached
            .upsert_query(7, "replacementneedle", 2)
            .expect("explicit replacement remains available");
        if !broken_replica {
            drop(primary_rt);
            assert!(reattached
                .percolate("stalereservedneedle")
                .expect("deleted predicate on failover")
                .is_empty());
            assert_eq!(
                reattached
                    .percolate("replacementneedle")
                    .expect("replacement on failover"),
                vec![7]
            );
            assert!(reattached.transport_metrics().total_errors() > 0);
        }
    }
}
