//! ADR-180 remote blue/green resize over gRPC: a remote cluster moves its whole corpus onto a new
//! layout on fresh nodes, with zero false negatives, fenced writes during the copy, a single
//! atomic control-plane commit, and fail-closed recovery when the resize cannot complete.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, RemoteResizeRequest,
    ShardAssignment, ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

mod durable;
mod faults;

fn spawn(rt: &tokio::runtime::Runtime, norm: &Arc<reverse_rusty::normalize::Normalizer>) -> String {
    let server = ShardServer::pending(Arc::clone(norm), EngineConfig::default());
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
    let address: SocketAddr = incoming.local_addr().expect("bound address");
    let _task: JoinHandle<()> = rt.spawn(async move {
        server.serve_with_incoming(incoming).await.expect("serve");
    });
    format!("http://{address}")
}

fn descriptor(id: u64, endpoint: &str) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(endpoint.to_string()),
        role: NodeRole::Data,
    }
}

struct Fixture {
    rt: tokio::runtime::Runtime,
    cluster: ClusterEngine,
    targets: Vec<NodeDescriptor>,
    queries: Vec<(u64, String)>,
    titles: Vec<String>,
}

/// A K=3 remote cluster on three nodes whose committed assignments name those nodes, plus
/// `targets` fresh, empty servers.
fn fixture(targets: usize) -> Fixture {
    let (mut queries, titles) = build_corpus();
    queries.truncate(500);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let blue: Vec<String> = (0..3).map(|_| spawn(&rt, &norm)).collect();
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &blue,
        rt.handle(),
        0xB1E0_0001,
    )
    .expect("connect blue");
    cluster.ingest(&queries).expect("ingest");
    for (position, endpoint) in blue.iter().enumerate() {
        let id = position as u64 + 1;
        cluster
            .register_node(descriptor(id, endpoint))
            .expect("register blue node");
        cluster
            .reassign_shard(ShardAssignment {
                position: position as u32,
                primary: NodeId(id),
                replicas: Vec::new(),
            })
            .expect("seed assignment");
    }
    let targets = (0..targets)
        .map(|i| descriptor(11 + i as u64, &spawn(&rt, &norm)))
        .collect();
    Fixture {
        rt,
        cluster,
        targets,
        queries,
        titles,
    }
}

fn matches(cluster: &ClusterEngine, titles: &[String]) -> Vec<HashSet<u64>> {
    titles
        .iter()
        .map(|t| {
            cluster
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect()
        })
        .collect()
}

#[test]
fn grpc_remote_resize_moves_the_corpus_with_zero_false_negatives() {
    let Fixture {
        rt: _rt,
        mut cluster,
        targets,
        mut queries,
        titles,
    } = fixture(5);
    // Churn before the resize: a remove and a versioned upsert must carry over exactly.
    let removed = queries[2].0;
    cluster.remove_query(removed).expect("remove");
    queries.retain(|(id, _)| *id != removed);
    let upserted = queries[4].0;
    cluster
        .upsert_query(upserted, "1994 vertex zzresized", 3)
        .expect("upsert");
    for query in &mut queries {
        if query.0 == upserted {
            query.1 = "1994 vertex zzresized".to_string();
        }
    }
    let before = matches(&cluster, &titles);
    let generation = cluster.placement_generation().0;

    let report = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 77,
            num_shards: 5,
            targets: targets.clone(),
        })
        .expect("remote resize");
    assert_eq!(report.old_num_shards, 3);
    assert_eq!(report.num_shards, 5);
    assert_eq!(report.placement_generation, generation + 1);
    assert_eq!(report.retired_slots, 3);
    assert!(report.finished);
    assert!(report.exported > 0 && report.loaded >= report.exported);

    let state = cluster.control_state().expect("state");
    assert_eq!(state.num_shards, 5);
    assert_eq!(state.placement_generation, generation + 1);
    assert!(state.moves.resize.is_none());
    for assignment in &state.assignments {
        assert_eq!(
            assignment.primary,
            targets[assignment.position as usize % targets.len()].id
        );
    }
    assert_eq!(cluster.num_shards(), 5);

    let after = matches(&cluster, &titles);
    assert_eq!(after, before, "the resize must preserve every match");
    let brute = Brute::build(&queries);
    let mut lc = String::new();
    let mut feats = Vec::new();
    for (title, got) in titles.iter().zip(&after) {
        assert_eq!(got, &brute.matches(title, &mut lc, &mut feats), "{title:?}");
    }

    // Writes reopen and land on the new layout.
    cluster
        .add_query(9_900_001, "zzpostresize widget")
        .expect("write after the cutover");
    assert!(cluster
        .percolate("zzpostresize widget lamp")
        .expect("percolate")
        .contains(&9_900_001));
    let versioned = cluster
        .export_live_corpus(&mut |_| Ok(()))
        .expect("export from the new layout");
    assert_eq!(versioned, report.exported + 1);
}

#[test]
fn grpc_remote_resize_pauses_writes_until_the_new_layout_is_installed() {
    let Fixture {
        rt: _rt,
        mut cluster,
        targets,
        ..
    } = fixture(2);
    // A server releases its own write serialization from this callback, so it must run only
    // once the fence already refuses writes.
    let refused_when_fenced = std::cell::Cell::new(None);
    let prepared = cluster
        .prepare_remote_resize_then(
            &RemoteResizeRequest {
                operation_id: 5,
                num_shards: 2,
                targets,
            },
            || {
                let write = cluster.add_query(9_800_003, "zzfenced early");
                refused_when_fenced.set(Some(matches!(write, Err(ShardError::ControlPlane(_)))));
            },
        )
        .expect("prepare");
    assert_eq!(refused_when_fenced.get(), Some(true));
    // The new layout is committed but not yet serving: reads still work, writes are refused,
    // and so is the exclusive lock a vocabulary rebuild would queue for.
    assert_eq!(cluster.control_state().expect("state").num_shards, 2);
    assert!(cluster.ensure_resize_write_fence_open().is_err());
    // Between `Commit` and installation the old layout may no longer be the layout of record, so
    // reads fail loud rather than answer from it.
    let read = cluster.percolate("1994 acme");
    assert!(
        matches!(read, Err(ShardError::ControlPlane(_))),
        "reads must not answer from the retired layout after the commit: {read:?}"
    );
    for write in [
        cluster.add_query(9_800_001, "zzfenced widget").map(|_| ()),
        cluster.remove_query(1).map(|_| ()),
        cluster
            .upsert_query(9_800_002, "zzfenced gadget", 1)
            .map(|_| ()),
    ] {
        assert!(
            matches!(write, Err(ShardError::ControlPlane(_))),
            "a write during the copy must be refused: {write:?}"
        );
    }
    let retired = cluster.install_remote_resize(prepared).expect("install");
    cluster
        .ensure_resize_write_fence_open()
        .expect("install lowers the fence");
    cluster
        .percolate("1994 acme")
        .expect("reads serve the installed layout");
    cluster
        .add_query(9_800_001, "zzfenced widget")
        .expect("writes reopen after install");
    let report = cluster.finish_remote_resize(retired).expect("finish");
    assert_eq!(report.num_shards, 2);
    assert!(report.finished);
}

#[test]
fn grpc_remote_resize_refuses_colocated_or_dirty_targets_and_keeps_serving() {
    let Fixture {
        rt: _rt,
        mut cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let before = matches(&cluster, &titles);
    let blue_endpoint = cluster.control_state().expect("state").nodes[1]
        .addr
        .clone()
        .expect("blue endpoint");

    // A target that already hosts a slot of the current layout is refused before any change.
    let colocated = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 9,
        num_shards: 2,
        targets: vec![targets[0].clone(), descriptor(1, &blue_endpoint)],
    });
    assert!(colocated.is_err(), "{colocated:?}");

    // A dirty target (already loaded by a previous attempt) fails loud and aborts cleanly.
    let first = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 10,
            num_shards: 2,
            targets: targets.clone(),
        })
        .expect("first resize onto the targets");
    assert_eq!(first.num_shards, 2);
    // The retired old-layout node still holds its data, so it cannot become a target.
    let reused = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 11,
        num_shards: 3,
        targets: vec![descriptor(21, &blue_endpoint)],
    });
    assert!(reused.is_err(), "{reused:?}");
    let state = cluster.control_state().expect("state");
    assert_eq!(state.num_shards, 2, "a failed resize commits nothing");
    assert!(
        state.moves.resize.is_none(),
        "a failed resize aborts its intent"
    );
    cluster
        .add_query(9_700_001, "zzstillwritable widget")
        .expect("a failed resize lowers the write fence");
    let after = matches(&cluster, &titles);
    for (b, a) in before.iter().zip(&after) {
        assert!(b.is_subset(a), "no match may be lost");
    }
}

#[test]
fn grpc_remote_resize_preserves_admitted_class_d_rows_when_the_knob_is_off() {
    let queries: Vec<(u64, String)> = vec![
        (1, "1994 acme".into()),
        (2, "1995 vertex".into()),
        (3, "-zzforbidden".into()),
    ];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let blue: Vec<String> = (0..2).map(|_| spawn(&rt, &norm)).collect();
    let mut accepting = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    accepting.per_shard.accept_class_d = true;
    let writer = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &accepting,
        &blue,
        rt.handle(),
    )
    .expect("connect accepting coordinator");
    writer.ingest(&queries).expect("ingest including class D");
    // A shared coordinator cannot keep other writers off the source slots, so it may not resize.
    let shared = writer.prepare_remote_resize(&RemoteResizeRequest {
        operation_id: 1,
        num_shards: 3,
        targets: vec![descriptor(31, "http://127.0.0.1:9")],
    });
    assert!(
        matches!(&shared, Err(ShardError::Config(message)) if message.contains("exclusive")),
        "a shared coordinator must be refused: {:?}",
        shared.as_ref().err()
    );
    drop(writer);

    // A later coordinator runs with the admission knob off; stored class-D rows must survive.
    let strict = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &strict,
        &blue,
        rt.handle(),
        0xB1E0_0002,
    )
    .expect("connect strict coordinator");
    for (position, endpoint) in blue.iter().enumerate() {
        let id = position as u64 + 1;
        cluster
            .register_node(descriptor(id, endpoint))
            .expect("register");
        cluster
            .reassign_shard(ShardAssignment {
                position: position as u32,
                primary: NodeId(id),
                replicas: Vec::new(),
            })
            .expect("assign");
    }
    let mut before = std::collections::BTreeSet::new();
    cluster
        .export_live_corpus(&mut |q| {
            before.insert(q.logical_id);
            Ok(())
        })
        .expect("export before");
    assert!(
        before.contains(&3),
        "precondition: the class-D row is stored"
    );
    let targets = vec![
        descriptor(11, &spawn(&rt, &norm)),
        descriptor(12, &spawn(&rt, &norm)),
    ];
    cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 51,
            num_shards: 2,
            targets,
        })
        .expect("resize");
    let mut after = std::collections::BTreeSet::new();
    cluster
        .export_live_corpus(&mut |q| {
            after.insert(q.logical_id);
            Ok(())
        })
        .expect("export after");
    assert_eq!(
        after, before,
        "every stored row, including class D, survives"
    );
}

#[derive(Default)]
struct CountingSink {
    chunks: usize,
}

impl reverse_rusty::delivery::ChunkSink for CountingSink {
    fn send_chunk(
        &mut self,
        _chunk: &reverse_rusty::delivery::MatchChunk,
    ) -> Result<(), reverse_rusty::delivery::ChunkSinkError> {
        self.chunks += 1;
        Ok(())
    }
}

fn exhaustive(cluster: &ClusterEngine, title: &str) -> Result<(), ShardError> {
    cluster
        .try_percolate_filtered_all(
            title,
            &[],
            reverse_rusty::QueryScope::WithBroad,
            None,
            64,
            None,
            &mut CountingSink::default(),
        )
        .map(|_| ())
}

#[test]
fn grpc_remote_resize_restores_logical_id_convergence_for_an_attached_coordinator() {
    let Fixture {
        rt,
        cluster,
        targets,
        queries,
        titles,
    } = fixture(2);
    let committed = cluster.control_state().expect("state");
    let before = matches(&cluster, &titles);
    drop(cluster);
    // A restarted coordinator attaches to the populated layout: membership is enumerable, but
    // enumeration cannot attest that every cross-shard write converged, so exhaustive delivery
    // stays refused until the corpus is rebuilt onto fresh shards.
    let norm = Arc::new(vocab());
    let blue: Vec<String> = (1..=3u64)
        .map(|id| {
            committed
                .nodes
                .iter()
                .find(|node| node.id == NodeId(id))
                .and_then(|node| node.addr.clone())
                .expect("blue endpoint")
        })
        .collect();
    let mut attached = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        frozen_dict_over(&queries, &norm),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 3,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &blue,
        rt.handle(),
        0xB1E0_0001,
    )
    .expect("attach")
    .with_control_plane(Box::new(reverse_rusty::cluster::InMemoryControlPlane::new(
        committed,
    )));
    assert!(exhaustive(&attached, &titles[0]).is_err());

    attached
        .resize_remote(&RemoteResizeRequest {
            operation_id: 81,
            num_shards: 2,
            targets,
        })
        .expect("resize");
    // The rebuilt layout was loaded coherently from a fixed snapshot, so its directory is
    // authoritative and converged.
    exhaustive(&attached, &titles[0]).expect("exhaustive delivery after the rebuild");
    assert_eq!(matches(&attached, &titles), before);
}
