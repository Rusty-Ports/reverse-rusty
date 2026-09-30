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
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &blue,
        rt.handle(),
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
    let prepared = cluster
        .prepare_remote_resize(&RemoteResizeRequest {
            operation_id: 5,
            num_shards: 2,
            targets,
        })
        .expect("prepare");
    // The new layout is committed but not yet serving: reads still work, writes are refused.
    assert_eq!(cluster.control_state().expect("state").num_shards, 2);
    cluster
        .percolate("1994 acme")
        .expect("reads keep serving the old layout");
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

/// A control plane that can apply a resize commit but lose its reply (and later reads), or refuse
/// one `Finish`, to exercise ambiguous-outcome handling.
struct Faulty {
    inner: reverse_rusty::cluster::InMemoryControlPlane,
    lose_commit_reply: std::sync::atomic::AtomicBool,
    reads_broken: std::sync::atomic::AtomicBool,
    refuse_finish_once: std::sync::atomic::AtomicBool,
}

impl Faulty {
    fn install(
        cluster: ClusterEngine,
        lose_commit_reply: bool,
        refuse_finish_once: bool,
    ) -> ClusterEngine {
        let initial = cluster.control_state().expect("state");
        cluster.with_control_plane(Box::new(Self {
            inner: reverse_rusty::cluster::InMemoryControlPlane::new(initial),
            lose_commit_reply: std::sync::atomic::AtomicBool::new(lose_commit_reply),
            reads_broken: std::sync::atomic::AtomicBool::new(false),
            refuse_finish_once: std::sync::atomic::AtomicBool::new(refuse_finish_once),
        }))
    }

    fn broken() -> reverse_rusty::cluster::ControlError {
        reverse_rusty::cluster::ControlError::Backend("injected control-plane fault".into())
    }
}

impl reverse_rusty::cluster::ControlPlane for Faulty {
    fn cluster_state(
        &self,
    ) -> Result<Arc<reverse_rusty::cluster::ClusterState>, reverse_rusty::cluster::ControlError>
    {
        if self.reads_broken.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Self::broken());
        }
        self.inner.cluster_state()
    }

    fn version(
        &self,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.version()
    }

    fn propose(
        &self,
        change: reverse_rusty::cluster::ClusterStateChange,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.propose(change)
    }

    fn propose_resize(
        &self,
        command: reverse_rusty::cluster::ResizeCommand,
    ) -> Result<reverse_rusty::cluster::MoveProposalResult, reverse_rusty::cluster::ControlError>
    {
        use std::sync::atomic::Ordering;
        match &command {
            reverse_rusty::cluster::ResizeCommand::Commit { .. }
                if self.lose_commit_reply.swap(false, Ordering::SeqCst) =>
            {
                self.inner.propose_resize(command)?;
                self.reads_broken.store(true, Ordering::SeqCst);
                Err(Self::broken())
            }
            reverse_rusty::cluster::ResizeCommand::Finish { .. }
                if self.refuse_finish_once.swap(false, Ordering::SeqCst) =>
            {
                Err(Self::broken())
            }
            _ => {
                if self.reads_broken.load(Ordering::SeqCst) {
                    return Err(Self::broken());
                }
                self.inner.propose_resize(command)
            }
        }
    }

    fn change_membership(
        &self,
        voters: Vec<NodeId>,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, reverse_rusty::cluster::ControlError> {
        self.inner.leader()
    }
}

#[test]
fn grpc_remote_resize_keeps_writes_paused_when_the_commit_outcome_is_unknown() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        ..
    } = fixture(2);
    let mut cluster = Faulty::install(cluster, true, false);
    let failed = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 31,
        num_shards: 2,
        targets,
    });
    assert!(failed.is_err(), "{failed:?}");
    // Consensus may already name the new layout, so writes on the old one must stay refused.
    let write = cluster.add_query(9_600_001, "zzambiguous widget");
    assert!(
        matches!(write, Err(ShardError::ControlPlane(_))),
        "an ambiguous commit must keep writes paused: {write:?}"
    );
    let remove = cluster.remove_query(1);
    assert!(
        matches!(remove, Err(ShardError::ControlPlane(_))),
        "{remove:?}"
    );
}

#[test]
fn grpc_remote_resize_finishes_a_leftover_committed_intent_before_the_next_one() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(5);
    let mut cluster = Faulty::install(cluster, false, true);
    let before = matches(&cluster, &titles);
    let first = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 41,
            num_shards: 2,
            targets: targets[..2].to_vec(),
        })
        .expect("first resize");
    assert!(
        !first.finished,
        "the injected fault leaves the intent committed"
    );
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_some());
    let second = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 42,
            num_shards: 3,
            targets: targets[2..].to_vec(),
        })
        .expect("the next resize finishes the leftover intent first");
    assert!(second.finished);
    assert_eq!(cluster.num_shards(), 3);
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_none());
    assert_eq!(matches(&cluster, &titles), before);
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
    drop(writer);

    // A later coordinator runs with the admission knob off; stored class-D rows must survive.
    let strict = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &strict,
        &blue,
        rt.handle(),
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

fn spawn_durable(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<reverse_rusty::normalize::Normalizer>,
    dir: std::path::PathBuf,
) -> String {
    let server = ShardServer::pending_durable(Arc::clone(norm), EngineConfig::default(), dir);
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
    let address: SocketAddr = incoming.local_addr().expect("bound address");
    let _task: JoinHandle<()> = rt.spawn(async move {
        server.serve_with_incoming(incoming).await.expect("serve");
    });
    format!("http://{address}")
}

#[test]
fn grpc_remote_resize_of_a_durable_cluster_requires_durable_targets() {
    let queries: Vec<(u64, String)> = (1..=40)
        .map(|i| (i, format!("zzdurable{i} widget")))
        .collect();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let root =
        std::env::temp_dir().join(format!("rr_remote_resize_durable_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let blue: Vec<String> = (0..2)
        .map(|i| spawn_durable(&rt, &norm, root.join(format!("blue{i}"))))
        .collect();
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &blue,
        rt.handle(),
    )
    .expect("connect");
    cluster.ingest(&queries).expect("ingest");
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

    // Volatile targets cannot hold a durable corpus: the resize fails cleanly.
    let volatile = vec![
        descriptor(11, &spawn(&rt, &norm)),
        descriptor(12, &spawn(&rt, &norm)),
    ];
    let refused = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 61,
        num_shards: 2,
        targets: volatile,
    });
    assert!(refused.is_err(), "{refused:?}");
    let state = cluster.control_state().expect("state");
    assert_eq!(state.num_shards, 2);
    assert_eq!(state.assignments[0].primary, NodeId(1), "nothing committed");
    assert!(state.moves.resize.is_none());
    cluster
        .add_query(9_500_001, "zzdurablewrite widget")
        .expect("writes reopen after a clean refusal");

    // Durable targets are sealed before the evidence and the resize commits.
    let durable = vec![
        descriptor(21, &spawn_durable(&rt, &norm, root.join("green0"))),
        descriptor(22, &spawn_durable(&rt, &norm, root.join("green1"))),
        descriptor(23, &spawn_durable(&rt, &norm, root.join("green2"))),
    ];
    let report = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 62,
            num_shards: 3,
            targets: durable,
        })
        .expect("resize onto durable targets");
    assert_eq!(report.num_shards, 3);
    for i in 0..3 {
        assert!(
            root.join(format!("green{i}"))
                .join("shard_000")
                .join("shard.ckpt")
                .exists()
                || root
                    .join(format!("green{i}"))
                    .join(format!("shard_{i:03}"))
                    .join("shard.ckpt")
                    .exists(),
            "target {i} committed a durable checkpoint"
        );
    }
    assert!(cluster
        .percolate("zzdurable7 widget lamp")
        .expect("percolate")
        .contains(&7));
    let _ = std::fs::remove_dir_all(&root);
}
