//! ADR-180 remote resize of a durable cluster: the targets must persist whenever the source does.

use super::*;

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
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &blue,
        rt.handle(),
        0xB1E0_0003,
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
