use std::time::{Duration, Instant};

use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;

use super::*;
use crate::cluster::proto::shard_service_client::ShardServiceClient;

/// Serve `server` on a loopback port and return a plain client plus slot 0's engine state, which
/// the served server keeps using.
async fn serve(
    server: ShardServer,
) -> (
    ShardServiceClient<tonic::transport::Channel>,
    Arc<super::super::ServerState>,
) {
    let (_, state) = server.loaded_slot(0).expect("slot 0");
    let incoming =
        tonic::transport::server::TcpIncoming::bind("127.0.0.1:0".parse().expect("addr"))
            .expect("bind");
    let address = incoming.local_addr().expect("bound address");
    tokio::spawn(async move { server.serve_with_incoming(incoming).await.expect("serve") });
    let client = ShardServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect");
    (client, state)
}

fn batch(shard_id: u32, ids: std::ops::Range<u64>) -> proto::IngestRequest {
    proto::IngestRequest {
        items: ids
            .map(|logical_id| proto::AddItem {
                logical_id,
                dsl: "stageneedle".into(),
                version: 1,
                tags: Vec::new(),
                placement: Some(placed_at(0, 1)),
            })
            .collect(),
        shard_id,
    }
}

fn three_row_segments() -> EngineConfig {
    EngineConfig {
        memtable_flush_threshold: 3,
        ..EngineConfig::default()
    }
}

fn open(
    client: &ShardServiceClient<tonic::transport::Channel>,
) -> (
    Sender<proto::IngestRequest>,
    tokio::task::JoinHandle<Result<proto::IngestReply, tonic::Status>>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut client = client.clone();
    let reply = tokio::spawn(async move {
        client
            .stage_ingest(ReceiverStream::new(receiver))
            .await
            .map(tonic::Response::into_inner)
    });
    (sender, reply)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_seals_threshold_sized_segments_and_rejects_a_second_shard() {
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["stageneedle"], &normalizer));
    let server = ShardServer::new(normalizer, dict, three_row_segments());
    let (client, state) = serve(server).await;
    let baseline = state.shard.metrics_snapshot().num_segments();

    // Four two-row requests seal two four-row segments, not one segment per request.
    let (sender, reply) = open(&client);
    for start in [0, 2, 4, 6] {
        sender.send(batch(0, start..start + 2)).await.expect("send");
    }
    drop(sender);
    let reply = reply.await.expect("join").expect("staged load");
    assert_eq!(reply.ingested, 8);
    assert_eq!(reply.rejected_parse + reply.rejected_class_d, 0);
    assert_eq!(state.shard.metrics_snapshot().num_segments(), baseline + 2);

    // An empty stream loads nothing.
    let (sender, reply) = open(&client);
    drop(sender);
    let reply = reply.await.expect("join").expect("empty load");
    assert_eq!(reply.ingested, 0);
    assert_eq!(state.shard.metrics_snapshot().num_segments(), baseline + 2);

    // One stream addresses one shard.
    let (sender, reply) = open(&client);
    sender.send(batch(0, 100..101)).await.expect("send");
    let _ = sender.send(batch(1, 101..102)).await;
    drop(sender);
    let status = reply
        .await
        .expect("join")
        .expect_err("a second shard is refused");
    assert_eq!(status.code(), Code::InvalidArgument);
}

fn source_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if entry.file_name().to_string_lossy().starts_with("sources") {
                found.push(path);
            }
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_writes_the_source_store_once_when_the_stream_closes() {
    let dir = std::env::temp_dir().join(format!("rr_stage_ingest_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["stageneedle"], &normalizer));
    let server = ShardServer::new_durable(normalizer, dict, three_row_segments(), dir.clone())
        .expect("server");
    let (client, state) = serve(server).await;
    let baseline = state.shard.metrics_snapshot().num_segments();
    assert!(
        source_files(&dir).is_empty(),
        "a fresh slot has no source store"
    );

    let (sender, reply) = open(&client);
    sender.send(batch(0, 0..2)).await.expect("send");
    sender.send(batch(0, 2..4)).await.expect("send");
    // Wait for the first staged segment: it is durable, but the source store is not yet written.
    let deadline = Instant::now() + Duration::from_secs(10);
    while state.shard.metrics_snapshot().num_segments() == baseline {
        assert!(
            Instant::now() < deadline,
            "the first segment was never sealed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        source_files(&dir).is_empty(),
        "a staged segment must not rewrite the source store"
    );
    sender.send(batch(0, 4..5)).await.expect("send");
    drop(sender);
    let reply = reply.await.expect("join").expect("staged load");
    assert_eq!(reply.ingested, 5);
    assert!(
        !source_files(&dir).is_empty(),
        "closing the stream writes the store"
    );
    assert!(state.shard.persistence_healthy());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_fails_when_its_checkpoint_sidecar_cannot_be_written() {
    let dir = std::env::temp_dir().join(format!("rr_stage_ingest_ckpt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["stageneedle"], &normalizer));
    let server = ShardServer::new_durable(normalizer, dict, three_row_segments(), dir.clone())
        .expect("server");
    let (client, _state) = serve(server).await;
    // A directory where the sidecar's temp file goes makes every sidecar write fail. A restart
    // would reopen from the stale sidecar and lose the load, so the load must not succeed.
    let mut sidecar_dirs = vec![dir.clone()];
    let mut pending = vec![dir.clone()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            if entry.path().is_dir() {
                pending.push(entry.path());
            } else if entry.file_name() == "shard.ckpt" {
                sidecar_dirs.push(next.clone());
            }
        }
    }
    for sidecar_dir in &sidecar_dirs {
        std::fs::create_dir_all(sidecar_dir.join("shard.ckpt.tmp")).expect("obstruct sidecar");
    }

    let (sender, reply) = open(&client);
    sender.send(batch(0, 0..2)).await.expect("send");
    drop(sender);
    let status = reply
        .await
        .expect("join")
        .expect_err("an unwritable sidecar fails the load");
    assert!(status.message().contains("shard.ckpt"), "{status:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_jobs_hold_the_install_barrier_and_refuse_a_replaced_slot() {
    use crate::cluster::server::service::run_installed;

    let normalizer = norm();
    let first = frozen_dict(&["stageneedle"], &normalizer);
    let second = frozen_dict(&["stageneedle", "otherneedle"], &normalizer);
    let server = ShardServer::pending(Arc::clone(&normalizer), EngineConfig::default());
    server
        .adopt_dict(adopt_req_shard(&first, 0))
        .await
        .expect("adopt");
    let (_, replaced) = server.loaded_slot(0).expect("slot");

    // Adopting a different dict onto the empty slot installs a new engine state. A job captured
    // against the old state — for example a worker detached by a cancelled stream — is refused
    // rather than writing the old engine's files over the replacement's.
    server
        .adopt_dict(adopt_req_shard(&second, 0))
        .await
        .expect("re-adopt onto the empty slot");
    let (_, current) = server.loaded_slot(0).expect("slot");
    assert!(!Arc::ptr_eq(&replaced, &current));
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    let refused = run_installed(&server, 0, &replaced, move |_| {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    assert_eq!(refused.expect_err("replaced").code(), Code::Aborted);
    assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));

    // A job against the installed state runs while holding the barrier that adoption, recovery,
    // and removal take, so none of them can replace the slot until it finishes.
    let lease = Arc::clone(&server.coordinator_lease);
    let held = run_installed(&server, 0, &current, move |_| lease.install_is_held())
        .await
        .expect("job runs");
    assert!(held);
    assert!(!server.coordinator_lease.install_is_held());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_compacts_to_the_segment_policy_before_finishing() {
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["stageneedle"], &normalizer));
    let config = EngineConfig {
        memtable_flush_threshold: 3,
        max_segments: 2,
        ..EngineConfig::default()
    };
    let server = ShardServer::new(normalizer, dict, config);
    let (client, state) = serve(server).await;

    // Five two-row requests seal three staged segments; with the pre-existing one that is four,
    // above the two-segment policy, so the finished load must have been compacted.
    let (sender, reply) = open(&client);
    for start in [0, 2, 4, 6, 8] {
        sender.send(batch(0, start..start + 2)).await.expect("send");
    }
    drop(sender);
    let reply = reply.await.expect("join").expect("staged load");
    assert_eq!(reply.ingested, 10);
    let snapshot = state.shard.metrics_snapshot();
    assert!(
        snapshot.num_segments() <= 2,
        "{} segments exceed the policy",
        snapshot.num_segments()
    );
    assert_eq!(
        crate::cluster::shard::Shard::num_queries(&state.shard).expect("count"),
        10,
        "compaction keeps every staged row"
    );
}
