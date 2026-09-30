use std::time::Duration;

use tokio_stream::StreamExt;

use super::*;

fn fixture(bytes: usize) -> (ShardServer, proto::LiveSourcesRequest) {
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["exportneedle"], &normalizer));
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let request = proto::LiveSourcesRequest {
        shard_id: 0,
        dict_fingerprint: dict.fingerprint(),
        tag_dict_fingerprint: tags.fingerprint(),
        placement_generation: 1,
        num_shards: 1,
        max_documents: 10_000,
        remaining_micros: 5_000_000,
    };
    let server = ShardServer::new(normalizer, dict, EngineConfig::default())
        .with_max_grpc_result_bytes(bytes)
        .expect("cap");
    (server, request)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_reader_cannot_pin_the_snapshot_permit_past_the_deadline() {
    let (server, request) = fixture(96);
    let rows: Vec<_> = (0..200)
        .map(|id| (id, "exportneedle".to_string()))
        .collect();
    server.ingest_dsl(&rows);
    // Tiny frames and a short deadline: the bounded channel fills and nobody reads it.
    let stalled = server
        .live_sources(Request::new(proto::LiveSourcesRequest {
            remaining_micros: 200_000,
            ..request
        }))
        .await
        .expect("export starts")
        .into_inner();
    assert_eq!(server.logical_id_permits.available_permits(), 0);
    let mut released = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if server.logical_id_permits.available_permits() == 1 {
            released = true;
            break;
        }
    }
    assert!(
        released,
        "the producer must release the permit at its deadline"
    );
    drop(stalled);

    // A reader that does consume sees every document once, in order, then completion.
    let mut stream = server
        .live_sources(Request::new(request))
        .await
        .expect("export")
        .into_inner();
    let mut ids = Vec::new();
    let mut complete = false;
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("frame");
        assert!(reverse_rusty_shard_proto::encoded_len(&frame) <= 96);
        complete |= frame.complete;
        ids.extend(frame.documents.iter().map(|d| d.logical_id));
    }
    assert!(complete);
    assert_eq!(ids, (0..200).collect::<Vec<u64>>());
}

#[tokio::test]
async fn a_checkpoint_flush_attests_a_durable_sidecar() {
    let dir = std::env::temp_dir().join(format!("rr_live_sources_ckpt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["exportneedle"], &normalizer));
    let server = ShardServer::new_durable(
        normalizer,
        Arc::clone(&dict),
        EngineConfig::default(),
        dir.clone(),
    )
    .expect("durable server");
    server
        .insert_extracted(insert_req_single(7, "exportneedle"))
        .await
        .expect("write");
    let plain = server
        .flush(Request::new(proto::FlushRequest {
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
            checkpoint: false,
        }))
        .await
        .expect("plain flush")
        .into_inner();
    assert!(
        !plain.checkpointed,
        "a plain flush makes no checkpoint claim"
    );
    let durable = server
        .flush(Request::new(proto::FlushRequest {
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
            checkpoint: true,
        }))
        .await
        .expect("checkpoint flush")
        .into_inner();
    assert!(durable.checkpointed);
    assert!(
        dir.join("shard_000").join("shard.ckpt").exists(),
        "the checkpoint writes the slot sidecar"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
