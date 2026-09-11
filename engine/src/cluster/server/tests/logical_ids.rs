use std::time::Duration;

use tokio_stream::StreamExt;

use crate::cluster::logical_id_wire::{IdCollector, MAX_LIVE_LOGICAL_IDS};
use crate::cluster::shard::Shard;

use super::*;

fn fixture(bytes: usize) -> (ShardServer, proto::LiveLogicalIdsRequest) {
    let normalizer = norm();
    let dict = Arc::new(frozen_dict(&["snapshotneedle"], &normalizer));
    let mut tags = TagDict::new();
    tags.mark_finalized();
    let request = proto::LiveLogicalIdsRequest {
        shard_id: 0,
        dict_fingerprint: dict.fingerprint(),
        tag_dict_fingerprint: tags.fingerprint(),
        placement_generation: 1,
        num_shards: 1,
        max_ids: 10_000,
        remaining_micros: 5_000_000,
    };
    let server = ShardServer::new(normalizer, dict, EngineConfig::default())
        .with_max_grpc_result_bytes(bytes)
        .expect("cap");
    (server, request)
}

#[tokio::test]
async fn logical_ids_snapshot_is_stable_through_writes_and_bounded_by_frame_bytes() {
    let (server, request) = fixture(128);
    let rows: Vec<_> = (0..100)
        .map(|id| (id, "snapshotneedle".to_string()))
        .collect();
    server.ingest_dsl(&rows);
    let mut stream = server
        .live_logical_ids(Request::new(request))
        .await
        .expect("snapshot")
        .into_inner();
    let state = server.loaded_slot(0).expect("loaded").1;
    state
        .shard
        .delete_by_logical_id(0)
        .expect("delete after capture");
    server
        .insert_extracted(insert_req_single(u64::MAX, "snapshotneedle"))
        .await
        .expect("new row");

    let mut collector = IdCollector::new(&request);
    let mut frames = 0;
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("frame");
        assert!(reverse_rusty_shard_proto::encoded_len(&frame) <= 128);
        collector.push(frame).expect("valid frame");
        frames += 1;
    }
    assert!(frames > 2);
    assert_eq!(
        collector.finish().expect("complete"),
        (0..100).collect::<Vec<_>>()
    );
    assert_eq!(server.logical_id_permits.available_permits(), 1);
}

#[tokio::test]
async fn logical_ids_validate_identity_limits_empty_completion_and_result_cap() {
    let (server, request) = fixture(128);
    for bad in [
        proto::LiveLogicalIdsRequest {
            max_ids: 0,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            max_ids: MAX_LIVE_LOGICAL_IDS as u64 + 1,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            dict_fingerprint: 0,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            tag_dict_fingerprint: 0,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            placement_generation: 0,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            shard_id: 9,
            ..request
        },
        proto::LiveLogicalIdsRequest {
            remaining_micros: 0,
            ..request
        },
    ] {
        assert!(server.live_logical_ids(Request::new(bad)).await.is_err());
        assert_eq!(server.logical_id_permits.available_permits(), 1);
    }
    let mut stream = server
        .live_logical_ids(Request::new(request))
        .await
        .expect("empty")
        .into_inner();
    let mut collector = IdCollector::new(&request);
    collector
        .push(stream.next().await.expect("summary").expect("frame"))
        .expect("complete");
    assert!(stream.next().await.is_none());
    assert!(collector.finish().expect("empty set").is_empty());

    server.ingest_dsl(&[(0, "snapshotneedle".into()), (1, "snapshotneedle".into())]);
    assert!(server
        .live_logical_ids(Request::new(proto::LiveLogicalIdsRequest {
            max_ids: 1,
            ..request
        }))
        .await
        .is_err());
    let (tiny, req) = fixture(1);
    let mut stream = tiny
        .live_logical_ids(Request::new(req))
        .await
        .expect("snapshot")
        .into_inner();
    assert_eq!(
        stream.next().await.expect("error").expect_err("cap").code(),
        Code::ResourceExhausted
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn logical_ids_release_admission_on_drop_and_unpolled_deadline() {
    let (server, request) = fixture(128);
    let first = server
        .live_logical_ids(Request::new(request))
        .await
        .expect("first");
    let Err(error) = server.live_logical_ids(Request::new(request)).await else {
        panic!("second snapshot must not bypass admission");
    };
    assert_eq!(error.code(), Code::ResourceExhausted);
    drop(first);
    assert_eq!(server.logical_id_permits.available_permits(), 1);
    let mut expired = server
        .live_logical_ids(Request::new(proto::LiveLogicalIdsRequest {
            remaining_micros: 100_000,
            ..request
        }))
        .await
        .expect("short snapshot")
        .into_inner();
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.logical_id_permits.available_permits() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("unpolled deadline releases snapshot");
    assert_eq!(
        expired
            .next()
            .await
            .expect("expired")
            .expect_err("deadline")
            .code(),
        Code::DeadlineExceeded
    );
    assert!(expired.next().await.is_none());
}
