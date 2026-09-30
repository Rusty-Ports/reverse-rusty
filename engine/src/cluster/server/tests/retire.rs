use tonic::Status;

use super::*;

fn retire_req(dict: &Dict, operation_id: u64) -> Request<proto::RetireRequest> {
    Request::new(proto::RetireRequest {
        operation_id,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.get(),
        num_shards: TEST_NUM_SHARDS,
        dict_fingerprint: dict.fingerprint(),
        tag_dict_fingerprint: empty_tag_fp(),
        successor_generation: crate::ownership::PlacementGeneration::INITIAL.get() + 1,
    })
}

fn unretire_req(operation_id: u64) -> Request<proto::UnretireRequest> {
    Request::new(proto::UnretireRequest { operation_id })
}

async fn count(srv: &ShardServer) -> Result<u64, Status> {
    srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 }))
        .await
        .map(|reply| reply.into_inner().count)
}

async fn retired_operation(srv: &ShardServer) -> u64 {
    srv.dict_fingerprint(Request::new(proto::Empty {}))
        .await
        .expect("the handshake still answers")
        .into_inner()
        .retired_operation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retired_node_serves_nothing_until_its_own_operation_lifts_it_across_restarts() {
    let n = norm();
    let d = frozen_dict(&["retireneedle"], &n);
    let dir = std::env::temp_dir().join(format!("rr_retire_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    srv.adopt_dict(adopt_req(&d)).await.expect("adopt");
    srv.ingest_dsl(&[
        (1, "retireneedle".to_string()),
        (2, "retireneedle".to_string()),
    ]);
    assert_eq!(count(&srv).await.expect("serving"), 2);

    let reply = srv
        .retire(retire_req(&d, 7))
        .await
        .expect("retire")
        .into_inner();
    assert_eq!(reply.slots.len(), 1);
    assert_eq!((reply.slots[0].shard_id, reply.slots[0].live_count), (0, 2));
    srv.retire(retire_req(&d, 7))
        .await
        .expect("retiring again under the same operation is idempotent");
    assert_eq!(
        srv.retire(retire_req(&d, 8))
            .await
            .expect_err("another operation")
            .code(),
        Code::FailedPrecondition
    );

    // Reads, writes, and new slots are all refused; the handshake still answers and says why.
    let refused = count(&srv)
        .await
        .expect_err("a retired node serves no reads");
    assert!(
        refused.message().contains("retired by remote resize 7"),
        "{refused:?}"
    );
    assert!(srv.adopt_dict(adopt_req_shard(&d, 1)).await.is_err());
    assert_eq!(retired_operation(&srv).await, 7);

    // The retirement is durable.
    drop(srv);
    let reopened = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("reopen");
    assert!(count(&reopened).await.is_err());
    assert_eq!(retired_operation(&reopened).await, 7);

    // Only the retiring operation lifts it, durably.
    assert_eq!(
        reopened
            .unretire(unretire_req(8))
            .await
            .expect_err("wrong operation")
            .code(),
        Code::FailedPrecondition
    );
    assert!(
        reopened
            .unretire(unretire_req(7))
            .await
            .expect("lift")
            .into_inner()
            .was_retired
    );
    assert!(
        !reopened
            .unretire(unretire_req(7))
            .await
            .expect("again")
            .into_inner()
            .was_retired
    );
    assert_eq!(count(&reopened).await.expect("serving again"), 2);
    drop(reopened);
    let restarted = ShardServer::open_durable(n, EngineConfig::default(), dir.clone())
        .expect("reopen after lifting");
    assert_eq!(count(&restarted).await.expect("still serving"), 2);
    assert_eq!(retired_operation(&restarted).await, 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn retirement_must_name_the_nodes_current_layout() {
    let n = norm();
    let d = frozen_dict(&["retireneedle"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    srv.adopt_dict(adopt_req(&d)).await.expect("adopt");
    let mut stale = retire_req(&d, 9);
    stale.get_mut().placement_generation += 1;
    stale.get_mut().successor_generation += 1;
    assert_eq!(
        srv.retire(stale)
            .await
            .expect_err("wrong generation")
            .code(),
        Code::FailedPrecondition
    );
    let mut backwards = retire_req(&d, 9);
    backwards.get_mut().successor_generation = backwards.get_ref().placement_generation;
    assert_eq!(
        srv.retire(backwards)
            .await
            .expect_err("no successor")
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(count(&srv).await.expect("still serving"), 0);
}
