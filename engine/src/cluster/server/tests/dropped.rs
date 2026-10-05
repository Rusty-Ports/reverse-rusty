use super::*;

// ---- a shard this node gave up is not re-adopted as an empty serving slot (ADR-189) ----

const GEN: u64 = 1;

fn fence_req(shard_id: u32, generation: u64, fp: u64) -> Request<proto::FenceRequest> {
    Request::new(proto::FenceRequest {
        generation,
        dict_fingerprint: fp,
        tag_dict_fingerprint: empty_tag_fp(),
        shard_id,
        placement_generation: GEN,
        num_shards: TEST_NUM_SHARDS,
    })
}

fn drop_req_16(shard_id: u32, fence: u64, fp: u64) -> Request<proto::DropShardRequest> {
    Request::new(proto::DropShardRequest {
        shard_id,
        expected_fence_generation: fence,
        dict_fingerprint: fp,
        tag_dict_fingerprint: empty_tag_fp(),
        placement_generation: GEN,
        num_shards: TEST_NUM_SHARDS,
    })
}

fn read_req(shard_id: u32, title: &str) -> Request<proto::PercolateRequest> {
    Request::new(proto::PercolateRequest {
        title: title.to_string(),
        include_broad: true,
        filter: Vec::new(),
        rank: None,
        shard_id,
        ownership: Some(proto::ownership_to_proto(
            &crate::ownership::OwnershipContext::new(
                crate::ownership::PlacementGeneration::INITIAL,
                TEST_NUM_SHARDS,
                vec![shard_id],
                None,
            )
            .expect("ownership context"),
        )),
    })
}

/// Adopt slot `shard_id`, store one query, then fence and drop it: what a
/// handoff away from this node followed by orphan GC leaves behind.
fn adopt_fill_and_drop(
    rt: &tokio::runtime::Runtime,
    srv: &ShardServer,
    dict: &Dict,
    shard_id: u32,
) {
    let fp = dict.fingerprint();
    rt.block_on(srv.adopt_dict(adopt_req_shard(dict, shard_id)))
        .expect("adopt");
    rt.block_on(srv.insert_extracted(insert_req(shard_id, 10, "pro")))
        .expect("write");
    let hits = rt
        .block_on(srv.percolate(read_req(shard_id, "pro edition")))
        .expect("read")
        .into_inner();
    assert_eq!(
        hits.ids,
        vec![10],
        "precondition: the slot serves its query"
    );
    rt.block_on(srv.fence(fence_req(shard_id, 7, fp)))
        .expect("fence");
    let dropped = rt
        .block_on(srv.drop_shard(drop_req_16(shard_id, 7, fp)))
        .expect("drop")
        .into_inner();
    assert!(dropped.dropped);
}

/// Every data RPC on `shard_id` fails loud, as an ownership mismatch.
fn assert_refuses_to_serve(rt: &tokio::runtime::Runtime, srv: &ShardServer, shard_id: u32) {
    let read = rt
        .block_on(srv.percolate(read_req(shard_id, "pro edition")))
        .expect_err("a re-created empty slot must not answer a read");
    assert_eq!(read.code(), Code::FailedPrecondition, "{read:?}");
    assert!(read.message().contains("dropped"), "{read:?}");
    assert!(
        matches!(
            crate::cluster::ranked_wire::parse(&read),
            Some(crate::cluster::shard::ShardError::OwnershipMismatch(_))
        ),
        "coordinators must see an ownership mismatch, not a transport error: {read:?}"
    );
    let write = rt
        .block_on(srv.insert_extracted(insert_req(shard_id, 11, "pro")))
        .expect_err("nor accept a write");
    assert_eq!(write.code(), Code::FailedPrecondition, "{write:?}");
    let count = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id })))
        .expect_err("nor report a count");
    assert_eq!(count.code(), Code::FailedPrecondition, "{count:?}");
}

/// The bug: a coordinator with a stale view re-adopts a shard this node gave
/// up, gets a brand-new empty slot, and its reads succeed with no matches.
/// Adoption itself must still succeed (a handoff back to this node starts the
/// same way), but the slot serves nothing until a peer recovery fills it.
#[test]
fn a_dropped_shard_re_adopted_empty_refuses_to_serve() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    adopt_fill_and_drop(&rt, &srv, &d, 3);

    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
        .expect("re-adoption creates the slot a recovery will fill");
    assert_refuses_to_serve(&rt, &srv, 3);

    // A slot this node never dropped is unaffected.
    rt.block_on(srv.add_shard(add_shard_req(4, d.fingerprint(), empty_tag_fp())))
        .expect("add a fresh slot");
    rt.block_on(srv.insert_extracted(insert_req(4, 20, "pro")))
        .expect("a never-dropped slot serves");
}

/// The co-location path (`AddShard`) creates slots too and gets the same rule.
#[test]
fn a_dropped_shard_re_added_empty_refuses_to_serve() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    // Slot 0 keeps the node's space adopted while slot 3 comes and goes.
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    adopt_fill_and_drop(&rt, &srv, &d, 3);

    rt.block_on(srv.add_shard(add_shard_req(3, d.fingerprint(), empty_tag_fp())))
        .expect("re-adding creates the slot");
    assert_refuses_to_serve(&rt, &srv, 3);
}

/// The slot can still be fenced and dropped again: an abandoned handoff back
/// must not leave a slot that orphan GC cannot clean up.
#[test]
fn a_slot_awaiting_recovery_can_be_fenced_and_dropped() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let fp = d.fingerprint();
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    adopt_fill_and_drop(&rt, &srv, &d, 3);
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
        .expect("re-adopt");

    rt.block_on(srv.fence(fence_req(3, 9, fp))).expect("fence");
    let dropped = rt
        .block_on(srv.drop_shard(drop_req_16(3, 9, fp)))
        .expect("drop")
        .into_inner();
    assert!(dropped.dropped);
    // And it is still remembered.
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
        .expect("re-adopt again");
    assert_refuses_to_serve(&rt, &srv, 3);
}

/// Adopting a different layout is a legitimate fresh start: the node was
/// emptied, and the shard ids of the old layout mean nothing in the new one.
#[test]
fn adoption_under_a_new_placement_generation_serves() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    adopt_fill_and_drop(&rt, &srv, &d, 3);

    let mut next_generation = adopt_req_shard(&d, 3);
    next_generation.get_mut().placement_generation = GEN + 1;
    rt.block_on(srv.adopt_dict(next_generation))
        .expect("adopt under the next placement generation");
    let mut write = insert_req(3, 30, "pro");
    write.get_mut().item.as_mut().expect("item").placement = Some(proto::placement_to_proto(
        &crate::ownership::QueryPlacement::selective(
            crate::ownership::PlacementGeneration(GEN + 1),
            TEST_NUM_SHARDS,
            vec![3],
        )
        .expect("placement"),
    ));
    rt.block_on(srv.insert_extracted(write))
        .expect("a slot of the new layout serves");
}

/// The record is durable: a restarted node still knows which shards it gave up.
#[test]
fn a_restart_remembers_dropped_shards() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = std::env::temp_dir().join(format!(
        "rr_dropped_restart_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    {
        let srv =
            ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
        // Slot 0 stays, so the node's adopted space survives the restart.
        rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
            .expect("adopt slot 0");
        adopt_fill_and_drop(&rt, &srv, &d, 3);
    }
    let srv = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("restart");
    rt.block_on(srv.add_shard(add_shard_req(3, d.fingerprint(), empty_tag_fp())))
        .expect("re-add after the restart");
    assert_refuses_to_serve(&rt, &srv, 3);
    drop(srv);

    // A slot created while awaiting recovery is still awaiting after another restart.
    let srv = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("second restart");
    assert_refuses_to_serve(&rt, &srv, 3);
    let _ = std::fs::remove_dir_all(&dir);
}
