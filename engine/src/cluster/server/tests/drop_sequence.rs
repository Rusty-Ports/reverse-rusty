use super::dropped::{
    adopt_fill_and_drop, assert_refuses_to_serve, drop_req_16, fence_req, read_req, temp_node_dir,
};
use super::*;

// ---- the drop itself: tombstone, record, remove (ADR-189) ----

/// A slot ready to drop: adopted, holding one query, fenced at generation 7.
fn adopt_fill_and_fence(
    rt: &tokio::runtime::Runtime,
    srv: &ShardServer,
    dict: &Dict,
    shard_id: u32,
) {
    rt.block_on(srv.adopt_dict(adopt_req_shard(dict, shard_id)))
        .expect("adopt");
    rt.block_on(srv.insert_extracted(insert_req(shard_id, 10, "pro")))
        .expect("write");
    rt.block_on(srv.fence(fence_req(shard_id, 7, dict.fingerprint())))
        .expect("fence");
}

fn fence_of(srv: &ShardServer, shard_id: u32) -> u64 {
    srv.slot(shard_id)
        .expect("slot is hosted")
        .fenced_at_generation
        .load(std::sync::atomic::Ordering::Acquire)
}

/// Recording a drop writes and syncs a file on a durable node. That must happen
/// before the slot is removed, and without the lock every other shard on the
/// node resolves its slot through.
#[test]
fn a_drop_is_recorded_before_removal_and_outside_the_slot_map_lock() {
    use std::sync::atomic::{AtomicU8, Ordering};
    const LOCK_FREE: u8 = 1;
    const STILL_HOSTED: u8 = 2;

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    adopt_fill_and_fence(&rt, &srv, &d, 3);

    let seen = Arc::new(AtomicU8::new(0));
    let (shards, observed) = (Arc::clone(&srv.shards), Arc::clone(&seen));
    super::super::dropped::arm_while_recording_a_drop(move || {
        // Observe only; the verdict is asserted after the drop returns.
        let mut state = 0;
        if let Ok(map) = shards.try_write() {
            state |= LOCK_FREE;
            if map.contains_key(&3) {
                state |= STILL_HOSTED;
            }
        }
        observed.store(state | 0x80, Ordering::Release);
    });
    // `block_on` runs the handler on this thread, where the seam is armed.
    let dropped = rt
        .block_on(srv.drop_shard(drop_req_16(3, 7, d.fingerprint())))
        .expect("drop")
        .into_inner();
    assert!(dropped.dropped);

    let seen = seen.load(Ordering::Acquire);
    assert_ne!(seen & 0x80, 0, "the drop was recorded");
    assert_ne!(
        seen & LOCK_FREE,
        0,
        "the record is written with the slot-map lock free"
    );
    assert_ne!(
        seen & STILL_HOSTED,
        0,
        "the record is written before the slot is removed"
    );
    assert!(srv.slot(3).is_err(), "the slot is gone afterwards");
}

/// A drop that cannot be recorded does not happen: the slot stays hosted at its
/// fence, nothing is remembered, and the retry goes through.
#[cfg(unix)]
#[test]
fn a_drop_that_cannot_be_recorded_keeps_the_slot() {
    use std::os::unix::fs::PermissionsExt;
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("unrecordable");
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    adopt_fill_and_fence(&rt, &srv, &d, 3);

    // The record is written at the node root; the slot's own directory stays writable.
    let writable = std::fs::metadata(&dir).expect("node dir").permissions();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).expect("read-only");
    let refused = rt.block_on(srv.drop_shard(drop_req_16(3, 7, d.fingerprint())));
    std::fs::set_permissions(&dir, writable).expect("restore permissions");

    let refused = refused.expect_err("an unrecorded drop must be refused");
    assert_eq!(refused.code(), Code::Internal, "{refused:?}");
    assert!(
        refused.message().contains("dropped-shard record"),
        "{refused:?}"
    );
    assert_eq!(fence_of(&srv, 3), 7, "the pre-drop fence is restored");
    assert!(
        !srv.was_dropped(3).expect("record"),
        "nothing is remembered"
    );
    assert!(
        !dir.join("dropped_shards.bin").exists(),
        "and nothing was written"
    );

    let dropped = rt
        .block_on(srv.drop_shard(drop_req_16(3, 7, d.fingerprint())))
        .expect("the retry drops")
        .into_inner();
    assert!(dropped.dropped);
    assert!(srv.was_dropped(3).expect("record"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A drop whose directory rename fails takes its record back, durably: the slot
/// was not given up, so a restart must not find it listed.
#[test]
fn a_drop_that_cannot_quarantine_takes_its_record_back() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("unquarantined");
    {
        let srv =
            ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
        adopt_fill_and_fence(&rt, &srv, &d, 3);
        let refused = srv
            .remove_slot_if_fenced_at_with(3, 7, || -> Result<(), tonic::Status> {
                Err(tonic::Status::internal("injected rename failure"))
            })
            .expect_err("a failed quarantine must reject the drop");
        assert_eq!(refused.code(), Code::Internal, "{refused:?}");
        assert_eq!(fence_of(&srv, 3), 7, "the pre-drop fence is restored");
        assert!(!srv.was_dropped(3).expect("record"));
    }
    let srv = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("reopen");
    let hits = rt
        .block_on(srv.percolate(read_req(3, "pro edition")))
        .expect("the slot was never dropped, so it serves after a restart")
        .into_inner();
    assert_eq!(hits.ids, vec![10]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The removal re-checks, under the map lock, that it is removing the slot it
/// tombstoned. A different slot under the same id is left alone.
#[test]
fn a_drop_never_removes_a_slot_it_did_not_tombstone() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    adopt_fill_and_fence(&rt, &srv, &d, 3);
    let hosted = srv
        .slot(3)
        .expect("slot")
        .loaded_state()
        .expect("loaded state");
    let replacement = super::super::ShardSlot::loaded(super::super::ServerState::new(
        Arc::clone(&hosted.dict),
        Arc::clone(&hosted.tag_dict),
        crate::cluster::shard::LocalShard::new(
            Arc::clone(&n),
            Arc::clone(&hosted.dict),
            Arc::clone(&hosted.tag_dict),
            EngineConfig::default(),
        ),
        &srv.events,
    ));
    let (shards, incoming) = (Arc::clone(&srv.shards), Arc::clone(&replacement));
    super::super::dropped::arm_while_recording_a_drop(move || {
        // `try_write`, so a regression that records under the map lock fails this test
        // instead of deadlocking it.
        if let Ok(mut map) = shards.try_write() {
            map.insert(3, incoming);
        }
    });
    let refused = srv
        .remove_slot_if_fenced_at_with(3, 7, || Ok(()))
        .expect_err("the tombstoned slot is no longer the hosted one");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    assert!(
        Arc::ptr_eq(&srv.slot(3).expect("still hosted"), &replacement),
        "the replacement is left in place"
    );
    assert!(!srv.was_dropped(3).expect("record"), "no drop happened");
}

/// A slot that was already awaiting recovery is dropped again, and that drop
/// fails. The record predates the failed drop, so it stays: taking it back
/// would let the empty slot serve after a restart.
#[test]
fn a_failed_drop_keeps_a_record_it_did_not_make() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("earlier_record");
    {
        let srv =
            ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
        adopt_fill_and_drop(&rt, &srv, &d, 3);
        rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
            .expect("re-adopt: slot 3 awaits recovery");
        rt.block_on(srv.fence(fence_req(3, 8, d.fingerprint())))
            .expect("fence the abandoned slot");
        srv.remove_slot_if_fenced_at_with(3, 8, || -> Result<(), tonic::Status> {
            Err(tonic::Status::internal("injected rename failure"))
        })
        .expect_err("the second drop fails");
        assert!(
            srv.was_dropped(3).expect("record"),
            "the record of the first drop is kept"
        );
        assert_refuses_to_serve(&rt, &srv, 3);
    }
    let srv = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("reopen");
    assert_refuses_to_serve(&rt, &srv, 3);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The quarantine fails and so does taking the record back. The slot stays
/// hosted, and the error says the record is still there, because that record
/// makes the shard await recovery after a restart.
#[cfg(unix)]
#[test]
fn a_record_that_cannot_be_taken_back_is_reported() {
    use std::os::unix::fs::PermissionsExt;
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("kept_record");
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    adopt_fill_and_fence(&rt, &srv, &d, 3);

    let writable = std::fs::metadata(&dir).expect("node dir").permissions();
    let refused = srv.remove_slot_if_fenced_at_with(3, 7, || -> Result<(), tonic::Status> {
        // The record is already written; from here the node root rejects writes.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).expect("read-only");
        Err(tonic::Status::internal("injected rename failure"))
    });
    std::fs::set_permissions(&dir, writable).expect("restore permissions");

    let refused = refused.expect_err("the drop fails");
    assert_eq!(refused.code(), Code::Internal, "{refused:?}");
    assert!(
        refused.message().contains("injected rename failure")
            && refused.message().contains("could not be taken back"),
        "{refused:?}"
    );
    assert_eq!(fence_of(&srv, 3), 7, "the pre-drop fence is restored");
    assert!(
        srv.was_dropped(3).expect("record"),
        "memory agrees with the record left on disk"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// How many shards the record file under `dir` lists.
fn shards_on_record(dir: &std::path::Path) -> u32 {
    let blob = std::fs::read(dir.join("dropped_shards.bin")).expect("record file");
    u32::from_le_bytes(blob[36..40].try_into().expect("count field"))
}

/// A recovery clears the record, and that write fails after its rename: the
/// file no longer lists the shard, the recovery fails, and the slot keeps
/// refusing. Dropping that slot must put the shard back on record. It used to
/// see the shard still in memory and skip the write, so a restart forgot the
/// drop and served the shard empty.
#[test]
fn a_record_left_in_doubt_is_rewritten_by_the_next_drop() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("in_doubt_recovery");
    {
        let srv =
            ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
        adopt_fill_and_drop(&rt, &srv, &d, 3);
        rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
            .expect("re-adopt: slot 3 awaits recovery");
        let slot = srv.slot(3).expect("slot");

        super::super::dropped::arm_fail_after_rename();
        srv.mark_recovered(3, &slot)
            .expect_err("the record write failed at the directory sync");
        assert_eq!(
            shards_on_record(&dir),
            0,
            "precondition: the renamed file no longer lists the shard"
        );
        assert!(srv.was_dropped(3).expect("record"));
        assert_refuses_to_serve(&rt, &srv, 3);

        // Orphan GC removes the abandoned recovery target.
        rt.block_on(srv.fence(fence_req(3, 8, d.fingerprint())))
            .expect("fence");
        let dropped = rt
            .block_on(srv.drop_shard(drop_req_16(3, 8, d.fingerprint())))
            .expect("drop")
            .into_inner();
        assert!(dropped.dropped);
        assert_eq!(shards_on_record(&dir), 1, "the drop is on record again");
    }
    let srv = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("reopen");
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
        .expect("a stale coordinator re-adopts the shard");
    assert_refuses_to_serve(&rt, &srv, 3);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A drop whose record write fails after its rename is refused, and the shard
/// counts as dropped from then on: a restart may find it on record.
#[test]
fn a_drop_whose_record_is_left_in_doubt_is_refused_and_remembered() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let dir = temp_node_dir("in_doubt_drop");
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    adopt_fill_and_fence(&rt, &srv, &d, 3);

    super::super::dropped::arm_fail_after_rename();
    let refused = rt
        .block_on(srv.drop_shard(drop_req_16(3, 7, d.fingerprint())))
        .expect_err("the record write failed at the directory sync");
    assert_eq!(refused.code(), Code::Internal, "{refused:?}");
    assert_eq!(fence_of(&srv, 3), 7, "the pre-drop fence is restored");
    assert_eq!(
        shards_on_record(&dir),
        1,
        "the renamed file lists the shard"
    );
    assert!(
        srv.was_dropped(3).expect("record"),
        "memory honours what a restart may read"
    );
    let hits = rt
        .block_on(srv.percolate(read_req(3, "pro edition")))
        .expect("the slot was not dropped, so it still serves")
        .into_inner();
    assert_eq!(hits.ids, vec![10]);

    let dropped = rt
        .block_on(srv.drop_shard(drop_req_16(3, 7, d.fingerprint())))
        .expect("the retry drops")
        .into_inner();
    assert!(dropped.dropped);
    assert!(srv.was_dropped(3).expect("record"));
    let _ = std::fs::remove_dir_all(&dir);
}
