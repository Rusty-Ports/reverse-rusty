//! The unfinished-bulk-load mark a node keeps for a slot (ADR-196).

use super::*;

fn state(rt: &tokio::runtime::Runtime, server: &ShardServer, shard_id: u32) -> bool {
    rt.block_on(server.bulk_load_state(Request::new(proto::ShardRef { shard_id })))
        .expect("bulk-load state")
        .into_inner()
        .incomplete
}

fn set(rt: &tokio::runtime::Runtime, server: &ShardServer, shard_id: u32, incomplete: bool) {
    let reply = rt
        .block_on(
            server.set_bulk_load_state(Request::new(proto::SetBulkLoadStateRequest {
                shard_id,
                incomplete,
            })),
        )
        .expect("set the bulk-load state")
        .into_inner();
    assert_eq!(reply.incomplete, incomplete);
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rr_bulk_load_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn a_slot_is_unmarked_until_told_and_the_mark_can_be_cleared() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let n = norm();
    let d = Arc::new(frozen_dict(&["alpha"], &n));
    let server = ShardServer::new(Arc::clone(&n), d, EngineConfig::default());
    assert!(!state(&rt, &server, 0));
    set(&rt, &server, 0, true);
    assert!(state(&rt, &server, 0));
    set(&rt, &server, 0, false);
    assert!(!state(&rt, &server, 0));
    // Clearing a mark that is not there is not an error: a retry must be able to repeat it.
    set(&rt, &server, 0, false);
}

#[test]
fn the_mark_belongs_to_one_hosted_slot() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let n = norm();
    let d = Arc::new(frozen_dict(&["alpha"], &n));
    let server = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    rt.block_on(server.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    rt.block_on(server.add_shard(add_shard_req(1, d.fingerprint(), empty_tag_fp())))
        .expect("a second slot");
    set(&rt, &server, 1, true);
    assert!(state(&rt, &server, 1));
    assert!(!state(&rt, &server, 0), "slot 0 was never marked");

    let unhosted = rt
        .block_on(
            server.set_bulk_load_state(Request::new(proto::SetBulkLoadStateRequest {
                shard_id: 9,
                incomplete: true,
            })),
        )
        .expect_err("slot 9 is not hosted");
    assert_eq!(unhosted.code(), Code::NotFound);
    let unhosted = rt
        .block_on(server.bulk_load_state(Request::new(proto::ShardRef { shard_id: 9 })))
        .expect_err("slot 9 is not hosted");
    assert_eq!(unhosted.code(), Code::NotFound);
}

/// The whole point of the mark: the node that restarts still knows a load into the slot
/// never finished, when the coordinator that was loading it is gone.
#[test]
fn a_durable_node_remembers_the_mark_across_a_restart_and_forgets_it_once_cleared() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let n = norm();
    let d = Arc::new(frozen_dict(&["alpha"], &n));
    let dir = scratch("restart");
    {
        let server =
            ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
        rt.block_on(server.adopt_dict(adopt_req_shard(&d, 0)))
            .expect("adopt slot 0");
        set(&rt, &server, 0, true);
        assert!(state(&rt, &server, 0));
    }
    {
        let restarted =
            ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
                .expect("restart");
        assert!(
            state(&rt, &restarted, 0),
            "a restarted node must still report the unfinished load"
        );
        set(&rt, &restarted, 0, false);
        assert!(!state(&rt, &restarted, 0));
    }
    let again = ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
        .expect("second restart");
    assert!(!state(&rt, &again, 0), "a cleared mark stays cleared");
    let _ = std::fs::remove_dir_all(&dir);
}
