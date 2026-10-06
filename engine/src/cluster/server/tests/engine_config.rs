use super::*;

// ---- a shard node's engine configuration reaches every slot it builds (ADR-192) ----

/// What the node's `/_metrics` says about one slot's translog: 1 when every write is fsynced.
fn sync_gauge(srv: &ShardServer, shard_id: u32) -> Option<u64> {
    let series = format!("reverse_rusty_shard_translog_sync_on_write{{shard=\"{shard_id}\"}} ");
    srv.metrics_source()
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&series)?.trim().parse().ok())
}

fn config(sync: bool) -> EngineConfig {
    EngineConfig {
        wal_sync_on_write: sync,
        ..EngineConfig::default()
    }
}

fn node_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rr_engine_config_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

/// The node's sync policy applies to a slot however the slot comes to exist: adopted, added
/// beside another, or restored from disk at startup. It is not stored with the data, so a
/// restart under a different flag changes it.
#[test]
fn every_slot_opens_its_translog_with_the_nodes_sync_policy() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let (fp, tag_fp) = (d.fingerprint(), empty_tag_fp());
    for sync in [true, false] {
        let dir = node_dir(if sync { "sync" } else { "nosync" });
        {
            let srv = ShardServer::pending_durable(Arc::clone(&n), config(sync), dir.clone());
            rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 3)))
                .expect("adopt slot 3");
            rt.block_on(srv.add_shard(add_shard_req(5, fp, tag_fp)))
                .expect("add slot 5");
            rt.block_on(srv.insert_extracted(insert_req(3, 10, "pro")))
                .expect("write");
            assert_eq!(sync_gauge(&srv, 3), Some(u64::from(sync)), "adopted slot");
            assert_eq!(sync_gauge(&srv, 5), Some(u64::from(sync)), "added slot");
        }
        let srv = ShardServer::open_durable(Arc::clone(&n), config(sync), dir.clone())
            .expect("restart under the same flag");
        assert_eq!(
            sync_gauge(&srv, 3),
            Some(u64::from(sync)),
            "slot restored from disk"
        );
        drop(srv);
        let srv = ShardServer::open_durable(Arc::clone(&n), config(!sync), dir.clone())
            .expect("restart under the other flag");
        assert_eq!(
            sync_gauge(&srv, 3),
            Some(u64::from(!sync)),
            "the policy is the running node's, not the data's"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A node without a data directory has nothing to sync, whatever its flag says, and reports
/// that rather than a promise it cannot keep.
#[test]
fn an_in_memory_node_reports_that_it_syncs_nothing() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), config(true));
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt");
    assert_eq!(sync_gauge(&srv, 0), Some(0));
}
