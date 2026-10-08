use super::*;

// ---- a shard node's shards own their commit record (ADR-214) ----

/// A shard node restarts each shard from that shard's checkpoint file, so that file is the
/// record of which segment files are live, and every shard a node takes into a slot is told
/// so. A shard that was not told would keep every file it replaces for ever (the safe side of
/// the mistake, and a disk that fills).
#[test]
fn a_nodes_shard_owns_its_commit_record() {
    let n = norm();
    let d = Arc::new(frozen_dict(&["ownrecordneedle"], &n));
    let dir = std::env::temp_dir().join(format!("rr_own_commit_record_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let owns = |srv: &ShardServer| {
        srv.slot(0)
            .expect("slot 0")
            .loaded_state()
            .expect("loaded state")
            .shard
            .owns_its_commit_record()
    };
    let in_memory = ShardServer::new(Arc::clone(&n), Arc::clone(&d), EngineConfig::default());
    assert!(owns(&in_memory));
    let durable = ShardServer::new_durable(
        Arc::clone(&n),
        Arc::clone(&d),
        EngineConfig::default(),
        dir.clone(),
    )
    .expect("a durable node");
    assert!(owns(&durable));
    drop(durable);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A shard's state is built in one place, which tells the shard that it owns its commit
/// record. A state built anywhere else would hold a shard that was not told.
#[test]
fn every_shard_state_is_built_through_the_constructor_that_tells_the_shard() {
    fn sources(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = std::fs::read_to_string(&path).expect("read source");
                out.push((path, text));
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cluster");
    let mut files = vec![(
        root.join("server.rs"),
        std::fs::read_to_string(root.join("server.rs")).expect("server.rs"),
    )];
    sources(&root.join("server"), &mut files);
    assert!(files.len() > 10, "the scan found the server's sources");
    let literal = ["ServerState", " {"].concat();
    let mut built = Vec::new();
    for (path, text) in &files {
        for (number, line) in text.lines().enumerate() {
            let line = line.trim_start();
            if line.contains(&literal)
                && !line.starts_with("struct ")
                && !line.starts_with("impl ")
                && !line.starts_with("//")
            {
                built.push(format!("{}:{}", path.display(), number + 1));
            }
        }
    }
    assert_eq!(
        built.len(),
        1,
        "a shard state is built outside `ServerState::new`, which tells its shard that it \
         owns its commit record: {built:?}"
    );
    assert!(built[0].contains("server.rs:"), "{built:?}");
}

/// A node removes a file its shard has replaced only in a worker that holds the installation
/// barrier on the slot's installed shard: the seal worker and the staged-load worker. The
/// removal is by name, and the barrier is what keeps it apart from a recovery writing
/// received files into the same directory under names that can be the same.
#[test]
fn only_a_worker_that_holds_the_installation_barrier_removes_replaced_files() {
    fn sources(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name != "tests") {
                    sources(&path, out);
                }
            } else if path.extension().is_some_and(|ext| ext == "rs")
                && path.file_name().is_some_and(|name| name != "tests.rs")
            {
                let text = std::fs::read_to_string(&path).expect("read source");
                out.push((path, text));
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cluster");
    let mut files = vec![(
        root.join("server.rs"),
        std::fs::read_to_string(root.join("server.rs")).expect("server.rs"),
    )];
    sources(&root.join("server"), &mut files);
    assert!(files.len() > 10, "the scan found the server's sources");
    let call = [".remove_replaced", "_files()"].concat();
    let mut callers = Vec::new();
    for (path, text) in &files {
        let lines: Vec<&str> = text.lines().collect();
        for (number, line) in lines.iter().enumerate() {
            if !line.contains(&call) {
                continue;
            }
            let worker = lines[..number]
                .iter()
                .rposition(|line| line.contains("spawn_blocking(move || {"))
                .expect("the removal is not inside a blocking worker");
            assert!(
                lines[worker..number]
                    .iter()
                    .any(|line| line.trim() == "let _install = install;"),
                "{}:{}: a replaced file is removed by a worker that does not hold the \
                 installation barrier",
                path.display(),
                number + 1
            );
            callers.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    callers.sort();
    assert_eq!(
        callers,
        ["service.rs", "stage_ingest.rs"],
        "the seal worker and the staged-load worker, and nothing else"
    );
}

/// A durable node whose slot 0 holds rows 1 and 2 in a sealed segment, with row 1 then
/// deleted: the next seal of that shard rewrites the segment its checkpoint file names.
fn a_node_with_a_deletion_in_a_sealed_segment(
    tag: &str,
) -> (tokio::runtime::Runtime, ShardServer, std::path::PathBuf) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["keptneedle", "otherneedle"], &n);
    let dir = std::env::temp_dir().join(format!("rr_node_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    rt.block_on(srv.adopt_dict(adopt_req(&d))).expect("adopt");
    for (id, dsl) in [(1, "keptneedle"), (2, "otherneedle")] {
        rt.block_on(srv.insert_extracted(insert_req(0, id, dsl)))
            .expect("insert");
    }
    rt.block_on(srv.seal(seal_req())).expect("seal");
    rt.block_on(srv.delete(Request::new(proto::DeleteRequest {
        logical_id: 1,
        shard_id: 0,
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
    })))
    .expect("delete");
    (rt, srv, dir)
}

fn seal_req() -> Request<proto::SealRequest> {
    Request::new(proto::SealRequest {
        shard_id: 0,
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
    })
}

/// The `.seg` files in slot 0's directory, and the ones its shard's checkpoint file names.
fn on_disk_and_named(srv: &ShardServer, dir: &std::path::Path) -> (Vec<String>, Vec<String>) {
    let mut on_disk: Vec<String> =
        std::fs::read_dir(super::super::shard_dir(dir, 0).join("segments"))
            .expect("the slot's segments")
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
            })
            .collect();
    on_disk.sort();
    let state = srv.slot(0).expect("slot 0").loaded_state().expect("state");
    let mut named =
        crate::cluster::shard::Shard::segment_filenames(&state.shard).expect("segment files");
    named.sort();
    (on_disk, named)
}

/// A `Seal` on a node rewrites the segment that holds a deletion, writes the shard's
/// checkpoint file, and then removes the file the rewrite replaced: the slot's directory
/// holds exactly what the checkpoint file names.
#[test]
fn a_seal_on_a_node_removes_the_file_its_checkpoint_file_no_longer_names() {
    let (rt, srv, dir) = a_node_with_a_deletion_in_a_sealed_segment("seal_removes");
    let (before, _) = on_disk_and_named(&srv, &dir);
    rt.block_on(srv.seal(seal_req())).expect("seal");
    let (on_disk, named) = on_disk_and_named(&srv, &dir);
    assert!(
        !named.contains(&before[0]),
        "precondition: the seal rewrote the segment: {named:?}"
    );
    assert_eq!(
        on_disk, named,
        "the seal left a file its checkpoint file does not name"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Serving as a recovery source seals the shard too, outside the installation barrier, so
/// it removes nothing the shard had kept: a recovery into this same slot could be writing a
/// received file under that name. The next `Seal` removes it.
#[test]
fn serving_as_a_recovery_source_removes_no_kept_file() {
    let (rt, srv, dir) = a_node_with_a_deletion_in_a_sealed_segment("source_keeps");
    let (before, _) = on_disk_and_named(&srv, &dir);
    let fingerprint = current_fp(&srv);
    rt.block_on(async {
        srv.fetch_segments(Request::new(proto::FetchSegmentsRequest {
            dict_fingerprint: fingerprint,
            tag_dict_fingerprint: empty_tag_fp(),
            shard_id: 0,
            placement_generation: 1,
            num_shards: TEST_NUM_SHARDS,
        }))
        .await
        .map(drop)
    })
    .expect("fetch segments");
    let (on_disk, named) = on_disk_and_named(&srv, &dir);
    assert!(
        !named.contains(&before[0]),
        "precondition: serving as a source rewrote the segment: {named:?}"
    );
    assert!(
        on_disk.contains(&before[0]),
        "serving as a recovery source removed a kept file: {on_disk:?}"
    );
    rt.block_on(srv.seal(seal_req())).expect("seal");
    let (on_disk, named) = on_disk_and_named(&srv, &dir);
    assert_eq!(on_disk, named, "the seal removes it");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A recovery into a slot that fails, here before it has received anything, leaves the
/// slot's shard as it was: the next seal still removes what the shard replaced. (Nothing is
/// switched off for the length of a recovery, so there is nothing to switch back on.)
#[test]
fn a_recovery_that_fails_does_not_stop_the_shard_removing_what_it_replaces() {
    let (rt, srv, dir) = a_node_with_a_deletion_in_a_sealed_segment("failed_recovery");
    let fingerprint = current_fp(&srv);
    let failed = rt.block_on(srv.recover_from(Request::new(proto::RecoverFromRequest {
        shard_id: 0,
        source_endpoint: "http://127.0.0.1:1".to_string(),
        dict_fingerprint: fingerprint,
        tag_dict_fingerprint: empty_tag_fp(),
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
    })));
    assert!(failed.is_err(), "precondition: the recovery failed");
    rt.block_on(srv.seal(seal_req())).expect("seal");
    let (on_disk, named) = on_disk_and_named(&srv, &dir);
    assert_eq!(
        on_disk, named,
        "after a failed recovery the shard no longer removes what it replaces"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
