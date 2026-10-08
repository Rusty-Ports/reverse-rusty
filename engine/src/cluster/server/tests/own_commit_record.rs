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

/// A recovery tells the shard it will replace to remove nothing more from its directory, and
/// it tells it before the first received file is written there: a received file can carry
/// the name of a file that shard has listed for release. (The shard's side of this is tested
/// where the shard is; here, that the recovery says it, and says it first.)
#[test]
fn a_recovery_tells_the_shard_it_replaces_before_it_writes_into_its_directory() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/cluster/server/service/recovery.rs"),
    )
    .expect("recovery.rs");
    let handler = source
        .find("pub(super) async fn recover_from(")
        .expect("the RecoverFrom handler");
    let body = &source[handler..];
    let told = body
        .find(".leave_the_directory_to_a_recovery();")
        .expect("the recovery does not tell the shard it replaces");
    let first_write = body
        .find("drain_recovery_stream(")
        .expect("the recovery receives its files");
    assert!(
        told < first_write,
        "the recovery writes received files before it tells the shard it replaces"
    );
}
