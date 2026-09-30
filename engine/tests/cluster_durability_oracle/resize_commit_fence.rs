//! Post-swap resize commit fence: a resize whose serving swap succeeded but whose control or
//! durable commit failed must not let later writes reach the coordinator log under the
//! uncommitted placement generation. Otherwise a crash before the healing retry reopens the old
//! manifest and replays a mutation stamped for the new layout, which fails recovery.

use crate::harness::*;
fn matches(cluster: &ClusterEngine, title: &str) -> HashSet<u64> {
    cluster.percolate(title).unwrap().into_iter().collect()
}

fn seeded(dir: &std::path::Path) -> ClusterEngine {
    let cfg = durable_cfg(3, dir.to_path_buf(), false);
    let cluster = ClusterEngine::build(
        vocab(),
        &cfg,
        &[(1, "package adapter".into()), (2, "vintage lamp".into())],
    )
    .expect("durable cluster");
    FailFirstProposal::install(cluster)
}

#[test]
fn uncommitted_resize_pauses_placed_writes_until_crash_recovery_is_safe() {
    let dir = unique_dir("resize_commit_fence_crash");
    let mut cluster = seeded(&dir);

    let failed = cluster.resize(5);
    assert!(
        matches!(failed, Err(ShardError::ControlPlane(_))),
        "the control transition must fail after the serving swap: {failed:?}"
    );
    assert_eq!(cluster.num_shards(), 5, "the serving swap already happened");

    let add = cluster.add_query(3, "brass compass");
    assert!(matches!(add, Err(ShardError::Log(_))), "add: {add:?}");
    let upsert = cluster.upsert_query(1, "package charger", 2);
    assert!(
        matches!(upsert, Err(ShardError::Log(_))),
        "upsert: {upsert:?}"
    );
    // Removes carry no placement, so they stay available and replay over either layout.
    assert!(cluster.remove_query(2).expect("remove stays available") > 0);
    // Reads keep serving the swapped layout.
    assert!(matches(&cluster, "package adapter").contains(&1));
    drop(cluster);

    let reopened = ClusterEngine::open(&dir, vocab(), None)
        .expect("a crash after an uncommitted resize must leave the cluster openable");
    assert_eq!(
        reopened.num_shards(),
        3,
        "the previous manifest stays authoritative"
    );
    assert!(matches(&reopened, "package adapter").contains(&1));
    assert!(!matches(&reopened, "brass compass").contains(&3));
    assert!(
        !matches(&reopened, "vintage lamp").contains(&2),
        "an acknowledged remove must replay over the previous layout"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_count_retry_commits_the_layout_and_reopens_writes() {
    let dir = unique_dir("resize_commit_fence_retry");
    let mut cluster = seeded(&dir);
    assert!(cluster.resize(5).is_err(), "first control proposal fails");
    assert!(cluster.add_query(3, "brass compass").is_err());

    assert_eq!(cluster.resize(5).expect("same-count retry heals"), 0);
    cluster
        .add_query(3, "brass compass")
        .expect("writes resume once the layout is committed");
    drop(cluster);

    let reopened = ClusterEngine::open(&dir, vocab(), None).expect("reopen");
    assert_eq!(reopened.num_shards(), 5);
    assert!(matches(&reopened, "brass compass").contains(&3));
    assert!(matches(&reopened, "package adapter").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn uncommitted_vocabulary_rebuild_pauses_placed_writes() {
    let dir = unique_dir("vocab_commit_fence");
    let mut cluster = seeded(&dir);
    let failed = cluster.import_alias_synonyms("package, pkg");
    assert!(
        matches!(failed, Err(ShardError::ControlPlane(_))),
        "the control transition must fail after the serving swap: {failed:?}"
    );
    let add = cluster.add_query(3, "brass compass");
    assert!(matches!(add, Err(ShardError::Log(_))), "add: {add:?}");
    drop(cluster);

    let reopened = ClusterEngine::open(&dir, vocab(), None)
        .expect("a crash after an uncommitted vocabulary rebuild must stay openable");
    assert!(matches(&reopened, "package adapter").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_later_checkpoint_commits_the_serving_layout() {
    let dir = unique_dir("resize_commit_fence_checkpoint");
    let mut cluster = seeded(&dir);
    assert!(cluster.resize(5).is_err(), "first control proposal fails");
    cluster
        .checkpoint()
        .expect("checkpoint commits the serving layout");
    cluster
        .add_query(3, "brass compass")
        .expect("writes resume after the checkpoint");
    drop(cluster);

    let reopened = ClusterEngine::open(&dir, vocab(), None).expect("reopen");
    assert_eq!(reopened.num_shards(), 5);
    assert!(matches(&reopened, "brass compass").contains(&3));
    let _ = std::fs::remove_dir_all(&dir);
}

fn source_sidecars(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read cluster dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("shard_"))
        })
        .flat_map(|shard| {
            std::fs::read_dir(shard.path())
                .expect("read shard dir")
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| {
                    n.starts_with("sources")
                        && std::path::Path::new(n)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("dat"))
                })
                .map(move |n| format!("{}/{n}", shard.file_name().to_string_lossy()))
                .collect::<Vec<_>>()
        })
        .collect();
    names.sort();
    names
}

#[test]
fn committed_rebuilds_do_not_accumulate_superseded_source_sidecars() {
    let dir = unique_dir("resize_sidecar_gc");
    let (queries, _) = build_corpus();
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable build");
    for k in [4, 2, 5, 3] {
        cluster.resize(k).expect("resize");
    }
    let sidecars = source_sidecars(&dir);
    assert_eq!(
        sidecars.len(),
        3,
        "exactly one committed source sidecar per shard should remain: {sidecars:?}"
    );
    drop(cluster);
    let reopened = ClusterEngine::open(&dir, vocab(), None).expect("reopen");
    assert_eq!(reopened.num_shards(), 3);
    let _ = std::fs::remove_dir_all(&dir);
}
