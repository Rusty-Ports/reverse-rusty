//! ADR-215: a build takes a directory that holds no cluster, starts again over what its own
//! unfinished attempt left, and refuses anything else it finds.

use super::*;

const MARK: &str = "build.incomplete";

fn durable(tag: &str, num_shards: usize) -> (PathBuf, ClusterConfig) {
    let dir = scratch_dir(tag);
    let cfg = ClusterConfig {
        num_shards,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    (dir, cfg)
}

fn corpus() -> Vec<(u64, String)> {
    vec![
        (1, "wireless mouse".into()),
        (2, "mechanical keyboard".into()),
        (3, "usb hub".into()),
    ]
}

/// How many rows a cluster built from [`corpus`] in an empty directory stores (a short query
/// is stored on every shard, so this is more than the number of queries).
fn rows_of_a_clean_build(num_shards: usize) -> usize {
    let (dir, cfg) = durable(&format!("clean_build_{num_shards}"), num_shards);
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus()).expect("a clean build");
    let rows = cluster.num_queries().expect("count");
    drop(cluster);
    let _ = std::fs::remove_dir_all(dir);
    rows
}

fn matched(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut ids = cluster.percolate(title).expect("percolate");
    ids.sort_unstable();
    ids
}

/// Every file and directory under `dir`, with each file's length: what a refusal must leave
/// exactly as it was.
fn contents(dir: &std::path::Path) -> Vec<(String, u64)> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, u64)>) {
        for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
            let path = entry.path();
            let name = path
                .strip_prefix(root)
                .expect("under the root")
                .to_string_lossy()
                .into_owned();
            if path.is_dir() {
                out.push((name, 0));
                walk(root, &path, out);
            } else {
                out.push((name, entry.metadata().expect("metadata").len()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn refused(result: Result<ClusterEngine, ShardError>, what: &str) -> String {
    match result {
        Err(ShardError::Config(message)) => message,
        Err(other) => panic!("{what}: refused with the wrong kind of error: {other:?}"),
        Ok(_) => panic!("{what}: the build went ahead"),
    }
}

/// A build loads the corpus into its shard directories and then writes the manifest. One
/// that stops in between (here: the manifest cannot be written) leaves shard state and no
/// manifest, and the next start builds again. It used to build over that state: each shard
/// restored what the first attempt had left, the corpus was loaded a second time on top,
/// and the build failed after writing a manifest for the doubled state. Now the second
/// attempt removes what the first left and the cluster holds the corpus once.
#[test]
fn a_build_that_stopped_before_its_manifest_is_built_again_from_the_start() {
    let (dir, three_shards) = durable("interrupted_build", 3);
    std::fs::create_dir_all(&dir).expect("data dir");
    // The manifest is written to this name and renamed: a directory there stops the write.
    let blocker = dir.join("cluster_manifest.cmanifest.tmp");
    std::fs::create_dir(&blocker).expect("block the manifest");
    let first = ClusterEngine::build(vocab(), &three_shards, &corpus());
    assert!(first.is_err(), "precondition: the first build stopped");
    drop(first);
    std::fs::remove_dir(&blocker).expect("unblock");
    assert!(
        dir.join("shard_002").join("shard.ckpt").exists()
            && !dir.join(CLUSTER_MANIFEST_FILE).exists(),
        "precondition: shard state and no manifest"
    );
    assert!(
        dir.join(MARK).exists(),
        "the unfinished build left its mark"
    );

    // The next start has fewer shards, so the first attempt also left a directory this
    // build does not use.
    let (_, two_shards) = durable_in(&dir, 2);
    let cluster = ClusterEngine::build(vocab(), &two_shards, &corpus())
        .expect("the build starts again over what the unfinished one left");
    assert_eq!(
        cluster.num_queries().expect("count"),
        rows_of_a_clean_build(2),
        "the corpus, once: what a build in an empty directory stores"
    );
    assert_eq!(matched(&cluster, "a wireless mouse"), vec![1]);
    cluster
        .add_query(4, "laptop stand")
        .expect("a new query is admitted");
    assert!(!dir.join(MARK).exists(), "a finished build leaves no mark");
    assert!(
        !dir.join("shard_002").exists(),
        "a shard directory of the unfinished build was left behind"
    );
    drop(cluster);

    let reopened =
        ClusterEngine::open(dir.clone(), vocab(), Some(&two_shards)).expect("the cluster reopens");
    assert_eq!(matched(&reopened, "a wireless mouse"), vec![1]);
    assert_eq!(matched(&reopened, "laptop stand"), vec![4]);
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

fn durable_in(dir: &std::path::Path, num_shards: usize) -> (PathBuf, ClusterConfig) {
    let cfg = ClusterConfig {
        num_shards,
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    (dir.to_path_buf(), cfg)
}

/// Shard state with no manifest and no mark was not left by a build that says what it is
/// doing. It is a cluster that has lost its manifest, or what a release before the mark left
/// of a first start. A build over it would mix the surviving corpus with the new one, so it
/// is refused and nothing in the directory changes. With the cluster log still there it can
/// only be a cluster that was working.
#[test]
fn shard_state_with_no_manifest_and_no_mark_is_refused() {
    for log_is_there in [false, true] {
        let (dir, cfg) = durable(&format!("unmarked_leftovers_{log_is_there}"), 3);
        drop(ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster"));
        std::fs::remove_file(dir.join(CLUSTER_MANIFEST_FILE)).expect("lose the manifest");
        if !log_is_there {
            std::fs::remove_file(dir.join(CLUSTER_LOG_FILE)).expect("lose the log");
        }
        let before = contents(&dir);

        let message = refused(
            ClusterEngine::build(vocab(), &cfg, &corpus()),
            "shard state and no manifest",
        );
        assert!(
            message.contains("shard_000") && message.contains("no manifest"),
            "{message}"
        );
        assert_eq!(
            message.contains("was a working cluster"),
            log_is_there,
            "{message}"
        );
        assert_eq!(contents(&dir), before, "a refusal changed the directory");
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A build in a directory that holds a cluster is refused, and the cluster is as it was.
/// (It used to restore the cluster's shards, load the new corpus on top and replace the
/// manifest.)
#[test]
fn a_build_does_not_replace_a_cluster() {
    let (dir, cfg) = durable("build_over_cluster", 3);
    drop(ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster"));
    let before = contents(&dir);
    let message = refused(
        ClusterEngine::build(vocab(), &cfg, &[(9, "standing desk".into())]),
        "a build over a cluster",
    );
    assert!(message.contains("already holds a cluster"), "{message}");
    assert_eq!(contents(&dir), before, "a refusal changed the directory");
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    assert_eq!(
        reopened.num_queries().expect("count"),
        rows_of_a_clean_build(3)
    );
    assert_eq!(matched(&reopened, "standing desk"), Vec::<u64>::new());
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

/// A build that stops after its manifest and before its last step leaves a cluster, and
/// its mark. The start that follows opens the cluster, and the mark goes with that open:
/// left in place, it would let a later build throw this cluster's shards away if the
/// manifest were ever lost.
#[test]
fn an_open_clears_the_mark_of_a_build_that_stopped_after_its_manifest() {
    let (dir, cfg) = durable("mark_after_manifest", 3);
    drop(ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster"));
    std::fs::write(dir.join(MARK), b"left by a build that stopped late").expect("the mark");

    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    assert_eq!(matched(&reopened, "a wireless mouse"), vec![1]);
    assert!(!dir.join(MARK).exists(), "the open left the build's mark");
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

/// A mark says that the shard state beside it may be thrown away, and that holds only for
/// a build that never reached its manifest. A cluster log beside the mark means the build
/// did reach it (the log is created after the manifest), so the manifest has been lost:
/// refused, and nothing is removed.
#[test]
fn a_mark_beside_a_cluster_log_is_refused() {
    let (dir, cfg) = durable("mark_and_log", 3);
    drop(ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster"));
    std::fs::write(dir.join(MARK), b"left by a build that stopped late").expect("the mark");
    std::fs::remove_file(dir.join(CLUSTER_MANIFEST_FILE)).expect("lose the manifest");
    let before = contents(&dir);

    let message = refused(
        ClusterEngine::build(vocab(), &cfg, &corpus()),
        "a mark beside a cluster log",
    );
    assert!(message.contains("had written its manifest"), "{message}");
    assert_eq!(contents(&dir), before, "a refusal changed the directory");
    let _ = std::fs::remove_dir_all(dir);
}

/// The mark is on disk before the first shard directory exists: a build stopped at any
/// later point is recognised. (Here the build stops at once, on a shard directory it cannot
/// create.)
#[test]
fn the_mark_is_written_before_any_shard_state() {
    let (dir, cfg) = durable("mark_first", 2);
    std::fs::create_dir_all(&dir).expect("data dir");
    // A file where the first shard's directory would go is shard state the build did not
    // expect, so it is refused before the mark...
    std::fs::write(dir.join("shard_000"), b"not a directory").expect("a file in the way");
    refused(
        ClusterEngine::build(vocab(), &cfg, &corpus()),
        "an entry named like a shard",
    );
    assert!(!dir.join(MARK).exists(), "a refused build leaves no mark");
    std::fs::remove_file(dir.join("shard_000")).expect("clear");

    // ...and a build that is taken and then fails has left it.
    let blocker = dir.join("cluster_manifest.cmanifest.tmp");
    std::fs::create_dir(&blocker).expect("block the manifest");
    assert!(ClusterEngine::build(vocab(), &cfg, &corpus()).is_err());
    assert!(dir.join(MARK).exists());
    let _ = std::fs::remove_dir_all(dir);
}

/// A build creates its shards with the constructor that refuses a directory holding a
/// shard. The one a shard node uses restores what it finds, which is how an unfinished
/// build's rows came back.
#[test]
fn a_build_creates_its_shards_and_never_takes_one_up() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/cluster/coordinator/lifecycle/build.rs"),
    )
    .expect("build.rs");
    let restoring = ["LocalShard::new", "_durable("].concat();
    assert!(source.contains("LocalShard::create_durable("));
    assert!(
        !source.contains(&restoring),
        "a build creates a shard with the constructor that restores what a directory holds"
    );
}
