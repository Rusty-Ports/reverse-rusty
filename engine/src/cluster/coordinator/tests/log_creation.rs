//! ADR-212 and ADR-213: a cluster whose log creation was interrupted reopens; one whose log
//! has lost its content, or is gone, does not.

use super::*;

fn durable(tag: &str) -> (PathBuf, ClusterConfig) {
    let dir = scratch_dir(tag);
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    (dir, cfg)
}

fn matched(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut ids = cluster.percolate(title).expect("percolate");
    ids.sort_unstable();
    ids
}

fn epoch_on_disk(dir: &std::path::Path) -> u64 {
    crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
        .expect("read manifest")
        .epoch
}

/// Make the manifest the one `build` writes before it creates the log: epoch 0. That is how
/// a build that stopped part-way leaves it, and how a release before ADR-213 left every
/// cluster until its first checkpoint.
fn as_an_older_release_built_it(dir: &std::path::Path) {
    let path = dir.join(CLUSTER_MANIFEST_FILE);
    let mut manifest = crate::storage::read_cluster_manifest(&path).expect("read manifest");
    assert_eq!(
        manifest.epoch,
        crate::storage::ClusterManifest::FIRST_EPOCH_WITH_A_LOG,
        "a manifest `build` just wrote"
    );
    assert_eq!(manifest.snapshot_pos, 0);
    manifest.epoch = 0;
    crate::storage::write_cluster_manifest(&manifest, &path).expect("write manifest");
}

fn open(dir: &std::path::Path, cfg: &ClusterConfig, what: &str) -> ClusterEngine {
    match ClusterEngine::open(dir.to_path_buf(), vocab(), Some(cfg)) {
        Ok(cluster) => cluster,
        Err(error) => panic!("{what}: {error:?}"),
    }
}

/// `build` writes its manifest at epoch 0 and then creates the log, and releases before
/// ADR-212 created the log file and then wrote its header. A crash or a full disk in between
/// left a built cluster with a log shorter than its header, and every later start refused it
/// ("clog too small"). Under that manifest the cluster reopens: it serves what was built,
/// takes a write, and reopens again with that write.
#[test]
fn a_cluster_whose_log_creation_was_interrupted_reopens() {
    for held in [0usize, 3, 7] {
        let (dir, cfg) = durable(&format!("log_interrupted_{held}"));
        let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
            .expect("durable cluster");
        drop(built);
        as_an_older_release_built_it(&dir);
        let log = dir.join(CLUSTER_LOG_FILE);
        let header = std::fs::read(&log).expect("the log build created");
        assert_eq!(
            header.len(),
            8,
            "a built cluster's log holds only its header"
        );
        std::fs::write(&log, &header[..held]).expect("interrupt the creation");

        let reopened = open(&dir, &cfg, &format!("{held} header bytes"));
        assert_eq!(
            epoch_on_disk(&dir),
            1,
            "the reopen finished the build: the manifest now says the log exists"
        );
        assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
        reopened
            .add_query(2, "mechanical keyboard")
            .expect("a write after the reopen");
        drop(reopened);

        let again = open(&dir, &cfg, &format!("{held} header bytes, second start"));
        assert_eq!(matched(&again, "blue wireless mouse"), vec![1]);
        assert_eq!(matched(&again, "a mechanical keyboard"), vec![2]);
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A manifest at epoch 1 or later was written by a checkpoint, which needs the log open and
/// replaces it through a rename; `build` ends with one. Under it a log shorter than its
/// header has lost its content. The
/// cluster is refused and the file is left as it was found. That holds for a checkpoint that
/// ran before the first write too: it records position zero, and the write that follows it
/// is acknowledged into the log.
#[test]
fn a_cluster_whose_manifest_says_the_log_was_whole_is_not_given_an_empty_log() {
    for order in ["built", "write, checkpoint", "checkpoint, write"] {
        let (dir, cfg) = durable(&format!("log_cut_{}", order.len()));
        let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
            .expect("durable cluster");
        match order {
            "write, checkpoint" => {
                cluster.add_query(2, "mechanical keyboard").expect("write");
                cluster.checkpoint().expect("checkpoint");
            }
            "checkpoint, write" => {
                cluster.checkpoint().expect("checkpoint");
                cluster.add_query(2, "mechanical keyboard").expect("write");
            }
            _ => {}
        }
        drop(cluster);
        let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
            .expect("read manifest");
        assert_eq!(
            manifest.epoch,
            if order == "built" { 1 } else { 2 },
            "{order}: `build` ends at epoch 1 and a checkpoint bumps it"
        );
        assert_eq!(
            manifest.snapshot_pos != 0,
            order == "write, checkpoint",
            "{order}: only a checkpoint after a write records a position"
        );

        let log = dir.join(CLUSTER_LOG_FILE);
        let header = std::fs::read(&log).expect("read log");
        std::fs::write(&log, &header[..4]).expect("shorten the log");
        match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
            Err(ShardError::Log(reason)) => {
                assert!(
                    reason.contains("clog too small"),
                    "{order}: refused for another reason: {reason}"
                );
            }
            Err(other) => panic!("{order}: refused for another reason: {other:?}"),
            Ok(_) => panic!("{order}: the cluster was given an empty log"),
        }
        assert_eq!(std::fs::read(&log).expect("read log"), &header[..4]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A log that is gone is refused the same way, and nothing is created in its place: the
/// writes acknowledged since the manifest were in it. Before ADR-213 the reopen made an
/// empty log and served without them. The refusal changes nothing: with the log back in
/// place the same directory opens with every write.
#[test]
fn a_cluster_whose_log_is_gone_is_refused() {
    for checkpointed in [false, true] {
        let (dir, cfg) = durable(&format!("log_gone_{checkpointed}"));
        let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
            .expect("durable cluster");
        if checkpointed {
            cluster.checkpoint().expect("checkpoint");
        }
        cluster.add_query(2, "mechanical keyboard").expect("write");
        drop(cluster);
        let log = dir.join(CLUSTER_LOG_FILE);
        let held = std::fs::read(&log).expect("the log");
        std::fs::remove_file(&log).expect("lose the log");

        for attempt in 1..=2 {
            match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
                Err(ShardError::Log(reason)) => {
                    assert!(
                        reason.contains("is missing") && reason.contains("Restore"),
                        "refused for another reason: {reason}"
                    );
                }
                Err(other) => panic!("refused for another reason: {other:?}"),
                Ok(cluster) => panic!(
                    "attempt {attempt}: opened without its log; the acknowledged write \
                     matches: {:?}",
                    matched(&cluster, "a mechanical keyboard")
                ),
            }
            assert!(!log.exists(), "a refused open created a log");
        }

        std::fs::write(&log, &held).expect("put the log back");
        let reopened = open(&dir, &cfg, "with its log");
        assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
        assert_eq!(
            matched(&reopened, "a mechanical keyboard"),
            vec![2],
            "the refusals changed something: the write in the log did not come back"
        );
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Under a manifest at epoch 0 a missing log may be a build that stopped before it created
/// one. It is created, as before, and the reopen then finishes the build: it writes the
/// manifest that says the log exists. From then on a log that goes missing is refused. So a
/// cluster from an older release is lenient about its log for one start, not for ever.
#[test]
fn a_reopen_finishes_a_build_that_stopped_before_its_log() {
    let (dir, cfg) = durable("log_not_yet");
    let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    drop(built);
    as_an_older_release_built_it(&dir);
    let log = dir.join(CLUSTER_LOG_FILE);
    std::fs::remove_file(&log).expect("the log was never created");
    let reopened = open(&dir, &cfg, "an unfinished build");
    assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
    assert_eq!(reopened.epoch(), 1);
    assert_eq!(
        epoch_on_disk(&dir),
        1,
        "the manifest now says the log exists"
    );
    drop(reopened);

    std::fs::remove_file(&log).expect("lose the log");
    assert!(
        ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).is_err(),
        "after the reopen finished the build, a lost log was still recreated"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// `build` ends with a checkpoint, so a built cluster is at epoch 1: its manifest says its log
/// exists.
#[test]
fn a_built_cluster_has_a_manifest_that_says_its_log_exists() {
    let (dir, cfg) = durable("built_epoch");
    let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    assert_eq!(built.epoch(), 1);
    assert_eq!(epoch_on_disk(&dir), 1);
    assert_eq!(std::fs::read(dir.join(CLUSTER_LOG_FILE)).unwrap().len(), 8);
    drop(built);
    // That checkpoint is made once. A reopen of a cluster that is past epoch 0 makes none.
    for start in 1..=2 {
        let reopened = open(&dir, &cfg, "reopen");
        assert_eq!(reopened.epoch(), 1, "start {start} made a checkpoint");
        assert_eq!(epoch_on_disk(&dir), 1, "start {start} made a checkpoint");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A build that cannot create its log fails, and the next start recovers: it finds the
/// manifest the build wrote first (epoch 0), creates the log, finishes the build, and serves
/// each built query once.
///
/// The manifest is written before the log for this reason. With the log first, the failed
/// build left shard state and no manifest; the next start built again over that state, each
/// shard restored the rows its own checkpoint file listed, and the corpus was ingested a
/// second time on top of them.
#[test]
fn a_start_after_a_build_that_could_not_create_its_log_recovers() {
    let (dir, cfg) = durable("build_without_log");
    let corpus = [
        (1u64, "wireless mouse".to_string()),
        (2, "mechanical keyboard".to_string()),
    ];
    let log = dir.join(CLUSTER_LOG_FILE);
    let blocker = crate::storage::framed_log::replacement_path(&log);
    std::fs::create_dir_all(&blocker).expect("block the log's creation");
    assert!(
        ClusterEngine::build(vocab(), &cfg, &corpus).is_err(),
        "the cluster was built without a log"
    );
    assert!(!log.exists());
    assert_eq!(
        epoch_on_disk(&dir),
        0,
        "the manifest of a build that did not finish does not say a log exists"
    );
    std::fs::remove_dir_all(&blocker).expect("unblock");

    // What the server does at its next start: a manifest is there, so it opens.
    assert!(ClusterEngine::cluster_exists(&dir));
    let reopened = open(&dir, &cfg, "the start after the failed build");
    assert_eq!(epoch_on_disk(&dir), 1);
    // The same rows as a build that was never interrupted: nothing was ingested twice.
    // (The count is of stored rows, and a row that every shard holds counts on each.)
    let (reference_dir, reference_cfg) = durable("build_without_log_reference");
    let reference =
        ClusterEngine::build(vocab(), &reference_cfg, &corpus).expect("an undisturbed build");
    assert_eq!(
        reopened.num_queries().expect("count"),
        reference.num_queries().expect("count"),
        "the recovered cluster holds other rows than an undisturbed build"
    );
    drop(reference);
    assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
    assert_eq!(matched(&reopened, "a mechanical keyboard"), vec![2]);
    reopened
        .add_query(3, "usb hub")
        .expect("the id directory is complete: an insert is admitted");
    drop(reopened);
    let again = open(&dir, &cfg, "second start");
    assert_eq!(matched(&again, "a usb hub"), vec![3]);
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&reference_dir);
}

fn shard_translogs(dir: &std::path::Path, shards: usize) -> Vec<Vec<u8>> {
    (0..shards)
        .map(|shard| {
            std::fs::read(shard_dir(dir, shard).join(crate::cluster::translog::TRANSLOG_FILE))
                .expect("a shard translog")
        })
        .collect()
}

/// An open that refuses has touched nothing. Attaching a shard resets its translog, and when
/// the cluster log is gone or cut short those translogs are the only place the writes since
/// the last checkpoint still exist. The log is therefore checked before any shard is
/// attached. (Checked after, the refused open left every translog an empty header.)
#[test]
fn a_refused_open_leaves_the_shard_translogs_as_they_were() {
    for damage in ["gone", "cut short"] {
        let (dir, cfg) = durable(&format!("refused_touches_nothing_{}", damage.len()));
        let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
            .expect("durable cluster");
        // Selective rows, added after the build, so that they land in shard translogs.
        for (id, dsl) in [
            (2u64, "mechanical keyboard"),
            (3, "usb hub"),
            (4, "laptop stand"),
        ] {
            cluster.add_query(id, dsl).expect("write");
        }
        drop(cluster);
        let before = shard_translogs(&dir, cfg.num_shards);
        assert!(
            before.iter().any(|translog| translog.len() > 8),
            "precondition: some shard translog holds a write"
        );

        let log = dir.join(CLUSTER_LOG_FILE);
        if damage == "gone" {
            std::fs::remove_file(&log).expect("lose the log");
        } else {
            let header = std::fs::read(&log).expect("read log");
            std::fs::write(&log, &header[..4]).expect("shorten the log");
        }
        assert!(
            ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).is_err(),
            "{damage}: the cluster opened"
        );
        assert_eq!(
            shard_translogs(&dir, cfg.num_shards),
            before,
            "{damage}: a refused open reset a shard translog"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
