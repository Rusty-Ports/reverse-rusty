//! ADR-212 and ADR-213: a cluster whose log creation was interrupted reopens; one whose log
//! has lost its content, or is gone, does not.

use super::*;
use crate::events::EngineEvent;
use std::sync::{Arc, Mutex};

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

/// Make the manifest the one a release before ADR-213 wrote at build: epoch 0, written
/// before the log was created.
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

/// What the cluster reported about a lost log when it started.
fn lost_log_events(cluster: &ClusterEngine) -> Vec<String> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    cluster.set_observer(Arc::new(move |event: &EngineEvent| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::LogLost,
            error,
            ..
        } = event
        {
            seen.lock().unwrap().push(error.clone());
        }
    }));
    let seen = events.lock().unwrap().clone();
    seen
}

fn open(dir: &std::path::Path, cfg: &ClusterConfig, what: &str) -> ClusterEngine {
    match ClusterEngine::open(dir.to_path_buf(), vocab(), Some(cfg)) {
        Ok(cluster) => cluster,
        Err(error) => panic!("{what}: {error:?}"),
    }
}

/// Releases before ADR-213 wrote the manifest (at epoch 0) and then created the log, and
/// releases before ADR-212 created the log file and then wrote its header. A crash or a full
/// disk in between left a built cluster with a log shorter than its header, and every later
/// start refused it ("clog too small"). Under that manifest the cluster reopens: it serves
/// what was built, takes a write, and reopens again with that write.
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

/// A manifest at epoch 1 or later was written with the log in place: by `build`, which
/// creates the log first, or by a checkpoint, which needs the log open and replaces it
/// through a rename. Under it a log shorter than its header has lost its content. The
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
            "{order}: `build` writes epoch 1 and a checkpoint bumps it"
        );
        assert_eq!(
            manifest.snapshot_pos != 0,
            order == "write, checkpoint",
            "{order}: only a checkpoint after a write records a position"
        );

        let log = dir.join(CLUSTER_LOG_FILE);
        let header = std::fs::read(&log).expect("read log");
        std::fs::write(&log, &header[..4]).expect("shorten the log");
        // Accepting a lost log is about a log that is gone. It does not make a damaged
        // one acceptable.
        let accepting = ClusterConfig {
            accept_lost_log: true,
            ..cfg.clone()
        };
        for config in [&cfg, &accepting] {
            match ClusterEngine::open(dir.clone(), vocab(), Some(config)) {
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
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A log that is gone is refused the same way, and nothing is created in its place: the
/// writes acknowledged since the manifest were in it. Before ADR-213 the reopen made an
/// empty log and served without them.
///
/// With `accept_lost_log` the cluster starts from its last checkpoint, says what it lost, and
/// from then on reopens as usual.
#[test]
fn a_cluster_whose_log_is_gone_is_refused_unless_the_loss_is_accepted() {
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
        std::fs::remove_file(&log).expect("lose the log");

        match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
            Err(ShardError::Log(reason)) => {
                assert!(
                    reason.contains("is missing") && reason.contains("accept_lost_log"),
                    "refused for another reason: {reason}"
                );
            }
            Err(other) => panic!("refused for another reason: {other:?}"),
            Ok(cluster) => panic!(
                "opened without its log; the acknowledged write matches: {:?}",
                matched(&cluster, "a mechanical keyboard")
            ),
        }
        assert!(!log.exists(), "a refused open created a log");

        let accepting = ClusterConfig {
            accept_lost_log: true,
            ..cfg.clone()
        };
        let reopened = open(&dir, &accepting, "the loss was accepted");
        let lost = lost_log_events(&reopened);
        assert_eq!(lost.len(), 1, "the loss is reported once: {lost:?}");
        assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
        assert!(
            matched(&reopened, "a mechanical keyboard").is_empty(),
            "the write that was only in the log"
        );
        reopened
            .add_query(3, "usb hub")
            .expect("a write after the reopen");
        drop(reopened);

        // The log exists again: no flag is needed, and nothing is reported.
        let again = open(&dir, &cfg, "second start");
        assert!(lost_log_events(&again).is_empty());
        assert_eq!(matched(&again, "a usb hub"), vec![3]);
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The flag accepts a loss; it does not make one. With the log present it changes nothing.
#[test]
fn accepting_a_lost_log_changes_nothing_when_the_log_is_there() {
    let (dir, cfg) = durable("log_there_and_accepted");
    let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    cluster.add_query(2, "mechanical keyboard").expect("write");
    drop(cluster);
    let accepting = ClusterConfig {
        accept_lost_log: true,
        ..cfg.clone()
    };
    let reopened = open(&dir, &accepting, "the log is there");
    assert!(lost_log_events(&reopened).is_empty());
    assert_eq!(matched(&reopened, "a mechanical keyboard"), vec![2]);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Under the manifest an older release wrote before it created the log (epoch 0), a missing
/// log may be a build that was interrupted. It is created, as before.
#[test]
fn a_cluster_an_older_release_built_may_have_no_log_yet() {
    let (dir, cfg) = durable("log_not_yet");
    let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    drop(built);
    as_an_older_release_built_it(&dir);
    std::fs::remove_file(dir.join(CLUSTER_LOG_FILE)).expect("the log was never created");
    let reopened = open(&dir, &cfg, "an older release's build");
    assert!(lost_log_events(&reopened).is_empty());
    assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `build` creates the log before it writes the manifest, so nothing says the cluster exists
/// before its log does. A build that cannot create the log leaves no manifest, and the
/// directory is not a cluster.
#[test]
fn a_build_that_cannot_create_its_log_leaves_no_manifest() {
    let (dir, cfg) = durable("build_without_log");
    let blocker = crate::storage::framed_log::replacement_path(&dir.join(CLUSTER_LOG_FILE));
    std::fs::create_dir_all(&blocker).expect("block the log's creation");
    let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())]);
    assert!(built.is_err(), "the cluster was built without a log");
    assert!(
        !dir.join(CLUSTER_MANIFEST_FILE).exists(),
        "a manifest was written before the log existed"
    );
    assert!(
        !ClusterEngine::cluster_exists(&dir),
        "the directory passes for a cluster"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
