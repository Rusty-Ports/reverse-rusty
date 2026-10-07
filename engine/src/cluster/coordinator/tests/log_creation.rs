//! ADR-212: a cluster whose log creation was interrupted reopens; one whose log has lost its
//! content does not.

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

/// `build` writes the manifest and then creates the log. Releases before ADR-212 created the
/// log file and then wrote its header, so a crash or a full disk between the two left a
/// built cluster with a log shorter than its header, and every later start refused it
/// ("clog too small"). The cluster reopens: it serves what was built, takes a write, and
/// reopens again with that write.
#[test]
fn a_cluster_whose_log_creation_was_interrupted_reopens() {
    for held in [0usize, 3, 7] {
        let (dir, cfg) = durable(&format!("log_interrupted_{held}"));
        let built = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
            .expect("durable cluster");
        drop(built);
        let log = dir.join(CLUSTER_LOG_FILE);
        let header = std::fs::read(&log).expect("the log build created");
        assert_eq!(
            header.len(),
            8,
            "a built cluster's log holds only its header"
        );
        std::fs::write(&log, &header[..held]).expect("interrupt the creation");

        let reopened = match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
            Ok(cluster) => cluster,
            Err(error) => panic!("{held} header bytes: {error:?}"),
        };
        assert_eq!(matched(&reopened, "blue wireless mouse"), vec![1]);
        reopened
            .add_query(2, "mechanical keyboard")
            .expect("a write after the reopen");
        drop(reopened);

        let again = match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
            Ok(cluster) => cluster,
            Err(error) => panic!("{held} header bytes, second start: {error:?}"),
        };
        assert_eq!(matched(&again, "blue wireless mouse"), vec![1]);
        assert_eq!(matched(&again, "a mechanical keyboard"), vec![2]);
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// After a checkpoint the manifest records a log position, so the log has been whole, and it
/// is only ever replaced through a rename. A log shorter than its header there has lost its
/// content. The cluster is refused and the file is left as it was found.
#[test]
fn a_cluster_that_has_checkpointed_is_not_given_an_empty_log() {
    let (dir, cfg) = durable("log_lost_after_checkpoint");
    let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    cluster.add_query(2, "mechanical keyboard").expect("write");
    cluster.checkpoint().expect("checkpoint");
    drop(cluster);
    let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
        .expect("read manifest");
    assert!(
        manifest.snapshot_pos > 0,
        "the checkpoint recorded no position"
    );

    let log = dir.join(CLUSTER_LOG_FILE);
    let header = std::fs::read(&log).expect("read log");
    std::fs::write(&log, &header[..4]).expect("shorten the log");
    match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
        Err(ShardError::Log(reason)) => {
            assert!(
                reason.contains("clog too small"),
                "refused for another reason: {reason}"
            );
        }
        Err(other) => panic!("refused for another reason: {other:?}"),
        Ok(_) => panic!("a cluster that had checkpointed was given an empty log"),
    }
    assert_eq!(std::fs::read(&log).expect("read log"), &header[..4]);
    let _ = std::fs::remove_dir_all(&dir);
}
