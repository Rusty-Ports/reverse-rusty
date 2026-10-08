//! ADR-216: the loss of the cluster log can be accepted, by the token its refusal names,
//! and it is on record before the log is replaced.

use super::*;
use crate::storage::{accepted_log_losses, ACCEPTED_LOG_LOSSES_FILE};

fn matched(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut ids = cluster.percolate(title).expect("percolate");
    ids.sort_unstable();
    ids
}

/// A durable cluster with query 1 in its base and query 2 acknowledged since (only in the
/// cluster log), and then the log gone.
fn a_cluster_that_lost_its_log(tag: &str) -> (PathBuf, ClusterConfig) {
    let dir = scratch_dir(tag);
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "wireless mouse".into())])
        .expect("durable cluster");
    cluster.add_query(2, "mechanical keyboard").expect("write");
    drop(cluster);
    std::fs::remove_file(dir.join(CLUSTER_LOG_FILE)).expect("lose the log");
    (dir, cfg)
}

fn accepting(cfg: &ClusterConfig, token: &str) -> ClusterConfig {
    ClusterConfig {
        accept_lost_log: Some(token.to_string()),
        ..cfg.clone()
    }
}

/// The refusal of the cluster in `dir`, and the token it names.
fn refusal(dir: &std::path::Path, cfg: &ClusterConfig) -> (String, String) {
    let reason = match ClusterEngine::open(dir.to_path_buf(), vocab(), Some(cfg)) {
        Err(ShardError::Log(reason)) => reason,
        Err(other) => panic!("refused with the wrong kind of error: {other:?}"),
        Ok(_) => panic!("a cluster without its log opened"),
    };
    let named = reason
        .split("accept_lost_log set to \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("the refusal names no token: {reason}"))
        .to_string();
    (reason, named)
}

/// Every file under `dir` with its length, without the record of accepted losses.
fn contents(dir: &std::path::Path) -> Vec<(String, u64)> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, u64)>) {
        for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let name = path.strip_prefix(root).expect("under the root");
                out.push((
                    name.to_string_lossy().into_owned(),
                    entry.metadata().expect("metadata").len(),
                ));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// A cluster whose log is gone refuses, and the refusal names a token for that loss. Opened
/// with it, the cluster records the loss, puts an empty log in place and opens with what
/// its last checkpoint held. It reports the loss, takes writes, checkpoints and reopens. A
/// second loss is refused under the first token.
#[test]
fn the_loss_of_the_cluster_log_is_accepted_by_the_token_its_refusal_names() {
    let (dir, cfg) = a_cluster_that_lost_its_log("accepted_cluster_loss");
    let before = contents(&dir);
    let (_, token) = refusal(&dir, &cfg);
    assert!(
        token.starts_with("cluster.log:epoch-1-") && token.ends_with(":1"),
        "{token}"
    );
    // A token for some other loss accepts nothing and changes nothing.
    let (reason, _) = refusal(&dir, &accepting(&cfg, "cluster.log:epoch-9-pos-0:1"));
    assert!(reason.contains("names a different loss"), "{reason}");
    assert_eq!(contents(&dir), before, "a refusal changed the directory");

    let cluster = ClusterEngine::open(dir.clone(), vocab(), Some(&accepting(&cfg, &token)))
        .expect("the loss is accepted");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    cluster.set_observer(Arc::new(move |event: &crate::events::EngineEvent| {
        if let crate::events::EngineEvent::DurabilityFailure { op, .. } = event {
            sink.lock().unwrap().push(*op);
        }
    }));
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [DurabilityOp::LogLost],
        "the start that accepts a loss reports it"
    );
    assert_eq!(matched(&cluster, "a wireless mouse"), vec![1]);
    assert_eq!(
        matched(&cluster, "a mechanical keyboard"),
        Vec::<u64>::new(),
        "the write that was only in the log is gone"
    );
    cluster.add_query(3, "usb hub").expect("a write");
    cluster.checkpoint().expect("a checkpoint");
    cluster.add_query(4, "laptop stand").expect("a write");
    drop(cluster);
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert!(recorded[0].applied && recorded[0].log == CLUSTER_LOG_FILE);
    // A backup carries the record.
    let copy = scratch_dir("accepted_cluster_loss_copy");
    crate::storage::copy_cluster_dir(&dir, &copy).expect("backup");
    assert_eq!(
        accepted_log_losses(&copy).expect("the copy's record"),
        recorded
    );
    let _ = std::fs::remove_dir_all(copy);

    // A later start needs no token, and the old one accepts nothing more.
    for cfg in [cfg.clone(), accepting(&cfg, &token)] {
        let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
        assert_eq!(matched(&reopened, "a usb hub"), vec![3]);
        assert_eq!(matched(&reopened, "a laptop stand"), vec![4]);
    }
    std::fs::remove_file(dir.join(CLUSTER_LOG_FILE)).expect("lose the log again");
    let (reason, second) = refusal(&dir, &accepting(&cfg, &token));
    assert!(reason.contains("names a different loss"), "{reason}");
    assert!(second.ends_with(":2"), "{second}");
    assert_eq!(accepted_log_losses(&dir).expect("the record").len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

/// The loss is on record before the log is replaced, and before any shard is attached. A
/// start that accepts it and cannot create the empty log has left a pending entry, no log,
/// and the shards as they were. Without the token the cluster is still refused; with the
/// same token it is finished, and recorded once.
#[test]
fn the_loss_of_the_cluster_log_is_recorded_before_the_log_is_replaced() {
    let (dir, cfg) = a_cluster_that_lost_its_log("cluster_loss_recorded_first");
    let (_, token) = refusal(&dir, &cfg);
    let before = contents(&dir);
    // The empty log is written to this name and renamed: a directory there stops it.
    let blocker = dir.join("cluster.log.tmp");
    std::fs::create_dir(&blocker).expect("block the new log");
    assert!(
        ClusterEngine::open(dir.clone(), vocab(), Some(&accepting(&cfg, &token))).is_err(),
        "precondition: the start stopped after accepting"
    );
    std::fs::remove_dir(&blocker).expect("unblock");

    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "the loss is on record: {recorded:?}");
    assert!(!recorded[0].applied, "and not carried out");
    let mut now = contents(&dir);
    now.retain(|(name, _)| name != ACCEPTED_LOG_LOSSES_FILE);
    assert_eq!(now, before, "the start changed more than the record");

    let (_, again) = refusal(&dir, &cfg);
    assert_eq!(again, token, "an unfinished acceptance changes the token");

    let cluster = ClusterEngine::open(dir.clone(), vocab(), Some(&accepting(&cfg, &token)))
        .expect("finished");
    assert_eq!(matched(&cluster, "a wireless mouse"), vec![1]);
    drop(cluster);
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "recorded twice: {recorded:?}");
    assert!(recorded[0].applied);
    let _ = std::fs::remove_dir_all(dir);
}
