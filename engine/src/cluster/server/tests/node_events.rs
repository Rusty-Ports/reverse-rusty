use super::*;
use crate::events::{DurabilityOp, EngineEvent};
use std::sync::Mutex;

// ---- what a shard node's shards report about durability reaches the node (ADR-213) ----

fn node_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_node_events_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The node's count of one kind of durability failure, as its `/_metrics` shows it.
fn failures(srv: &ShardServer, op: &str) -> Option<u64> {
    let series = format!("reverse_rusty_shard_durability_failures_total{{op=\"{op}\"}} ");
    srv.metrics_source()
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&series)?.trim().parse().ok())
}

fn collect(srv: &ShardServer) -> Arc<Mutex<Vec<(DurabilityOp, String)>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    srv.set_event_sink(move |event: &EngineEvent| {
        if let EngineEvent::DurabilityFailure { op, error, .. } = event {
            sink.lock().unwrap().push((*op, error.clone()));
        }
    });
    seen
}

/// A shard node started with `accept_lost_log` over a slot whose translog is gone reports
/// the loss: to the sink its binary installs, although the shard was opened before any sink
/// existed, and in its metrics. Before, a shard node gave none of its shards a sink, so this
/// (and a repaired torn tail) was said to nobody.
#[test]
fn a_shard_nodes_accepted_loss_reaches_its_sink_and_its_metrics() {
    let n = norm();
    let d = Arc::new(frozen_dict(&["nodeeventneedle"], &n));
    let dir = node_dir("accepted_loss");
    drop(
        ShardServer::new_durable(
            Arc::clone(&n),
            Arc::clone(&d),
            EngineConfig::default(),
            dir.clone(),
        )
        .expect("a durable node"),
    );
    let translog = super::super::shard_dir(&dir, 0).join(crate::cluster::translog::TRANSLOG_FILE);
    std::fs::remove_file(&translog).expect("lose the translog");

    assert!(
        ShardServer::new_durable(
            Arc::clone(&n),
            Arc::clone(&d),
            EngineConfig::default(),
            dir.clone()
        )
        .is_err(),
        "a node whose slot lost its translog started without being told to"
    );

    let accepting = EngineConfig {
        accept_lost_log: true,
        ..EngineConfig::default()
    };
    let srv = ShardServer::new_durable(Arc::clone(&n), Arc::clone(&d), accepting, dir.clone())
        .expect("the loss was accepted");
    // Counted as soon as the shard is wired, with no sink installed yet.
    assert_eq!(failures(&srv, "log_lost"), Some(1));
    let seen = collect(&srv);
    let reported: Vec<DurabilityOp> = seen.lock().unwrap().iter().map(|(op, _)| *op).collect();
    assert_eq!(
        reported,
        vec![DurabilityOp::LogLost],
        "what the shard queued while it was opened is handed to the sink"
    );
    // Handed over once.
    let again = collect(&srv);
    assert!(again.lock().unwrap().is_empty());
    assert_eq!(failures(&srv, "log_lost"), Some(1));
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every durability operation is listed at zero before it ever happens, on a serving node and
/// on a pending one. A counter series that first appears at 1 shows no increase, so an alert
/// on the increase would miss the first failure of its kind.
#[test]
fn every_durability_operation_is_listed_before_anything_fails() {
    let n = norm();
    let d = Arc::new(frozen_dict(&["nodeeventneedle"], &n));
    let serving = ShardServer::new(Arc::clone(&n), d, EngineConfig::default());
    let pending = ShardServer::pending(n, EngineConfig::default());
    for op in DurabilityOp::ALL {
        assert_eq!(failures(&serving, op.as_str()), Some(0), "{}", op.as_str());
        assert_eq!(failures(&pending, op.as_str()), Some(0), "{}", op.as_str());
    }
}

/// A shard's state is built in one place, which wires the shard to the node's event channel.
/// A state built anywhere else would hold a shard whose reports go nowhere.
#[test]
fn every_shard_state_is_built_through_the_constructor_that_wires_it() {
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
        "a shard state is built outside `ServerState::new`, which wires its shard to the \
         node's event channel: {built:?}"
    );
    assert!(built[0].contains("server.rs:"), "{built:?}");
}
