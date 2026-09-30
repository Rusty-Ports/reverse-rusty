//! A successful recovery commits the target's own restart selector before acknowledgement.
//! Restart the actual target task without a later seal, including after replacing an old base.

use std::collections::HashSet;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use tokio::runtime::Runtime;

use crate::harness::*;

fn connect(rt: &Runtime, norm: &Arc<Normalizer>, dict: &Arc<Dict>, ep: &str) -> ClusterEngine {
    ClusterEngine::connect_remote(
        Arc::clone(norm),
        Arc::clone(dict),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &[ep.to_string()],
        rt.handle(),
    )
    .expect("connect coordinator")
}

fn assert_live(cluster: &ClusterEngine, queries: &[(u64, String)], titles: &[String]) {
    for (title, expected) in titles.iter().zip(build_oracle(queries, titles)) {
        let actual: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(actual, expected, "restarted recovered target on {title:?}");
    }
}

#[test]
fn grpc_recovered_checkpoint_survives_restart_and_replaces_old_floor() {
    let queries = vec![
        (11, "+nike +shoe".to_string()),
        (12, "+sony +tv".to_string()),
    ];
    let titles = vec!["nike shoe".to_string(), "sony tv".to_string()];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    for reused in [false, true] {
        let rt = Runtime::new().expect("runtime");
        let source = RestartableNode::start(&rt, &norm, "checkpoint_source");
        let target = RestartableNode::start(&rt, &norm, "checkpoint_target");
        let cluster = connect(&rt, &norm, &dict, &source.endpoint);
        cluster.ingest(&queries).expect("source bulk corpus");
        let verify = connect(&rt, &norm, &dict, &target.endpoint);
        if reused {
            // The previous sidecar has a nonzero watermark and a different segment list.
            // Recovery must replace both; that old watermark must not filter the new tail.
            for id in 100..110 {
                verify.add_query(id, "+sony +tv").expect("old target write");
            }
            verify.checkpoint().expect("old target checkpoint");
        }
        let (_, hwm) = cluster
            .peer_recover_replica(0, &source.endpoint, &target.endpoint, rt.handle())
            .expect("recover target");
        cluster.remove_query(12).expect("source tail remove");
        cluster.add_query(13, "+sony +tv").expect("source tail add");
        cluster
            .catch_up_recovered_replica(0, &source.endpoint, &target.endpoint, hwm, rt.handle())
            .expect("catch up target tail");
        let final_queries = vec![queries[0].clone(), (13, queries[1].1.clone())];
        assert_live(&verify, &final_queries, &titles);
        let counts = verify.shard_query_counts().expect("pre-restart counts");
        drop(verify);
        // No checkpoint/FetchSegments on the recovered target between recovery and restart.
        let target = target.restart(&rt, &norm);
        let reopened = connect(&rt, &norm, &dict, &target.endpoint);
        assert_live(&reopened, &final_queries, &titles);
        assert_eq!(
            reopened.shard_query_counts().expect("restarted counts"),
            counts
        );
        let _ = std::fs::remove_dir_all(&source.dir);
        let _ = std::fs::remove_dir_all(&target.dir);
    }
}

#[test]
fn grpc_recovery_checkpoint_failure_is_not_acknowledged() {
    let queries = vec![(11, "+nike +shoe".to_string())];
    let titles = vec!["nike shoe".to_string()];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = Runtime::new().expect("runtime");
    let source = RestartableNode::start(&rt, &norm, "checkpoint_fail_source");
    let target = RestartableNode::start(&rt, &norm, "checkpoint_fail_target");
    let cluster = connect(&rt, &norm, &dict, &source.endpoint);
    cluster.ingest(&queries).expect("source corpus");
    let verify = connect(&rt, &norm, &dict, &target.endpoint);
    let obstruction = target.dir.join("shard_000/shard.ckpt.tmp");
    std::fs::create_dir(&obstruction).expect("block checkpoint temp file");
    let error = cluster
        .peer_recover_replica(0, &source.endpoint, &target.endpoint, rt.handle())
        .expect_err("checkpoint failure must fail recovery");
    assert!(
        error
            .to_string()
            .contains("committing recovered checkpoint"),
        "{error}"
    );
    assert_eq!(
        verify.shard_query_counts().expect("unpublished target"),
        vec![0]
    );
    std::fs::remove_dir(&obstruction).expect("unblock checkpoint");
    cluster
        .peer_recover_replica(0, &source.endpoint, &target.endpoint, rt.handle())
        .expect("retry recovery");
    drop(verify);
    let target = target.restart(&rt, &norm);
    assert_live(
        &connect(&rt, &norm, &dict, &target.endpoint),
        &queries,
        &titles,
    );
    let _ = std::fs::remove_dir_all(&source.dir);
    let _ = std::fs::remove_dir_all(&target.dir);
}

#[test]
fn grpc_remote_checkpoint_trims_translog_and_replays_later_writes() {
    let queries = vec![(11, "+nike +shoe".to_string())];
    let titles = vec!["nike shoe".to_string()];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = Runtime::new().expect("runtime");
    let node = RestartableNode::start(&rt, &norm, "remote_seal");
    let cluster = connect(&rt, &norm, &dict, &node.endpoint);
    let mut client = rt.block_on(async {
        reverse_rusty_shard_proto::shard_service_client::ShardServiceClient::connect(
            node.endpoint.clone(),
        )
        .await
        .expect("lease client")
    });
    let lease_request = reverse_rusty_shard_proto::RetentionLeaseRequest {
        dict_fingerprint: dict.fingerprint(),
        tag_dict_fingerprint: empty_tag_dict().fingerprint(),
        placement_generation: 1,
        num_shards: 1,
        ..Default::default()
    };
    let lease = rt
        .block_on(client.retention_lease(lease_request.clone()))
        .expect("pin the source tail before its first write")
        .into_inner();
    cluster.add_query(11, &queries[0].1).expect("logged add");
    let log = node.dir.join("shard_000/translog.clog");
    let before = std::fs::metadata(&log).expect("log size").len();
    let obstruction = node.dir.join("shard_000/shard.ckpt.tmp");
    std::fs::create_dir(&obstruction).expect("block seal checkpoint");
    cluster
        .checkpoint()
        .expect_err("a failed remote seal must fail checkpoint");
    assert_eq!(
        std::fs::metadata(&log)
            .expect("untrimmed failed seal")
            .len(),
        before
    );
    std::fs::remove_dir(&obstruction).expect("unblock seal checkpoint");
    cluster
        .checkpoint()
        .expect("seal with active retention lease");
    assert_eq!(
        std::fs::metadata(&log).expect("lease-retained tail").len(),
        before
    );
    rt.block_on(
        client.retention_lease(reverse_rusty_shard_proto::RetentionLeaseRequest {
            op: 2,
            lease_id: lease.lease_id,
            ..lease_request
        }),
    )
    .expect("release retention lease");
    cluster.checkpoint().expect("seal remote primary");
    assert!(std::fs::metadata(&log).expect("trimmed log").len() < before);
    let metrics = cluster.transport_metrics();
    assert!(metrics
        .methods
        .iter()
        .any(|m| m.method == "seal" && m.calls == 3 && m.errors == 1));
    cluster
        .add_query(12, &queries[0].1)
        .expect("post-seal logged add");
    drop(cluster);
    let node = node.restart(&rt, &norm);
    let final_queries = vec![queries[0].clone(), (12, queries[0].1.clone())];
    assert_live(
        &connect(&rt, &norm, &dict, &node.endpoint),
        &final_queries,
        &titles,
    );
    let _ = std::fs::remove_dir_all(&node.dir);
}
