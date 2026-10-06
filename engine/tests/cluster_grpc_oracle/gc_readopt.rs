//! A shard a node gave up is never re-adopted as an empty serving slot (ADR-189).
//!
//! After a relocation and an orphan-GC sweep the old owner no longer holds the shard. A
//! coordinator started from the outdated topology still names it, re-adopts it, and used to
//! get a brand-new empty slot whose reads succeeded with no matches. The slot must instead
//! refuse to serve until a peer recovery fills it, which is also how a shard legitimately
//! moves back to a node that once gave it up.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, NodeId, ReassignOutcome};

use crate::harness::*;
use crate::relocation::{seed_map, spin_n_servers};

fn converge_repairs(cluster: &ClusterEngine) {
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");
}

fn assert_zero_fn(cluster: &ClusterEngine, titles: &[String], oracle: &[HashSet<u64>], ctx: &str) {
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "{ctx}: cluster vs brute on {title:?}");
    }
}

fn move_position_zero(cluster: &ClusterEngine, to: NodeId, rt: &tokio::runtime::Runtime) {
    let outcome = cluster
        .reassign_and_move(0, to, rt.handle())
        .expect("reassign_and_move");
    assert!(
        matches!(outcome, ReassignOutcome::Moved { .. }),
        "{outcome:?}"
    );
    converge_repairs(cluster);
}

#[test]
fn grpc_gc_dropped_slot_moves_back_by_recovery_and_fails_loud_for_a_stale_topology() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // A=0 hosts co-located {0,1}; B=1 hosts {2}; C=2 hosts {3}; D=3 is the relocation target.
    let nodes = spin_n_servers(&rt, &norm, "gc_readopt", 4);
    let endpoints = vec![
        nodes[0].ep.clone(),
        nodes[0].ep.clone(),
        nodes[1].ep.clone(),
        nodes[2].ep.clone(),
    ];
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect co-located cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    seed_map(&cluster, &nodes, &[0, 0, 1, 2]);
    cluster.flush().expect("flush to segments");

    // Position 0 leaves A for D, and the sweep drops A's stranded slot.
    move_position_zero(&cluster, NodeId(4), &rt);
    let report = cluster.gc_orphan_slots(rt.handle()).expect("gc sweep");
    assert_eq!(report.dropped.len(), 1, "A's slot 0: {report:?}");
    assert_zero_fn(&cluster, &titles, &oracle, "after the first move");

    // The legitimate way back: adoption re-creates A's slot 0, a peer recovery fills it.
    move_position_zero(&cluster, NodeId(1), &rt);
    let counts = cluster.shard_query_counts().expect("counts");
    assert!(counts[0] > 0, "A serves position 0 again: {counts:?}");
    assert_zero_fn(&cluster, &titles, &oracle, "after moving back");
    let report = cluster.gc_orphan_slots(rt.handle()).expect("second sweep");
    assert_eq!(report.dropped.len(), 1, "D's slot 0: {report:?}");
    assert_eq!(report.dropped[0].node, NodeId(4));
    assert_zero_fn(&cluster, &titles, &oracle, "after the second sweep");

    // A coordinator restarted from an OUTDATED topology still places position 0 on D, which
    // gave it up. It may fail to connect, or connect and have its reads refused, but it must
    // never read a silently incomplete result from an empty stand-in slot.
    let stale_endpoints = vec![
        nodes[3].ep.clone(),
        nodes[0].ep.clone(),
        nodes[1].ep.clone(),
        nodes[2].ep.clone(),
    ];
    let stale = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &stale_endpoints,
        rt.handle(),
    );
    if let Ok(stale) = stale {
        let mut refused = 0usize;
        for (i, title) in titles.iter().enumerate() {
            match stale.percolate(title) {
                Ok(ids) => {
                    let got: HashSet<u64> = ids.into_iter().collect();
                    assert_eq!(
                        got, oracle[i],
                        "a stale topology read a silently incomplete result for {title:?}"
                    );
                }
                Err(_) => refused += 1,
            }
        }
        assert!(
            refused > 0,
            "reads that need the dropped slot must fail loud, not succeed"
        );
    }

    for node in &nodes {
        let _ = std::fs::remove_dir_all(&node.dir);
    }
}
