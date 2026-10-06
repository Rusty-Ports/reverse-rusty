//! ADR-185 (RR-004): a concurrent unfenced reader never sees a cluster upsert half-done.
//!
//! The reader loops the ordinary read paths (no PIT, no consistent view) for a title that
//! satisfies every version of the query while a writer cycles the query through re-puts
//! that keep its placement, edits that move it to another shard, and edits that move it
//! between the selective and broad lanes. Every read must return the id exactly once and
//! never an ownership error.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::rank::{RankProgramSpec, RankSpec};
use reverse_rusty::result::TopKOptions;
use std::sync::atomic::{AtomicBool, Ordering};

const ID: u64 = 999;

/// Reads for `title` until stopped; returns the anomalies and the number of passes.
fn read_until_stopped(
    cluster: &ClusterEngine,
    title: &str,
    stop: &AtomicBool,
) -> (Vec<String>, u64) {
    let program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("rank program");
    let mut anomalies = Vec::new();
    let mut passes = 0u64;
    while !stop.load(Ordering::Relaxed) {
        passes += 1;
        match cluster.percolate_with_broad(title, true) {
            Ok(ids) if ids == vec![ID] => {}
            other => anomalies.push(format!("percolate: {other:?}")),
        }
        match cluster.percolate_filtered_ranked(title, &[], true, &RankSpec::default()) {
            Ok((scored, _)) if scored.len() == 1 && scored[0].0 == ID => {}
            other => anomalies.push(format!("ranked: {:?}", other.map(|(s, _)| s))),
        }
        let options = TopKOptions {
            query_scope: reverse_rusty::result::QueryScope::WithBroad,
            ..TopKOptions::default()
        };
        match cluster.try_percolate_filtered_top_k_batch(&[title], &[], options, &program, None) {
            Ok(found)
                if found.titles[0].hits.len() == 1 && found.titles[0].hits[0].logical_id == ID => {}
            Ok(found) => anomalies.push(format!(
                "top-k batch hits: {:?}",
                found.titles[0]
                    .hits
                    .iter()
                    .map(|hit| hit.logical_id)
                    .collect::<Vec<_>>()
            )),
            Err(error) => anomalies.push(format!("top-k batch: {error}")),
        }
        match cluster.document_exists(ID) {
            Ok(true) => {}
            other => anomalies.push(format!("point read: {other:?}")),
        }
    }
    (anomalies, passes)
}

/// Cycle `bodies` through upserts while a reader watches `title`.
fn churn(
    cluster: &ClusterEngine,
    title: &str,
    bodies: &[(&str, &[(String, String)])],
    rounds: u32,
) {
    let stop = AtomicBool::new(false);
    let (anomalies, passes) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| read_until_stopped(cluster, title, &stop));
        let mut failure = None;
        for round in 0..rounds {
            let (body, tags) = bodies[(round as usize) % bodies.len()];
            if let Err(error) = cluster.upsert_query_with_tags(ID, body, round + 2, tags) {
                failure = Some(format!("round {round}: {error}"));
                break;
            }
        }
        // Stop the reader before any assertion, so the scope can join it.
        stop.store(true, Ordering::Relaxed);
        let observed = reader.join().expect("reader thread");
        assert!(failure.is_none(), "{failure:?}");
        observed
    });
    assert!(passes > 0, "the reader overlapped the churn");
    assert!(
        anomalies.is_empty(),
        "{} anomalies in {passes} read passes; first: {:?}",
        anomalies.len(),
        &anomalies[..anomalies.len().min(5)]
    );
}

fn placed_on(cluster: &ClusterEngine, probe_id: u64, dsl: &str) -> AddOutcome {
    let outcome = cluster.add_query(probe_id, dsl).expect("probe add");
    cluster.remove_query(probe_id).expect("remove probe");
    outcome
}

/// Re-puts, a tag-only edit, and moves between shards.
#[test]
fn unfenced_reads_stay_exact_across_selective_upsert_churn() {
    for num_shards in [1usize, 4] {
        let cfg = ClusterConfig {
            num_shards,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
        let tokens: Vec<String> = (0..64).map(|i| format!("zzchurn{i}")).collect();
        let home = placed_on(&cluster, 800_000, &tokens[0]);
        let away = tokens
            .iter()
            .enumerate()
            .skip(1)
            .find(|(i, token)| {
                num_shards == 1 || placed_on(&cluster, 800_000 + *i as u64, token) != home
            })
            .map(|(_, token)| token.as_str())
            .expect("a second body");
        let old = tokens[0].as_str();
        let title = format!("{old} {away}");
        let tagged = [("tier".to_string(), "gold".to_string())];
        cluster.upsert_query(ID, old, 1).expect("seed");
        churn(
            &cluster,
            &title,
            &[
                (old, &[]),      // same body: placement unchanged
                (old, &tagged),  // tag-only edit: placement unchanged
                (away, &[]),     // moves to another shard (K > 1)
                (away, &tagged), // tag-only edit on the new shard
            ],
            600,
        );
    }
}

/// An edit that moves the query between the selective lane and the replicated broad lane
/// changes which shard owns it for a title (lowest common position vs. the per-title broad
/// evaluator), the case no fixed per-shard order can keep reader-atomic.
#[test]
fn unfenced_reads_stay_exact_across_lane_changing_upserts() {
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Make `zzhot` common enough to carry a top-64 mask bit, so a query requiring only it
    // is broad-lane while one that also requires a rare term is selective.
    let corpus: Vec<(u64, String)> = (0..200u64)
        .map(|i| (i, format!("zzhot zzfill{i}")))
        .collect();
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus).expect("cluster");
    let (selective, broad) = ("zzhot zzrare", "zzhot");
    assert!(
        matches!(
            placed_on(&cluster, 800_000, selective),
            AddOutcome::Placed { .. }
        ),
        "precondition: the rare-anchored body is selective"
    );
    assert!(
        matches!(
            placed_on(&cluster, 800_001, broad),
            AddOutcome::Replicated { .. }
        ),
        "precondition: the hot-only body is replicated to the broad lane"
    );
    cluster.upsert_query(ID, selective, 1).expect("seed");
    // The title also matches the 200 filler-free... it matches only ID: the fillers each
    // require their own `zzfill` token.
    churn(
        &cluster,
        "zzhot zzrare",
        &[
            (broad, &[]),
            (selective, &[]),
            (selective, &[]),
            (broad, &[]),
        ],
        400,
    );
}
