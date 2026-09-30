//! ADR-179 governed resize loop: a persistent split recommendation becomes exactly one bounded,
//! recorded resize, after which the cooldown holds further growth.

use reverse_rusty::cluster::ResizeGovernorConfig;

use crate::cluster_mode::resize_loop::{spawn_resize_loop, AutoscaleResizeConfig};
use crate::resize_ops::{ResizeOrigin, ResizeState};

use super::*;

/// Distinct selective anchors so the corpus spreads across positions.
fn corpus(n: u64) -> Vec<(u64, String)> {
    (1..=n)
        .map(|i| (i, format!("zzanchor{i} vintage")))
        .collect()
}

fn titles(n: u64) -> Vec<String> {
    (1..=n)
        .map(|i| format!("zzanchor{i} vintage lamp"))
        .collect()
}

fn matches(state: &Arc<ClusterAppState>, titles: &[String]) -> Vec<Vec<u64>> {
    let cluster = state.cluster.read();
    titles
        .iter()
        .map(|t| {
            let mut ids = cluster.percolate(t).expect("percolate");
            ids.sort_unstable();
            ids
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_recommendation_grows_once_then_cools_down() {
    let queries = corpus(60);
    let probe = titles(60);
    let state = test_state(&queries);
    let before = matches(&state, &probe);
    let start_shards = state.cluster.read().num_shards();

    let task = spawn_resize_loop(
        Arc::clone(&state),
        AutoscaleResizeConfig {
            interval: Duration::from_millis(10),
            split_corpus_threshold: 5,
            governor: ResizeGovernorConfig {
                required_observations: 2,
                cooldown: Duration::from_hours(1),
                max_step: 2,
                max_shards: 16,
                min_relief_percent: 0,
            },
        },
    );

    let mut succeeded = None;
    for _ in 0..500 {
        succeeded =
            state.resize_operations.list().into_iter().find(|r| {
                r.origin == ResizeOrigin::Autoscaler && r.state == ResizeState::Succeeded
            });
        if succeeded.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let record = succeeded.expect("the loop must execute one governed resize");
    assert_eq!(record.num_shards, start_shards + 2, "one bounded step");
    assert!(record.if_placement_generation.is_some());
    assert!(record.operation_id.starts_with("autoscale-"));

    // Keep observing: the cooldown must hold further growth even though the recommendation
    // persists at the larger layout.
    tokio::time::sleep(Duration::from_millis(200)).await;
    task.abort();
    let autoscaler_ops = state
        .resize_operations
        .list()
        .into_iter()
        .filter(|r| r.origin == ResizeOrigin::Autoscaler)
        .count();
    assert_eq!(autoscaler_ops, 1, "no second resize within the cooldown");
    assert_eq!(state.cluster.read().num_shards(), start_shards + 2);

    let status = state
        .resize_operations
        .autoscale_status()
        .expect("the loop records its latest observation");
    assert_eq!(status.num_shards, start_shards + 2);
    let verdict = serde_json::to_value(&status).expect("status JSON");
    assert!(
        matches!(
            verdict["verdict"].as_str(),
            Some("cooling_down" | "deferred" | "idle")
        ),
        "{verdict}"
    );

    assert_eq!(
        matches(&state, &probe),
        before,
        "a governed resize must preserve every match"
    );

    let (status, _, bytes) = send_raw(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cluster/resize")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(
        body["autoscaler"]["enabled"], false,
        "test registry is not flagged"
    );
    assert!(
        body["autoscaler"]["last_observation"]["verdict"].is_string(),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_automatic_commit_is_retried_until_it_heals() {
    let queries = corpus(60);
    let base = test_state(&queries);
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let state = {
        let config = ClusterConfig {
            num_shards: 3,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &queries,
        )
        .expect("cluster")
        .with_control_plane(Box::new(super::retry::FailResizeProposals {
            inner: InMemoryControlPlane::new(initial),
            remaining: AtomicUsize::new(1),
        }));
        state_from_cluster(cluster)
    };
    let start_shards = state.cluster.read().num_shards();

    // max_shards equals the first target, so after the failed swap the governor would only
    // report `at_ceiling`: the retry must not depend on a new growth decision.
    let task = spawn_resize_loop(
        Arc::clone(&state),
        AutoscaleResizeConfig {
            interval: Duration::from_millis(10),
            split_corpus_threshold: 5,
            governor: ResizeGovernorConfig {
                required_observations: 1,
                cooldown: Duration::ZERO,
                max_step: 2,
                max_shards: start_shards + 2,
                min_relief_percent: 0,
            },
        },
    );
    let mut healed = None;
    for _ in 0..500 {
        healed =
            state.resize_operations.list().into_iter().find(|r| {
                r.origin == ResizeOrigin::Autoscaler && r.state == ResizeState::Succeeded
            });
        if healed.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    task.abort();
    let record = healed.expect("the loop must retry the failed commit until it heals");
    assert!(record.uncommitted_generation.is_none());
    assert_eq!(
        state
            .resize_operations
            .list()
            .iter()
            .filter(|r| r.origin == ResizeOrigin::Autoscaler)
            .count(),
        1,
        "the heal reuses the failed operation's ID"
    );
    let control = state.cluster.read().control_state().expect("state");
    assert_eq!(control.num_shards as usize, start_shards + 2);
    assert_eq!(
        control.placement_generation,
        state.cluster.read().placement_generation().0
    );
}
