//! The coordinator's HTTP router: every route, and the layer stack around them.
//!
//! Built by a function, not inline in the startup code, so the tests drive the same stack
//! the server serves (ADR-199). The stack matches the single-node one in `crate::router`.

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{any, get, post, put},
    Router,
};

use crate::auth;
use crate::handlers::{
    alias_discover_method_not_allowed, alias_discover_record_method_not_allowed,
    alias_feedback_apply_method_not_allowed, alias_feedback_read_method_not_allowed,
    alias_feedback_reset_method_not_allowed, alias_import_method_not_allowed,
    alias_learn_apply_method_not_allowed, alias_read_method_not_allowed, cluster_backup,
    cluster_bulk_route, cluster_cancel_job, cluster_cat_segments, cluster_cat_shards,
    cluster_cat_stats, cluster_checkpoint, cluster_compact, cluster_create_job_route,
    cluster_delete_doc, cluster_deregister_node, cluster_discover_aliases,
    cluster_discover_and_record_aliases, cluster_flush_route, cluster_gc,
    cluster_get_alias_feedback, cluster_get_aliases, cluster_get_doc, cluster_get_job,
    cluster_get_job_stream, cluster_get_settings, cluster_get_vocab, cluster_handoff,
    cluster_health, cluster_import_aliases, cluster_learn_aliases, cluster_learn_and_apply_vocab,
    cluster_learn_vocab, cluster_metrics, cluster_mpercolate_route, cluster_put_doc,
    cluster_put_settings, cluster_put_vocab, cluster_reassign, cluster_rebalance,
    cluster_reconcile, cluster_register_node, cluster_reset_alias_feedback, cluster_resize,
    cluster_resize_operation, cluster_resync, cluster_root, cluster_search_route, cluster_state,
    cluster_stats, cluster_v2_mpercolate_route, cluster_v2_search_route,
    cluster_validate_and_apply_feedback, settings_method_not_allowed,
    vocab_learn_apply_method_not_allowed, vocab_learn_method_not_allowed, vocab_method_not_allowed,
    ALIAS_DISCOVER_BODY_LIMIT, ALIAS_DISCOVER_RECORD_BODY_LIMIT, ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    ALIAS_FEEDBACK_READ_BODY_LIMIT, ALIAS_FEEDBACK_RESET_BODY_LIMIT, ALIAS_IMPORT_BODY_LIMIT,
    ALIAS_LEARN_APPLY_BODY_LIMIT, ALIAS_READ_BODY_LIMIT, BACKUP_BODY_LIMIT,
    CAT_SEGMENTS_BODY_LIMIT, CAT_SHARDS_BODY_LIMIT, CHECKPOINT_BODY_LIMIT, CLUSTER_GC_BODY_LIMIT,
    CLUSTER_HANDOFF_BODY_LIMIT, CLUSTER_NODE_DEREGISTER_BODY_LIMIT,
    CLUSTER_NODE_REGISTER_BODY_LIMIT, CLUSTER_REASSIGN_BODY_LIMIT, CLUSTER_REBALANCE_BODY_LIMIT,
    CLUSTER_RECONCILE_BODY_LIMIT, CLUSTER_RESIZE_BODY_LIMIT, CLUSTER_RESYNC_BODY_LIMIT,
    CLUSTER_STATE_BODY_LIMIT, EXHAUSTIVE_JOB_BODY_LIMIT, HEALTH_BODY_LIMIT, METRICS_BODY_LIMIT,
    PIT_BODY_LIMIT, SETTINGS_READ_BODY_LIMIT, SETTINGS_WRITE_BODY_LIMIT, STATS_BODY_LIMIT,
    VOCAB_LEARN_APPLY_BODY_LIMIT, VOCAB_LEARN_BODY_LIMIT, VOCAB_READ_BODY_LIMIT,
    VOCAB_WRITE_BODY_LIMIT,
};
use crate::router::DEFAULT_BODY_LIMIT;
use crate::state::{request_id_middleware, ClusterAppState};

/// Build the coordinator's router. Each endpoint works on at most
/// `max_in_flight_per_endpoint` requests at once; one more waits for a slot of that endpoint.
pub(crate) fn build_cluster_router(
    state: Arc<ClusterAppState>,
    max_in_flight_per_endpoint: usize,
) -> Router {
    Router::new()
        .route("/", get(cluster_root))
        .route(
            "/_doc/{id}",
            get(cluster_get_doc)
                .put(cluster_put_doc)
                .delete(cluster_delete_doc),
        )
        .route(
            "/_search",
            get(cluster_search_route).post(cluster_search_route),
        )
        .route("/v2/_search", post(cluster_v2_search_route))
        .route("/v2/_mpercolate", post(cluster_v2_mpercolate_route))
        .route(
            "/v2/_pit",
            post(crate::handlers::cluster_open_pit_route)
                .delete(crate::handlers::cluster_close_pit_route)
                .layer(DefaultBodyLimit::max(PIT_BODY_LIMIT)),
        )
        .route(
            "/_percolate/jobs",
            post(cluster_create_job_route).layer(DefaultBodyLimit::max(EXHAUSTIVE_JOB_BODY_LIMIT)),
        )
        .route(
            "/_percolate/jobs/{id}",
            get(cluster_get_job).delete(cluster_cancel_job),
        )
        .route("/_percolate/jobs/{id}/stream", any(cluster_get_job_stream))
        .route("/_mpercolate", post(cluster_mpercolate_route))
        .route("/_bulk", post(cluster_bulk_route))
        .route("/_flush", any(cluster_flush_route))
        .route(
            "/_checkpoint",
            any(cluster_checkpoint).layer(DefaultBodyLimit::max(CHECKPOINT_BODY_LIMIT)),
        )
        .route(
            "/_backup",
            any(cluster_backup).layer(DefaultBodyLimit::max(BACKUP_BODY_LIMIT)),
        )
        .route("/_compact", post(cluster_compact))
        .route("/_forcemerge", post(cluster_compact))
        .route(
            "/_stats",
            any(cluster_stats).layer(DefaultBodyLimit::max(STATS_BODY_LIMIT)),
        )
        .route(
            "/_cat/shards",
            any(cluster_cat_shards).layer(DefaultBodyLimit::max(CAT_SHARDS_BODY_LIMIT)),
        )
        .route("/_cat/stats", get(cluster_cat_stats))
        .route(
            "/_cat/segments",
            any(cluster_cat_segments).layer(DefaultBodyLimit::max(CAT_SEGMENTS_BODY_LIMIT)),
        )
        .route(
            "/_vocab",
            get(cluster_get_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_READ_BODY_LIMIT))
                .merge(put(cluster_put_vocab).layer(DefaultBodyLimit::max(VOCAB_WRITE_BODY_LIMIT)))
                .fallback(vocab_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn",
            post(cluster_learn_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_BODY_LIMIT))
                .fallback(vocab_learn_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn_and_apply",
            post(cluster_learn_and_apply_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_APPLY_BODY_LIMIT))
                .fallback(vocab_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases",
            get(cluster_get_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_READ_BODY_LIMIT))
                .fallback(alias_read_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/import",
            post(cluster_import_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_IMPORT_BODY_LIMIT))
                .fallback(alias_import_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(cluster_learn_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_LEARN_APPLY_BODY_LIMIT))
                .fallback(alias_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover",
            post(cluster_discover_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_BODY_LIMIT))
                .fallback(alias_discover_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover_and_record",
            post(cluster_discover_and_record_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_RECORD_BODY_LIMIT))
                .fallback(alias_discover_record_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/feedback",
            get(cluster_get_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_READ_BODY_LIMIT))
                .fallback(alias_feedback_read_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/feedback/reset",
            post(cluster_reset_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_RESET_BODY_LIMIT))
                .fallback(alias_feedback_reset_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/validate_and_apply",
            post(cluster_validate_and_apply_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_APPLY_BODY_LIMIT))
                .fallback(alias_feedback_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_settings",
            get(cluster_get_settings)
                .layer(DefaultBodyLimit::max(SETTINGS_READ_BODY_LIMIT))
                .merge(
                    put(cluster_put_settings)
                        .layer(DefaultBodyLimit::max(SETTINGS_WRITE_BODY_LIMIT)),
                )
                .fallback(settings_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_cluster/state",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/state/{metric}",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/state/{metric}/{target}",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/nodes",
            any(cluster_register_node)
                .layer(DefaultBodyLimit::max(CLUSTER_NODE_REGISTER_BODY_LIMIT)),
        )
        .route(
            "/_cluster/nodes/{id}",
            any(cluster_deregister_node)
                .layer(DefaultBodyLimit::max(CLUSTER_NODE_DEREGISTER_BODY_LIMIT)),
        )
        .route(
            "/_cluster/rebalance",
            any(cluster_rebalance).layer(DefaultBodyLimit::max(CLUSTER_REBALANCE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/reassign",
            any(cluster_reassign).layer(DefaultBodyLimit::max(CLUSTER_REASSIGN_BODY_LIMIT)),
        )
        .route(
            "/_cluster/reconcile",
            any(cluster_reconcile).layer(DefaultBodyLimit::max(CLUSTER_RECONCILE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/gc",
            any(cluster_gc).layer(DefaultBodyLimit::max(CLUSTER_GC_BODY_LIMIT)),
        )
        .route(
            "/_cluster/resize",
            any(cluster_resize).layer(DefaultBodyLimit::max(CLUSTER_RESIZE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/resize/{operation_id}",
            any(cluster_resize_operation),
        )
        .route(
            "/_cluster/resync",
            any(cluster_resync).layer(DefaultBodyLimit::max(CLUSTER_RESYNC_BODY_LIMIT)),
        )
        .route(
            "/_cluster/handoff",
            any(cluster_handoff).layer(DefaultBodyLimit::max(CLUSTER_HANDOFF_BODY_LIMIT)),
        )
        .route(
            "/_health",
            any(cluster_health).layer(DefaultBodyLimit::max(HEALTH_BODY_LIMIT)),
        )
        .route(
            "/_metrics",
            any(cluster_metrics).layer(DefaultBodyLimit::max(METRICS_BODY_LIMIT)),
        )
        .layer(DefaultBodyLimit::max(DEFAULT_BODY_LIMIT))
        // A limit PER ENDPOINT, on purpose (ADR-199). `Router::layer` gives every route, and
        // every method of a route, its own clone of this layer, and each clone has its own
        // semaphore. Do not replace it with `GlobalConcurrencyLimitLayer` or any pool that
        // endpoints share: some requests wait in flight for another request (a job-status
        // poll for the job's stream, a write or a source-enriched search for a lock a job
        // holds until its stream is read), and in a shared pool the waiters fill it and the
        // request they wait for is never admitted.
        .layer(tower::limit::ConcurrencyLimitLayer::new(
            max_in_flight_per_endpoint,
        ))
        // Auth sits OUTSIDE the limiter: an unauthenticated flood is rejected by a cheap
        // header compare without taking a slot from legitimate traffic (ADR-062).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware::<ClusterAppState>,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            request_id_middleware::<ClusterAppState>,
        ))
        .with_state(state)
}
