//! The single-node HTTP router: every route, and the layer stack around them.
//!
//! Built by a function, not inline in `main`, so the tests drive the same stack the
//! server serves (ADR-199).

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
    alias_learn_apply_method_not_allowed, alias_read_method_not_allowed, api_root, backup_route,
    bulk_route, cancel_job, cat_segments, cat_stats, close_pit_route, compact_route,
    create_job_route, delete_doc, discover_aliases, discover_and_record_aliases, flush_route,
    force_merge_route, get_alias_feedback, get_aliases, get_doc, get_job, get_job_stream,
    get_settings, get_vocab, health, import_aliases, learn_and_apply_aliases,
    learn_and_apply_vocab, learn_vocab, mpercolate_route, open_pit_route, prometheus_metrics,
    put_doc, put_settings, put_vocab, reset_alias_feedback, search_route,
    settings_method_not_allowed, stats, v2_mpercolate_route, v2_search_route,
    validate_and_apply_feedback, vocab_learn_apply_method_not_allowed,
    vocab_learn_method_not_allowed, vocab_method_not_allowed, ALIAS_DISCOVER_BODY_LIMIT,
    ALIAS_DISCOVER_RECORD_BODY_LIMIT, ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    ALIAS_FEEDBACK_READ_BODY_LIMIT, ALIAS_FEEDBACK_RESET_BODY_LIMIT, ALIAS_IMPORT_BODY_LIMIT,
    ALIAS_LEARN_APPLY_BODY_LIMIT, ALIAS_READ_BODY_LIMIT, BACKUP_BODY_LIMIT,
    CAT_SEGMENTS_BODY_LIMIT, EXHAUSTIVE_JOB_BODY_LIMIT, HEALTH_BODY_LIMIT, METRICS_BODY_LIMIT,
    PIT_BODY_LIMIT, SETTINGS_READ_BODY_LIMIT, SETTINGS_WRITE_BODY_LIMIT, STATS_BODY_LIMIT,
    VOCAB_LEARN_APPLY_BODY_LIMIT, VOCAB_LEARN_BODY_LIMIT, VOCAB_READ_BODY_LIMIT,
    VOCAB_WRITE_BODY_LIMIT,
};
use crate::state::{request_id_middleware, AppState};

pub(crate) mod admission;
#[cfg(test)]
pub(crate) mod held;
#[cfg(test)]
mod tests;

pub(crate) use admission::{RequestPool, MAX_IN_FLIGHT_REQUESTS};

/// The largest request body a route accepts unless it sets its own limit.
pub(crate) const DEFAULT_BODY_LIMIT: usize = 100 * 1024 * 1024;

/// Build the single-node router. At most `max_in_flight` requests are in flight together,
/// the probes aside; one more waits for a slot (see [`admission`]).
pub(crate) fn build_router(state: Arc<AppState>, max_in_flight: usize) -> Router {
    Router::new()
        .route("/", get(api_root))
        .route("/_doc/{id}", get(get_doc).put(put_doc).delete(delete_doc))
        .route("/_search", get(search_route).post(search_route))
        .route("/v2/_search", post(v2_search_route))
        .route("/v2/_mpercolate", post(v2_mpercolate_route))
        .route(
            "/v2/_pit",
            post(open_pit_route)
                .delete(close_pit_route)
                .layer(DefaultBodyLimit::max(PIT_BODY_LIMIT)),
        )
        .route(
            "/_percolate/jobs",
            post(create_job_route).layer(DefaultBodyLimit::max(EXHAUSTIVE_JOB_BODY_LIMIT)),
        )
        .route("/_percolate/jobs/{id}", get(get_job).delete(cancel_job))
        .route("/_percolate/jobs/{id}/stream", any(get_job_stream))
        .route("/_mpercolate", post(mpercolate_route))
        .route("/_bulk", post(bulk_route))
        .route("/_flush", any(flush_route))
        .route("/_compact", any(compact_route))
        .route("/_forcemerge", any(force_merge_route))
        .route(
            "/_backup",
            any(backup_route).layer(DefaultBodyLimit::max(BACKUP_BODY_LIMIT)),
        )
        .route(
            "/_stats",
            any(stats).layer(DefaultBodyLimit::max(STATS_BODY_LIMIT)),
        )
        .route(
            "/_cat/stats",
            any(cat_stats).layer(DefaultBodyLimit::max(STATS_BODY_LIMIT)),
        )
        .route(
            "/_cat/segments",
            any(cat_segments).layer(DefaultBodyLimit::max(CAT_SEGMENTS_BODY_LIMIT)),
        )
        .route(
            "/_vocab",
            get(get_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_READ_BODY_LIMIT))
                .merge(put(put_vocab).layer(DefaultBodyLimit::max(VOCAB_WRITE_BODY_LIMIT)))
                .fallback(vocab_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/learn",
            post(learn_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_BODY_LIMIT))
                .fallback(vocab_learn_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/learn_and_apply",
            post(learn_and_apply_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_APPLY_BODY_LIMIT))
                .fallback(vocab_learn_apply_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases",
            get(get_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_READ_BODY_LIMIT))
                .fallback(alias_read_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/import",
            post(import_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_IMPORT_BODY_LIMIT))
                .fallback(alias_import_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(learn_and_apply_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_LEARN_APPLY_BODY_LIMIT))
                .fallback(alias_learn_apply_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/discover",
            post(discover_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_BODY_LIMIT))
                .fallback(alias_discover_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/discover_and_record",
            post(discover_and_record_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_RECORD_BODY_LIMIT))
                .fallback(alias_discover_record_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/feedback",
            get(get_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_READ_BODY_LIMIT))
                .fallback(alias_feedback_read_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/feedback/reset",
            post(reset_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_RESET_BODY_LIMIT))
                .fallback(alias_feedback_reset_method_not_allowed::<AppState>),
        )
        .route(
            "/_vocab/aliases/validate_and_apply",
            post(validate_and_apply_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_APPLY_BODY_LIMIT))
                .fallback(alias_feedback_apply_method_not_allowed::<AppState>),
        )
        .route(
            "/_settings",
            get(get_settings)
                .layer(DefaultBodyLimit::max(SETTINGS_READ_BODY_LIMIT))
                .merge(put(put_settings).layer(DefaultBodyLimit::max(SETTINGS_WRITE_BODY_LIMIT)))
                .fallback(settings_method_not_allowed::<AppState>),
        )
        .route(
            "/_health",
            any(health).layer(DefaultBodyLimit::max(HEALTH_BODY_LIMIT)),
        )
        .route(
            "/_metrics",
            any(prometheus_metrics).layer(DefaultBodyLimit::max(METRICS_BODY_LIMIT)),
        )
        .layer(DefaultBodyLimit::max(DEFAULT_BODY_LIMIT))
        // ONE pool for every route (ADR-199). The middleware decides by method and path what
        // a request needs, so a route is never outside the pool by where it was added.
        .layer(middleware::from_fn_with_state(
            RequestPool::new(max_in_flight),
            admission::admit,
        ))
        // Auth sits OUTSIDE admission: an unauthenticated flood is rejected by a cheap
        // header compare without taking a slot from legitimate traffic (ADR-062).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            request_id_middleware,
        ))
        .with_state(state)
}
