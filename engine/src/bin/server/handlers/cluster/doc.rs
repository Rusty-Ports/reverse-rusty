//! Cluster-mode `_doc` CRUD + `_bulk` (ADR-070). `PUT /_doc/{id}` is the
//! cluster-atomic index operation: the default uses ONE `ClusterMutation::Upsert`
//! log frame to replace every prior live copy and insert the new version (ES `index`
//! semantics, the ADR-067 contract at the coordinator), while `op_type=create` uses
//! the insert-only `Add` funnel. `_bulk` maps each index action onto the upsert path,
//! one per-item status each.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Path, Query, State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use tracing::{error, info, instrument, warn};

use reverse_rusty::cluster::{AddOutcome, ShardError};

use crate::dto::ApiError;
use crate::handlers::doc::{
    bulk_body_rejection, bulk_query_rejection, bulk_rejection, error_item, extract_ranked_ingest,
    fail_item, item_inner_mut, parse_bulk_request, pending_item, succeed_item, BulkActionKind,
    BulkItem, BulkParams, BulkResponse, DeleteDocParams, DeleteDocResponse, GetDocParams,
    GetDocResponse, ParsedBulkItem, PutDocBody, PutDocParams, PutDocResponse, CLASS_D_REJECT_MSG,
    QUERY_INDEX,
};
use crate::state::ClusterAppState;

use super::{shard_error_response, shard_error_status};

/// What a caller is told when not every shard took its index or create operation (ADR-194).
/// The write is a failure to retry: some shard does not hold it, and the repair is queued only
/// in this coordinator's memory. A retried index operation converges on any coordinator. A
/// create is retried as an index operation, because a coordinator that restarted in between
/// finds the id on the shards that took it and would answer a create with 409. Shared by
/// `PUT /_doc/{id}` and the bulk items so they cannot drift.
fn partial_write_guidance(create_only: bool, applied: &[usize], failed: &[usize]) -> String {
    let retry = if create_only {
        "retry it as an index operation (without op_type=create)"
    } else {
        "retry this idempotent index operation"
    };
    format!(
        "applied on shards {applied:?}, pending on {failed:?}; not stored on every shard — \
         {retry}, or POST /_cluster/resync while this coordinator remains running"
    )
}

/// Render one upsert outcome as the PUT /_doc response. Shared with the per-item
/// bulk mapping so single and bulk writes can never drift.
fn upsert_status(
    removed: usize,
    outcome: &AddOutcome,
) -> (StatusCode, &'static str, Option<String>) {
    match outcome {
        AddOutcome::Placed { .. } | AddOutcome::Replicated { .. } => {
            if removed > 0 {
                (StatusCode::OK, "updated", None)
            } else {
                (StatusCode::CREATED, "created", None)
            }
        }
        AddOutcome::RejectedClassD => (
            StatusCode::BAD_REQUEST,
            "rejected",
            Some(format!(
                "{CLASS_D_REJECT_MSG}; in cluster mode class-D queries are rejected at \
                 placement (the cluster always-candidate lane is ADR-065 criterion 8)"
            )),
        ),
        AddOutcome::RejectedParse(e) => (
            StatusCode::BAD_REQUEST,
            "error",
            Some(format!("parse error: {e}")),
        ),
    }
}

mod bulk;
mod delete;
mod get;
mod put;

pub(crate) use bulk::cluster_bulk_route;
pub(crate) use delete::cluster_delete_doc;
pub(crate) use get::cluster_get_doc;
pub(crate) use put::cluster_put_doc;
