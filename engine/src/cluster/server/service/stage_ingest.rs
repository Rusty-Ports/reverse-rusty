//! `StageIngest` (ADR-180): load a fresh remote-resize target slot from one client stream.
//!
//! Rows are compiled as they arrive and sealed into segments of the engine's memtable flush
//! threshold, so the slot gets the segment shape ordinary writes would give it. The source store
//! and checkpoint sidecar are written once, when the client closes the stream, rather than once
//! per request. A stream that fails part-way leaves an unrouted, partially loaded slot; the
//! failed resize reports its targets for wiping.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::cluster::proto;
use crate::segment::PlacedQuery;

use super::super::{compile_item, ServerState, ShardServer};

pub(super) async fn stage_ingest(
    server: &ShardServer,
    request: Request<Streaming<proto::IngestRequest>>,
) -> Result<Response<proto::IngestReply>, Status> {
    let mut stream = request.into_inner();
    let mut reply = proto::IngestReply::default();
    let mut loaded: Option<(u32, Arc<ServerState>, usize)> = None;
    let mut pending: Vec<PlacedQuery> = Vec::new();
    while let Some(req) = stream.message().await? {
        let (state, segment_rows) = match &loaded {
            Some((shard_id, state, rows)) if *shard_id == req.shard_id => {
                (Arc::clone(state), *rows)
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "a staged load addresses exactly one shard",
                ))
            }
            None => {
                let (_, state) = server.loaded_slot(req.shard_id)?;
                let rows = state.shard.staged_segment_rows();
                loaded = Some((req.shard_id, Arc::clone(&state), rows));
                (state, rows)
            }
        };
        server.slot(req.shard_id)?.check_not_fenced()?;
        let (items, rejected_parse) =
            compile_ingest_items(server, req.shard_id, &state, req.items)?;
        reply.rejected_parse += rejected_parse;
        pending.extend(items);
        if pending.len() >= segment_rows {
            seal(&state, std::mem::take(&mut pending), &mut reply).await?;
        }
    }
    // An empty stream loads nothing: the position received no rows.
    let Some((shard_id, state, _)) = loaded else {
        return Ok(Response::new(reply));
    };
    server.slot(shard_id)?.check_not_fenced()?;
    if !pending.is_empty() {
        seal(&state, pending, &mut reply).await?;
    }
    tokio::task::spawn_blocking(move || state.shard.finish_staged_load())
        .await
        .map_err(|error| Status::internal(format!("staged load finish failed: {error}")))?
        .map_err(|error| Status::internal(error.to_string()))?;
    Ok(Response::new(reply))
}

/// Seal one staged segment off the async runtime and add its counts to `reply`.
async fn seal(
    state: &Arc<ServerState>,
    items: Vec<PlacedQuery>,
    reply: &mut proto::IngestReply,
) -> Result<(), Status> {
    let state = Arc::clone(state);
    let report = tokio::task::spawn_blocking(move || state.shard.ingest_staged(&items))
        .await
        .map_err(|error| Status::internal(format!("staged segment build failed: {error}")))?;
    reply.ingested += report.ingested as u64;
    reply.rejected_parse += report.rejected_parse as u64;
    reply.rejected_class_d += report.rejected_class_d as u64;
    Ok(())
}

/// Validate each item's placement against this node and slot, and compile it read-only against
/// the slot's frozen dict. Returns the compiled rows and the number that failed to parse.
pub(super) fn compile_ingest_items(
    server: &ShardServer,
    shard_id: u32,
    state: &ServerState,
    items: Vec<proto::AddItem>,
) -> Result<(Vec<PlacedQuery>, u64), Status> {
    let mut lc = String::new();
    let mut rejected_parse = 0u64;
    let mut extracted: Vec<PlacedQuery> = Vec::with_capacity(items.len());
    for it in items {
        let placement = proto::placement_from_proto(it.placement.clone())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        server.validate_placement_config(placement.generation(), placement.num_shards())?;
        placement
            .validate_for_shard(shard_id, placement.generation(), placement.num_shards())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        match compile_item(&server.norm, &state.dict, &it.dsl, &mut lc) {
            // Carry the raw tags forward; the shard's engine resolves them read-only against the
            // adopted frozen tag space (ADR-055).
            Some(ex) => extracted.push(PlacedQuery {
                logical: it.logical_id,
                ex,
                dsl: it.dsl,
                // Store the wire version verbatim — the coordinator's REST layer already
                // defaulted an absent version to 1 before placing, so an explicit value
                // (incl. 0) is caller-supplied and must round-trip identically to the
                // in-process / single-node path. Clamping here was a deployment-dependent
                // divergence: the coordinator logged N while the shard stored N.max(1).
                version: it.version,
                source_generation: None,
                tags: proto::tags_from_proto(it.tags),
                // The wire is dict-agnostic (raw tags only) — pre-resolved ids never arrive.
                tag_ids: Vec::new(),
                rank: crate::rank::RankValues::default(),
                placement,
            }),
            None => rejected_parse += 1,
        }
    }
    Ok((extracted, rejected_parse))
}
