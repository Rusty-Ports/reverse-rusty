//! `StageIngest` (ADR-180): load a fresh remote-resize target slot from one client stream.
//!
//! Rows are compiled as they arrive and sealed into segments of the engine's memtable flush
//! threshold, so the slot gets the segment shape ordinary writes would give it. The source store
//! and checkpoint sidecar are written once, when the client closes the stream, rather than once
//! per request. A stream that fails part-way leaves an unrouted, partially loaded slot; the
//! failed resize reports its targets for wiping.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::cluster::node_metrics::ShardRpc;
use crate::cluster::proto;
use crate::segment::PlacedQuery;

use super::super::{compile_item, ServerState, ShardServer};

pub(super) async fn stage_ingest(
    server: &ShardServer,
    request: Request<Streaming<proto::IngestRequest>>,
) -> Result<Response<proto::IngestReply>, Status> {
    let started = std::time::Instant::now();
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
            seal(
                server,
                req.shard_id,
                &state,
                std::mem::take(&mut pending),
                &mut reply,
            )
            .await?;
        }
    }
    // An empty stream loads nothing: the position received no rows.
    let Some((shard_id, state, _)) = loaded else {
        return Ok(Response::new(reply));
    };
    if !pending.is_empty() {
        seal(server, shard_id, &state, pending, &mut reply).await?;
    }
    run_installed(server, shard_id, &state, |state| {
        state.shard.finish_staged_load()
    })
    .await?
    .map_err(|error| Status::internal(error.to_string()))?;
    // A staged load is how a slot is bulk-loaded, by a bootstrap or by a resize: time it as
    // the slot's bulk ingest.
    server
        .slot(shard_id)?
        .latency
        .observe(ShardRpc::Ingest, started.elapsed());
    Ok(Response::new(reply))
}

/// Run one staged-load job on a blocking worker that owns the node's installation barrier, once
/// `state` is confirmed to still be the slot's installed, unfenced state. Cancelling the RPC
/// detaches the worker; holding the barrier until it finishes keeps adoption, recovery, and removal
/// from replacing the slot while the old engine still writes the same files, as `Seal` does.
pub(in crate::cluster::server) async fn run_installed<T: Send + 'static>(
    server: &ShardServer,
    shard_id: u32,
    state: &Arc<ServerState>,
    job: impl FnOnce(&ServerState) -> T + Send + 'static,
) -> Result<T, Status> {
    let install = server.coordinator_lease.lock_install_owned().await;
    let (slot, current) = server.loaded_slot(shard_id)?;
    if !Arc::ptr_eq(&current, state) {
        return Err(Status::aborted(
            "the slot was replaced during the staged load",
        ));
    }
    slot.check_not_fenced()?;
    tokio::task::spawn_blocking(move || {
        let _install = install;
        let done = job(&current);
        // A staged load commits by the checkpoint file and is not followed by a seal, so
        // what it replaced goes here, under the same barrier (ADR-214).
        current.shard.remove_replaced_files();
        done
    })
    .await
    .map_err(|error| Status::internal(format!("staged load worker failed: {error}")))
}

/// Seal one staged segment under the installation barrier and add its counts to `reply`.
async fn seal(
    server: &ShardServer,
    shard_id: u32,
    state: &Arc<ServerState>,
    items: Vec<PlacedQuery>,
    reply: &mut proto::IngestReply,
) -> Result<(), Status> {
    let report = run_installed(server, shard_id, state, move |state| {
        state.shard.ingest_staged(&items)
    })
    .await?;
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
