//! `ReplaceExtracted` (ADR-185): the wire form of the per-shard atomic replace. The slot's
//! `LocalShard` does the work — one engine critical section, one translog `Upsert` frame, one
//! published snapshot — so a reader of this node never sees the id missing between versions.

use tonic::{Request, Response, Status};

use crate::cluster::proto;
use crate::cluster::shard::{PlacedWrite, ReplaceMode, ReplaceStatus, Shard};

use super::super::{compile_item, ShardServer};

pub(super) fn replace_extracted(
    server: &ShardServer,
    request: Request<proto::ReplaceRequest>,
) -> Result<Response<proto::ReplaceReply>, Status> {
    let req = request.into_inner();
    let (slot, st) = server.loaded_slot(req.shard_id)?;
    slot.check_not_fenced()?;
    let item = req
        .item
        .ok_or_else(|| Status::invalid_argument("ReplaceRequest.item is required"))?;
    let placement = proto::placement_from_proto(item.placement.clone())
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    server.validate_placement_config(placement.generation(), placement.num_shards())?;
    placement
        .validate_for_shard(req.shard_id, placement.generation(), placement.num_shards())
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let reply = |status: proto::ReplaceStatus, removed: usize| {
        Ok(Response::new(proto::ReplaceReply {
            status: status as i32,
            removed: removed as u64,
        }))
    };
    let mut lc = String::new();
    let Some(ex) = compile_item(&server.norm, &st.dict, &item.dsl, &mut lc) else {
        // The coordinator parsed before placing, so this should not happen. Nothing was
        // replaced, and a failed replace never deletes.
        return reply(proto::ReplaceStatus::Rejected, 0);
    };
    let tags = proto::tags_from_proto(item.tags);
    let mode = if req.only_if_same_placement {
        ReplaceMode::IfSamePlacement
    } else {
        ReplaceMode::Unconditional
    };
    let status = st
        .shard
        .replace_placed(
            &PlacedWrite {
                ex: &ex,
                logical: item.logical_id,
                version: item.version,
                text: &item.dsl,
                tags: &tags,
                placement: &placement,
            },
            mode,
        )
        .map_err(|e| Status::internal(e.to_string()))?;
    match status {
        ReplaceStatus::Replaced { removed } => reply(proto::ReplaceStatus::Replaced, removed),
        ReplaceStatus::Inserted => reply(proto::ReplaceStatus::Inserted, 0),
        ReplaceStatus::Absent => reply(proto::ReplaceStatus::Absent, 0),
        ReplaceStatus::PlacementMismatch => reply(proto::ReplaceStatus::PlacementMismatch, 0),
        ReplaceStatus::Rejected => reply(proto::ReplaceStatus::Rejected, 0),
    }
}
