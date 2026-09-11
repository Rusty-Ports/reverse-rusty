//! Snapshot once, then stream bounded ID frames without holding the engine lock.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use tokio::sync::OwnedSemaphorePermit;
use tonic::{Request, Response, Status};

use crate::cluster::logical_id_wire::{
    invalid, LOGICAL_IDS_PER_FRAME, MAX_ENUMERATION_DURATION, MAX_LIVE_LOGICAL_IDS,
};
use crate::cluster::proto;

use super::ranked::{deadline_from_remaining, read_status};
use super::{LogicalIdsStream, ShardServer};

struct Snapshot {
    ids: Vec<u64>,
    offset: usize,
    _permit: OwnedSemaphorePermit,
}

pub(super) async fn live_logical_ids(
    server: &ShardServer,
    request: Request<proto::LiveLogicalIdsRequest>,
) -> Result<Response<LogicalIdsStream>, Status> {
    let req = request.into_inner();
    let deadline = deadline_from_remaining(req.remaining_micros)?
        .min(Instant::now() + MAX_ENUMERATION_DURATION);
    let max_ids = usize::try_from(req.max_ids)
        .ok()
        .filter(|limit| (1..=MAX_LIVE_LOGICAL_IDS).contains(limit))
        .ok_or_else(|| Status::invalid_argument("invalid logical-ID enumeration limit"))?;
    server.validate_placement_config(
        crate::ownership::PlacementGeneration(req.placement_generation),
        req.num_shards,
    )?;
    let (_, state) = server.loaded_slot(req.shard_id)?;
    if state.dict.fingerprint() != req.dict_fingerprint
        || state.tag_dict.fingerprint() != req.tag_dict_fingerprint
    {
        return Err(invalid("logical-ID enumeration fingerprint mismatch"));
    }
    let permit = Arc::clone(&server.logical_id_permits)
        .try_acquire_owned()
        .map_err(|_| Status::resource_exhausted("logical-ID enumeration already in progress"))?;
    // A queued/timed-out closure keeps its permit until it actually exits. This
    // prevents expired requests from filling Tokio's blocking queue without bound.
    let worker = tokio::task::spawn_blocking(move || {
        let ids = state
            .shard
            .bounded_live_logical_ids(max_ids, deadline)
            .map_err(|error| read_status(&error))?;
        Ok::<_, Status>(Snapshot {
            ids,
            offset: 0,
            _permit: permit,
        })
    });
    let snapshot = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), worker)
        .await
        .map_err(|_| Status::deadline_exceeded("logical-ID snapshot deadline exhausted"))?
        .map_err(|_| Status::internal("logical-ID snapshot worker failed"))??;
    let total_ids = snapshot.ids.len() as u64;
    let snapshot = Arc::new(Mutex::new(Some(snapshot)));

    // Release memory/admission at the server deadline even when a raw client
    // stops polling the response. A dropped stream ends this watcher promptly.
    let expiry = Arc::downgrade(&snapshot);
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::select! {
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                if let Some(snapshot) = expiry.upgrade() {
                    snapshot.lock().unwrap_or_else(PoisonError::into_inner).take();
                }
            }
            _ = closed_rx => {}
        }
    });
    let max_bytes = server.max_grpc_result_bytes;
    // Leave room for identity/count fields; check exact encoded size below even
    // for a very small configured cap or maximum-width integer identity.
    let per_frame = (max_bytes.saturating_sub(64) / 8).clamp(1, LOGICAL_IDS_PER_FRAME);
    let mut finished = false;
    let frames = std::iter::from_fn(move || {
        let _keep_watcher = &closed_tx;
        if finished {
            return None;
        }
        let mut held = snapshot.lock().unwrap_or_else(PoisonError::into_inner);
        if Instant::now() >= deadline || held.is_none() {
            finished = true;
            held.take();
            return Some(Err(Status::deadline_exceeded(
                "logical-ID stream deadline exhausted",
            )));
        }
        let current = held.as_mut()?;
        let complete = current.offset == current.ids.len();
        let end = current
            .offset
            .saturating_add(per_frame)
            .min(current.ids.len());
        let logical_ids = current.ids[current.offset..end].to_vec();
        current.offset = end;
        let frame = proto::LiveLogicalIdsFrame {
            logical_ids,
            total_ids,
            complete,
            shard_id: req.shard_id,
            placement_generation: req.placement_generation,
            num_shards: req.num_shards,
        };
        if reverse_rusty_shard_proto::encoded_len(&frame) > max_bytes {
            finished = true;
            held.take();
            return Some(Err(Status::resource_exhausted(
                "logical-ID frame exceeds result byte cap",
            )));
        }
        if complete {
            finished = true;
            held.take();
        }
        Some(Ok(frame))
    });
    Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
}
