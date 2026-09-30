//! `Retire` and `Unretire` (ADR-180): durable retirement of a whole node by a remote resize.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cluster::proto;

use super::super::retirement::Retirement;
use super::super::ShardServer;

pub(super) async fn retire(
    server: &ShardServer,
    request: Request<proto::RetireRequest>,
) -> Result<Response<proto::RetireReply>, Status> {
    let req = request.into_inner();
    if req.operation_id == 0 || req.successor_generation <= req.placement_generation {
        return Err(Status::invalid_argument(
            "Retire needs a non-zero operation id and a successor generation above the current one",
        ));
    }
    // Serialize with adoption, recovery, and slot removal, so the slots fingerprinted below are
    // exactly the slots retired, and no install can race the retirement.
    let install = server.coordinator_lease.lock_install_owned().await;
    let space = server
        .node_dict
        .load_full()
        .ok_or_else(|| Status::failed_precondition("node has not adopted a feature space"))?;
    if space.placement_generation.0 != req.placement_generation
        || space.num_shards != req.num_shards
        || space.dict.fingerprint() != req.dict_fingerprint
        || space.tag_dict.fingerprint() != req.tag_dict_fingerprint
    {
        return Err(Status::failed_precondition(format!(
            "Retire does not match this node: node generation {}/{} shards, request generation \
             {}/{} shards (or a divergent feature space)",
            space.placement_generation.0,
            space.num_shards,
            req.placement_generation,
            req.num_shards
        )));
    }
    server.record_retirement(Retirement {
        operation_id: req.operation_id,
        successor_generation: req.successor_generation,
    })?;
    let states: Vec<(u32, Arc<super::super::ServerState>)> = server
        .shards
        .read()
        .map_err(|_| Status::internal("shard map lock poisoned"))?
        .iter()
        .filter_map(|(&shard_id, slot)| slot.state.load_full().map(|state| (shard_id, state)))
        .collect();
    // Fingerprinting takes each engine lock and hashes the live set: blocking work that keeps the
    // installation barrier until it finishes, even if this RPC is cancelled.
    let slots = tokio::task::spawn_blocking(move || {
        let _install = install;
        states
            .into_iter()
            .map(|(shard_id, state)| {
                let (fp_lo, fp_hi, live_count) = state
                    .shard
                    .content_fingerprint128()
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                Ok(proto::RetiredSlot {
                    shard_id,
                    fp_lo,
                    fp_hi,
                    live_count,
                })
            })
            .collect::<Result<Vec<_>, Status>>()
    })
    .await
    .map_err(|error| Status::internal(format!("retire fingerprint worker failed: {error}")))??;
    Ok(Response::new(proto::RetireReply { slots }))
}

pub(super) async fn unretire(
    server: &ShardServer,
    request: Request<proto::UnretireRequest>,
) -> Result<Response<proto::UnretireReply>, Status> {
    let req = request.into_inner();
    if req.operation_id == 0 {
        return Err(Status::invalid_argument(
            "Unretire needs a non-zero operation id",
        ));
    }
    let _install = server.coordinator_lease.lock_install().await;
    let was_retired = server.lift_retirement(req.operation_id)?;
    Ok(Response::new(proto::UnretireReply { was_retired }))
}
