//! A remote shard starts a write without waiting for its reply (ADR-224).
//!
//! The request is sent as a task on the RPC runtime, and what the caller gets back blocks
//! for that task when it is asked to. Until then the caller is free to send the same write
//! to the next shard. Building the request and reading the reply are the same functions the
//! blocking calls use, so the two forms cannot drift apart.

use super::{proto, CallKind, RemoteShard, RpcMethod, ShardError};
use crate::cluster::shard::{
    Applied, FannedWrite, PlacedWrite, ReplaceMode, ReplaceStatus, Started,
};
use crate::ownership::QueryPlacement;

impl RemoteShard {
    pub(super) fn replace_request(
        &self,
        write: &PlacedWrite<'_>,
        mode: ReplaceMode,
    ) -> Result<proto::ReplaceRequest, ShardError> {
        write.placement.validate_for_shard(
            self.shard_id,
            self.placement_generation,
            self.num_shards,
        )?;
        Ok(proto::ReplaceRequest {
            item: Some(proto::AddItem {
                logical_id: write.logical,
                dsl: write.text.to_string(),
                version: write.version,
                tags: proto::tags_to_proto(write.tags),
                placement: Some(proto::placement_to_proto(write.placement)),
            }),
            shard_id: self.shard_id,
            only_if_same_placement: mode == ReplaceMode::IfSamePlacement,
        })
    }

    pub(super) fn replace_status(
        &self,
        reply: &proto::ReplaceReply,
        mode: ReplaceMode,
        logical: u64,
    ) -> Result<ReplaceStatus, ShardError> {
        let conditional = mode == ReplaceMode::IfSamePlacement;
        match proto::ReplaceStatus::try_from(reply.status) {
            Ok(proto::ReplaceStatus::Replaced) => Ok(ReplaceStatus::Replaced {
                removed: reply.removed as usize,
            }),
            Ok(proto::ReplaceStatus::Inserted) => Ok(ReplaceStatus::Inserted),
            Ok(proto::ReplaceStatus::Rejected) => Ok(ReplaceStatus::Rejected),
            // A declined condition is only an honest answer to a conditional request.
            Ok(proto::ReplaceStatus::Absent) if conditional => Ok(ReplaceStatus::Absent),
            Ok(proto::ReplaceStatus::PlacementMismatch) if conditional => {
                Ok(ReplaceStatus::PlacementMismatch)
            }
            _ => Err(ShardError::Protocol(format!(
                "shard {} returned replace status {} for logical {logical}",
                self.shard_id, reply.status
            ))),
        }
    }

    pub(super) fn delete_request(&self, logical: u64) -> proto::DeleteRequest {
        proto::DeleteRequest {
            logical_id: logical,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        }
    }

    pub(super) fn insert_request(
        &self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &QueryPlacement,
    ) -> Result<proto::InsertRequest, ShardError> {
        placement.validate_for_shard(self.shard_id, self.placement_generation, self.num_shards)?;
        Ok(proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
                placement: Some(proto::placement_to_proto(placement)),
            }),
            shard_id: self.shard_id,
        })
    }

    /// [`Shard::start_write`](crate::cluster::shard::Shard::start_write) for a remote shard.
    /// A request that cannot be built is the write's answer at once.
    pub(super) fn start_fanned<'a>(&'a self, write: FannedWrite<'_>) -> Started<'a> {
        let client = self.client.clone();
        match write {
            FannedWrite::Replace { write, mode } => {
                let req = match self.replace_request(write, mode) {
                    Ok(req) => req,
                    Err(refused) => return Started::done(Err(refused)),
                };
                let logical = write.logical;
                let reply = self.start_call(RpcMethod::Replace, CallKind::Write, move || {
                    let (mut client, req) = (client.clone(), req.clone());
                    async move {
                        client
                            .replace_extracted(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                });
                Started::pending(move || {
                    self.replace_status(&reply()?, mode, logical)
                        .map(Applied::Replaced)
                })
            }
            FannedWrite::Delete { logical } => {
                let req = self.delete_request(logical);
                let reply = self.start_call(RpcMethod::Delete, CallKind::Write, move || {
                    let mut client = client.clone();
                    async move { client.delete(req).await.map(tonic::Response::into_inner) }
                });
                Started::pending(move || Ok(Applied::Deleted(reply()?.removed as usize)))
            }
            FannedWrite::Insert {
                logical,
                version,
                text,
                tags,
                placement,
                ..
            } => {
                let req = match self.insert_request(logical, version, text, tags, placement) {
                    Ok(req) => req,
                    Err(refused) => return Started::done(Err(refused)),
                };
                let reply = self.start_call(RpcMethod::Insert, CallKind::Write, move || {
                    let (mut client, req) = (client.clone(), req.clone());
                    async move {
                        client
                            .insert_extracted(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                });
                Started::pending(move || {
                    let reply = reply()?;
                    Ok(Applied::Inserted(reply.present.then_some(reply.local_id)))
                })
            }
        }
    }
}
