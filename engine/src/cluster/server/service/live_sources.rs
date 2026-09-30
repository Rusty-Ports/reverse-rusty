//! `LiveSources` (ADR-180): export one slot's complete live corpus for a remote resize.
//!
//! A blocking producer snapshots the sorted live ids, then fetches bounded document pages under
//! short engine locks and sends byte-capped frames through a small bounded channel, so neither
//! side materializes the corpus and a slow reader applies backpressure. The producer holds the
//! node's single snapshot permit until it exits. A document that disappears mid-export, a
//! deadline, or a dropped receiver ends the stream; completion is an explicit empty frame whose
//! count the client checks against everything it received.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};

use crate::cluster::live_source_wire::{
    LIVE_SOURCES_PAGE, MAX_EXPORT_DURATION, MAX_LIVE_SOURCE_DOCUMENTS,
};
use crate::cluster::logical_id_wire::invalid;
use crate::cluster::proto;
use crate::cluster::shard::LocalShard;

use super::ranked::{deadline_from_remaining, read_status};
use super::{LiveSourcesStream, ShardServer};

/// Encoded-size headroom for the frame's identity and count fields.
const FRAME_HEADER_BYTES: usize = 64;

pub(super) fn live_sources(
    server: &ShardServer,
    request: Request<proto::LiveSourcesRequest>,
) -> Result<Response<LiveSourcesStream>, Status> {
    let req = request.into_inner();
    let deadline =
        deadline_from_remaining(req.remaining_micros)?.min(Instant::now() + MAX_EXPORT_DURATION);
    let max_documents = usize::try_from(req.max_documents)
        .ok()
        .filter(|limit| (1..=MAX_LIVE_SOURCE_DOCUMENTS).contains(limit))
        .ok_or_else(|| Status::invalid_argument("invalid live-source export limit"))?;
    server.validate_placement_config(
        crate::ownership::PlacementGeneration(req.placement_generation),
        req.num_shards,
    )?;
    let (_, state) = server.loaded_slot(req.shard_id)?;
    if state.dict.fingerprint() != req.dict_fingerprint
        || state.tag_dict.fingerprint() != req.tag_dict_fingerprint
    {
        return Err(invalid("live-source export fingerprint mismatch"));
    }
    let permit = Arc::clone(&server.logical_id_permits)
        .try_acquire_owned()
        .map_err(|_| Status::resource_exhausted("a live snapshot is already in progress"))?;
    let max_bytes = server.max_grpc_result_bytes;
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let _producer = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let identity = FrameIdentity {
            shard_id: req.shard_id,
            placement_generation: req.placement_generation,
            num_shards: req.num_shards,
        };
        if let Err(status) = produce(
            &state.shard,
            identity,
            max_documents,
            deadline,
            max_bytes,
            &sender,
        ) {
            // Never wait to report a failure: a stalled or departed receiver must not keep this
            // producer (and the node's snapshot permit) alive past the deadline.
            let _undelivered = sender.try_send(Err(status));
        }
    });
    Ok(Response::new(Box::pin(
        tokio_stream::wrappers::ReceiverStream::new(receiver),
    )))
}

#[derive(Clone, Copy)]
struct FrameIdentity {
    shard_id: u32,
    placement_generation: u64,
    num_shards: u32,
}

impl FrameIdentity {
    fn frame(self, total_documents: u64) -> proto::LiveSourcesFrame {
        proto::LiveSourcesFrame {
            documents: Vec::new(),
            total_documents,
            complete: false,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
        }
    }
}

type FrameSender = Sender<Result<proto::LiveSourcesFrame, Status>>;

/// Send one frame, waiting for channel capacity no later than `deadline`, so a receiver that
/// stops polling cannot pin the producer and the node's snapshot permit.
fn send(
    sender: &FrameSender,
    frame: proto::LiveSourcesFrame,
    deadline: Instant,
) -> Result<(), Status> {
    let handle = tokio::runtime::Handle::current();
    match handle.block_on(tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        sender.send(Ok(frame)),
    )) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(Status::cancelled("live-source export receiver closed")),
        Err(_) => Err(Status::deadline_exceeded(
            "live-source export deadline exhausted while the receiver was not reading",
        )),
    }
}

fn produce(
    shard: &LocalShard,
    identity: FrameIdentity,
    max_documents: usize,
    deadline: Instant,
    max_bytes: usize,
    sender: &FrameSender,
) -> Result<(), Status> {
    let ids = shard
        .live_source_ids(max_documents, deadline)
        .map_err(|error| read_status(&error))?;
    let total = ids.len() as u64;
    let mut frame = identity.frame(total);
    let mut frame_bytes = FRAME_HEADER_BYTES;
    for page in ids.chunks(LIVE_SOURCES_PAGE) {
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "live-source export deadline exhausted",
            ));
        }
        let documents = shard
            .live_source_page(page)
            .map_err(|error| read_status(&error))?;
        for (logical_id, dsl, version, _, raw_tags, _, _, _) in documents {
            let item = proto::LiveSource {
                logical_id,
                dsl,
                version,
                tags: raw_tags
                    .into_iter()
                    .map(|(key, value)| proto::TagKv { key, value })
                    .collect(),
            };
            let item_len = reverse_rusty_shard_proto::encoded_len(&item);
            // One repeated-field element: tag byte + length varint + body.
            let item_bytes = item_len.saturating_add(11);
            if FRAME_HEADER_BYTES.saturating_add(item_bytes) > max_bytes {
                return Err(Status::resource_exhausted(format!(
                    "live source {logical_id} exceeds the result byte cap"
                )));
            }
            if frame_bytes.saturating_add(item_bytes) > max_bytes {
                send(
                    sender,
                    std::mem::replace(&mut frame, identity.frame(total)),
                    deadline,
                )?;
                frame_bytes = FRAME_HEADER_BYTES;
            }
            frame.documents.push(item);
            frame_bytes += item_bytes;
        }
    }
    if !frame.documents.is_empty() {
        send(sender, frame, deadline)?;
    }
    let mut complete = identity.frame(total);
    complete.complete = true;
    send(sender, complete, deadline)
}
