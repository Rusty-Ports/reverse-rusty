//! `StageIngest` client (ADR-180): stream a remote-resize load into one fresh target slot.

use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::segment::{IngestReport, PlacedQuery};

use super::{proto, refuse_wire_tag_ids, rpc_err, RemoteShard, RpcMethod, RpcOutcome, ShardError};

type StageReply = Result<proto::IngestReply, tonic::Status>;

/// One open staged load. [`Self::finish`] closes the stream, which is what makes the target
/// persist the load; dropping an unfinished load cancels the call instead, so a failed resize never
/// leaves a target that treats a partial load as complete.
pub(crate) struct StagedLoad<'a> {
    shard: &'a RemoteShard,
    sender: Option<Sender<proto::IngestRequest>>,
    reply: Option<JoinHandle<StageReply>>,
    started: Instant,
    deadline: Instant,
}

impl RemoteShard {
    /// Open a staged load onto this slot, bounded by `deadline` end to end.
    pub(crate) fn open_staged_load(&self, deadline: Instant) -> StagedLoad<'_> {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let mut client = self.client.clone();
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        request.set_timeout(deadline.saturating_duration_since(Instant::now()));
        let reply = self.handle.spawn(async move {
            client
                .stage_ingest(request)
                .await
                .map(tonic::Response::into_inner)
        });
        StagedLoad {
            shard: self,
            sender: Some(sender),
            reply: Some(reply),
            started: Instant::now(),
            deadline,
        }
    }
}

impl StagedLoad<'_> {
    /// Send one bounded batch, waiting for stream capacity no later than the deadline. When the
    /// target has already ended the call, the call's own error is returned.
    pub(crate) fn send(&mut self, items: &[PlacedQuery]) -> Result<(), ShardError> {
        refuse_wire_tag_ids(items)?;
        let request = proto::IngestRequest {
            items: items
                .iter()
                .map(|q| proto::AddItem {
                    logical_id: q.logical,
                    dsl: q.dsl.clone(),
                    version: q.version,
                    tags: proto::tags_to_proto(&q.tags),
                    placement: Some(proto::placement_to_proto(&q.placement)),
                })
                .collect(),
            shard_id: self.shard.shard_id,
        };
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| ShardError::Protocol("staged load already finished".into()))?;
        let deadline = tokio::time::Instant::from_std(self.deadline);
        let sent = self
            .shard
            .block_on(async move { tokio::time::timeout_at(deadline, sender.send(request)).await });
        match sent {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_closed)) => Err(match self.await_reply() {
                Err(error) => error,
                Ok(_) => ShardError::Protocol("staged load ended before its stream closed".into()),
            }),
            Err(_elapsed) => Err(ShardError::DeadlineExceeded),
        }
    }

    /// Close the stream and return the target's totals once it has persisted the load.
    pub(crate) fn finish(mut self) -> Result<IngestReport, ShardError> {
        drop(self.sender.take());
        self.await_reply().map(|reply| IngestReport {
            ingested: reply.ingested as usize,
            rejected_parse: reply.rejected_parse as usize,
            rejected_class_d: reply.rejected_class_d as usize,
        })
    }

    /// Wait for the call's result and record it once.
    fn await_reply(&mut self) -> Result<proto::IngestReply, ShardError> {
        let Some(mut reply) = self.reply.take() else {
            return Err(ShardError::Protocol("staged load already finished".into()));
        };
        let deadline = tokio::time::Instant::from_std(self.deadline);
        let joined = self.shard.block_on(async {
            let joined = tokio::time::timeout_at(deadline, &mut reply).await;
            if joined.is_err() {
                reply.abort();
            }
            joined
        });
        let result = match joined {
            Ok(Ok(Ok(reply))) => Ok(reply),
            Ok(Ok(Err(status))) => Err(rpc_err(&status)),
            Ok(Err(error)) => Err(ShardError::Remote(format!(
                "staged load task failed: {error}"
            ))),
            Err(_elapsed) => Err(ShardError::DeadlineExceeded),
        };
        self.record(&result);
        result
    }

    fn record<T>(&self, result: &Result<T, ShardError>) {
        let outcome = match result {
            Ok(_) => RpcOutcome::Ok,
            Err(ShardError::DeadlineExceeded) => RpcOutcome::Timeout,
            Err(_) => RpcOutcome::Error,
        };
        self.shard
            .metrics
            .record(RpcMethod::StageIngest, outcome, self.started.elapsed(), 0);
    }
}

impl Drop for StagedLoad<'_> {
    fn drop(&mut self) {
        // Cancel the call BEFORE the sender closes the stream: a cleanly closed stream tells the
        // target the load is complete. Waiting for the cancellation keeps that ordering.
        if let Some(reply) = self.reply.take() {
            reply.abort();
            let _cancelled = self.shard.block_on(reply);
            self.record::<()>(&Err(ShardError::Protocol("staged load abandoned".into())));
        }
    }
}
