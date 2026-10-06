//! `StageIngest` client (ADR-180): stream a load into one slot. A remote resize fills each fresh
//! target this way, in batches its caller bounds, and a bulk load sends each shard's bucket this
//! way, split into bounded messages here (ADR-193).

use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::segment::{IngestReport, PlacedQuery};

use super::ingest_chunks::{bounded_requests, wire_items};
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
    /// The transport-metrics row this load is recorded under.
    method: RpcMethod,
}

impl RemoteShard {
    /// Open a staged load onto this slot, bounded by `deadline` end to end.
    pub(crate) fn open_staged_load(&self, deadline: Instant) -> StagedLoad<'_> {
        self.open_load(RpcMethod::StageIngest, deadline)
    }

    /// Bulk-load one bucket through a staged load (ADR-193): the node seals segments of its own
    /// flush threshold as the messages arrive, and compacts and writes its source store once at
    /// the end. Sending the bucket as separate `IngestExtracted` requests would instead leave one
    /// segment, and one rewrite of the whole source store, per request. The budget is one write
    /// timeout per message plus one for the node to finish.
    pub(super) fn bulk_load(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        let requests = bounded_requests(items)?;
        let rounds = u32::try_from(requests.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let deadline = Instant::now()
            .checked_add(self.transport.write_timeout.saturating_mul(rounds))
            .ok_or_else(|| ShardError::Config("bulk load deadline overflows".into()))?;
        // Recorded as `ingest`, one call per bucket, as it was when a bucket was one request.
        let mut load = self.open_load(RpcMethod::Ingest, deadline);
        for request in requests {
            load.send_request(request)?;
        }
        load.finish()
    }

    fn open_load(&self, method: RpcMethod, deadline: Instant) -> StagedLoad<'_> {
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
            method,
        }
    }
}

impl StagedLoad<'_> {
    /// Send one bounded batch, waiting for stream capacity no later than the deadline. When the
    /// target has already ended the call, the call's own error is returned.
    pub(crate) fn send(&mut self, items: &[PlacedQuery]) -> Result<(), ShardError> {
        refuse_wire_tag_ids(items)?;
        self.send_request(wire_items(items))
    }

    fn send_request(&mut self, items: Vec<proto::AddItem>) -> Result<(), ShardError> {
        let request = proto::IngestRequest {
            items,
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
            .record(self.method, outcome, self.started.elapsed(), 0);
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
