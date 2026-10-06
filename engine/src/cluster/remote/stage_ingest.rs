//! `StageIngest` client (ADR-180): stream a load into one slot. A remote resize fills each fresh
//! target this way, in batches its caller bounds, and a bulk load sends each shard's bucket this
//! way, split into bounded messages here (ADR-193).

use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::segment::{IngestReport, PlacedQuery};

use super::ingest_chunks::{bounded_requests, wire_items};
use super::{
    no_live_coordinator_lease_status, proto, refuse_wire_tag_ids, rpc_err, RemoteShard, RpcMethod,
    RpcOutcome, ShardError,
};

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
    /// The target refused the call because it holds no lease for this coordinator.
    lease_lost: bool,
    /// The caller reclaims a lost lease and sends the load again, so a refusal for that reason
    /// is not this load's recorded outcome.
    reclaims_lease: bool,
}

/// Why one attempt to stream a bucket failed.
struct BucketFailure {
    error: ShardError,
    /// The node refused the call for a missing lease, before any handler ran.
    lease_lost: bool,
}

/// Run `attempt`. When the node refused it for a lease it no longer holds, `reclaim` the lease
/// and run it once more. A node checks the lease before any handler runs, so a call refused
/// that way applied nothing and sending the bucket again cannot apply it twice. `attempt` is
/// told whether a lease refusal will be retried.
fn with_lease_reclaim<T>(
    can_reclaim: bool,
    mut attempt: impl FnMut(bool) -> Result<T, BucketFailure>,
    reclaim: impl FnOnce() -> Result<(), ShardError>,
) -> Result<T, ShardError> {
    match attempt(can_reclaim) {
        Err(failure) if failure.lease_lost && can_reclaim => {
            reclaim()?;
            attempt(false).map_err(|failure| failure.error)
        }
        outcome => outcome.map_err(|failure| failure.error),
    }
}

impl RemoteShard {
    /// Open a staged load onto this slot, bounded by `deadline` end to end.
    pub(crate) fn open_staged_load(&self, deadline: Instant) -> StagedLoad<'_> {
        self.open_load(RpcMethod::StageIngest, deadline)
    }

    /// Bulk-load one bucket through a staged load (ADR-193): the node seals segments of its own
    /// flush threshold as the messages arrive, and compacts and writes its source store once at
    /// the end. Sending the bucket as separate `IngestExtracted` requests would instead leave one
    /// segment, and one rewrite of the whole source store, per request.
    ///
    /// A node that restarted since this coordinator connected holds no lease for it and refuses
    /// the call. A unary write reclaims the lease and retries (`call`); so does this, or a
    /// restarted replica would be marked out of sync and miss the bucket.
    pub(super) fn bulk_load(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        let requests = bounded_requests(items)?;
        with_lease_reclaim(
            self.coordinator_id.is_some(),
            |reclaims_lease| self.stream_bucket(&requests, reclaims_lease),
            || {
                let started = Instant::now();
                self.reclaim_coordinator_lease(None).inspect_err(|_| {
                    self.metrics
                        .record(RpcMethod::Ingest, RpcOutcome::Error, started.elapsed(), 0);
                })
            },
        )
    }

    /// One attempt at streaming a bucket's messages. The budget is one write timeout per
    /// message plus one for the node to finish.
    fn stream_bucket(
        &self,
        requests: &[Vec<proto::AddItem>],
        reclaims_lease: bool,
    ) -> Result<IngestReport, BucketFailure> {
        let rounds = u32::try_from(requests.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let deadline = Instant::now()
            .checked_add(self.transport.write_timeout.saturating_mul(rounds))
            .ok_or_else(|| BucketFailure {
                error: ShardError::Config("bulk load deadline overflows".into()),
                lease_lost: false,
            })?;
        // Recorded as `ingest`, one call per bucket, as it was when a bucket was one request.
        let mut load = self.open_load(RpcMethod::Ingest, deadline);
        load.reclaims_lease = reclaims_lease;
        let sent = requests
            .iter()
            .try_for_each(|request| load.send_request(request.clone()));
        let outcome = match sent {
            Ok(()) => load.close(),
            Err(error) => Err(error),
        };
        outcome.map_err(|error| BucketFailure {
            error,
            lease_lost: load.lease_lost,
        })
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
            lease_lost: false,
            reclaims_lease: false,
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
        self.close()
    }

    fn close(&mut self) -> Result<IngestReport, ShardError> {
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
            Ok(Ok(Err(status))) => {
                self.lease_lost = no_live_coordinator_lease_status(&status);
                Err(rpc_err(&status))
            }
            Ok(Err(error)) => Err(ShardError::Remote(format!(
                "staged load task failed: {error}"
            ))),
            Err(_elapsed) => Err(ShardError::DeadlineExceeded),
        };
        if !(self.lease_lost && self.reclaims_lease) {
            self.record(&result);
        }
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

#[cfg(test)]
mod tests {
    use super::{with_lease_reclaim, BucketFailure, ShardError};
    use std::cell::Cell;

    fn refused(lease_lost: bool) -> BucketFailure {
        BucketFailure {
            error: ShardError::Remote("refused".into()),
            lease_lost,
        }
    }

    #[test]
    fn a_lost_lease_is_reclaimed_and_the_bucket_sent_once_more() {
        let attempts = Cell::new(0);
        let reclaims = Cell::new(0);
        let sent = with_lease_reclaim(
            true,
            |retries_lease_loss| {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    assert!(retries_lease_loss, "the first refusal will be retried");
                    Err(refused(true))
                } else {
                    assert!(!retries_lease_loss, "the second attempt is final");
                    Ok(7)
                }
            },
            || {
                reclaims.set(reclaims.get() + 1);
                Ok(())
            },
        );
        assert_eq!(sent.expect("the second attempt succeeds"), 7);
        assert_eq!((attempts.get(), reclaims.get()), (2, 1));
    }

    #[test]
    fn a_second_refusal_is_final() {
        let attempts = Cell::new(0);
        let sent: Result<(), _> = with_lease_reclaim(
            true,
            |_| {
                attempts.set(attempts.get() + 1);
                Err(refused(true))
            },
            || Ok(()),
        );
        assert!(sent.is_err());
        assert_eq!(attempts.get(), 2, "one retry, not a loop");
    }

    #[test]
    fn any_other_failure_is_not_sent_again() {
        let attempts = Cell::new(0);
        let sent: Result<(), _> = with_lease_reclaim(
            true,
            |_| {
                attempts.set(attempts.get() + 1);
                Err(refused(false))
            },
            || panic!("nothing to reclaim"),
        );
        assert!(sent.is_err());
        assert_eq!(
            attempts.get(),
            1,
            "the bucket may have been applied in part"
        );
    }

    #[test]
    fn a_client_without_a_claim_does_not_retry_and_a_failed_reclaim_is_the_error() {
        let attempts = Cell::new(0);
        let unclaimed: Result<(), _> = with_lease_reclaim(
            false,
            |retries_lease_loss| {
                assert!(!retries_lease_loss);
                attempts.set(attempts.get() + 1);
                Err(refused(true))
            },
            || panic!("this client cannot claim"),
        );
        assert!(unclaimed.is_err());
        assert_eq!(attempts.get(), 1);

        let failed: Result<(), _> = with_lease_reclaim(
            true,
            |_| Err(refused(true)),
            || {
                Err(ShardError::Remote(
                    "another coordinator owns the node".into(),
                ))
            },
        );
        let error = failed.expect_err("the reclaim failed").to_string();
        assert!(error.contains("another coordinator"), "{error}");
    }
}
