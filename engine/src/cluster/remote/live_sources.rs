//! `LiveSources` client (ADR-180): stream one remote slot's complete live corpus into a visitor.

use std::time::Instant;

use crate::cluster::live_source_wire::{
    CollectError, LiveSourceCollector, LiveSourceRow, MAX_EXPORT_DURATION,
    MAX_LIVE_SOURCE_DOCUMENTS,
};

use super::{
    proto, ranked_rpc_err, remaining_micros, RemoteShard, RpcMethod, RpcOutcome, ShardError,
};

impl RemoteShard {
    /// Export this slot's live corpus in one attempt. It is deliberately not retried: the
    /// visitor consumes documents as they arrive, so a retry would replay what it already saw.
    /// A transport failure, protocol violation, or visitor refusal fails the whole export.
    pub(super) fn export_live_sources(
        &self,
        visit: &mut (dyn FnMut(LiveSourceRow) -> Result<(), ShardError> + Send),
    ) -> Result<u64, ShardError> {
        let started = Instant::now();
        let deadline = started + MAX_EXPORT_DURATION;
        let body = proto::LiveSourcesRequest {
            shard_id: self.shard_id,
            dict_fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_dict_fp,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
            max_documents: MAX_LIVE_SOURCE_DOCUMENTS as u64,
            remaining_micros: remaining_micros(deadline.saturating_duration_since(started)),
        };
        let mut collector = LiveSourceCollector::new(&body);
        let mut request = tonic::Request::new(body);
        request.set_timeout(deadline.saturating_duration_since(started));
        let mut client = self.client.clone();
        let result = self.block_on(async {
            let mut stream = client
                .live_sources(request)
                .await
                .map_err(|status| ranked_rpc_err(&status))?
                .into_inner();
            while let Some(frame) = stream
                .message()
                .await
                .map_err(|status| ranked_rpc_err(&status))?
            {
                collector
                    .push(frame, &mut *visit)
                    .map_err(|error| match error {
                        CollectError::Wire(status) => ranked_rpc_err(&status),
                        CollectError::Visit(error) => error,
                    })?;
            }
            collector.finish().map_err(|status| ranked_rpc_err(&status))
        });
        let outcome = match &result {
            Ok(_) => RpcOutcome::Ok,
            Err(ShardError::DeadlineExceeded) => RpcOutcome::Timeout,
            Err(_) => RpcOutcome::Error,
        };
        self.metrics
            .record(RpcMethod::LiveSources, outcome, started.elapsed(), 0);
        result
    }
}
