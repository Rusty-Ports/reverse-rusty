//! The unfinished-bulk-load mark on a remote slot (ADR-196).

use super::{proto, CallKind, RemoteShard, RpcMethod, ShardError};

impl RemoteShard {
    /// Record or clear the slot's mark. A node that predates the mark cannot record it, and a
    /// load it cannot remember must not start: that is a configuration error naming the node.
    pub(super) fn set_bulk_load_mark(&self, incomplete: bool) -> Result<(), ShardError> {
        let req = proto::SetBulkLoadStateRequest {
            shard_id: self.shard_id,
            incomplete,
        };
        let client = self.client.clone();
        let recorded = self.call(RpcMethod::BulkLoadState, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                match client.set_bulk_load_state(req).await {
                    Ok(_) => Ok(true),
                    Err(status) if status.code() == tonic::Code::Unimplemented => Ok(false),
                    Err(status) => Err(status),
                }
            }
        })?;
        if recorded {
            return Ok(());
        }
        Err(ShardError::Config(format!(
            "shard node {} cannot record a bulk load in progress (it predates that record); \
             upgrade the shard nodes before loading a corpus in bulk",
            self.endpoint
        )))
    }

    /// Whether the slot carries the mark. A node that predates the mark cannot hold one.
    pub(super) fn bulk_load_mark(&self) -> Result<bool, ShardError> {
        let req = proto::ShardRef {
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        self.call(RpcMethod::BulkLoadState, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                match client.bulk_load_state(req).await {
                    Ok(reply) => Ok(reply.into_inner().incomplete),
                    Err(status) if status.code() == tonic::Code::Unimplemented => Ok(false),
                    Err(status) => Err(status),
                }
            }
        })
    }
}
