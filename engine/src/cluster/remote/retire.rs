//! Remote-resize node retirement clients (ADR-180).

use tokio::runtime::Handle;

use crate::cluster::security::ClientSecurity;

use super::{
    block_on_in_context, connect_channel, proto, rpc_err, CallKind, RemoteShard, RpcMethod,
    ShardError,
};

/// One retired slot's content fingerprint: `(shard_id, (fp_lo, fp_hi, live_count))`.
pub(crate) type RetiredSlot = (u32, (u64, u64, u64));

impl RemoteShard {
    /// Retire this client's whole node at its placement generation, in favour of
    /// `successor_generation`, returning every hosted slot's content fingerprint.
    pub(crate) fn retire(
        &self,
        operation_id: u64,
        successor_generation: u64,
    ) -> Result<Vec<RetiredSlot>, ShardError> {
        let req = proto::RetireRequest {
            operation_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
            dict_fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_dict_fp,
            successor_generation,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Retire, CallKind::Write, move || {
            let mut client = client.clone();
            async move { client.retire(req).await.map(tonic::Response::into_inner) }
        })?;
        Ok(reply
            .slots
            .into_iter()
            .map(|slot| (slot.shard_id, (slot.fp_lo, slot.fp_hi, slot.live_count)))
            .collect())
    }
}

fn not_adopted_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("has not adopted a dict yet")
}

/// Claim `endpoint` for `coordinator_id` through the fingerprint handshake, which a retired node
/// still answers, and report which remote resize retired it: `None` for a node that has adopted
/// nothing, `Some(0)` for one that is not retired. Fails while another coordinator's lease on the
/// node is live, so startup resolution never acts underneath a coordinator still running a
/// resize.
pub(crate) fn claim_retirement(
    endpoint: &str,
    handle: &Handle,
    security: &ClientSecurity,
    coordinator_id: u64,
) -> Result<Option<u64>, ShardError> {
    let mut claimant = connect_channel(endpoint, handle, security, Some(coordinator_id), true)?;
    let probed = block_on_in_context(handle, async move {
        claimant.dict_fingerprint(proto::Empty {}).await
    });
    match probed {
        Ok(reply) => Ok(Some(reply.into_inner().retired_operation)),
        Err(status) if not_adopted_status(&status) => Ok(None),
        Err(status) => Err(rpc_err(&status)),
    }
}

/// Lift `operation_id`'s retirement of `endpoint`, claiming the node for `coordinator_id` first.
/// Returns whether the node was retired.
pub(crate) fn unretire_node(
    endpoint: &str,
    handle: &Handle,
    security: &ClientSecurity,
    coordinator_id: u64,
    operation_id: u64,
) -> Result<bool, ShardError> {
    claim_retirement(endpoint, handle, security, coordinator_id)?;
    let mut owner = connect_channel(endpoint, handle, security, Some(coordinator_id), false)?;
    block_on_in_context(handle, async move {
        owner
            .unretire(proto::UnretireRequest { operation_id })
            .await
    })
    .map(|reply| reply.into_inner().was_retired)
    .map_err(|status| rpc_err(&status))
}
