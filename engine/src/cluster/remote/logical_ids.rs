use crate::cluster::logical_id_wire::{IdCollector, MAX_LIVE_LOGICAL_IDS};

use super::{proto, remaining_micros, RemoteShard, RpcMethod, ShardError};

impl RemoteShard {
    pub(super) fn enumerate_logical_ids(&self) -> Result<Vec<u64>, ShardError> {
        let absolute = self.bounded_deadline(None)?;
        let base = proto::LiveLogicalIdsRequest {
            shard_id: self.shard_id,
            dict_fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_dict_fp,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
            max_ids: MAX_LIVE_LOGICAL_IDS as u64,
            remaining_micros: 0,
        };
        let client = self.client.clone();
        self.call_until(RpcMethod::LiveLogicalIds, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base;
            body.remaining_micros = remaining_micros(remaining);
            let mut collector = IdCollector::new(&body);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            async move {
                let mut stream = client.live_logical_ids(request).await?.into_inner();
                while let Some(frame) = stream.message().await? {
                    collector.push(frame)?;
                }
                collector.finish()
            }
        })
    }
}
