//! Rebuild create-only admission before exposing a remote coordinator (ADR-176).

use crate::cluster::logical_id_wire::MAX_LIVE_LOGICAL_IDS;

use super::{ClusterEngine, DurabilityOp, EngineEvent, Shard, ShardError};

/// Accumulate every physical copy that will receive writes before constructing
/// a replicated coordinator. Primary-only read failover cannot prove absence on
/// a replica whose old in-sync state was lost during coordinator restart.
#[derive(Default)]
pub(super) struct RemoteLogicalIds {
    ids: Vec<u64>,
    failure: Option<ShardError>,
}

impl RemoteLogicalIds {
    pub(super) fn include(&mut self, shard: &dyn Shard, position: usize, copy: usize) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.include_copy(shard) {
            self.ids = Vec::new();
            self.failure = Some(ShardError::Protocol(format!(
                "enumerating logical IDs at position {position} copy {copy}: {error}"
            )));
        }
    }

    fn include_copy(&mut self, shard: &dyn Shard) -> Result<(), ShardError> {
        // A proven-empty physical copy needs no enumeration, preserving attach
        // compatibility with old empty peers. This proof must cover EACH copy.
        if matches!(shard.num_queries(), Ok(0)) {
            return Ok(());
        }
        let mut ids = shard.live_logical_ids()?;
        self.ids.try_reserve(ids.len()).map_err(|error| {
            ShardError::Config(format!("allocating logical-ID directory: {error}"))
        })?;
        self.ids.append(&mut ids);
        // Bound temporary storage independently of the number of copies.
        self.ids.sort_unstable();
        self.ids.dedup();
        if self.ids.len() > MAX_LIVE_LOGICAL_IDS {
            return Err(ShardError::Config(format!(
                "logical-ID directory exceeds {MAX_LIVE_LOGICAL_IDS} IDs"
            )));
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<u64>, ShardError> {
        match self.failure {
            Some(error) => Err(error),
            None => compact_ids(self.ids),
        }
    }
}

impl ClusterEngine {
    pub(super) fn with_remote_logical_ids(self) -> Self {
        // from_parts already proved an empty assembly by counting every shard.
        // Keep that compatibility path, including old peers without this RPC.
        if self.logical_ids_authoritative() {
            return self;
        }
        let mut collected = RemoteLogicalIds::default();
        for (position, shard) in self.shards.iter().enumerate() {
            collected.include(shard.as_ref(), position, 0);
        }
        self.with_collected_remote_logical_ids(collected)
    }

    pub(super) fn with_collected_remote_logical_ids(self, collected: RemoteLogicalIds) -> Self {
        let installed = collected.finish().and_then(|ids| {
            // Membership alone cannot attest lost cross-shard repair history.
            let converged = ids.is_empty();
            self.install_logical_ids(ids, converged)
        });
        if let Err(error) = installed {
            // from_parts counts only primary views for replicated positions.
            // An empty primary cannot hide a failed/stale replica enumeration.
            self.mark_logical_ids_unconverged();
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::LogicalIdDirectory,
                detail: "remote logical-ID directory unavailable; create-only writes are \
                         disabled, explicit upserts remain available"
                    .into(),
                error: error.to_string(),
            });
        }
        self
    }
}

// Deduplication changes length, not capacity. Do not retain physical placement
// copies in the compact directory's allocation for the coordinator's lifetime.
fn compact_ids(ids: Vec<u64>) -> Result<Vec<u64>, ShardError> {
    if ids.len() == ids.capacity() {
        return Ok(ids);
    }
    let mut compact = Vec::new();
    compact
        .try_reserve_exact(ids.len())
        .map_err(|error| ShardError::Config(format!("compacting logical-ID directory: {error}")))?;
    compact.extend(ids);
    Ok(compact)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_ids_compact_duplicate_placement_capacity_before_install() {
        let mut ids = Vec::with_capacity(2_000);
        ids.extend(0..1_000);
        ids.extend(0..1_000);
        ids.sort_unstable();
        ids.dedup();
        let compact = compact_ids(ids).expect("compact");
        assert_eq!(compact, (0..1_000).collect::<Vec<_>>());
        assert_eq!(compact.capacity(), compact.len());
        assert_eq!(
            compact_ids(Vec::with_capacity(100))
                .expect("empty")
                .capacity(),
            0
        );
    }
}
