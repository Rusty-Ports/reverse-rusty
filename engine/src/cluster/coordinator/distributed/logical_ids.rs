//! Rebuild create-only admission before exposing a remote coordinator (ADR-176).

use crate::cluster::logical_id_wire::MAX_LIVE_LOGICAL_IDS;

use super::{ClusterEngine, DurabilityOp, EngineEvent, ShardError};

impl ClusterEngine {
    pub(super) fn with_remote_logical_ids(self) -> Self {
        // from_parts already proved an empty assembly by counting every shard.
        // Keep that compatibility path, including old peers without this RPC.
        if self.logical_ids_authoritative() {
            return self;
        }
        if let Err(error) = self.seed_remote_logical_ids() {
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

    fn seed_remote_logical_ids(&self) -> Result<(), ShardError> {
        let mut collected = Vec::new();
        for (position, shard) in self.shards.iter().enumerate() {
            let mut ids = shard.live_logical_ids().map_err(|error| {
                ShardError::Protocol(format!(
                    "enumerating logical IDs at position {position}: {error}"
                ))
            })?;
            collected.try_reserve(ids.len()).map_err(|error| {
                ShardError::Config(format!("allocating logical-ID directory: {error}"))
            })?;
            collected.append(&mut ids);
            // Deduplicate each position before reading another, so replicated
            // placement cannot amplify temporary storage by the shard count.
            collected.sort_unstable();
            collected.dedup();
            if collected.len() > MAX_LIVE_LOGICAL_IDS {
                return Err(ShardError::Config(format!(
                    "logical-ID directory exceeds {MAX_LIVE_LOGICAL_IDS} IDs"
                )));
            }
        }
        // No partial directory has been published. Membership of a populated
        // remote corpus does NOT attest lost cross-shard repair history.
        let converged = collected.is_empty();
        self.install_logical_ids(compact_ids(collected)?, converged)
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
