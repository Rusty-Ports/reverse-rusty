//! Control-state repair and terminal attestation for in-process resize.

use crate::cluster::control::{ClusterState, ClusterStateChange};
use crate::cluster::shard::ShardError;

use super::ClusterEngine;
use crate::cluster::coordinator::layout::Layout;

impl ClusterEngine {
    /// Complete one exact post-swap resize transition before another rebuild
    /// may advance the live placement generation. A resize failure can leave
    /// only this shape: unchanged model/ring parameters, the prior shard count,
    /// and a control generation exactly one behind the live shards. Anything
    /// else may be a different unfinished model/topology transition and must
    /// fail loud rather than be reinterpreted as resize state.
    pub(super) fn finish_pending_resize_control_commit(
        &self,
        layout: &Layout,
    ) -> Result<(), ShardError> {
        let control = self.control.cluster_state()?;
        self.attest_resize_control_identity(layout, &control)?;

        let live_generation = layout.generation.0;
        if control.placement_generation == live_generation {
            return self.attest_resize_control_state(layout, &control);
        }

        let prior_generation = live_generation.checked_sub(1).ok_or_else(|| {
            ShardError::ControlPlane(
                "resize control state cannot precede placement generation zero".into(),
            )
        })?;
        let live_num_shards = Self::live_num_shards_for_control(layout)?;
        if control.placement_generation != prior_generation || control.num_shards == live_num_shards
        {
            return Err(ShardError::ControlPlane(format!(
                "resize found control state at generation {}/{} shards, but serving state is at \
                 generation {live_generation}/{live_num_shards} shards; only the exact prior \
                 resize generation with a different shard count can be repaired",
                control.placement_generation, control.num_shards
            )));
        }

        self.control.propose(ClusterStateChange::SetShardCount {
            num_shards: live_num_shards,
        })?;
        let repaired = self.control.cluster_state()?;
        self.attest_resize_control_state(layout, &repaired)
    }

    pub(super) fn attest_resize_control_state(
        &self,
        layout: &Layout,
        control: &ClusterState,
    ) -> Result<(), ShardError> {
        self.attest_resize_control_identity(layout, control)?;
        let live_num_shards = Self::live_num_shards_for_control(layout)?;
        let live_generation = layout.generation.0;
        if control.num_shards != live_num_shards || control.placement_generation != live_generation
        {
            return Err(ShardError::ControlPlane(format!(
                "resize terminal attestation failed: control is at generation {}/{} shards, \
                 serving state is at generation {live_generation}/{live_num_shards} shards",
                control.placement_generation, control.num_shards
            )));
        }
        Ok(())
    }

    fn live_num_shards_for_control(layout: &Layout) -> Result<u32, ShardError> {
        u32::try_from(layout.ring.num_shards()).map_err(|_| {
            ShardError::ControlPlane(
                "serving shard count exceeds the control-plane representation".into(),
            )
        })
    }

    fn attest_resize_control_identity(
        &self,
        layout: &Layout,
        control: &ClusterState,
    ) -> Result<(), ShardError> {
        if control.vnodes != self.vnodes || control.dict_fingerprint != layout.dict.fingerprint() {
            return Err(ShardError::ControlPlane(format!(
                "resize control identity diverged: control has {} vnodes/fingerprint {}, serving \
                 state has {} vnodes/fingerprint {}",
                control.vnodes,
                control.dict_fingerprint,
                self.vnodes,
                layout.dict.fingerprint()
            )));
        }
        if control.assignments.len() != control.num_shards as usize
            || !control
                .assignments
                .iter()
                .enumerate()
                .all(|(position, assignment)| assignment.position as usize == position)
        {
            return Err(ShardError::ControlPlane(format!(
                "resize control topology is incomplete: {} shard(s), {} canonical assignment(s)",
                control.num_shards,
                control.assignments.len()
            )));
        }
        Ok(())
    }
}
