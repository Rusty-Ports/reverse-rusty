//! Retiring the current layout's nodes before a remote resize commits (ADR-180).
//!
//! Retirement is what makes the data nodes, not this coordinator's memory, enforce which layout
//! may serve: a retired node refuses every read and write from any coordinator. Each retirement
//! also reports its slots' content fingerprints, which must equal the fingerprints taken once the
//! write fence drained and before the export. That proves the copy is complete even if some other
//! writer reached a source node in between (for example after the node restarted and forgot this
//! coordinator's lease), so completeness does not rest on lease exclusivity alone.

use std::collections::BTreeMap;

use crate::cluster::control::normalized_move_endpoint;
use crate::cluster::remote::RetiredSlot;

use super::{ClusterEngine, ResizeProgress, ShardError};

/// A content fingerprint: `(fp_lo, fp_hi, live_count)`.
pub(super) type Fingerprint = (u64, u64, u64);

impl ClusterEngine {
    /// Fingerprint every position of the current layout, in position order. Call it after the
    /// write fence has drained and before the export.
    pub(super) fn source_fingerprints(
        &self,
        handle: &tokio::runtime::Handle,
        expected: &[String],
    ) -> Result<Vec<Fingerprint>, ShardError> {
        expected
            .iter()
            .enumerate()
            .map(|(position, endpoint)| {
                self.slot_client(handle, endpoint, position)?
                    .content_fingerprint()
            })
            .collect()
    }

    /// Durably retire every node of the current layout in favour of `successor_generation`,
    /// checking that no position changed since `before` was taken. Records each node in `progress`
    /// before asking it, so a lost reply is still unretired on failure. Returns the number of
    /// retired slots.
    pub(super) fn retire_old_layout(
        &self,
        handle: &tokio::runtime::Handle,
        operation_id: u64,
        successor_generation: u64,
        expected: &[String],
        before: &[Fingerprint],
        progress: &ResizeProgress,
    ) -> Result<usize, ShardError> {
        let mut retired_slots = 0;
        for (endpoint, positions) in positions_by_node(expected) {
            progress
                .retired_endpoints
                .borrow_mut()
                .push(expected[positions[0]].clone());
            let slots = self
                .slot_client(handle, &expected[positions[0]], positions[0])?
                .retire(operation_id, successor_generation)?;
            check_unchanged(&endpoint, &positions, before, &slots)?;
            retired_slots += slots.len();
        }
        Ok(retired_slots)
    }
}

/// The current layout's positions grouped by normalized node endpoint: a node may host several.
fn positions_by_node(expected: &[String]) -> BTreeMap<String, Vec<usize>> {
    let mut nodes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (position, endpoint) in expected.iter().enumerate() {
        nodes
            .entry(normalized_move_endpoint(endpoint))
            .or_default()
            .push(position);
    }
    nodes
}

/// Refuse the resize unless every position this node hosts reports exactly the fingerprint taken
/// before the export.
fn check_unchanged(
    endpoint: &str,
    positions: &[usize],
    before: &[Fingerprint],
    retired: &[RetiredSlot],
) -> Result<(), ShardError> {
    for &position in positions {
        let now = retired
            .iter()
            .find(|(shard_id, _)| *shard_id as usize == position)
            .map(|(_, fingerprint)| *fingerprint);
        if now != before.get(position).copied() {
            return Err(ShardError::Protocol(format!(
                "position {position} on {endpoint} changed between the export and its \
                 retirement; the copy would miss that change"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_unchanged, positions_by_node};

    #[test]
    fn a_retirement_must_report_every_hosted_position_unchanged() {
        let before = [(1, 2, 3), (4, 5, 6)];
        assert!(check_unchanged("n", &[0, 1], &before, &[(0, (1, 2, 3)), (1, (4, 5, 6))]).is_ok());
        // A write landed after the export.
        assert!(check_unchanged("n", &[1], &before, &[(1, (4, 5, 7))]).is_err());
        // A hosted position is missing from the reply.
        assert!(check_unchanged("n", &[0, 1], &before, &[(0, (1, 2, 3))]).is_err());
    }

    #[test]
    fn positions_group_by_normalized_node() {
        let nodes = positions_by_node(&[
            "http://A:1".to_string(),
            "http://b:2".to_string(),
            "http://a:1/".to_string(),
        ]);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes["http://a:1"], vec![0, 2]);
    }
}
