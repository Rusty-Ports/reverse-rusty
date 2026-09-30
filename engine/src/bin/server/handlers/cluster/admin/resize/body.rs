//! The strict `POST /_cluster/resize` JSON body and its validation (ADR-167/179/180).

use serde::Deserialize;

use crate::resize_ops::valid_operation_id;

use super::MAX_CLUSTER_RESIZE_SHARDS;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClusterResizeBody {
    num_shards: usize,
    #[serde(default, deserialize_with = "present")]
    operation_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    if_placement_generation: Option<u64>,
    /// Fresh target nodes for a remote resize (ADR-180).
    #[serde(default, deserialize_with = "present")]
    targets: Option<Vec<ClusterResizeTarget>>,
}

/// One remote-resize target node.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResizeTarget {
    id: u64,
    endpoint: String,
}

/// Upper bound on remote-resize targets, matching the shard-count bound.
const MAX_CLUSTER_RESIZE_TARGETS: usize = MAX_CLUSTER_RESIZE_SHARDS;

/// Optional body fields may be omitted but, like every resize field, never `null`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl ClusterResizeBody {
    pub(super) fn validate(self) -> Result<ClusterResizeRequest, String> {
        if self.num_shards == 0 {
            return Err("`num_shards` must be at least 1".to_string());
        }
        if self.num_shards > MAX_CLUSTER_RESIZE_SHARDS {
            return Err(format!(
                "`num_shards` must not exceed {MAX_CLUSTER_RESIZE_SHARDS}"
            ));
        }
        if let Some(id) = self.operation_id.as_deref() {
            if !valid_operation_id(id) {
                return Err(format!(
                    "`operation_id` must be 1..={} characters of ASCII letters, digits, `-`, \
                     `_`, `.`, or `:`",
                    crate::resize_ops::MAX_RESIZE_OPERATION_ID_LEN
                ));
            }
        }
        let targets = match self.targets {
            None => Vec::new(),
            Some(targets) => {
                if targets.is_empty() || targets.len() > MAX_CLUSTER_RESIZE_TARGETS {
                    return Err(format!(
                        "`targets` must list 1..={MAX_CLUSTER_RESIZE_TARGETS} nodes"
                    ));
                }
                let mut ids: Vec<u64> = targets.iter().map(|t| t.id).collect();
                ids.sort_unstable();
                ids.dedup();
                if ids.len() != targets.len() {
                    return Err("`targets` need distinct ids".to_string());
                }
                // Unknown targets are registered as data nodes before the resize connects to
                // them, so each endpoint must be a valid mesh origin, exactly as node
                // registration requires.
                targets
                    .into_iter()
                    .enumerate()
                    .map(|(index, t)| {
                        let addr =
                            crate::handlers::cluster::node_register::validate_node_addr(t.endpoint)
                                .map_err(|reason| {
                                    format!("`targets[{index}].endpoint`: {reason}")
                                })?;
                        Ok(reverse_rusty::cluster::NodeDescriptor {
                            id: reverse_rusty::cluster::NodeId(t.id),
                            addr: Some(addr),
                            role: reverse_rusty::cluster::NodeRole::Data,
                        })
                    })
                    .collect::<Result<_, String>>()?
            }
        };
        Ok(ClusterResizeRequest {
            num_shards: self.num_shards,
            operation_id: self.operation_id,
            if_placement_generation: self.if_placement_generation,
            targets,
        })
    }
}

/// A validated resize body.
pub(super) struct ClusterResizeRequest {
    pub(super) num_shards: usize,
    pub(super) operation_id: Option<String>,
    pub(super) if_placement_generation: Option<u64>,
    pub(super) targets: Vec<reverse_rusty::cluster::NodeDescriptor>,
}
