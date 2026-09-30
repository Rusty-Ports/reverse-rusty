//! Post-commit reclamation of superseded per-shard source sidecars.
//!
//! A blue/green rebuild (resize or vocabulary change) writes every copy of every position —
//! primary and in-process replicas — to a fresh `sources_g<generation>.dat` named by the new
//! placement generation, and the coordinator manifest selects the primary's sidecar atomically
//! with the segment registry. Peer recovery, by contrast, always restores a replica into the
//! canonical `sources.dat`. So after a manifest commits at generation `G`, an active sidecar is
//! either `sources_g<G>.dat` or `sources.dat`; a generation-named sidecar below `G` belongs to a
//! superseded layout that no copy can still select. Only those are reclaimed. `sources.dat` is
//! never removed here: it may be a recovered replica's live store, and removing it under a
//! concurrent lazy remap would silently empty that store. Without reclamation, each rebuild
//! would leave one full source corpus per shard copy on disk.

use std::path::Path;

use crate::cluster::coordinator::{shard_dir, ClusterEngine};
use crate::events::{DurabilityOp, EngineEvent};

/// The generation encoded in a `sources_g<20 digits>.dat` sidecar name, or `None` for any other
/// name (including the canonical `sources.dat`).
fn superseded_generation(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("sources_g")?.strip_suffix(".dat")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

impl ClusterEngine {
    /// Reclaim generation-named source sidecars older than `committed_generation` in each
    /// position's primary directory and its in-process replica directories.
    pub(super) fn gc_superseded_source_sidecars(
        &self,
        dir: &Path,
        num_shards: usize,
        committed_generation: u64,
    ) {
        for s in 0..num_shards {
            let primary = shard_dir(dir, s);
            self.remove_superseded_sidecars(&primary, committed_generation);
            let Ok(entries) = std::fs::read_dir(&primary) else {
                continue;
            };
            for entry in entries.flatten() {
                let is_replica = entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("replica_"))
                    && entry.file_type().is_ok_and(|t| t.is_dir());
                if is_replica {
                    self.remove_superseded_sidecars(&entry.path(), committed_generation);
                }
            }
        }
    }

    /// Remove every generation-named sidecar in `dir` older than `committed_generation`. Best
    /// effort: a file left behind is never selected and is retried by the next checkpoint.
    fn remove_superseded_sidecars(&self, dir: &Path, committed_generation: u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let superseded = superseded_generation(name)
                .is_some_and(|generation| generation < committed_generation);
            if !superseded || !entry.file_type().is_ok_and(|t| t.is_file()) {
                continue;
            }
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => self.emit(EngineEvent::DurabilityFailure {
                    op: DurabilityOp::WalReset,
                    detail: format!(
                        "removing superseded source sidecar {name} after checkpoint failed \
                         (never selected on open; retried by the next checkpoint)"
                    ),
                    error: e.to_string(),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::superseded_generation;

    #[test]
    fn only_exact_generation_names_are_candidates() {
        assert_eq!(
            superseded_generation("sources_g00000000000000000007.dat"),
            Some(7)
        );
        assert_eq!(superseded_generation("sources.dat"), None);
        assert_eq!(superseded_generation("sources_g7.dat"), None);
        assert_eq!(
            superseded_generation("sources_g0000000000000000000x.dat"),
            None
        );
        assert_eq!(
            superseded_generation("sources_g00000000000000000007.dat.tmp"),
            None
        );
        assert_eq!(superseded_generation("segments"), None);
    }
}
