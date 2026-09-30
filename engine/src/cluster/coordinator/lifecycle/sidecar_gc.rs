//! Post-commit reclamation of superseded per-shard source sidecars.
//!
//! A blue/green rebuild (resize or vocabulary change) writes each position's complete source
//! corpus to a generation-named sidecar, and the coordinator manifest selects it atomically with
//! the segment registry. Once a manifest commits, every other sidecar in that position's primary
//! and replica directories is unreachable: the committed manifest never selects it, and a later
//! rebuild at the same generation deletes any stale copy before writing. Without reclamation each
//! rebuild would leave one full corpus copy per shard copy on disk.

use std::path::Path;

use crate::cluster::coordinator::{shard_dir, ClusterEngine};
use crate::events::{DurabilityOp, EngineEvent};

/// Whether `name` has the exact shape of a cluster source sidecar: the legacy `sources.dat` or
/// a generation-selected `sources_g<20 digits>.dat`. Anything else is left untouched.
fn is_source_sidecar(name: &str) -> bool {
    if name == "sources.dat" {
        return true;
    }
    name.strip_prefix("sources_g")
        .and_then(|rest| rest.strip_suffix(".dat"))
        .is_some_and(|digits| digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
}

impl ClusterEngine {
    /// Reclaim source sidecars superseded by the just-committed manifest in each position's
    /// primary directory and its in-process replica directories.
    pub(super) fn gc_superseded_source_sidecars(&self, dir: &Path, committed: &[String]) {
        for (s, keep) in committed.iter().enumerate() {
            let primary = shard_dir(dir, s);
            self.remove_superseded_sidecars(&primary, keep);
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
                    self.remove_superseded_sidecars(&entry.path(), keep);
                }
            }
        }
    }

    /// Remove every source sidecar in `dir` other than `keep`. Best effort: a file left behind is
    /// never selected by the committed manifest and is retried by the next checkpoint.
    fn remove_superseded_sidecars(&self, dir: &Path, keep: &str) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == keep
                || !is_source_sidecar(name)
                || !entry.file_type().is_ok_and(|t| t.is_file())
            {
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
    use super::is_source_sidecar;

    #[test]
    fn recognizes_only_exact_sidecar_names() {
        assert!(is_source_sidecar("sources.dat"));
        assert!(is_source_sidecar("sources_g00000000000000000007.dat"));
        assert!(!is_source_sidecar("sources_g7.dat"));
        assert!(!is_source_sidecar("sources_g0000000000000000000x.dat"));
        assert!(!is_source_sidecar("sources_g00000000000000000007.dat.tmp"));
        assert!(!is_source_sidecar("sources.dat.bak"));
        assert!(!is_source_sidecar("segments"));
    }
}
