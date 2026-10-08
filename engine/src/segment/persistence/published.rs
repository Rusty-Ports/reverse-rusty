//! What the engine knows about the manifest on disk, and what it must not do while that is
//! "renamed into place and not synced" (ADR-222).

use super::Engine;

/// What is known about the manifest on disk (ADR-222).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::segment) enum ManifestOnDisk {
    /// It was recovered at open, or renamed into place and its directory synced.
    Synced,
    /// This engine renamed it into place, and the directory has not been synced since. It is
    /// what a restart reads; the manifest it replaced is what a power loss may bring back.
    /// While this holds, no file is removed (either manifest may name it) and the log is
    /// neither checkpointed nor reset (the older manifest needs it). The next commit whose
    /// directory sync succeeds ends it.
    RenamedNotSynced,
}

impl Engine {
    /// Whether the manifest on disk was renamed into place and is not known to be synced.
    pub(in crate::segment) fn manifest_awaits_sync(&self) -> bool {
        self.manifest_on_disk == ManifestOnDisk::RenamedNotSynced
    }
}
