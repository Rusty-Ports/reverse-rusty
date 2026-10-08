//! What the engine knows about the manifest on disk, and what it must not do while that is
//! "renamed into place and not synced" (ADR-222).

use super::Engine;

/// What is known about the manifest on disk (ADR-222).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::segment) enum ManifestOnDisk {
    /// It was recovered at open, or renamed into place and its directory synced.
    Synced,
    /// This engine renamed it into place and the directory sync after the rename failed. It
    /// is what a restart reads; the manifest it replaced is what a power loss may bring
    /// back. From then on, for the life of the process: no file is removed (either manifest
    /// may name it), nothing more is committed, the log is neither checkpointed nor reset
    /// (the older manifest needs every record), and no write names a row by its position.
    /// Reads go on, and so do writes that name a row by its id, which the log holds. A
    /// restart reads the disk and starts from what is there.
    RenamedNotSynced,
}

impl Engine {
    /// Whether the manifest on disk was renamed into place and is not known to be synced.
    pub(in crate::segment) fn manifest_awaits_sync(&self) -> bool {
        self.manifest_on_disk == ManifestOnDisk::RenamedNotSynced
    }
}
