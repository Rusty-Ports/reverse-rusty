//! What the engine knows about the manifest on disk, and what it must not do while that is
//! "renamed into place and not synced" (ADR-222).

use super::Engine;

/// What every writer is told once a manifest's outcome is not known.
pub(super) const NOTHING_UNTIL_A_RESTART: &str =
    "a manifest was renamed into place and could not be synced, so it is not known which \
     manifest a power loss leaves; the node is read-only until a restart";

/// What is known about the manifest on disk (ADR-222).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::segment) enum ManifestOnDisk {
    /// It was recovered at open, or renamed into place and its directory synced.
    Synced,
    /// This engine renamed it into place and the directory sync after the rename failed. It
    /// is what a restart reads; the manifest it replaced is what a power loss may bring
    /// back. From then on, for the life of the process, the data directory does not change:
    ///
    /// - the log is closed, so every mutation is refused (each appends there before it is
    ///   applied). A record admitted now would have to mean the same over both manifests,
    ///   and they can differ in what they hold: a bulk load is in its manifest and not in
    ///   the log;
    /// - no file of a commit is written (a sidecar written now would take the name the
    ///   renamed manifest selects);
    /// - no file is removed (either manifest may name it).
    ///
    /// Reads go on. A restart reads the disk and starts from what is there, which is a
    /// state a crash at that point of a commit leaves.
    RenamedNotSynced,
}

impl Engine {
    /// Whether the manifest on disk was renamed into place and is not known to be synced.
    pub(in crate::segment) fn manifest_awaits_sync(&self) -> bool {
        self.manifest_on_disk == ManifestOnDisk::RenamedNotSynced
    }

    /// What the writers of a commit's files (a segment, a source sidecar, the manifest) ask
    /// before they write: an error once a manifest's outcome is not known. Holding it there
    /// is what lets every commit path, present and future, stop without knowing why.
    pub(in crate::segment) fn refuse_a_commit_awaiting_restart(&self) -> std::io::Result<()> {
        if self.manifest_awaits_sync() {
            return Err(std::io::Error::other(NOTHING_UNTIL_A_RESTART));
        }
        Ok(())
    }
}
