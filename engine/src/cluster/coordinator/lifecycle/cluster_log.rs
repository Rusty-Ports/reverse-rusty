//! Opening the coordinator's log on a reopen, and what its manifest says about it
//! (ADR-212, ADR-213).

use std::path::Path;

use crate::cluster::clog::{FileClusterLog, IfMissing, LogPos};
use crate::cluster::coordinator::{
    ClusterConfig, ClusterEngine, CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE,
};
use crate::cluster::shard::ShardError;
use crate::storage::ClusterManifest;

/// Open the cluster log under `manifest`, or refuse.
///
/// A manifest at epoch 1 or later was written by a checkpoint, which needs the log open and
/// replaces it only through a rename. Under it a log that is missing, or shorter than its
/// header, has been lost with the writes acknowledged since that manifest, and the cluster is
/// refused; nothing is put in the log's place.
///
/// Epoch 0 is the manifest `build` writes before it creates the log. `build` ends with a
/// checkpoint, so a cluster that is still at epoch 0 is one whose build stopped part-way, or
/// one from a release before ADR-213 that has never checkpointed. Under it a missing log is
/// created, and a log shorter than its header (which releases before ADR-212 could leave) is
/// finished. The open then writes the epoch-1 manifest the build did not
/// ([`ClusterEngine::commit_the_log_into_the_manifest`]).
///
/// This runs before any shard is attached: attaching resets a shard's translog, and when the
/// cluster log is gone those translogs are the only place its writes still exist. An open
/// that refuses must not have touched them.
pub(super) fn open_cluster_log(
    data_dir: &Path,
    manifest: &ClusterManifest,
    config: Option<&ClusterConfig>,
) -> Result<FileClusterLog, ShardError> {
    let refused = |e: std::io::Error| ShardError::Log(format!("opening cluster log: {e}"));
    let log_path = data_dir.join(CLUSTER_LOG_FILE);
    let fsync = config.is_some_and(|c| c.wal_sync_on_write);
    if !manifest.written_with_its_log() {
        FileClusterLog::finish_interrupted_creation(&log_path).map_err(refused)?;
        return FileClusterLog::open(
            &log_path,
            fsync,
            LogPos(manifest.snapshot_pos),
            IfMissing::Create,
        )
        .map_err(refused);
    }
    // The one check is in `open`: told to refuse, it creates nothing. A log that is not
    // there is reported with what this owner knows about it.
    match FileClusterLog::open(
        &log_path,
        fsync,
        LogPos(manifest.snapshot_pos),
        IfMissing::Refuse,
    ) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(refused(crate::storage::framed_log::lost_log(
                &log_path,
                &format!(
                    "the cluster manifest (epoch {}, log position {}) was written after it \
                     existed",
                    manifest.epoch, manifest.snapshot_pos
                ),
                "Restore the data directory from a backup. The shards' translogs have not \
                 been touched.",
            )))
        }
        opened => opened.map_err(refused),
    }
}

impl ClusterEngine {
    /// Record in the manifest that the cluster log exists: the manifest this engine was
    /// built or opened with, written again at epoch 1, and nothing else. `build` ends with
    /// this, and so does a reopen that found a manifest still at epoch 0. From then on a
    /// reopen that finds no log refuses, instead of creating one. A no-op for a cluster
    /// without a data directory and for one already past epoch 0.
    ///
    /// It is not a checkpoint. A checkpoint also seals the shards, rewrites segments that
    /// hold deletions, and rewrites the log; a start has no need of any of that, and each is
    /// a way for a start to fail or to change what is on disk before the manifest says so.
    /// Here the only thing written is the manifest, through its usual rename, so a failure
    /// leaves the directory as it was and the next start tries again. A manifest in an older
    /// format is not written at all.
    ///
    /// The caller owns an engine that has not been shared yet.
    pub(super) fn commit_the_log_into_the_manifest(&self) -> Result<(), ShardError> {
        let Some(dir) = &self.data_dir else {
            return Ok(());
        };
        if self.epoch() >= ClusterManifest::FIRST_EPOCH_WITH_A_LOG {
            return Ok(());
        }
        let held = self
            .committed_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(mut manifest) = held else {
            return Ok(());
        };
        // A manifest in an older format is left exactly as it is. Writing always produces
        // the current format, which records the feature model's fingerprint (ADR-184); a
        // manifest read from before that has none, and an open is no place to upgrade a
        // format or to pin a feature model. Such a cluster stays at epoch 0 until its first
        // checkpoint, which does both, as it always has.
        if manifest.feature_model_fingerprint.is_none() {
            return Ok(());
        }
        manifest.epoch = ClusterManifest::FIRST_EPOCH_WITH_A_LOG;
        crate::storage::write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|e| ShardError::Log(format!("writing cluster manifest: {e}")))?;
        self.epoch
            .store(manifest.epoch, std::sync::atomic::Ordering::Relaxed);
        self.record_committed_manifest(manifest);
        Ok(())
    }
}
