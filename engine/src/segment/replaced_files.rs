//! What an engine does with a segment file it has replaced (ADR-214).
//!
//! A compaction, the rewrite of a segment that holds deletions, and a vocabulary recompile
//! each write a new segment file in place of old ones. An engine that owns its manifest
//! commits a manifest that names the new file and then removes the old ones. A cluster
//! shard's engine owns no manifest, and what it may do with the old files depends on who
//! names them: [`ReplacedFiles`].

use std::path::{Path, PathBuf};

use super::Engine;

/// Reads the segment file names that a shard's commit record names now, from disk. `None`
/// when the record cannot be read, which is treated as naming every file.
pub(crate) type CommitRecordNames = Box<dyn Fn() -> Option<Vec<String>> + Send + Sync>;

/// What an engine that owns no manifest does with a segment file it has replaced (ADR-214).
///
/// Such an engine is not the one that commits which of its files are live, so it must not
/// unlink a file that its owner's record still names: a crash before the owner's next commit
/// would leave a committed record that names a file that is gone, and a store that cannot
/// reopen. Who names its files decides.
pub(crate) enum ReplacedFiles {
    /// A coordinator's manifest names them (a primary of an in-process cluster). The file
    /// is left where it is. After each commit the coordinator removes what its committed
    /// manifest no longer names from the shard's directory. The default: the worst a wrong
    /// default does is keep files.
    LeftForTheOwnersSweep,
    /// The shard's own checkpoint file names them (a shard on a shard node). The record is
    /// read from disk at the moment of the decision. A replaced file it does not name is
    /// removed at once. One it names is kept, and the node removes it later, once the record
    /// no longer names it ([`Engine::remove_kept_files_the_record_no_longer_names`]).
    ///
    /// Only a shard node says this, so a build without the node never constructs it.
    #[cfg_attr(not(any(test, feature = "distributed")), allow(dead_code))]
    KeptWhileItsRecordNamesThem(CommitRecordNames),
    /// Nothing names them (a replica of an in-process cluster, which is in no manifest and
    /// is rebuilt from its primary on reopen). There is no record a removal could
    /// contradict, so the file is removed at once.
    RemovedAtOnce,
}

impl Engine {
    /// Retire segment files that this engine has replaced: after a compaction, after the
    /// rewrite of a segment that holds deletions, after a vocabulary recompile. Every caller
    /// retires through here, because what may be done with such a file depends on who names
    /// this engine's files (ADR-214).
    ///
    /// An engine that owns its manifest has just committed a manifest that no longer names
    /// them, and removes them. An engine that owns none has committed nothing: see
    /// [`ReplacedFiles`].
    pub(in crate::segment) fn cleanup_segment_files(&self, paths: &[PathBuf]) {
        if self.owns_manifest {
            for p in paths {
                self.best_effort_remove_segment(p);
            }
            return;
        }
        match &self.replaced_files {
            ReplacedFiles::LeftForTheOwnersSweep => {}
            ReplacedFiles::RemovedAtOnce => {
                for p in paths {
                    self.best_effort_remove_segment(p);
                }
            }
            ReplacedFiles::KeptWhileItsRecordNamesThem(record) => {
                // The record is read now, from disk, so the decision does not rest on
                // anything remembered about what an earlier write of it said.
                let named = record();
                let mut kept = self
                    .kept_segment_files
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for p in paths {
                    if names(named.as_deref(), p) {
                        kept.push(p.clone());
                    } else {
                        self.best_effort_remove_segment(p);
                    }
                }
            }
        }
    }

    /// Say who names this engine's segment files, which decides what it does with one it
    /// has replaced. Said by whoever takes the shard in, before the shard replaces anything.
    pub(crate) fn set_replaced_files(&mut self, replaced_files: ReplacedFiles) {
        self.replaced_files = replaced_files;
    }

    #[cfg(all(test, feature = "distributed"))]
    pub(crate) fn keeps_what_its_record_names(&self) -> bool {
        matches!(
            self.replaced_files,
            ReplacedFiles::KeptWhileItsRecordNamesThem(_)
        )
    }

    /// Remove the kept files that the shard's commit record, read from disk now, no longer
    /// names (ADR-214). One it still names stays kept.
    ///
    /// A kept file is removed by its name, some time after the engine replaced it. The
    /// caller must be the only one that can be writing into this engine's directory at that
    /// moment: on a shard node a recovery writes received files there, and one of them can
    /// carry the name of a kept file.
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn remove_kept_files_the_record_no_longer_names(&self) {
        let ReplacedFiles::KeptWhileItsRecordNamesThem(record) = &self.replaced_files else {
            return;
        };
        let mut kept = self
            .kept_segment_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if kept.is_empty() {
            return;
        }
        let named = record();
        kept.retain(|path| {
            let still_named = names(named.as_deref(), path);
            if !still_named {
                self.best_effort_remove_segment(path);
            }
            still_named
        });
    }
}

/// Whether a commit record names the segment file at `path`. A record that could not be
/// read (`None`) is taken to name every file: nothing is removed on the strength of a
/// record nobody has seen.
fn names(record: Option<&[String]>, path: &Path) -> bool {
    let Some(record) = record else {
        return true;
    };
    path.file_name()
        .and_then(|name| name.to_str())
        .is_none_or(|name| record.iter().any(|named| named == name))
}
