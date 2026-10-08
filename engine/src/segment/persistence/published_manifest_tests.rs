//! ADR-222: a manifest that was renamed into place and whose directory sync failed is not a
//! failed commit and not a finished one. From then on the engine serves reads and its data
//! directory does not change: every write is refused, no file is written or removed. A
//! restart reads what is on disk.

use super::*;
use crate::config::EngineConfig;
use crate::fault::{Scope, Step};
use crate::normalize::Normalizer;
use crate::segment::MatchScratch;

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocabulary")
}

fn config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        ..EngineConfig::default()
    }
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rr_published_manifest_{tag}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the scratch directory");
    dir
}

fn matches(engine: &Engine, title: &str) -> Vec<u64> {
    let (mut scratch, mut out) = (MatchScratch::new(), Vec::new());
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

/// Two segments on disk and an upsert and a delete in the log.
fn seeded(dir: &std::path::Path) -> Engine {
    let mut engine = Engine::with_config(norm(), config(dir));
    engine
        .try_bulk_ingest(&[
            (1, "package adapter".to_string()),
            (2, "vintage lamp".to_string()),
        ])
        .expect("bulk load");
    engine
        .try_insert_live("brass compass", 3, 1)
        .expect("insert");
    engine.flush();
    engine
        .try_upsert_live("package charger", 1, 2)
        .expect("upsert");
    engine.delete_by_logical_id(2).expect("delete");
    engine
}

fn what_is_acknowledged(engine: &Engine, when: &str) {
    assert_eq!(matches(engine, "brass compass"), [3], "{when}");
    assert_eq!(matches(engine, "package charger"), [1], "{when}");
    assert_eq!(matches(engine, "package adapter"), [0u64; 0], "{when}");
    assert_eq!(matches(engine, "vintage lamp"), [0u64; 0], "{when}");
}

fn the_manifest_directory_sync() -> Step {
    Step {
        name: "sync_dir",
        path: "manifest.bin".to_string(),
    }
}

fn steps(scope: &Scope) -> Vec<String> {
    scope.taken().iter().map(ToString::to_string).collect()
}

/// Every file under the data directory, with its bytes.
fn everything_in(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut held = Vec::new();
    for folder in [dir.to_path_buf(), dir.join("segments")] {
        for entry in std::fs::read_dir(&folder).expect("list").flatten() {
            let path = entry.path();
            if path.is_file() {
                let name = path.strip_prefix(dir).expect("under the directory");
                held.push((
                    name.display().to_string(),
                    std::fs::read(&path).expect("read"),
                ));
            }
        }
    }
    held.sort();
    held
}

/// The directory holds the files it held, with the bytes they had.
fn nothing_changed_since(dir: &std::path::Path, then: &[(String, Vec<u8>)]) {
    let now = everything_in(dir);
    assert_eq!(
        now.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        then.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "a file was added to or removed from a directory two manifests may describe"
    );
    for ((name, bytes), (_, before)) in now.iter().zip(then) {
        assert!(bytes == before, "{name} was written");
    }
}

/// Every file the manifest on disk names is on disk.
fn the_manifest_names_only_files_that_exist(dir: &std::path::Path) {
    let manifest = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        assert!(
            dir.join("segments").join(name).exists(),
            "the manifest names the segment {name}, which is gone"
        );
    }
    assert!(
        dir.join(&manifest.source_file_name).exists(),
        "the manifest names the source sidecar {}, which is gone",
        manifest.source_file_name
    );
}

/// After the failed sync: nothing was removed, the log was neither checkpointed nor reset,
/// and the files of the manifest that was replaced are still there.
fn nothing_either_manifest_names_was_touched(
    dir: &std::path::Path,
    scope: &Scope,
    before: &crate::storage::Manifest,
) {
    the_manifest_names_only_files_that_exist(dir);
    for name in &before.segment_files {
        assert!(
            dir.join("segments").join(name).exists(),
            "{name}, which the replaced manifest names, was removed"
        );
    }
    assert!(dir.join(&before.source_file_name).exists());
    let taken = steps(scope);
    assert!(
        !taken.iter().any(|step| step.starts_with("remove ")),
        "a file was removed while two manifests may name it: {taken:?}"
    );
    assert!(
        !taken.iter().any(|step| step == "rename wal.log"),
        "the log was reset, and the older manifest needs it: {taken:?}"
    );
    assert!(
        !taken.iter().any(|step| step == "append wal.log"),
        "a checkpoint was written to the log, which tells the older manifest's recovery to \
         skip records it needs: {taken:?}"
    );
}

#[test]
fn a_compaction_whose_manifest_was_renamed_and_not_synced_keeps_what_both_manifests_name() {
    let dir = scratch_dir("compaction");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let before = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    scope.reset();
    scope.fail(&the_manifest_directory_sync(), 0);
    engine.compact_all();
    assert!(scope.failed().is_some(), "the directory sync was reached");
    what_is_acknowledged(&engine, "after the compaction");
    nothing_either_manifest_names_was_touched(&dir, &scope, &before);
    assert!(!engine.persistence_healthy(), "the node says so");
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    assert_eq!(reopened.skipped_segments, 0, "no segment was skipped");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_flush_whose_manifest_was_renamed_and_not_synced_keeps_the_log_and_both_sets_of_files() {
    let dir = scratch_dir("flush");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let before = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    scope.reset();
    scope.fail(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some(), "the directory sync was reached");
    what_is_acknowledged(&engine, "after the flush");
    nothing_either_manifest_names_was_touched(&dir, &scope, &before);
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Once the outcome of a publication is not known, the data directory does not change until
/// a restart: every write is refused, by id or by position, and a flush, a compaction and a
/// bulk load write nothing. Reads go on. A restart has what was acknowledged, and commits.
#[test]
fn the_data_directory_does_not_change_until_a_restart_and_every_write_is_refused() {
    let dir = scratch_dir("read_only");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some());
    let unknown = everything_in(&dir);

    scope.reset();
    let refused = [
        engine
            .try_insert_live("copper kettle", 4, 1)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        engine
            .try_upsert_live("brass sextant", 3, 2)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        engine
            .delete_by_logical_id(1)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        engine
            .try_bulk_ingest(&[(7, "silver spoon".to_string())])
            .map(|_| ())
            .map_err(|error| error.to_string()),
    ];
    for (write, outcome) in ["insert", "upsert", "delete by id", "bulk load"]
        .iter()
        .zip(&refused)
    {
        let Err(why) = outcome else {
            panic!("a {write} was taken while two manifests may be read");
        };
        assert!(why.contains("read-only until a restart"), "{write}: {why}");
    }
    engine.flush();
    engine.compact_all();
    // The writer of the manifest refuses by itself, whatever led to it.
    assert!(!engine.save_manifest_if_persistent());
    let taken = steps(&scope);
    assert!(taken.is_empty(), "a step was taken: {taken:?}");
    nothing_changed_since(&dir, &unknown);
    the_manifest_names_only_files_that_exist(&dir);
    what_is_acknowledged(&engine, "reads go on");
    assert_eq!(matches(&engine, "copper kettle"), [0u64; 0], "refused");
    assert_eq!(matches(&engine, "silver spoon"), [0u64; 0], "refused");
    drop(engine);

    // A restart reads the disk, and then the engine takes writes and commits again.
    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    assert!(reopened.persistence_healthy());
    reopened
        .try_insert_live("copper kettle", 4, 1)
        .expect("a write after the restart");
    scope.reset();
    reopened.flush();
    let taken = steps(&scope);
    assert!(
        taken.iter().any(|step| step == "rename manifest.bin")
            && taken.iter().any(|step| step == "rename wal.log"),
        "after a restart a flush commits and resets the log: {taken:?}"
    );
    drop(reopened);
    let again = Engine::open(norm(), config(&dir)).expect("reopen again");
    what_is_acknowledged(&again, "after the next restart");
    assert_eq!(matches(&again, "copper kettle"), [4]);
    drop(again);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The other outcome: a power loss leaves the manifest that was replaced. Its files were
/// kept and the log is as it was at the failed sync, so an open over it has every
/// acknowledged write.
#[test]
fn the_replaced_manifest_and_the_log_have_every_acknowledged_write() {
    for commit in ["flush", "compaction"] {
        let dir = scratch_dir(&format!("power_loss_{commit}"));
        let scope = Scope::open(&dir);
        let mut engine = seeded(&dir);
        let replaced = std::fs::read(dir.join("manifest.bin")).expect("manifest");
        scope.fail(&the_manifest_directory_sync(), 0);
        if commit == "flush" {
            engine.flush();
        } else {
            engine.compact_all();
        }
        assert!(scope.failed().is_some(), "{commit}");
        assert_ne!(
            std::fs::read(dir.join("manifest.bin")).expect("manifest"),
            replaced,
            "{commit}: the new manifest is in place"
        );
        // What a node does before anyone restarts it. After the compaction the memtable
        // still holds rows, so this flush has a segment to seal.
        let unknown = everything_in(&dir);
        engine.flush();
        engine.compact_all();
        assert!(!engine.commit_sources_and_manifest());
        nothing_changed_since(&dir, &unknown);
        what_is_acknowledged(&engine, commit);
        drop(engine);

        std::fs::write(dir.join("manifest.bin"), &replaced).expect("the power loss");
        let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
        assert_eq!(reopened.skipped_segments, 0, "{commit}");
        what_is_acknowledged(&reopened, commit);
        drop(reopened);
        drop(scope);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A bulk load is not in the log, so its manifest is all that holds it. When that manifest
/// was renamed and not synced the caller is told the batch failed, the batch is not served,
/// and a restart serves it: the manifest on disk names it.
#[test]
fn a_bulk_load_whose_manifest_was_not_synced_is_not_acknowledged_and_a_restart_has_it() {
    let dir = scratch_dir("bulk");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail(&the_manifest_directory_sync(), 0);
    let error = engine
        .try_bulk_ingest(&[(7, "silver spoon".to_string())])
        .expect_err("the batch is not acknowledged");
    assert!(scope.failed().is_some(), "the directory sync was reached");
    assert!(
        error.to_string().contains("a restart serves the batch"),
        "{error}"
    );
    assert_eq!(matches(&engine, "silver spoon"), [0u64; 0], "not served");
    what_is_acknowledged(&engine, "after the bulk load");
    the_manifest_names_only_files_that_exist(&dir);
    // The batch's id is absent here and present in the manifest a restart reads. A create
    // of it admitted now would be a second row for the id after that restart.
    assert!(
        engine.try_insert_live("silver ladle", 7, 1).is_err(),
        "a create of an id the batch holds"
    );
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(
        matches(&reopened, "silver spoon"),
        [7],
        "the manifest names it"
    );
    assert_eq!(matches(&reopened, "silver ladle"), [0u64; 0]);
    what_is_acknowledged(&reopened, "after a restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A delete by position goes to the log like every other write, so it is refused too: a
/// position is a place in one manifest's layout, and the log may be replayed over either.
#[test]
fn a_delete_by_position_is_refused_while_two_manifests_may_be_read() {
    let dir = scratch_dir("positional");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let address = engine
        .segment_address(1, 0, 3)
        .expect("an address before the failed sync");
    scope.fail(&the_manifest_directory_sync(), 0);
    // A commit that replaces no segment, so the address stays one the engine would take.
    assert!(
        !engine.commit_sources_and_manifest(),
        "the commit is reported as failed"
    );
    assert!(scope.failed().is_some(), "the directory sync was reached");
    assert!(
        engine
            .segment_generations
            .iter()
            .any(|live| std::sync::Arc::ptr_eq(live, &address.generation)),
        "the address's segment is still there"
    );

    let by_position = engine.tombstone(0).expect_err("a memtable position");
    assert!(
        by_position
            .to_string()
            .contains("read-only until a restart"),
        "{by_position}"
    );
    assert!(
        matches!(
            engine.tombstone_in(&address),
            Err(crate::error::TombstoneError::Wal(_))
        ),
        "a segment address"
    );
    what_is_acknowledged(&engine, "nothing was deleted");
    drop(engine);

    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    // After the restart positions are addresses again.
    let address = reopened
        .segment_address(1, 0, 3)
        .expect("an address after the restart");
    reopened
        .tombstone_in(&address)
        .expect("a delete by position");
    assert_eq!(matches(&reopened, "brass compass"), [0u64; 0]);
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An open publishes the manifest it read again before it builds on it, with the bytes it
/// has, and fails when it cannot: the one there may never have been made durable, and a
/// sync alone can report success over state the kernel has given up on.
#[test]
fn an_open_publishes_its_manifest_again_and_fails_when_it_cannot() {
    let dir = scratch_dir("open");
    let scope = Scope::open(&dir);
    drop(seeded(&dir));
    let before = std::fs::read(dir.join("manifest.bin")).expect("manifest");
    scope.reset();
    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after an open");
    drop(reopened);
    let publication = [
        "create manifest.manifest.tmp",
        "sync manifest.manifest.tmp",
        "rename manifest.bin",
        "sync_dir manifest.bin",
    ];
    let taken = steps(&scope);
    assert_eq!(
        taken.iter().take(4).map(String::as_str).collect::<Vec<_>>(),
        publication,
        "the publication comes before anything else"
    );
    assert_eq!(
        std::fs::read(dir.join("manifest.bin")).expect("manifest"),
        before,
        "the bytes are the ones that were read"
    );

    for step in publication {
        let (name, path) = step.split_once(' ').expect("a step is a name and a path");
        scope.reset();
        scope.fail(
            &Step {
                name: match name {
                    "create" => "create",
                    "sync" => "sync",
                    "rename" => "rename",
                    _ => "sync_dir",
                },
                path: path.to_string(),
            },
            0,
        );
        let Err(refused) = Engine::open(norm(), config(&dir)) else {
            panic!("an open whose manifest could not be published again ({step})");
        };
        assert!(
            refused.to_string().contains("again before opening"),
            "{step}: {refused}"
        );
        assert!(scope.failed().is_some(), "{step}: the step was reached");
        let reopened = Engine::open(norm(), config(&dir)).expect("the next open");
        what_is_acknowledged(&reopened, step);
    }
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A manifest that was not renamed is not in effect: the commit is rolled back as before,
/// what it wrote is removed, and the engine goes on committing.
#[test]
fn a_commit_whose_manifest_was_not_renamed_is_rolled_back_and_the_next_one_commits() {
    let dir = scratch_dir("not_renamed");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let before = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    scope.reset();
    scope.fail(
        &Step {
            name: "rename",
            path: "manifest.bin".to_string(),
        },
        0,
    );
    assert!(engine
        .try_bulk_ingest(&[(7, "silver spoon".to_string())])
        .is_err());
    assert!(scope.failed().is_some(), "the rename was reached");
    assert_eq!(matches(&engine, "silver spoon"), [0u64; 0], "rolled back");
    let after = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_eq!(after.segment_files, before.segment_files);
    let taken = steps(&scope);
    assert!(
        taken
            .iter()
            .any(|step| step.starts_with("remove segments/")),
        "the segment the batch wrote is removed: {taken:?}"
    );
    what_is_acknowledged(&engine, "after the refused batch");

    engine
        .try_bulk_ingest(&[(7, "silver spoon".to_string())])
        .expect("the next batch commits");
    drop(engine);
    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    assert_eq!(matches(&reopened, "silver spoon"), [7]);
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}
