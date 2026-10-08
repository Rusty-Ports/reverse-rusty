//! ADR-222: a manifest that was renamed into place and whose directory sync failed is not a
//! failed commit and not a finished one. The engine keeps every file both manifests may
//! name, commits nothing more and resets no log for the rest of the process, and a restart
//! reads what is on disk.

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

/// Once the outcome of a publication is not known, nothing more is committed: a commit on
/// top of either manifest would have to be right for both. Writes that name a row by its id
/// go on, in the log, and a restart has all of them.
#[test]
fn nothing_more_is_committed_until_a_restart_and_writes_by_id_go_on() {
    let dir = scratch_dir("no_more_commits");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some());
    let unknown = std::fs::read(dir.join("manifest.bin")).expect("manifest");

    scope.reset();
    engine
        .try_insert_live("copper kettle", 4, 1)
        .expect("an insert is taken");
    engine
        .try_upsert_live("brass sextant", 3, 2)
        .expect("an upsert is taken");
    engine
        .delete_by_logical_id(1)
        .expect("a delete by id is taken");
    engine.flush();
    engine.compact_all();
    let bulk = engine
        .try_bulk_ingest(&[(7, "silver spoon".to_string())])
        .expect_err("a bulk load is refused");
    assert!(bulk.to_string().contains("until a restart"), "{bulk}");
    let taken = steps(&scope);
    assert!(
        !taken.iter().any(|step| step.contains("manifest")),
        "a manifest was written on top of one whose outcome is not known: {taken:?}"
    );
    assert!(
        !taken
            .iter()
            .any(|step| step.starts_with("remove ") || step == "rename wal.log"),
        "{taken:?}"
    );
    assert_eq!(
        std::fs::read(dir.join("manifest.bin")).expect("manifest"),
        unknown,
        "the manifest on disk is the one that was renamed"
    );
    the_manifest_names_only_files_that_exist(&dir);
    assert_eq!(matches(&engine, "copper kettle"), [4]);
    assert_eq!(matches(&engine, "brass sextant"), [3]);
    assert_eq!(
        matches(&engine, "silver spoon"),
        [0u64; 0],
        "the refused batch"
    );
    drop(engine);

    // A restart reads the disk, and then the engine commits again.
    let mut reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(matches(&reopened, "copper kettle"), [4]);
    assert_eq!(matches(&reopened, "brass sextant"), [3]);
    assert_eq!(matches(&reopened, "brass compass"), [0u64; 0]);
    assert_eq!(matches(&reopened, "package charger"), [0u64; 0], "deleted");
    assert_eq!(matches(&reopened, "silver spoon"), [0u64; 0]);
    assert!(reopened.persistence_healthy());
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
    assert_eq!(matches(&again, "copper kettle"), [4]);
    assert_eq!(matches(&again, "brass sextant"), [3]);
    drop(again);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(
        matches(&reopened, "silver spoon"),
        [7],
        "the manifest names it"
    );
    what_is_acknowledged(&reopened, "after a restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A position is a place in one manifest's layout. While two manifests may be the one a
/// recovery reads, a delete by position is refused and a delete by id is taken, and the
/// delete by id holds after a restart.
#[test]
fn a_delete_by_position_is_refused_while_two_manifests_may_be_read() {
    let dir = scratch_dir("positional");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let address = engine
        .segment_address(1, 0, 3)
        .expect("an address before the failed sync");
    scope.fail(&the_manifest_directory_sync(), 0);
    // A compaction of nothing new: the vocabulary-free way to publish a manifest without
    // replacing the segment the address is in.
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
    engine
        .try_insert_live("copper kettle", 4, 1)
        .expect("a write is taken");

    let by_position = engine.tombstone(0).expect_err("a memtable position");
    assert!(
        by_position.to_string().contains("delete by logical id"),
        "{by_position}"
    );
    assert!(engine.tombstone_in(&address).is_err(), "a segment address");
    for segment in 0..engine.segments.len() {
        assert!(
            engine.segment_address(segment, 0, 3).is_err(),
            "no address is given out for segment {segment}"
        );
    }
    assert_eq!(
        matches(&engine, "copper kettle"),
        [4],
        "nothing was deleted"
    );
    assert_eq!(
        matches(&engine, "brass compass"),
        [3],
        "nothing was deleted"
    );

    assert!(engine.delete_by_logical_id(3).expect("a delete by id") > 0);
    drop(engine);
    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(matches(&reopened, "brass compass"), [0u64; 0]);
    assert_eq!(matches(&reopened, "copper kettle"), [4]);
    assert_eq!(matches(&reopened, "package charger"), [1]);
    // The restart synced the directory, so positions are addresses again.
    assert!(reopened.segment_address(0, 0, 1).is_ok() || reopened.segments.len() > 1);
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An open syncs the directories before it builds on what they hold, and fails when it
/// cannot: the manifest there may be one whose rename was never made durable.
#[test]
fn an_open_syncs_the_directory_first_and_fails_when_it_cannot() {
    let dir = scratch_dir("open");
    let scope = Scope::open(&dir);
    drop(seeded(&dir));
    scope.reset();
    drop(Engine::open(norm(), config(&dir)).expect("reopen"));
    let taken = steps(&scope);
    assert_eq!(
        taken.first().map(String::as_str),
        Some("sync_dir "),
        "the directory is synced before anything else: {taken:?}"
    );
    assert!(taken.iter().any(|step| step == "sync_dir segments"));

    scope.fail(
        &Step {
            name: "sync_dir",
            path: String::new(),
        },
        0,
    );
    let Err(refused) = Engine::open(norm(), config(&dir)) else {
        panic!("an open over a directory that cannot be synced");
    };
    assert!(
        refused.to_string().contains("before opening it"),
        "{refused}"
    );
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
