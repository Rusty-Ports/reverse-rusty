//! ADR-222: a manifest that was renamed into place is in effect, whatever the directory
//! sync after the rename says. The files it names are not removed, the files of the
//! manifest it replaced are kept while it may come back, and the log is not reset.

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

#[test]
fn a_compaction_whose_manifest_was_renamed_and_not_synced_keeps_what_the_manifest_names() {
    let dir = scratch_dir("compaction");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail(&the_manifest_directory_sync(), 0);
    engine.compact_all();
    assert!(scope.failed().is_some(), "the directory sync was reached");
    what_is_acknowledged(&engine, "after the compaction");
    the_manifest_names_only_files_that_exist(&dir);
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    assert_eq!(reopened.skipped_segments, 0, "no segment was skipped");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_flush_whose_manifest_was_renamed_and_not_synced_keeps_the_log_and_the_old_files() {
    let dir = scratch_dir("flush");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    let before = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    scope.reset();
    // A flush commits twice (the seal, then the merge that follows it). The directory
    // goes on refusing its sync, so neither is known to be on disk.
    scope.fail_from(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some(), "the directory sync was reached");
    what_is_acknowledged(&engine, "after the flush");
    the_manifest_names_only_files_that_exist(&dir);
    // The manifest that was replaced may be the one a power loss brings back: its files
    // and the log it needs are still there.
    for name in &before.segment_files {
        assert!(
            dir.join("segments").join(name).exists(),
            "{name} was removed"
        );
    }
    assert!(dir.join(&before.source_file_name).exists());
    let taken: Vec<String> = scope.taken().iter().map(ToString::to_string).collect();
    assert!(
        !taken.iter().any(|step| step == "rename wal.log"),
        "the log was reset although the commit before it is not known to be on disk: {taken:?}"
    );
    assert!(
        !taken.iter().any(|step| step == "append wal.log"),
        "a checkpoint was written to the log, which tells the older manifest's recovery to \
         skip records it needs: {taken:?}"
    );
    assert!(
        !taken.iter().any(|step| step.starts_with("remove ")),
        "a file was removed while two manifests may name it: {taken:?}"
    );
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_next_commit_that_is_synced_ends_the_keeping() {
    let dir = scratch_dir("next_commit");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail_from(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some());
    engine
        .try_insert_live("copper kettle", 4, 1)
        .expect("a write after the failed sync");
    // The directory takes syncs again.
    scope.reset();
    engine.flush();
    let taken: Vec<String> = scope.taken().iter().map(ToString::to_string).collect();
    assert!(
        taken.iter().any(|step| step == "rename wal.log"),
        "a commit that was synced resets the log again: {taken:?}"
    );
    the_manifest_names_only_files_that_exist(&dir);
    drop(engine);
    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    what_is_acknowledged(&reopened, "after a restart");
    assert_eq!(matches(&reopened, "copper kettle"), [4]);
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A bulk load is not in the log, so its manifest is all that holds it. When that manifest
/// was renamed and not synced the batch is in effect, and the caller is told that it is
/// not known to be on disk.
#[test]
fn a_bulk_load_whose_manifest_was_not_synced_is_served_and_not_acknowledged() {
    let dir = scratch_dir("bulk");
    let scope = Scope::open(&dir);
    let mut engine = seeded(&dir);
    scope.fail_from(&the_manifest_directory_sync(), 0);
    let outcome = engine.try_bulk_ingest(&[(7, "silver spoon".to_string())]);
    assert!(scope.failed().is_some(), "the directory sync was reached");
    let error = outcome.expect_err("the batch is not acknowledged as durable");
    assert!(
        error.to_string().contains("not known to be on disk"),
        "{error}"
    );
    assert_eq!(matches(&engine, "silver spoon"), [7], "it is served");
    what_is_acknowledged(&engine, "after the bulk load");
    the_manifest_names_only_files_that_exist(&dir);
    drop(engine);

    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
    assert_eq!(
        matches(&reopened, "silver spoon"),
        [7],
        "and after a restart"
    );
    what_is_acknowledged(&reopened, "after a restart");
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A manifest that was not renamed is not in effect: the commit is rolled back as before,
/// and what it wrote is removed.
#[test]
fn a_commit_whose_manifest_was_not_renamed_is_rolled_back() {
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
    let taken: Vec<String> = scope.taken().iter().map(ToString::to_string).collect();
    assert!(
        taken
            .iter()
            .any(|step| step.starts_with("remove segments/")),
        "the segment the batch wrote is removed: {taken:?}"
    );
    what_is_acknowledged(&engine, "after the refused batch");
    drop(engine);
    let reopened = Engine::open(norm(), config(&dir)).expect("reopen");
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
    // No merge after the flush: the segment that holds id 3 is the same segment before
    // and after, so an address minted now is refused for the manifest's sake and not
    // because its segment was replaced.
    engine.set_config(EngineConfig {
        auto_compact_on_flush: false,
        ..config(&dir)
    });
    let address = engine
        .segment_address(1, 0, 3)
        .expect("an address before the failed sync");
    scope.fail_from(&the_manifest_directory_sync(), 0);
    engine.flush();
    assert!(scope.failed().is_some(), "the directory sync was reached");
    engine
        .try_insert_live("copper kettle", 4, 1)
        .expect("a write is taken");

    let by_position = engine.tombstone(0).expect_err("a memtable position");
    assert!(
        by_position.to_string().contains("delete by logical id"),
        "{by_position}"
    );
    assert!(
        engine
            .segment_generations
            .iter()
            .any(|live| std::sync::Arc::ptr_eq(live, &address.generation)),
        "the address's segment is still there"
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
    drop(reopened);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
}
