//! ADR-221: every step of every operation of a durable single-node engine, failed in turn.
//!
//! The same matrix as the cluster's (`cluster/coordinator/tests/crash_matrix.rs`), for one
//! engine: learn the steps an operation takes, then fail each of them and crash at once,
//! after more writes, or after a retry. What reopens must hold every acknowledged write and
//! no acknowledged delete, take a write and a flush, and reopen the same again.
//!
//! Two operations have one more rule. A commit whose manifest was renamed and whose
//! directory sync failed has two outcomes, and both are opened: the manifest that a restart
//! reads, and the one it replaced, which a power loss may bring back (ADR-222). A backup's
//! destination, when there is one, opens as an engine that holds what the engine held.

use crate::config::EngineConfig;
use crate::fault::model::{Acknowledged, TEXTS};
use crate::fault::{Scope, Step};
use crate::normalize::Normalizer;
use crate::segment::{Engine, MatchScratch};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocabulary")
}

fn config(data: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(data.to_path_buf()),
        // Every append is then two steps, the write and its sync.
        wal_sync_on_write: true,
        ..EngineConfig::default()
    }
}

/// A directory of its own for each call, holding the engine's `data` and, for a backup, its
/// destination, so one scope sees both.
fn fresh_root(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root =
        std::env::temp_dir().join(format!("rr_engine_matrix_{tag}_{n}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create the root");
    root
}

fn matched(engine: &Engine, title: &str) -> Vec<u64> {
    let (mut scratch, mut out) = (MatchScratch::new(), Vec::new());
    engine.match_title(title, &mut scratch, &mut out, true);
    out
}

fn settle(acked: &Acknowledged, engine: &Engine) -> Result<Acknowledged, String> {
    acked.settle(|title| Ok(matched(engine, title)))
}

fn insert(engine: &mut Engine, acked: &mut Acknowledged, id: u64, text: &str) -> bool {
    let outcome = engine.try_insert_live(text, id, 1);
    acked.note(id, &outcome, Some(text));
    outcome.is_ok()
}

fn upsert(engine: &mut Engine, acked: &mut Acknowledged, id: u64, text: &str) -> bool {
    let outcome = engine.try_upsert_live(text, id, 9);
    acked.note(id, &outcome, Some(text));
    outcome.is_ok()
}

fn delete(engine: &mut Engine, acked: &mut Acknowledged, id: u64) -> bool {
    let outcome = engine.delete_by_logical_id(id);
    acked.note(id, &outcome, None);
    outcome.is_ok()
}

/// Insert a row and then delete it by its position in the memtable. Replay has to apply
/// that delete to the row it named, whatever was sealed, merged or committed in between
/// (ADR-223). Either write may be refused; the model keeps what each one said.
fn insert_and_delete_by_position(engine: &mut Engine, acked: &mut Acknowledged, id: u64) {
    let inserted = engine.try_insert_live("vintage lamp", id, 1);
    acked.note(id, &inserted, Some("vintage lamp"));
    if let Ok(crate::segment::InsertOutcome::Inserted(stored)) = inserted {
        let deleted = engine.tombstone(stored.local);
        acked.note(id, &deleted, None);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Insert,
    Upsert,
    Delete,
    BulkLoad,
    Flush,
    Compact,
    Backup,
}

impl Op {
    const ALL: [Op; 7] = [
        Op::Insert,
        Op::Upsert,
        Op::Delete,
        Op::BulkLoad,
        Op::Flush,
        Op::Compact,
        Op::Backup,
    ];

    /// Run the operation. Returns whether it said it succeeded, for the two that say.
    fn run(self, engine: &mut Engine, acked: &mut Acknowledged, root: &Path) -> bool {
        match self {
            Op::Insert => insert(engine, acked, 4, "copper kettle"),
            Op::Upsert => upsert(engine, acked, 3, "brass sextant"),
            Op::Delete => delete(engine, acked, 1),
            Op::BulkLoad => {
                let outcome = engine.try_bulk_ingest(&[(6, "silver spoon".to_string())]);
                acked.note(6, &outcome, Some("silver spoon"));
                outcome.is_ok()
            }
            Op::Flush => {
                engine.flush();
                true
            }
            Op::Compact => {
                engine.compact_all();
                true
            }
            Op::Backup => engine.backup_to(&root.join("backup")).is_ok(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Then {
    Crash,
    WriteThenCrash,
    RetryThenCrash,
}

/// A durable engine with two segments on disk and a tail of every kind of write in its log.
fn seeded(root: &Path) -> (Engine, Acknowledged) {
    let corpus = [(1, TEXTS[0].to_string()), (2, TEXTS[1].to_string())];
    let mut engine = Engine::with_config(norm(), config(&root.join("data")));
    engine.try_bulk_ingest(&corpus).expect("seed bulk load");
    let mut acked = Acknowledged::of(&corpus);
    assert!(insert(&mut engine, &mut acked, 3, "brass compass"));
    engine.flush();
    assert!(upsert(&mut engine, &mut acked, 1, "package charger"));
    assert!(delete(&mut engine, &mut acked, 2));
    // Ids written twice in the tail: a replay that applied only the first record of one, or
    // both as rows, would show.
    assert!(insert(&mut engine, &mut acked, 8, "vintage lamp"));
    assert!(upsert(&mut engine, &mut acked, 8, "silver spoon"));
    assert!(insert(&mut engine, &mut acked, 9, "brass sextant"));
    assert!(delete(&mut engine, &mut acked, 9));
    // And one that only the log holds: a commit that does not seal the memtable (a merge, a
    // bulk load) moves the watermark past this record, and replay still owes it.
    assert!(insert(&mut engine, &mut acked, 10, "copper kettle"));
    insert_and_delete_by_position(&mut engine, &mut acked, 11);
    (engine, acked)
}

/// A step's path with the part a backup makes up each time (its staging directory's process
/// id and sequence number) left out, so that the same step has the same path twice.
fn stable(path: &str) -> String {
    const STAGING: &str = ".backup.tmp.";
    match path.find(STAGING) {
        Some(at) => {
            let rest = &path[at + STAGING.len()..];
            let after = rest.find('/').map_or("", |slash| &rest[slash..]);
            format!("{}{STAGING}{after}", &path[..at])
        }
        None => path.to_string(),
    }
}

fn same_step(a: &Step, b: &Step) -> bool {
    a.name == b.name && stable(&a.path) == stable(&b.path)
}

/// The steps `op` takes on a seeded engine when nothing fails.
fn steps_of(op: Op) -> Vec<Step> {
    let root = fresh_root(&format!("steps_{op:?}"));
    let scope = Scope::open(&root);
    let (mut engine, mut acked) = seeded(&root);
    scope.reset();
    assert!(
        op.run(&mut engine, &mut acked, &root),
        "{op:?} with no fault"
    );
    let steps = scope.taken();
    drop(engine);
    drop(scope);
    let _ = std::fs::remove_dir_all(&root);
    steps
}

/// Reopen the engine under `root` and require the allowed state, a working write, a flush,
/// and the same again after one more reopen.
fn reopens(root: &Path, acked: &Acknowledged) -> Result<(), String> {
    let data = root.join("data");
    let mut engine = Engine::open(norm(), config(&data)).map_err(|e| format!("open: {e}"))?;
    // From here on every id has one state: the one this open found.
    let mut acked = settle(acked, &engine)?;
    if !insert(&mut engine, &mut acked, 90, "silver spoon") {
        return Err("a write after reopening was refused".into());
    }
    engine.flush();
    settle(&acked, &engine).map_err(|e| format!("after the flush: {e}"))?;
    drop(engine);
    let again = Engine::open(norm(), config(&data)).map_err(|e| format!("second open: {e}"))?;
    settle(&acked, &again)
        .map(|_| ())
        .map_err(|e| format!("after the second open: {e}"))
}

/// A backup that said it succeeded left a destination, and a destination, whenever there
/// is one, opens as an engine that holds what `at_the_backup` allows. A backup can say it
/// failed and leave a whole destination: its last step syncs the directory the destination
/// was renamed into, and that sync can fail.
fn the_backup_is_whole_or_absent(
    root: &Path,
    succeeded: bool,
    at_the_backup: &Acknowledged,
) -> Result<(), String> {
    let dest = root.join("backup");
    if !dest.exists() {
        return if succeeded {
            Err("a backup that said it succeeded left no destination".into())
        } else {
            Ok(())
        };
    }
    let restored =
        Engine::open(norm(), config(&dest)).map_err(|e| format!("opening the backup: {e}"))?;
    settle(at_the_backup, &restored)
        .map(|_| ())
        .map_err(|e| format!("the backup: {e}"))
}

/// Copy the files of `from` into `to`, one level of directories down.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create");
    for entry in std::fs::read_dir(from).expect("list").flatten() {
        let (path, dest) = (entry.path(), to.join(entry.file_name()));
        if path.is_dir() {
            copy_tree(&path, &dest);
        } else {
            std::fs::copy(&path, &dest).expect("copy");
        }
    }
}

/// The other outcome of a manifest that was renamed and not synced: a power loss leaves the
/// manifest it replaced (`replaced`, or none). A copy of the store with that manifest put
/// back has to reopen as well as the store itself.
fn the_replaced_manifest_reopens(
    root: &Path,
    replaced: Option<&[u8]>,
    acked: &Acknowledged,
) -> Result<(), String> {
    let lost = root.join("power_loss");
    copy_tree(&root.join("data"), &lost.join("data"));
    let manifest = lost.join("data").join("manifest.bin");
    match replaced {
        Some(bytes) => std::fs::write(&manifest, bytes).expect("put the manifest back"),
        None => std::fs::remove_file(&manifest).expect("take the manifest away"),
    }
    reopens(&lost, acked).map_err(|e| format!("with the replaced manifest: {e}"))
}

/// One case: `op` with a fault at the `nth` step it takes, then `then`.
fn case(op: Op, steps: &[Step], nth: usize, then: Then) -> Result<(), String> {
    let step = steps[nth].clone();
    let earlier = steps[..nth]
        .iter()
        .filter(|other| same_step(other, &step))
        .count();
    let root = fresh_root(&format!("{op:?}_{nth}_{then:?}"));
    let scope = Scope::open(&root);
    let (mut engine, mut acked) = seeded(&root);
    scope.reset();
    // An operation can publish more than one manifest (a flush, then the compaction it
    // starts). The one a publication replaces is the one in place at its rename.
    let replaced = std::sync::Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let (planned, at_the_rename, manifest) = (
        step.clone(),
        std::sync::Arc::clone(&replaced),
        root.join("data").join("manifest.bin"),
    );
    scope.fail_where(
        move |taken| {
            if taken.name == "rename" && taken.path == "data/manifest.bin" {
                *at_the_rename.lock().expect("lock") = std::fs::read(&manifest).ok();
            }
            same_step(taken, &planned)
        },
        earlier,
    );
    let at_the_op = acked.clone();
    let succeeded = op.run(&mut engine, &mut acked, &root);
    if !scope
        .failed()
        .is_some_and(|failed| same_step(&failed, &step))
    {
        return Err(
            "the operation did not reach the step: its steps are not the same twice".into(),
        );
    }
    if op == Op::Backup {
        the_backup_is_whole_or_absent(&root, succeeded, &at_the_op)?;
    }
    match then {
        Then::Crash => {}
        Then::WriteThenCrash => {
            // Each is acknowledged or refused; what was acknowledged has to survive.
            insert(&mut engine, &mut acked, 5, "copper kettle");
            upsert(&mut engine, &mut acked, 3, "brass sextant");
            delete(&mut engine, &mut acked, 1);
            // A bulk load is in its manifest and not in the log. After one that said it
            // failed, its id is absent here and may be present in what a restart reads, so
            // a create of that id is either refused or the only row for it.
            if op == Op::BulkLoad && !succeeded {
                insert(&mut engine, &mut acked, 6, "brass compass");
            }
            // A position in the memtable as it is now: after a flush that failed and the
            // merge that followed, the log still holds the records of rows that are gone.
            insert_and_delete_by_position(&mut engine, &mut acked, 12);
            settle(&acked, &engine).map_err(|e| format!("before the crash: {e}"))?;
        }
        Then::RetryThenCrash => {
            op.run(&mut engine, &mut acked, &root);
            settle(&acked, &engine)
                .map_err(|e| format!("before the crash, after the retry: {e}"))?;
        }
    }
    drop(engine);
    // The scope stays open with nothing planned: what follows is then not really synced.
    let manifest_not_synced = step.name == "sync_dir" && step.path == "data/manifest.bin";
    let outcome = if manifest_not_synced {
        let replaced = replaced.lock().expect("lock").clone();
        the_replaced_manifest_reopens(&root, replaced.as_deref(), &acked)
    } else {
        Ok(())
    }
    .and_then(|()| reopens(&root, &acked));
    drop(scope);
    let _ = std::fs::remove_dir_all(&root);
    outcome
}

/// Run every step of `op` each way, and list what failed.
fn every_step_of(op: Op) -> Vec<String> {
    let steps = steps_of(op);
    let mut failures = Vec::new();
    for nth in 0..steps.len() {
        for then in [Then::Crash, Then::WriteThenCrash, Then::RetryThenCrash] {
            if let Err(what) = case(op, &steps, nth, then) {
                failures.push(format!(
                    "{op:?}, fault at step {nth} `{}`, then {then:?}: {what}",
                    steps[nth]
                ));
            }
        }
    }
    failures
}

fn assert_none(failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn a_write_failed_at_any_step_loses_nothing_acknowledged() {
    for op in [Op::Insert, Op::Upsert, Op::Delete] {
        assert_none(&every_step_of(op));
    }
}

#[test]
fn a_bulk_load_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::BulkLoad));
}

#[test]
fn a_flush_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::Flush));
}

#[test]
fn a_compaction_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::Compact));
}

#[test]
fn a_backup_failed_at_any_step_is_whole_or_absent_and_harms_nothing() {
    assert_none(&every_step_of(Op::Backup));
}

/// A backup that is not interrupted opens as an engine that holds what the engine held.
#[test]
fn a_whole_backup_opens() {
    let root = fresh_root("whole_backup");
    let scope = Scope::open(&root);
    let (mut engine, mut acked) = seeded(&root);
    let at_the_backup = acked.clone();
    assert!(Op::Backup.run(&mut engine, &mut acked, &root));
    the_backup_is_whole_or_absent(&root, true, &at_the_backup).expect("the backup");
    drop(engine);
    drop(scope);
    let _ = std::fs::remove_dir_all(&root);
}

/// How many steps of each name an operation takes, so that a kind of step which disappears
/// or stops being named is noticed.
#[test]
fn the_steps_of_each_operation_are_the_ones_written_here() {
    let mut names = BTreeSet::new();
    let mut total = 0;
    for op in Op::ALL {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for step in steps_of(op) {
            *counts.entry(step.name).or_default() += 1;
        }
        names.extend(counts.keys().copied());
        total += counts.values().sum::<usize>();
        println!("{op:?}: {counts:?}");
    }
    println!("the operations on an engine: {total} steps");
    assert_eq!(
        names.into_iter().collect::<Vec<_>>(),
        ["append", "copy", "create", "remove", "rename", "sync", "sync_dir"],
        "every kind of step is taken by some operation in the matrix"
    );
    // A floor, not the exact number, which moves with every file an operation writes.
    assert!(total > 80, "{total}");
}
