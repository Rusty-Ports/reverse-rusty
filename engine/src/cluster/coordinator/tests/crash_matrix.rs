//! ADR-221: every step of every cluster operation, failed in turn.
//!
//! For each operation the test first runs it once on a seeded durable cluster, with a scope
//! recording, to learn which steps it takes. Then, for each of those steps, it takes a fresh
//! copy of that cluster, plans a fault at the step, runs the operation, and goes one of three
//! ways: crash at once; keep serving, write, then crash; retry the operation, then crash. A
//! crash is the engine dropped with no checkpoint.
//!
//! What reopens must hold every acknowledged write and no acknowledged remove, must take a
//! write and a checkpoint, and must reopen the same again. Nobody has to think of the window:
//! a step added to an operation is in the list the next time the test runs.

use super::*;
use crate::fault::model::{Acknowledged, TEXTS};
use crate::fault::{Scope, Step};
use std::collections::{BTreeMap, BTreeSet};

const SHARDS: usize = 3;

/// The model's writes, made on a cluster.
struct Writes<'a>(&'a ClusterEngine);

impl Writes<'_> {
    fn add(&self, acked: &mut Acknowledged, id: u64, text: &str) -> Result<(), ShardError> {
        let outcome = self.0.add_query(id, text).map(|_| ());
        acked.note(id, &outcome, Some(text));
        outcome
    }

    fn upsert(&self, acked: &mut Acknowledged, id: u64, text: &str) -> Result<(), ShardError> {
        let outcome = self.0.upsert_query(id, text, 9).map(|_| ());
        acked.note(id, &outcome, Some(text));
        outcome
    }

    fn remove(&self, acked: &mut Acknowledged, id: u64) -> Result<(), ShardError> {
        let outcome = self.0.remove_query(id).map(|_| ());
        acked.note(id, &outcome, None);
        outcome
    }
}

/// `cluster` holds one of the states the model allows for every id; returns those states.
fn settle(acked: &Acknowledged, cluster: &ClusterEngine) -> Result<Acknowledged, String> {
    acked.settle(|title| cluster.percolate(title).map_err(|e| e.to_string()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Add,
    Upsert,
    Remove,
    Checkpoint,
    Grow,
    Shrink,
    AliasImport,
}

impl Op {
    const ALL: [Op; 7] = [
        Op::Add,
        Op::Upsert,
        Op::Remove,
        Op::Checkpoint,
        Op::Grow,
        Op::Shrink,
        Op::AliasImport,
    ];

    fn run(self, cluster: &ClusterEngine, acked: &mut Acknowledged) -> Result<(), ShardError> {
        match self {
            Op::Add => Writes(cluster).add(acked, 4, "copper kettle"),
            Op::Upsert => Writes(cluster).upsert(acked, 3, "brass sextant"),
            Op::Remove => Writes(cluster).remove(acked, 1),
            Op::Checkpoint => cluster.checkpoint(),
            Op::Grow => cluster.resize(SHARDS + 2).map(|_| ()),
            Op::Shrink => cluster.resize(SHARDS - 1).map(|_| ()),
            Op::AliasImport => cluster.import_alias_synonyms("kettle, pot").map(|_| ()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Then {
    /// The step fails, the operation handles that, and the cluster is dropped.
    Crash,
    WriteThenCrash,
    RetryThenCrash,
    /// The step and every step after it fail: on disk, the process died at the step. An
    /// operation whose step fails goes on to what it does about that, so this is the only
    /// way to stop between two steps.
    Stop,
}

fn config(dir: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        num_shards: SHARDS,
        data_dir: Some(dir.to_path_buf()),
        // Every append is then two steps, the write and its sync.
        wal_sync_on_write: true,
        ..Default::default()
    }
}

/// A directory of its own for each call: tests run side by side, and two of them ask for
/// the steps of the same operation.
fn fresh_dir(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    scratch_dir(&format!("crash_matrix_{tag}_{n}"))
}

/// A durable cluster with a base, and a tail of every kind of write in its log.
fn seeded(dir: &std::path::Path) -> (ClusterEngine, Acknowledged) {
    let corpus = [(1, TEXTS[0].to_string()), (2, TEXTS[1].to_string())];
    let cluster = ClusterEngine::build(vocab(), &config(dir), &corpus).expect("seed build");
    let mut acked = Acknowledged::of(&corpus);
    let writes = Writes(&cluster);
    writes
        .add(&mut acked, 3, "brass compass")
        .expect("seed add");
    writes
        .upsert(&mut acked, 1, "package charger")
        .expect("seed upsert");
    writes.remove(&mut acked, 2).expect("seed remove");
    (cluster, acked)
}

/// The steps `op` takes on a seeded cluster when nothing fails.
fn steps_of(op: Op) -> Vec<Step> {
    let dir = fresh_dir(&format!("steps_{op:?}"));
    let scope = Scope::open(&dir);
    let (cluster, mut acked) = seeded(&dir);
    scope.reset();
    op.run(&cluster, &mut acked)
        .unwrap_or_else(|e| panic!("{op:?} with no fault: {e}"));
    let steps = scope.taken();
    drop(cluster);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
    steps
}

/// Reopen `dir` and require the acknowledged state, a working write, a checkpoint, and the
/// same again after one more reopen.
fn reopens(dir: &std::path::Path, acked: &Acknowledged) -> Result<(), String> {
    let cluster = ClusterEngine::open(dir, vocab(), None).map_err(|e| format!("open: {e}"))?;
    // From here on every id has one state: the one this open found.
    let mut acked = settle(acked, &cluster)?;
    Writes(&cluster)
        .add(&mut acked, 90, "silver spoon")
        .map_err(|e| format!("a write after reopening: {e}"))?;
    cluster
        .checkpoint()
        .map_err(|e| format!("a checkpoint after reopening: {e}"))?;
    settle(&acked, &cluster).map_err(|e| format!("after the checkpoint: {e}"))?;
    drop(cluster);
    let again = ClusterEngine::open(dir, vocab(), None).map_err(|e| format!("second open: {e}"))?;
    settle(&acked, &again)
        .map(|_| ())
        .map_err(|e| format!("after the second open: {e}"))
}

/// One case: `op` with a fault at the `nth` step it takes, then `then`.
fn case(op: Op, steps: &[Step], nth: usize, then: Then) -> Result<(), String> {
    let step = &steps[nth];
    let earlier = steps[..nth].iter().filter(|other| *other == step).count();
    let dir = fresh_dir(&format!("{op:?}_{nth}_{then:?}"));
    let scope = Scope::open(&dir);
    let (cluster, mut acked) = seeded(&dir);
    scope.reset();
    if matches!(then, Then::Stop) {
        let planned = step.clone();
        scope.stop_at(move |taken| *taken == planned, earlier);
    } else {
        scope.fail(step, earlier);
    }
    let _failed_or_absorbed = op.run(&cluster, &mut acked);
    if scope.failed().as_ref() != Some(step) {
        return Err(
            "the operation did not reach the step: its steps are not the same twice".into(),
        );
    }
    match then {
        Then::Crash | Then::Stop => {}
        Then::WriteThenCrash => {
            // Each is acknowledged or refused; what was acknowledged has to survive.
            let writes = Writes(&cluster);
            let _ = writes.add(&mut acked, 5, "copper kettle");
            let _ = writes.upsert(&mut acked, 3, "brass sextant");
            let _ = writes.remove(&mut acked, 1);
            settle(&acked, &cluster).map_err(|e| format!("before the crash: {e}"))?;
        }
        Then::RetryThenCrash => {
            let _ = op.run(&cluster, &mut acked);
            settle(&acked, &cluster)
                .map_err(|e| format!("before the crash, after the retry: {e}"))?;
        }
    }
    drop(cluster);
    scope.heal();
    // The scope stays open with nothing planned: what follows is then not really synced.
    let outcome = reopens(&dir, &acked);
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
    outcome
}

/// Run every step of `op` each way, and list what failed.
fn every_step_of(op: Op) -> Vec<String> {
    let steps = steps_of(op);
    let mut failures = Vec::new();
    for nth in 0..steps.len() {
        for then in [
            Then::Crash,
            Then::WriteThenCrash,
            Then::RetryThenCrash,
            Then::Stop,
        ] {
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
    for op in [Op::Add, Op::Upsert, Op::Remove] {
        assert_none(&every_step_of(op));
    }
}

#[test]
fn a_checkpoint_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::Checkpoint));
}

#[test]
fn a_resize_that_grows_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::Grow));
}

#[test]
fn a_resize_that_shrinks_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::Shrink));
}

#[test]
fn an_alias_import_failed_at_any_step_loses_nothing_acknowledged() {
    assert_none(&every_step_of(Op::AliasImport));
}

/// The steps a build of `corpus` takes in an empty directory when nothing fails.
fn steps_of_a_build(corpus: &[(u64, String)]) -> Vec<Step> {
    let dir = fresh_dir("steps_build");
    let scope = Scope::open(&dir);
    drop(ClusterEngine::build(vocab(), &config(&dir), corpus).expect("a clean build"));
    let steps = scope.taken();
    drop(scope);
    let _ = std::fs::remove_dir_all(&dir);
    steps
}

/// A build that fails at any step leaves a directory that the next start can use: it builds
/// again, or, when the failed build had already committed its manifest, it says the directory
/// holds a cluster and that cluster opens with the whole corpus.
#[test]
fn a_build_failed_at_any_step_can_be_started_again() {
    let corpus = [(1, TEXTS[0].to_string()), (2, TEXTS[1].to_string())];
    let acked = Acknowledged::of(&corpus);
    let steps = steps_of_a_build(&corpus);
    let mut failures = Vec::new();
    let each_way = steps
        .iter()
        .enumerate()
        .flat_map(|step| [(step, false), (step, true)]);
    for ((nth, step), stopped) in each_way {
        let earlier = steps[..nth].iter().filter(|other| *other == step).count();
        let dir = fresh_dir(&format!("build_{nth}_{stopped}"));
        let scope = Scope::open(&dir);
        if stopped {
            // The process died at the step: nothing after it reached the disk.
            let planned = step.clone();
            scope.stop_at(move |taken| *taken == planned, earlier);
        } else {
            scope.fail(step, earlier);
        }
        drop(ClusterEngine::build(vocab(), &config(&dir), &corpus));
        let outcome = (|| -> Result<(), String> {
            if scope.failed().as_ref() != Some(step) {
                return Err("the build did not reach the step".into());
            }
            scope.heal();
            match ClusterEngine::build(vocab(), &config(&dir), &corpus) {
                Ok(again) => drop(again),
                Err(ShardError::Config(_)) if ClusterEngine::cluster_exists(&dir) => {}
                Err(e) => return Err(format!("the second build: {e}")),
            }
            reopens(&dir, &acked)
        })();
        if let Err(what) = outcome {
            let way = if stopped { "stopped" } else { "fault" };
            failures.push(format!("build, {way} at step {nth} `{step}`: {what}"));
        }
        drop(scope);
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_none(&failures);
}

/// How many steps of each name an operation takes. A step that is added shows up in the
/// matrix by itself; this is here so that one that disappears, or stops being named, is
/// noticed too.
#[test]
fn the_steps_of_each_operation_are_the_ones_written_here() {
    let counted = |op: Op| -> BTreeMap<&'static str, usize> {
        let mut counts = BTreeMap::new();
        for step in steps_of(op) {
            *counts.entry(step.name).or_default() += 1;
        }
        counts
    };
    let mut names = BTreeSet::new();
    let mut total = 0;
    for op in Op::ALL {
        let counts = counted(op);
        names.extend(counts.keys().copied());
        total += counts.values().sum::<usize>();
        println!("{op:?}: {counts:?}");
    }
    let build = steps_of_a_build(&[(1, TEXTS[0].to_string()), (2, TEXTS[1].to_string())]);
    println!(
        "build: {} steps; the operations on a cluster: {total}",
        build.len()
    );
    names.extend(build.iter().map(|step| step.name));
    assert_eq!(
        names.into_iter().collect::<Vec<_>>(),
        ["append", "create", "propose", "remove", "rename", "sync", "sync_dir"],
        "every kind of step is taken by some operation in the matrix"
    );
    // A floor, not the exact numbers, which move with every file an operation writes.
    assert!(
        total > 300 && build.len() > 40,
        "{total} and {}",
        build.len()
    );
}
