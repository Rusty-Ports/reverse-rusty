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
use crate::fault::{Scope, Step};
use std::collections::{BTreeMap, BTreeSet};

const SHARDS: usize = 3;

/// Every text a query is given here. A query is plain words, so a title holds it when it has
/// all of its words.
const TEXTS: [&str; 7] = [
    "package adapter",
    "vintage lamp",
    "brass compass",
    "package charger",
    "copper kettle",
    "brass sextant",
    "silver spoon",
];

fn holds(title: &str, query: &str) -> bool {
    query
        .split_whitespace()
        .all(|word| title.split_whitespace().any(|have| have == word))
}

/// What has been acknowledged: the text an id must match, or `None` for an id whose remove
/// was acknowledged. An id whose last write failed is absent, because either outcome is
/// allowed for it.
#[derive(Clone, Default)]
struct Acknowledged(BTreeMap<u64, Option<String>>);

impl Acknowledged {
    fn note<T>(&mut self, id: u64, outcome: &Result<T, ShardError>, text: Option<&str>) {
        if outcome.is_ok() {
            self.0.insert(id, text.map(str::to_string));
        } else {
            self.0.remove(&id);
        }
    }

    fn add(&mut self, cluster: &ClusterEngine, id: u64, text: &str) -> Result<(), ShardError> {
        let outcome = cluster.add_query(id, text).map(|_| ());
        self.note(id, &outcome, Some(text));
        outcome
    }

    fn upsert(&mut self, cluster: &ClusterEngine, id: u64, text: &str) -> Result<(), ShardError> {
        let outcome = cluster.upsert_query(id, text, 9).map(|_| ());
        self.note(id, &outcome, Some(text));
        outcome
    }

    fn remove(&mut self, cluster: &ClusterEngine, id: u64) -> Result<(), ShardError> {
        let outcome = cluster.remove_query(id).map(|_| ());
        self.note(id, &outcome, None);
        outcome
    }

    /// `cluster` answers every title as the acknowledged writes say it must.
    fn check(&self, cluster: &ClusterEngine) -> Result<(), String> {
        for title in TEXTS {
            let matched: BTreeSet<u64> = cluster
                .percolate(title)
                .map_err(|e| format!("percolate {title:?}: {e}"))?
                .into_iter()
                .collect();
            for (id, text) in &self.0 {
                let expected = text.as_deref().is_some_and(|query| holds(title, query));
                if matched.contains(id) != expected {
                    return Err(format!(
                        "title {title:?}: id {id} (acknowledged as {text:?}) {}",
                        if expected { "is missing" } else { "matches" }
                    ));
                }
            }
        }
        Ok(())
    }
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
            Op::Add => acked.add(cluster, 4, "copper kettle"),
            Op::Upsert => acked.upsert(cluster, 3, "brass sextant"),
            Op::Remove => acked.remove(cluster, 1),
            Op::Checkpoint => cluster.checkpoint(),
            Op::Grow => cluster.resize(SHARDS + 2).map(|_| ()),
            Op::Shrink => cluster.resize(SHARDS - 1).map(|_| ()),
            Op::AliasImport => cluster.import_alias_synonyms("kettle, pot").map(|_| ()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Then {
    Crash,
    WriteThenCrash,
    RetryThenCrash,
}

fn config(dir: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        num_shards: SHARDS,
        data_dir: Some(dir.to_path_buf()),
        wal_sync_on_write: false,
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
    let mut acked = Acknowledged::default();
    for (id, text) in &corpus {
        acked.0.insert(*id, Some(text.clone()));
    }
    acked.add(&cluster, 3, "brass compass").expect("seed add");
    acked
        .upsert(&cluster, 1, "package charger")
        .expect("seed upsert");
    acked.remove(&cluster, 2).expect("seed remove");
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
    acked.check(&cluster)?;
    let mut acked = acked.clone();
    acked
        .add(&cluster, 90, "silver spoon")
        .map_err(|e| format!("a write after reopening: {e}"))?;
    cluster
        .checkpoint()
        .map_err(|e| format!("a checkpoint after reopening: {e}"))?;
    acked
        .check(&cluster)
        .map_err(|e| format!("after the checkpoint: {e}"))?;
    drop(cluster);
    let again = ClusterEngine::open(dir, vocab(), None).map_err(|e| format!("second open: {e}"))?;
    acked
        .check(&again)
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
    scope.fail(step, earlier);
    let _failed_or_absorbed = op.run(&cluster, &mut acked);
    if scope.failed().as_ref() != Some(step) {
        return Err(
            "the operation did not reach the step: its steps are not the same twice".into(),
        );
    }
    match then {
        Then::Crash => {}
        Then::WriteThenCrash => {
            // Each is acknowledged or refused; what was acknowledged has to survive.
            let _ = acked.add(&cluster, 5, "copper kettle");
            let _ = acked.upsert(&cluster, 3, "brass sextant");
            let _ = acked.remove(&cluster, 1);
            acked
                .check(&cluster)
                .map_err(|e| format!("before the crash: {e}"))?;
        }
        Then::RetryThenCrash => {
            let _ = op.run(&cluster, &mut acked);
            acked
                .check(&cluster)
                .map_err(|e| format!("before the crash, after the retry: {e}"))?;
        }
    }
    drop(cluster);
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
    let mut acked = Acknowledged::default();
    for (id, text) in &corpus {
        acked.0.insert(*id, Some(text.clone()));
    }
    let steps = steps_of_a_build(&corpus);
    let mut failures = Vec::new();
    for (nth, step) in steps.iter().enumerate() {
        let earlier = steps[..nth].iter().filter(|other| *other == step).count();
        let dir = fresh_dir(&format!("build_{nth}"));
        let scope = Scope::open(&dir);
        scope.fail(step, earlier);
        drop(ClusterEngine::build(vocab(), &config(&dir), &corpus));
        let outcome = (|| -> Result<(), String> {
            if scope.failed().as_ref() != Some(step) {
                return Err("the build did not reach the step".into());
            }
            match ClusterEngine::build(vocab(), &config(&dir), &corpus) {
                Ok(again) => drop(again),
                Err(ShardError::Config(_)) if ClusterEngine::cluster_exists(&dir) => {}
                Err(e) => return Err(format!("the second build: {e}")),
            }
            reopens(&dir, &acked)
        })();
        if let Err(what) = outcome {
            failures.push(format!("build, fault at step {nth} `{step}`: {what}"));
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
