//! What a test plans for the steps under one directory.
//!
//! A scope is found by path, not by thread: the steps of one operation run on whichever
//! thread seals a shard or appends to a log, and every test has its own directory, so tests
//! that run side by side do not see each other's faults.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// One step: its name and the path it acted on, relative to its scope's directory.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Step {
    pub name: &'static str,
    pub path: String,
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.path)
    }
}

/// The steps taken under one directory while this value lives, and the fault planned for
/// one of them.
pub struct Scope(Arc<Inner>);

struct Inner {
    root: PathBuf,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    taken: Vec<Step>,
    planned: Option<Planned>,
    failed: Option<Step>,
}

struct Planned {
    step: Step,
    /// How many equal steps pass before the one that fails.
    after: usize,
    /// Every equal step after that one fails too.
    from_then_on: bool,
}

/// How many scopes are open. Zero in every process that is not a test of this kind, and
/// then a step costs this one load.
static OPEN: AtomicUsize = AtomicUsize::new(0);
static SCOPES: Mutex<Vec<Arc<Inner>>> = Mutex::new(Vec::new());

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Scope {
    /// Record every step taken under `root` from now until this value is dropped. `root` is
    /// compared with the paths the engine uses as they are written, so give the engine the
    /// same path.
    #[must_use]
    pub fn open(root: &Path) -> Self {
        let inner = Arc::new(Inner {
            root: root.to_path_buf(),
            state: Mutex::new(State::default()),
        });
        locked(&SCOPES).push(Arc::clone(&inner));
        OPEN.fetch_add(1, Ordering::SeqCst);
        Self(inner)
    }

    /// Every step taken so far, in the order taken.
    #[must_use]
    pub fn taken(&self) -> Vec<Step> {
        locked(&self.0.state).taken.clone()
    }

    /// Forget the steps taken so far, the planned fault and whether it happened.
    pub fn reset(&self) {
        *locked(&self.0.state) = State::default();
    }

    /// Plan one fault: of the steps equal to `step` taken from now on, the one after `after`
    /// others fails, once. The step is not performed and its caller gets an error.
    pub fn fail(&self, step: &Step, after: usize) {
        self.plan(step, after, false);
    }

    /// [`fail`](Self::fail), and every equal step after that one fails as well: a device
    /// that has stopped accepting the operation, not one error.
    pub fn fail_from(&self, step: &Step, after: usize) {
        self.plan(step, after, true);
    }

    /// Plan no fault. Steps are still recorded.
    pub fn heal(&self) {
        locked(&self.0.state).planned = None;
    }

    fn plan(&self, step: &Step, after: usize, from_then_on: bool) {
        let mut state = locked(&self.0.state);
        state.planned = Some(Planned {
            step: step.clone(),
            after,
            from_then_on,
        });
        state.failed = None;
    }

    /// The step that failed, once the planned fault has happened.
    #[must_use]
    pub fn failed(&self) -> Option<Step> {
        locked(&self.0.state).failed.clone()
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        locked(&SCOPES).retain(|open| !Arc::ptr_eq(open, &self.0));
        OPEN.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Record the step in every scope that holds `path`, and fail it if one of them planned
/// that. Returns whether any scope holds the path.
pub(super) fn on_step(name: &'static str, path: &Path) -> io::Result<bool> {
    if OPEN.load(Ordering::SeqCst) == 0 {
        return Ok(false);
    }
    let scopes: Vec<Arc<Inner>> = locked(&SCOPES)
        .iter()
        .filter(|scope| path.starts_with(&scope.root))
        .cloned()
        .collect();
    let seen = !scopes.is_empty();
    for scope in scopes {
        let relative = path.strip_prefix(&scope.root).unwrap_or(path);
        let step = Step {
            name,
            path: relative.to_string_lossy().replace('\\', "/"),
        };
        let mut state = locked(&scope.state);
        state.taken.push(step.clone());
        let fails = match &mut state.planned {
            Some(planned) if planned.step == step => {
                if planned.after == 0 {
                    true
                } else {
                    planned.after -= 1;
                    false
                }
            }
            _ => false,
        };
        if fails {
            if !state
                .planned
                .as_ref()
                .is_some_and(|planned| planned.from_then_on)
            {
                state.planned = None;
            }
            state.failed.get_or_insert_with(|| step.clone());
            return Err(io::Error::other(format!("planned fault at step `{step}`")));
        }
    }
    Ok(seen)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rr_fault_plan_{tag}_{}", std::process::id()))
    }

    #[test]
    fn a_scope_records_the_steps_under_its_directory_and_no_others() {
        let (mine, other) = (dir("mine"), dir("other"));
        let scope = Scope::open(&mine);
        on_step("create", &mine.join("a.tmp")).expect("no fault planned");
        on_step("rename", &mine.join("shard_000/a")).expect("no fault planned");
        on_step("create", &other.join("a.tmp")).expect("another directory");
        assert_eq!(
            scope
                .taken()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["create a.tmp", "rename shard_000/a"]
        );
    }

    #[test]
    fn the_planned_occurrence_fails_once_and_the_step_is_named() {
        let root = dir("occurrence");
        let scope = Scope::open(&root);
        let sync = Step {
            name: "sync",
            path: "a.tmp".into(),
        };
        scope.fail(&sync, 1);
        on_step("sync", &root.join("a.tmp")).expect("the first passes");
        on_step("sync", &root.join("b.tmp")).expect("another path");
        assert_eq!(scope.failed(), None);
        let error = on_step("sync", &root.join("a.tmp")).expect_err("the second fails");
        assert!(error.to_string().contains("sync a.tmp"), "{error}");
        assert_eq!(scope.failed(), Some(sync));
        on_step("sync", &root.join("a.tmp")).expect("and only the second");
        assert_eq!(scope.taken().len(), 4, "a failed step is recorded too");
    }

    #[test]
    fn a_fault_planned_from_a_step_on_fails_every_later_one_until_it_is_healed() {
        let root = dir("persistent");
        let scope = Scope::open(&root);
        let sync = Step {
            name: "sync_dir",
            path: "manifest.bin".into(),
        };
        scope.fail_from(&sync, 1);
        on_step("sync_dir", &root.join("manifest.bin")).expect("the first passes");
        for _ in 0..3 {
            on_step("sync_dir", &root.join("manifest.bin")).expect_err("and then none");
        }
        on_step("sync_dir", &root.join("other")).expect("another path is not it");
        assert_eq!(scope.failed(), Some(sync));
        scope.heal();
        on_step("sync_dir", &root.join("manifest.bin")).expect("healed");
    }

    #[test]
    fn a_dropped_scope_plans_nothing() {
        let root = dir("dropped");
        let scope = Scope::open(&root);
        let step = Step {
            name: "rename",
            path: "a".into(),
        };
        scope.fail(&step, 0);
        drop(scope);
        on_step("rename", &root.join("a")).expect("the scope is gone");
    }
}
