//! One operation, one layout, and only a search runs beside a layout change (ADR-208,
//! ADR-209).
//!
//! A layout change holds the layout lock alone. What may run beside it is an allow-list, not
//! whatever nobody thought to exclude: this test reads the coordinator's source and keeps
//! every function that touches the layout to its kind.
//!
//! - A **search** (the names in [`SEARCHES`]) loads the layout with no lock. It only reads
//!   shard data, through that one layout, so it can run while the layout is being changed.
//! - Every other **operation** takes the layout lock shared (`stable`, or `admit_mutation`
//!   for a write) or begins a layout change, and gets its layout from that.
//! - Either kind loads **once**, at its entry, and calls no other method that loads: a second
//!   shared hold of the layout lock would wait behind a change that waits for the first.
//! - A **helper** is handed its layout (`layout: &Layout`) or the change it runs inside
//!   (`change: &LayoutChange`). It never loads, and never calls a method that does.
//! - **Assembly** (the engine by value, before it is shared) may do as it likes.
//!
//! The rules above are about functions that touch the layout. Two more are about the ones
//! that do not, because what a layout change replaces is more than the layout (the control
//! state is keyed by shard position, for one):
//!
//! - Every **entry point** (a method of the engine that code outside the coordinator can
//!   call) takes the layout lock, or calls one that does, unless it is a search or is named in
//!   [`LAYOUT_FREE`]. A new method that nobody classified waits for a layout change.
//! - A **write to the control state** happens under the layout lock: in a function that takes
//!   it, or in a helper that is handed the layout or the change it runs inside.

use std::path::{Path, PathBuf};

/// The operations that load the layout with no lock, and so may run while it is being
/// changed. Each only reads shard data, through the one layout it loaded. It reads nothing
/// else that a layout change replaces (the logical-id directory, the repair queue, the control
/// state), or it fails closed when it does (a point-in-time read is checked against the
/// generation its pins were taken under). Adding a name here is a decision, not a fix.
const SEARCHES: &[&str] = &[
    // matching
    "percolate",
    "percolate_with_stats",
    "percolate_with_broad",
    "percolate_filtered",
    "percolate_filtered_with_broad",
    "percolate_filtered_ranked",
    "percolate_filtered_with_stats",
    // ranked and batch
    "try_percolate_filtered_top_k",
    "try_percolate_filtered_top_k_pit",
    "try_percolate_filtered_top_k_batch",
    "fetch_ranked_sources",
    "fetch_ranked_sources_bounded",
    "fetch_ranked_sources_batch_bounded",
    "explain_ranked_source",
    "check_pit",
    // one stored document
    "get_source",
    "get_document",
    "document_exists",
    // what the published layout is
    "shard_fanout",
    "num_shards",
    "num_queries",
    "shard_query_counts",
    "class_counts",
    "out_of_sync_replicas",
    "placement_generation",
    "normalizer",
    "dict",
    "vocab",
    "learn_vocab",
];

/// The entry points that take no lock and load no layout. Each reads one thing that no layout
/// change replaces, or one thing atomically, and writes nothing. Adding a name here is a
/// decision, as it is for [`SEARCHES`].
const LAYOUT_FREE: &[&str] = &[
    // compiled against the tag dictionary, which is the engine's and not the layout's
    "compile_tag_predicate",
    "compile_rank_spec",
    "compile_rank_program",
    "compile_rank_program_with_profiles",
    // the frozen view a search with sources runs under: it holds the mutation barrier, which
    // a layout is published under, and the searches inside it load for themselves
    "consistent_read_view",
    // configuration that is fixed at assembly, and counters
    "include_broad",
    "is_durable",
    "is_remote",
    "per_shard_config",
    "replication_factor",
    "transport_metrics",
    "has_tagged_queries",
    "epoch",
    // one atomic read of something that changes: the answer may be a moment old
    "control_state",
    "control_version",
    "assignment_for",
    "pending_repairs",
    "pending_repair_ids",
    "open_pit_count",
    "ensure_resize_write_fence_open",
    // runs the caller's lock-free read and says whether a layout change overlapped it
    "read_between_layout_changes",
    // test seams: the hook a rebuild calls, and the flag a remote resize raises
    "set_rebuild_hook_for_test",
    "set_resize_write_fence_for_test",
];

/// The ways a function gets a layout for itself.
const LOADS: [&str; 7] = [
    "self.layout()",
    "self.stable()",
    "self.stable_while(",
    "self.stable_by(",
    "self.admit_mutation()",
    "self.begin_layout_change()",
    "self.begin_cutover()",
];

/// The ways a function writes the control state: a proposal of any kind, a membership change,
/// or the control plane handed to a function that may do either. On `self`, or on an engine a
/// free function was given.
const WRITES: [&str; 3] = [
    ".control.propose",
    ".control.change_membership(",
    ".control.as_ref()",
];

struct Function {
    file: String,
    name: String,
    signature: String,
    body: String,
    /// What stands before `fn` on its line: the visibility, with any `const`.
    visibility: String,
    /// Defined in an `impl ClusterEngine` block.
    of_engine: bool,
}

impl Function {
    /// A method of the engine that code outside the coordinator can call on a shared engine.
    fn is_entry_point(&self) -> bool {
        let outside = match self.visibility.as_str() {
            "pub" | "pub(crate)" | "pub(incrate::cluster)" => true,
            // `super` of the coordinator's own root file is the cluster module.
            "pub(super)" => self.file == "coordinator.rs",
            _ => false,
        };
        self.of_engine && outside && self.signature.contains("(&self")
    }
}

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" {
                sources(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") && name != "tests.rs" {
            out.push(path);
        }
    }
}

/// The index just past the `}` that closes the `{` at `open`, skipping comments, strings and
/// character literals.
fn close_of(text: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    let mut at = open;
    while at < text.len() {
        match text[at] {
            b'/' if text.get(at + 1) == Some(&b'/') => {
                while at < text.len() && text[at] != b'\n' {
                    at += 1;
                }
                continue;
            }
            b'"' => {
                at += 1;
                while at < text.len() && text[at] != b'"' {
                    at += if text[at] == b'\\' { 2 } else { 1 };
                }
            }
            // A character literal, not a lifetime: 'x' or '\x'.
            b'\'' if text.get(at + 1) == Some(&b'\\') => at += 3,
            b'\'' if text.get(at + 2) == Some(&b'\'') => at += 2,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return at + 1;
                }
            }
            _ => {}
        }
        at += 1;
    }
    panic!("unbalanced braces");
}

fn functions(file: &Path) -> Vec<Function> {
    let text = std::fs::read_to_string(file).expect("read source");
    let bytes = text.as_bytes();
    // `impl ClusterEngine {`, or `impl super::ClusterEngine {`.
    let engine_impls: Vec<(usize, usize)> = text
        .match_indices("ClusterEngine {")
        .filter(|(at, _)| {
            let line = text[..*at].rfind('\n').map_or(0, |start| start + 1);
            text[line..*at].starts_with("impl")
        })
        .map(|(at, opening)| (at, close_of(bytes, at + opening.len() - 1)))
        .collect();
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(offset) = text[from..].find("fn ") {
        let start = from + offset;
        from = start + 3;
        let before = start.checked_sub(1).map_or(b' ', |at| bytes[at]);
        if before.is_ascii_alphanumeric() || before == b'_' {
            continue;
        }
        let name: String = text[start + 3..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // A definition has a body: the first `{` after the signature. A `fn` type or a trait
        // method without one reaches a `;` first. A `;` inside brackets is an array length.
        let mut depth = 0usize;
        let mut open = None;
        for (at, byte) in bytes.iter().enumerate().skip(start) {
            match byte {
                b'[' | b'(' => depth += 1,
                b']' | b')' => depth = depth.saturating_sub(1),
                b'{' if depth == 0 => {
                    open = Some(at);
                    break;
                }
                b';' if depth == 0 => break,
                _ => {}
            }
        }
        let Some(open) = open else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        let end = close_of(bytes, open);
        let line = text[..start].rfind('\n').map_or(0, |at| at + 1);
        found.push(Function {
            file: file
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned(),
            name,
            signature: squeeze(&text[start..open]),
            body: squeeze(&text[open..end]),
            visibility: squeeze(&text[line..start]).replace("const", ""),
            of_engine: engine_impls
                .iter()
                .any(|&(from, to)| from < start && start < to),
        });
    }
    found
}

/// Without whitespace, so that a call rustfmt broke across lines reads like any other.
fn squeeze(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

#[test]
fn every_operation_loads_the_layout_once_and_helpers_never_do() {
    let functions = coordinator_functions();

    let loads = |function: &Function| -> usize {
        LOADS
            .iter()
            .map(|load| function.body.matches(load).count())
            .sum()
    };
    let is_assembly = |function: &Function| {
        ["(&mutself", "(mutself", "(self,", "(self)"]
            .iter()
            .any(|receiver| function.signature.contains(receiver))
    };
    let is_helper = |function: &Function| {
        function.signature.contains("layout:&Layout")
            || function.signature.contains("change:&LayoutChange")
    };
    let loaders: Vec<&str> = functions
        .iter()
        .filter(|function| loads(function) > 0)
        .map(|function| function.name.as_str())
        // The ways of getting a layout are not operations themselves.
        .filter(|name| {
            !LOADS
                .iter()
                .any(|load| load.starts_with(&format!("self.{name}(")))
        })
        .collect();
    assert!(
        loaders.len() > 50,
        "the scan found only {} functions that load the layout; it is not reading the code",
        loaders.len()
    );
    for search in SEARCHES {
        assert!(
            functions
                .iter()
                .any(|function| function.name == *search && function.body.contains("self.layout()")),
            "`{search}` is listed as a search and loads no layout: take it off the list"
        );
    }

    let mut violations = Vec::new();
    for function in &functions {
        let count = loads(function);
        let place = format!("{}::{}", function.file, function.name);
        if is_assembly(function)
            || LOADS
                .iter()
                .any(|load| load.starts_with(&format!("self.{}(", function.name)))
        {
            continue;
        }
        let calls_a_loader = loaders.iter().find(|other| {
            **other != function.name && function.body.contains(&format!("self.{other}("))
        });
        if is_helper(function) {
            if count > 0 {
                violations.push(format!("{place} is handed its layout and loads another"));
            }
            if let Some(other) = calls_a_loader {
                violations.push(format!(
                    "{place} is handed its layout and calls `{other}`, which loads one"
                ));
            }
            continue;
        }
        if count == 0 {
            continue;
        }
        if count > 1 {
            violations.push(format!("{place} loads the layout {count} times"));
        }
        if let Some(other) = calls_a_loader {
            violations.push(format!(
                "{place} loads the layout and calls `{other}`, which loads it again"
            ));
        }
        if function.body.contains("self.layout()") && !SEARCHES.contains(&function.name.as_str()) {
            violations.push(format!(
                "{place} is not a search (see SEARCHES) and loads the layout without the \
                 layout lock: use `stable()`"
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "one operation, one layout; only a search runs beside a layout change:\n  {}",
        violations.join("\n  ")
    );
}

fn coordinator_functions() -> Vec<Function> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cluster");
    let mut files = vec![root.join("coordinator.rs")];
    sources(&root.join("coordinator"), &mut files);
    files.iter().flat_map(|file| functions(file)).collect()
}

/// The ways a function takes the layout lock: every way of loading but the search's.
fn takes_the_lock(function: &Function) -> bool {
    LOADS
        .iter()
        .filter(|load| **load != "self.layout()")
        .any(|load| function.body.contains(load))
}

#[test]
fn every_entry_point_takes_the_layout_lock_or_is_named() {
    let functions = coordinator_functions();
    let methods: Vec<&Function> = functions.iter().filter(|f| f.of_engine).collect();
    // The methods that take the lock, and then the ones that call one of those.
    let mut guarded: Vec<&str> = methods
        .iter()
        .filter(|method| takes_the_lock(method))
        .map(|method| method.name.as_str())
        .collect();
    loop {
        let more: Vec<&str> = methods
            .iter()
            .filter(|method| !guarded.contains(&method.name.as_str()))
            .filter(|method| {
                guarded
                    .iter()
                    .any(|callee| method.body.contains(&format!("self.{callee}(")))
            })
            .map(|method| method.name.as_str())
            .collect();
        if more.is_empty() {
            break;
        }
        guarded.extend(more);
    }

    let entry_points: Vec<&&Function> = methods
        .iter()
        .filter(|method| method.is_entry_point())
        .collect();
    assert!(
        entry_points.len() > 80,
        "the scan found only {} entry points; it is not reading the code",
        entry_points.len()
    );
    let mut violations = Vec::new();
    for free in LAYOUT_FREE {
        let Some(method) = entry_points.iter().find(|method| method.name == *free) else {
            violations.push(format!(
                "`{free}` is listed as layout-free and is not an entry point: take it off the list"
            ));
            continue;
        };
        if guarded.contains(free) || method.body.contains("self.layout()") {
            violations.push(format!(
                "`{free}` is listed as layout-free and gets a layout: take it off the list"
            ));
        }
        if method.body.contains(".control.propose") || method.body.contains(".control.as_ref()") {
            violations.push(format!(
                "`{free}` is listed as layout-free and writes the control state"
            ));
        }
    }
    for method in &entry_points {
        let name = method.name.as_str();
        if guarded.contains(&name) || SEARCHES.contains(&name) || LAYOUT_FREE.contains(&name) {
            continue;
        }
        violations.push(format!(
            "{}::{name} can be called from outside the coordinator and takes no layout lock: \
             use `stable()`, or name it in SEARCHES or LAYOUT_FREE if it may run beside a \
             layout change",
            method.file
        ));
    }
    assert!(
        violations.is_empty(),
        "only a search runs beside a layout change:\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn the_control_state_is_written_under_the_layout_lock() {
    let functions = coordinator_functions();
    let writers: Vec<&Function> = functions
        .iter()
        .filter(|function| WRITES.iter().any(|write| function.body.contains(write)))
        .collect();
    assert!(
        writers.len() > 5,
        "the scan found only {} writers of the control state; it is not reading the code",
        writers.len()
    );
    let violations: Vec<String> = writers
        .iter()
        .filter(|function| {
            // The engine by value, or a constructor that is still building it.
            let assembly = [
                "(&mutself",
                "(mutself",
                "(self,",
                "(self)",
                "->Self",
                "<Self,",
            ]
            .iter()
            .any(|mark| function.signature.contains(mark));
            let handed = function.signature.contains("layout:&Layout")
                || function.signature.contains("change:&LayoutChange");
            !(assembly || handed || takes_the_lock(function))
        })
        .map(|function| format!("{}::{}", function.file, function.name))
        .collect();
    assert!(
        violations.is_empty(),
        "these write the control state without the layout lock, and without a layout or a \
         layout change handed to them:\n  {}",
        violations.join("\n  ")
    );
}
