//! What a request reports about the published layout, it reads from one layout (ADR-210).
//!
//! Each accessor of the cluster engine reads the layout that is published when it is called.
//! The server used to hold a lock across a request, which kept two such reads on one layout.
//! Without it, a resize or a vocabulary change can swap its layout in between them: eight
//! shard counts beside a shard total of nine, or the old committed topology beside the new
//! shards, which a health probe takes for a fault.
//!
//! The rule is the one for anything behind an atomic pointer: load it once for an operation
//! and hand it down. A request that reports several things pins the layout
//! (`ClusterEngine::published`) and reads them from the pin. This test reads the server's
//! source and fails a function that
//!
//! - reads the layout from the engine more than once, or
//! - reads the layout and something a layout change replaces with it, one after the other
//!   (the control state, the repair queue, the open points in time),
//!
//! unless it is named below with the reason it may.

use std::path::{Path, PathBuf};

/// Reads of the published layout, each of which loads it anew. `published` pins it.
const LAYOUT_READS: &[&str] = &[
    "published",
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
    "check_pit",
    "get_source",
    "get_document",
    "document_exists",
];

/// Reads of what a layout change replaces together with the layout, one after the other.
const COUPLED_READS: &[&str] = &[
    "control_state",
    "assignment_for",
    "pending_repairs",
    "pending_repair_ids",
    "open_pit_count",
];

/// A function that holds the topology guard and write admission alone, so that the only
/// layout change that can run is its own. It may read as it likes.
const ALONE: &[&str] = &["resize_worker", "remote_resize_worker"];

/// A function that reads the layout from the engine more than once, and why it may.
const MORE_THAN_ONCE: &[(&str, &str)] = &[(
    "cluster_get_doc",
    "one read for each request: the existence check for HEAD, the document for GET",
)];

/// A function that compares the pinned layout with the control state. A rebuild publishes
/// the one and then commits the other, so each of these asks whether a rebuild is running
/// before it calls a difference a fault. That is checked.
const JOINS: &[&str] = &["collect_cluster_health", "collect_rows"];

/// A function that reports something coupled to the layout beside it without comparing the
/// two, and why the pair need not belong together.
const BESIDE: &[(&str, &str)] = &[
    (
        "cluster_stats",
        "the repair count is a number of its own beside the pinned layout's counts",
    ),
    (
        "counts_beside_a_failure",
        "counts reported with a red status, for whoever reads the failure",
    ),
    (
        "cluster_open_pit_route",
        "a gauge and a shard count beside a new point in time, which a rebuild makes stale",
    ),
    (
        "cluster_close_pit_route",
        "a gauge and a shard count beside the points in time it closed",
    ),
];

struct Function {
    file: String,
    name: String,
    body: String,
}

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" {
                sources(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && name != "tests.rs"
            && name != "test_support.rs"
            && !name.ends_with("_tests.rs")
        {
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
    let whole = std::fs::read_to_string(file).expect("read source");
    // A file's own unit tests are not the server.
    let text = whole
        .find("#[cfg(test)]\nmod tests")
        .map_or(whole.as_str(), |tests| &whole[..tests]);
    let bytes = text.as_bytes();
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
        let (Some(open), false) = (open, name.is_empty()) else {
            continue;
        };
        let end = close_of(bytes, open);
        found.push(Function {
            file: file
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned(),
            name,
            // Without whitespace, so a call that rustfmt broke across lines reads like any
            // other.
            body: text[open..end]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect(),
        });
    }
    found
}

/// How many times `function` calls one of `reads` on the cluster.
fn reads(function: &Function, reads: &[&str]) -> usize {
    reads
        .iter()
        .map(|read| function.body.matches(&format!("cluster.{read}(")).count())
        .sum()
}

#[test]
fn what_a_request_reports_about_the_layout_it_reads_from_one_layout() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/server");
    let mut files = Vec::new();
    sources(&root, &mut files);
    let functions: Vec<Function> = files.iter().flat_map(|file| functions(file)).collect();
    assert!(
        functions.len() > 500,
        "the scan found only {} functions; it is not reading the server",
        functions.len()
    );

    // The scan knows the engine by the name `cluster`. A parameter of that type under another
    // name would read past it.
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read source");
        for (at, _) in text.match_indices("ClusterEngine") {
            let before = text[..at].trim_end_matches("reverse_rusty::cluster::");
            let Some(before) = before.strip_suffix(": &") else {
                continue;
            };
            let name: String = before
                .chars()
                .rev()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            assert_eq!(
                name,
                "cluster",
                "{}: a parameter of type `&ClusterEngine` is named `{name}`; name it `cluster` \
                 so this test can see what it reads",
                file.display()
            );
        }
    }

    let layout_reads = |function: &Function| reads(function, LAYOUT_READS);
    let coupled_reads = |function: &Function| reads(function, COUPLED_READS);
    let in_pairs = |list: &[(&str, &str)], name: &str| list.iter().any(|(named, _)| *named == name);

    let mut violations = Vec::new();
    let mut pinned = 0;
    for function in &functions {
        let name = function.name.as_str();
        let (layout, coupled) = (layout_reads(function), coupled_reads(function));
        if function.body.contains("cluster.published()") {
            pinned += 1;
        }
        if ALONE.contains(&name) {
            continue;
        }
        if layout >= 2 && !in_pairs(MORE_THAN_ONCE, name) {
            violations.push(format!(
                "{}::{name} reads the published layout from the engine {layout} times; pin it \
                 once with `cluster.published()` and read from the pin",
                function.file
            ));
        }
        if layout >= 1 && coupled >= 1 && !JOINS.contains(&name) && !in_pairs(BESIDE, name) {
            violations.push(format!(
                "{}::{name} reads the layout and something a layout change replaces with it; \
                 name it in JOINS (it compares them) or BESIDE (it only reports them)",
                function.file
            ));
        }
    }
    for join in JOINS {
        let asks = functions.iter().any(|function| {
            function.name == *join
                && function
                    .body
                    .contains("cluster.layout_change_in_progress()")
        });
        if !asks {
            violations.push(format!(
                "`{join}` compares the layout with the control state and does not ask whether a \
                 rebuild is running"
            ));
        }
    }
    // Every name on a list is a function that still needs to be there.
    let still_twice = |function: &Function| layout_reads(function) >= 2;
    let still_beside =
        |function: &Function| layout_reads(function) >= 1 && coupled_reads(function) >= 1;
    let listed: Vec<(&str, bool)> = ALONE
        .iter()
        .map(|name| (*name, true))
        .chain(MORE_THAN_ONCE.iter().map(|(name, _)| (*name, false)))
        .chain(JOINS.iter().map(|name| (*name, true)))
        .chain(BESIDE.iter().map(|(name, _)| (*name, true)))
        .collect();
    for (name, either) in listed {
        let needed = functions.iter().any(|function| {
            function.name == name && (still_twice(function) || (either && still_beside(function)))
        });
        if !needed {
            violations.push(format!(
                "`{name}` is named as an exception and no longer needs to be: take it off the list"
            ));
        }
    }
    assert!(
        pinned >= 4,
        "only {pinned} functions pin the layout; the scan is not seeing them"
    );
    assert!(
        violations.is_empty(),
        "a request reads what it reports from one layout:\n  {}",
        violations.join("\n  ")
    );
}
