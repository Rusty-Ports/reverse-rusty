//! What a request reports about the published layout, it reads from one layout (ADR-210).
//!
//! Each accessor of the cluster engine reads the layout that is published when it is called.
//! The server used to hold a lock across a request, which kept two such reads on one layout.
//! Without it, a resize or a vocabulary change can swap its layout in between them: eight
//! shard counts beside a shard total of nine, or the old committed topology beside the new
//! shards, which a health probe takes for a fault.
//!
//! This test reads the server's source. A function that reads the layout twice, or the layout
//! and something a layout change replaces with it (the control state, the repair queue, the
//! open points in time), must do so inside `read_on_one_layout` or
//! `read_between_layout_changes`, or be named below with the reason it need not.

use std::path::{Path, PathBuf};

/// Reads of the published layout.
const LAYOUT_READS: &[&str] = &[
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

/// The two ways of reading coherently.
const COHERENT: [&str; 2] = ["read_on_one_layout(", "read_between_layout_changes("];

/// A function whose reads are made coherent by the one function that calls it:
/// `(function, caller)`. The caller is checked.
const UNDER: &[(&str, &str)] = &[
    ("compare_topology", "collect_cluster_health"),
    ("join_rows", "collect_rows"),
];

/// A function that holds the topology guard and write admission alone, so that the only
/// layout change that can run is its own.
const ALONE: &[&str] = &["resize_worker", "remote_resize_worker"];

/// A function whose reads need not belong together, and why.
const EACH_STANDS_ALONE: &[(&str, &str)] = &[
    (
        "cluster_get_doc",
        "one read for each request: the existence check for HEAD, the document for GET",
    ),
    (
        "collect_once",
        "three counts reported beside a probe that failed",
    ),
    (
        "unavailable_fallback",
        "three counts reported beside a probe that did not finish",
    ),
    (
        "rebuilding_health",
        "counts reported beside a status that says a rebuild is replacing them",
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

    let combines = |function: &Function| {
        let layout = reads(function, LAYOUT_READS);
        layout >= 2 || (layout >= 1 && reads(function, COUPLED_READS) >= 1)
    };
    let coherent =
        |function: &Function| COHERENT.iter().any(|reader| function.body.contains(reader));
    let named = |name: &str| {
        UNDER.iter().any(|(function, _)| *function == name)
            || ALONE.contains(&name)
            || EACH_STANDS_ALONE
                .iter()
                .any(|(function, _)| *function == name)
    };

    let mut violations = Vec::new();
    for function in functions.iter().filter(|function| combines(function)) {
        if !coherent(function) && !named(&function.name) {
            violations.push(format!(
                "{}::{} reads the layout more than once, or the layout and something replaced \
                 with it, outside `read_on_one_layout` and `read_between_layout_changes`",
                function.file, function.name
            ));
        }
    }
    // Every name on a list is a function that still needs to be there.
    let all_named = UNDER
        .iter()
        .map(|(function, _)| *function)
        .chain(ALONE.iter().copied())
        .chain(EACH_STANDS_ALONE.iter().map(|(function, _)| *function));
    for name in all_named {
        match functions.iter().find(|function| function.name == name) {
            Some(function) if combines(function) && !coherent(function) => {}
            _ => violations.push(format!(
                "`{name}` is named as an exception and no longer combines reads outside a \
                 coherent reader: take it off the list"
            )),
        }
    }
    for (function, caller) in UNDER {
        let hands_it_on = functions.iter().any(|candidate| {
            candidate.name == *caller && coherent(candidate) && candidate.body.contains(function)
        });
        if !hands_it_on {
            violations.push(format!(
                "`{function}` is said to read under `{caller}`, which does not hand it to a \
                 coherent reader"
            ));
        }
    }
    let readers = functions
        .iter()
        .filter(|function| combines(function) && coherent(function))
        .count();
    assert!(
        readers >= 2,
        "only {readers} functions read coherently; the scan is not seeing them"
    );
    assert!(
        violations.is_empty(),
        "a request reads what it reports from one layout:\n  {}",
        violations.join("\n  ")
    );
}
