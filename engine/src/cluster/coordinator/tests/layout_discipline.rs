//! One operation, one layout (ADR-208).
//!
//! A rebuild publishes a new layout while other operations run. An operation that routed a
//! title by one layout and matched it in another would be wrong, so every function that reads
//! the layout is one of three kinds, and this test keeps each to its rule by reading the
//! coordinator's source:
//!
//! - a **helper** is handed `layout: &Layout` and never loads another;
//! - an **operation** loads once, at its entry, and calls no other method that loads (a
//!   mutation loads through `admit_mutation`, after it holds the mutation barrier);
//! - a **writer** (`&mut self`, or the engine by value) holds the engine alone and may load
//!   whenever it likes.

use std::path::{Path, PathBuf};

struct Function {
    file: String,
    name: String,
    signature: String,
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
        // A declaration has its parameter list, then its body; a `fn` type or a trait method
        // without a body reaches a `;` first.
        let Some(open) = text[start..].find('{').map(|at| start + at) else {
            continue;
        };
        if name.is_empty() || text[start..open].contains(';') {
            continue;
        }
        let end = close_of(bytes, open);
        found.push(Function {
            file: file
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned(),
            name,
            signature: squeeze(&text[start..open]),
            body: squeeze(&text[open..end]),
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
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cluster");
    let mut files = vec![root.join("coordinator.rs")];
    sources(&root.join("coordinator"), &mut files);
    let functions: Vec<Function> = files.iter().flat_map(|file| functions(file)).collect();

    // A mutation loads through `admit_mutation`, which takes the mutation barrier first.
    let loads = |function: &Function| {
        function.body.matches("self.layout()").count()
            + function.body.matches("self.admit_mutation()").count()
    };
    let is_writer = |function: &Function| {
        ["(&mutself", "(mutself", "(self,", "(self)"]
            .iter()
            .any(|receiver| function.signature.contains(receiver))
    };
    let loaders: Vec<&str> = functions
        .iter()
        .filter(|function| loads(function) > 0)
        .map(|function| function.name.as_str())
        .collect();
    assert!(
        loaders.len() > 50,
        "the scan found only {} functions that load the layout; it is not reading the code",
        loaders.len()
    );

    let mut violations = Vec::new();
    for function in &functions {
        let count = loads(function);
        let place = format!("{}::{}", function.file, function.name);
        if function.signature.contains("layout:&Layout") && count > 0 {
            violations.push(format!("{place} is handed a layout and loads another"));
        }
        if count == 0 || is_writer(function) {
            continue;
        }
        if count > 1 {
            violations.push(format!("{place} loads the layout {count} times"));
        }
        for other in &loaders {
            if *other != function.name
                && *other != "layout"
                && *other != "admit_mutation"
                && function.body.contains(&format!("self.{other}("))
            {
                violations.push(format!(
                    "{place} loads the layout and calls `{other}`, which loads it again"
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "one operation, one layout (ADR-208):\n  {}",
        violations.join("\n  ")
    );
}
