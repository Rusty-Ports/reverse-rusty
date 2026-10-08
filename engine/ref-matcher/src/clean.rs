//! Byte cleaning: lowercase + diacritic fold + the punctuation-class table (ADR-058).
//!
//! Ported from `engine/src/normalize/core.rs::clean_with` (see the crate documentation for
//! what "ported" means for the differential). The tables it reads are in [`crate::tables`].
//! The SAME table runs over queries and titles, keeping the
//! feature spaces aligned. Whitespace runs are NOT collapsed here (the canonical view keeps the
//! cleaned text verbatim); run handling is the query-side / overlap-scan job in `normalize`.

use std::collections::HashMap;

use crate::tables::fold_diacritic;

/// How a non-alphanumeric character is handled during cleaning.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PunctClass {
    /// Word boundary — becomes a single space (the default for most punctuation).
    Split,
    /// Deleted, so its neighbours join into one token (`O'Brien` -> `obrien`).
    Fold,
    /// Left literally in place inside the token (`.` so `9.5` survives).
    Keep,
    /// Emitted as its own standalone token (` <c> `), so the number logic can tell `#2` / `/199`
    /// from decimal values.
    Marker,
}

/// The punctuation classification table: the historical default plus optional per-char overrides
/// (ADR-058). Default: `.` = [`Keep`](PunctClass::Keep), `#`/`/` = [`Marker`](PunctClass::Marker),
/// every other non-alphanumeric = [`Split`](PunctClass::Split).
#[derive(Clone, Debug, Default)]
pub struct PunctTable {
    overrides: HashMap<char, PunctClass>,
}

impl PunctTable {
    /// The historical default table (no overrides).
    #[must_use]
    pub fn new() -> Self {
        PunctTable {
            overrides: HashMap::new(),
        }
    }

    /// Override the class of a single character (e.g. declare `'` and `-` as `Fold`).
    pub fn set(&mut self, ch: char, class: PunctClass) {
        self.overrides.insert(ch, class);
    }

    /// The class of `ch`: an override if present, else the historical default.
    #[must_use]
    pub fn class_of(&self, ch: char) -> PunctClass {
        if let Some(&c) = self.overrides.get(&ch) {
            return c;
        }
        if crate::tables::KEEP.contains(&ch) {
            PunctClass::Keep
        } else if crate::tables::MARKERS.contains(&ch) {
            PunctClass::Marker
        } else {
            PunctClass::Split
        }
    }
}

/// Lowercase + fold diacritics + apply the punctuation table, returning the cleaned string.
///
/// Order matters: `fold_diacritic` runs first, so a folded char (`ć` -> `c`) is then treated as the
/// ASCII alphanumeric it became. Non-alphanumerics are dispatched by their [`PunctClass`]. Verbatim
/// translation of `clean_with`.
#[must_use]
pub fn clean(text: &str, punct: &PunctTable) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        let c = fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            // A separator ends the current token. It is not written twice in a row or
            // first, so a phrase is found wherever its words are consecutive tokens.
            let separator = |out: &mut String| {
                if !out.is_empty() && !out.ends_with(' ') {
                    out.push(' ');
                }
            };
            match punct.class_of(c) {
                PunctClass::Split => separator(&mut out),
                PunctClass::Fold => {} // delete: neighbours join into one token
                // A space is the separator whatever class it is given.
                PunctClass::Keep | PunctClass::Marker if c == ' ' => separator(&mut out),
                PunctClass::Keep => out.push(c),
                PunctClass::Marker => {
                    separator(&mut out);
                    out.push(c);
                    out.push(' ');
                }
            }
        }
    }
    out
}

/// The cleaned whitespace tokens of `text` (the same tokens the normalizer's phase-2 tokenizer
/// sees). Used to register an alias phrase's token sequence so it aligns with cleaned title text
/// (ADR-061), mirroring `core.rs::alias_form_tokens`.
#[must_use]
pub fn clean_tokens(text: &str, punct: &PunctTable) -> Vec<String> {
    clean(text, punct)
        .split_whitespace()
        .map(ToString::to_string)
        .collect()
}
