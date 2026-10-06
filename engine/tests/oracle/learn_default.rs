//! The default vocabulary learner never removes a match (ADR-202).
//!
//! `learn_and_apply` with no options used to install what it learned as collapse rules:
//! both sides rewritten to one canonical feature. That changes what a stored query means.
//! A query excluding one member of a learned pair began to exclude the other too, and a
//! learned phrase swallowed its component words. Each is a match the literal query had and
//! the learned vocabulary took away. The default now applies what it learns by expansion,
//! which can only add matches.

use std::collections::HashSet;

use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::{AnyOfLearnMode, CorpusLearnConfig};
use reverse_rusty::Normalizer;

/// A corpus whose any-of groups teach `pkg` ≡ `new` and `nib` ≡ `new in box` (each pair in
/// two queries, the default evidence floor), beside queries those rules would narrow.
fn corpus() -> Vec<(u64, String)> {
    vec![
        (1, "(pkg,new) widget".to_string()),
        (2, "(pkg,new) gadget".to_string()),
        (3, "(nib,new in box) widget".to_string()),
        (4, "(nib,new in box) gadget".to_string()),
        // Excludes one member of a learned pair.
        (10, "widget -new".to_string()),
        (11, "gadget -pkg".to_string()),
        // Names the words a learned phrase is made of, apart from each other.
        (12, "box widget".to_string()),
        (13, "widget in stock".to_string()),
        // Names one member positively.
        (14, "pkg sprocket".to_string()),
        (15, "new sprocket".to_string()),
    ]
}

fn titles() -> Vec<&'static str> {
    vec![
        "widget pkg",
        "widget new",
        "gadget new",
        "gadget pkg",
        "widget new in box",
        "box of widget parts new in stock",
        "widget in stock",
        "sprocket pkg",
        "sprocket new",
        "widget nib",
        "gadget nib",
    ]
}

fn matches(engine: &Engine) -> Vec<HashSet<u64>> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    titles()
        .into_iter()
        .map(|title| {
            engine.match_title(title, &mut scratch, &mut out, true);
            out.iter().copied().collect()
        })
        .collect()
}

fn learned(config: &CorpusLearnConfig) -> (Vec<HashSet<u64>>, Vec<HashSet<u64>>) {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.build_from_queries(&corpus());
    let before = matches(&engine);
    engine
        .learn_and_apply_with(config)
        .expect("learn and apply");
    (before, matches(&engine))
}

/// Every match the stored queries had before the default learner ran, they still have.
#[test]
fn the_default_learner_never_removes_a_match() {
    let (before, after) = learned(&CorpusLearnConfig::default());
    for ((title, before), after) in titles().into_iter().zip(&before).zip(&after) {
        let lost: Vec<&u64> = before.difference(after).collect();
        assert!(
            lost.is_empty(),
            "{title:?} lost {lost:?} to the learned rules"
        );
    }
    // And it learned something: a query naming one member now matches the other.
    let sprocket_new = titles().iter().position(|t| *t == "sprocket new").unwrap();
    assert!(
        after[sprocket_new].contains(&14),
        "`pkg sprocket` matches a title that says `new`"
    );
}

/// The collapse learner is still there for whoever asks for it, and it does narrow: this
/// is the behaviour the default no longer has. If collapse ever stops narrowing here, the
/// test above no longer shows that the default is the safer mode.
#[test]
fn the_collapse_learner_narrows_and_must_be_asked_for() {
    let (before, after) = learned(&CorpusLearnConfig {
        anyof_mode: AnyOfLearnMode::Collapse,
        ..CorpusLearnConfig::default()
    });
    let lost = titles()
        .into_iter()
        .zip(&before)
        .zip(&after)
        .any(|((_, before), after)| before.difference(after).next().is_some());
    assert!(lost, "collapse rules removed no match on this corpus");
}
