//! The grammar corpus through duplicates, frequency drift, flushes and both merges.
//!
//! Identical bodies stored at different times can plan differently: the first copies are
//! compiled before the frequency mask is frozen, and later ones after other queries have
//! moved the frequencies an any-of body's plan is chosen by. Dedup must never let one copy's
//! class decide which reads see another (ADR-186), and a merge must never hide a row a
//! default read returned (ADR-187). `dedup_visibility` pins both on small hand-shaped
//! bodies; here the bodies are written by the grammar generator: several groups, two-token
//! members, phrases and negations beside the groups, hundreds of bodies.

use std::collections::HashSet;

use crate::dedup::assert_on_equals_off;
use crate::dedup_visibility::{manual_cfg, new_engine, reads};
use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::grammar::{generate_grammar, GrammarConfig};
use reverse_rusty::segment::Engine;

/// The copies of one body, and a title that satisfies it.
struct Body {
    title: String,
    copies: Vec<u64>,
}

/// What is stored, in the order it is stored, and what to read it with.
struct Duplicated {
    /// Before the mask is frozen: the first copy of every body.
    early: Vec<(u64, String)>,
    /// The load that freezes the mask.
    mask: Vec<(u64, String)>,
    /// After it: other queries over the same words, which move frequencies, with the second
    /// and third copies of the bodies spread among them.
    late: Vec<(u64, String)>,
    bodies: Vec<Body>,
    titles: Vec<String>,
}

fn duplicated(seed: u64) -> Duplicated {
    // Many bodies made of any-of groups alone, on a small hot pool: the bodies whose class a
    // top-64 member decides.
    let shape = GrammarConfig {
        hot_tokens: 80,
        rare_tokens: 300,
        hot_frac: 0.6,
        groups_only_frac: 0.6,
        num_random_titles: 300,
        ..GrammarConfig::default()
    };
    let first = generate_grammar(&GrammarConfig {
        seed,
        num_queries: 300,
        ..shape.clone()
    });
    let mask = generate_grammar(&GrammarConfig {
        seed: seed ^ 0x00FF_00FF,
        num_queries: 600,
        first_id: 1_000_000,
        ..shape.clone()
    });
    let drift = generate_grammar(&GrammarConfig {
        seed: seed ^ 0x0F0F_0F0F,
        num_queries: 600,
        first_id: 2_000_000,
        ..shape
    });
    let mut bodies: Vec<Body> = first
        .queries
        .iter()
        .map(|query| Body {
            title: query.satisfying_title.clone(),
            copies: vec![query.id],
        })
        .collect();
    // Two more copies of every body, one in each half of the drift.
    let mut late = Vec::new();
    let mut next_id = 3_000_000u64;
    let half = drift.queries.len() / 2;
    for (round, chunk) in drift.queries.chunks(half).enumerate() {
        for (at, query) in chunk.iter().enumerate() {
            late.push((query.id, query.dsl.clone()));
            let body = (at + round * 7) % first.queries.len();
            if at % 2 == round % 2 {
                late.push((next_id, first.queries[body].dsl.clone()));
                bodies[body].copies.push(next_id);
                next_id += 1;
            }
        }
    }
    let mut titles = first.titles();
    titles.extend(drift.near_misses.iter().take(200).cloned());
    Duplicated {
        early: first.dsl(),
        mask: mask.dsl(),
        late,
        bodies,
        titles,
    }
}

/// The first copies, live, on an empty engine; then the load that freezes the mask.
fn start(eng: &mut Engine, corpus: &Duplicated) {
    for (id, dsl) in &corpus.early {
        eng.insert_live(dsl, *id, 1);
    }
    eng.bulk_ingest(&corpus.mask);
}

/// How many bodies have both a copy a default read returns and a copy it does not.
fn split_bodies(eng: &Engine, bodies: &[Body]) -> usize {
    bodies
        .iter()
        .filter(|body| {
            let (default, broad) = reads(eng, &body.title);
            let stored = body.copies.iter().filter(|id| broad.contains(id)).count();
            let visible = body.copies.iter().filter(|id| default.contains(id)).count();
            stored > 1 && visible != 0 && visible != stored
        })
        .count()
}

fn all_stored(corpus: &Duplicated) -> Vec<(u64, String)> {
    corpus
        .early
        .iter()
        .chain(&corpus.mask)
        .chain(&corpus.late)
        .cloned()
        .collect()
}

/// Every match of the stored queries, by brute force, against a read with broad on.
fn assert_equals_brute(eng: &Engine, brute: &Brute, titles: &[String], ctx: &str) {
    let mut lc = String::new();
    let mut feats = Vec::new();
    for title in titles {
        let (_, broad) = reads(eng, title);
        let want: HashSet<u64> = brute.matches(title, &mut lc, &mut feats);
        assert_eq!(broad, want, "{ctx}: {title:?}");
    }
}

/// Dedup on and dedup off return the same sets, with broad off and on, after every step:
/// the early copies, each run of late inserts in the memtable and flushed, and the merge.
/// And a read with broad on equals brute force at the end. The corpus has to hold bodies
/// with copies on both sides of the opt-in boundary, or none of this says anything about
/// the case it is for.
#[test]
fn a_duplicated_grammar_corpus_is_dedup_invariant_in_both_read_modes() {
    for seed in [0x6AA2_0D01u64, 0x6AA2_0D02] {
        let corpus = duplicated(seed);
        let build = |dedup: bool| {
            let mut eng = new_engine(manual_cfg(dedup));
            start(&mut eng, &corpus);
            eng
        };
        let (mut on, mut off) = (build(true), build(false));
        assert_on_equals_off(&on, &off, &corpus.titles, "early");
        for (step, chunk) in corpus.late.chunks(corpus.late.len() / 3).enumerate() {
            for (id, dsl) in chunk {
                on.insert_live(dsl, *id, 1);
                off.insert_live(dsl, *id, 1);
            }
            assert_on_equals_off(&on, &off, &corpus.titles, &format!("memtable {step}"));
            on.flush();
            off.flush();
            assert_on_equals_off(&on, &off, &corpus.titles, &format!("flushed {step}"));
        }
        let split = split_bodies(&off, &corpus.bodies);
        assert!(
            split >= 10,
            "seed {seed:#x}: only {split} bodies have copies on both sides of the opt-in \
             boundary"
        );
        let brute = Brute::build(&all_stored(&corpus));
        assert_equals_brute(&on, &brute, &corpus.titles, "before the merge");
        on.compact_all().expect("compaction ran");
        off.compact_all().expect("compaction ran");
        assert_on_equals_off(&on, &off, &corpus.titles, "compacted");
        assert_equals_brute(&on, &brute, &corpus.titles, "after the merge");
    }
}

/// The same corpus through the merge that re-derives each body's plan from current
/// frequencies. No row a default read returned before the merge is missing after it, and a
/// read with broad on still equals brute force.
#[test]
fn reanchoring_a_duplicated_grammar_corpus_never_hides_a_visible_row() {
    let corpus = duplicated(0x6AA2_0D03);
    let mut eng = new_engine(EngineConfig {
        compaction_reanchor: true,
        ..manual_cfg(true)
    });
    start(&mut eng, &corpus);
    for chunk in corpus.late.chunks(corpus.late.len() / 3) {
        for (id, dsl) in chunk {
            eng.insert_live(dsl, *id, 1);
        }
        eng.flush();
    }
    let split = split_bodies(&eng, &corpus.bodies);
    assert!(split >= 10, "only {split} bodies are split");
    let before: Vec<_> = corpus
        .titles
        .iter()
        .map(|title| reads(&eng, title))
        .collect();
    eng.compact_all().expect("re-anchoring compaction ran");
    for (title, (default_before, broad_before)) in corpus.titles.iter().zip(before) {
        let (default_after, broad_after) = reads(&eng, title);
        assert_eq!(
            broad_after, broad_before,
            "{title:?}: matches changed across the merge"
        );
        let hidden: Vec<_> = default_before.difference(&default_after).collect();
        assert!(
            hidden.is_empty(),
            "{title:?}: the re-anchoring merge hid default-visible rows {hidden:?}"
        );
    }
    let brute = Brute::build(&all_stored(&corpus));
    assert_equals_brute(&eng, &brute, &corpus.titles, "after the re-anchoring merge");
}
