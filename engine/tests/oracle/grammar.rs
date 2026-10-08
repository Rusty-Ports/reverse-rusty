//! The grammar corpus through duplicates, frequency drift, flushes and both merges.
//!
//! Identical bodies stored at different times can plan differently: the first copies are
//! compiled before the frequency mask is frozen, and later ones after other queries have
//! moved the frequencies an any-of body's plan is chosen by. Dedup must never let one copy's
//! class decide which reads see another (ADR-186), and a merge must never hide a row a
//! default read returned (ADR-187). `dedup_visibility` pins both on small hand-shaped
//! bodies; here the bodies are written by the grammar generator: several groups, two-token
//! members, phrases and negations beside the groups, hundreds of bodies.
//!
//! The reference is the **ungrouped twin**: the same queries, each with one more negation, a
//! forbidden phrase that is its own and is in no title. A negation takes no part in planning
//! and is not counted toward frequency, so every copy plans as it does in the original
//! corpus, and no two bodies are equal, so nothing can ever be grouped: not in the memtable
//! and not by either merge. (Switching dedup off is not
//! such a reference. The merges regroup whatever the switch says.) What a read returns from
//! the original corpus must be what it returns from the twin, in both read modes, at every
//! step.

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

/// The words the twin's extra negations are written in. Never in a title.
fn twin_word(digit: u64) -> String {
    format!("zzn{}", (b'a' + (digit % 26) as u8) as char)
}

/// Stored first in every engine here, the original and the twin alike: it gives the twin's
/// words their feature ids before any other query is compiled. The frozen top-64 mask breaks
/// frequency ties by id, so the twin can only be compared with the original if every other
/// feature has the same id in both. (A forbidden word is not counted toward frequency.)
fn priming_query() -> (u64, String) {
    let words: Vec<String> = (0..26)
        .map(|digit| format!("-{}", twin_word(digit)))
        .collect();
    (9_000_000, format!("zzprime {}", words.join(" ")))
}

/// `queries` with one more negation each: a forbidden phrase that spells the query's id in
/// the twin's words. No two bodies are equal, and no title holds the phrase.
fn ungrouped(queries: &[(u64, String)]) -> Vec<(u64, String)> {
    queries
        .iter()
        .map(|(id, dsl)| {
            let mut n = *id;
            let spelled: Vec<String> = (0..5)
                .map(|_| {
                    let word = twin_word(n);
                    n /= 26;
                    word
                })
                .collect();
            (*id, format!("{dsl} -\"{}\"", spelled.join(" ")))
        })
        .collect()
}

impl Duplicated {
    fn ungrouped_twin(&self) -> Duplicated {
        Duplicated {
            early: ungrouped(&self.early),
            mask: ungrouped(&self.mask),
            late: ungrouped(&self.late),
            bodies: Vec::new(),
            titles: Vec::new(),
        }
    }
}

/// Both reads of every title return the same from `eng` and from its ungrouped twin.
fn assert_reads_like_the_twin(eng: &Engine, twin: &Engine, titles: &[String], ctx: &str) {
    for title in titles {
        let (default, broad) = reads(eng, title);
        let (twin_default, twin_broad) = reads(twin, title);
        assert_eq!(broad, twin_broad, "{ctx}: {title:?}, broad on");
        let hidden: Vec<_> = twin_default.difference(&default).collect();
        let shown: Vec<_> = default.difference(&twin_default).collect();
        assert!(
            hidden.is_empty() && shown.is_empty(),
            "{ctx}: {title:?}: grouping hid {hidden:?} and showed {shown:?} in a default read"
        );
    }
}

/// The first copies, live, on an empty engine; then the load that freezes the mask.
fn start(eng: &mut Engine, corpus: &Duplicated) {
    let (id, dsl) = priming_query();
    eng.insert_live(&dsl, id, 1);
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
        let twin_corpus = corpus.ungrouped_twin();
        let mut twin = new_engine(manual_cfg(true));
        start(&mut twin, &twin_corpus);
        assert_on_equals_off(&on, &off, &corpus.titles, "early");
        assert_reads_like_the_twin(&on, &twin, &corpus.titles, "early");
        let third = corpus.late.len() / 3;
        for (step, (chunk, twin_chunk)) in corpus
            .late
            .chunks(third)
            .zip(twin_corpus.late.chunks(third))
            .enumerate()
        {
            for (id, dsl) in chunk {
                on.insert_live(dsl, *id, 1);
                off.insert_live(dsl, *id, 1);
            }
            for (id, dsl) in twin_chunk {
                twin.insert_live(dsl, *id, 1);
            }
            assert_on_equals_off(&on, &off, &corpus.titles, &format!("memtable {step}"));
            assert_reads_like_the_twin(&on, &twin, &corpus.titles, &format!("memtable {step}"));
            on.flush();
            off.flush();
            twin.flush();
            assert_on_equals_off(&on, &off, &corpus.titles, &format!("flushed {step}"));
            assert_reads_like_the_twin(&on, &twin, &corpus.titles, &format!("flushed {step}"));
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
        twin.compact_all().expect("compaction ran");
        assert_on_equals_off(&on, &off, &corpus.titles, "compacted");
        assert_reads_like_the_twin(&on, &twin, &corpus.titles, "compacted");
        assert_reads_like_the_twin(&off, &twin, &corpus.titles, "compacted, dedup off");
        assert_equals_brute(&on, &brute, &corpus.titles, "after the merge");
    }
}

/// The same corpus through the merge that re-derives each body's plan from current
/// frequencies. Grouping must not change what either read returns: before and after the
/// merge the corpus reads like its ungrouped twin, so no copy is hidden by an opt-in leader
/// and none is shown by a visible one. No row a default read returned before the merge is
/// missing after it, and a read with broad on still equals brute force.
#[test]
fn reanchoring_a_duplicated_grammar_corpus_reads_like_its_ungrouped_twin() {
    let corpus = duplicated(0x6AA2_0D03);
    let twin_corpus = corpus.ungrouped_twin();
    let build = |corpus: &Duplicated| {
        let mut eng = new_engine(EngineConfig {
            compaction_reanchor: true,
            ..manual_cfg(true)
        });
        start(&mut eng, corpus);
        for chunk in corpus.late.chunks(corpus.late.len() / 3) {
            for (id, dsl) in chunk {
                eng.insert_live(dsl, *id, 1);
            }
            eng.flush();
        }
        eng
    };
    let (mut eng, mut twin) = (build(&corpus), build(&twin_corpus));
    let split = split_bodies(&eng, &corpus.bodies);
    assert!(split >= 10, "only {split} bodies are split");
    assert_reads_like_the_twin(&eng, &twin, &corpus.titles, "before the re-anchoring merge");
    let before: Vec<_> = corpus
        .titles
        .iter()
        .map(|title| reads(&eng, title))
        .collect();
    eng.compact_all().expect("re-anchoring compaction ran");
    twin.compact_all().expect("re-anchoring compaction ran");
    assert_reads_like_the_twin(&eng, &twin, &corpus.titles, "after the re-anchoring merge");
    for (title, (default_before, _)) in corpus.titles.iter().zip(before) {
        let (default_after, _) = reads(&eng, title);
        let hidden: Vec<_> = default_before.difference(&default_after).collect();
        assert!(
            hidden.is_empty(),
            "{title:?}: the re-anchoring merge hid default-visible rows {hidden:?}"
        );
    }
    assert!(
        split_bodies(&eng, &corpus.bodies) >= 10,
        "the merge left no body with copies on both sides: nothing was at stake"
    );
    let brute = Brute::build(&all_stored(&corpus));
    assert_equals_brute(&eng, &brute, &corpus.titles, "after the re-anchoring merge");
}
