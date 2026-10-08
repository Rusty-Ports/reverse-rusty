//! The grammar differential: random queries that use every clause of the DSL in any order,
//! engine against the independent reference, plus the property that needs no reference at
//! all: a title built to satisfy a query retrieves it.
//!
//! [`generate`](reverse_rusty::gen::generate), which every other at-scale differential uses,
//! writes bare terms with single-term negations at the end. The shapes where clauses
//! interact (several any-of groups, a body with no bare term, phrases, negated groups, a
//! negation between two positive runs) were covered by hand-written cases only.

use std::collections::HashSet;

use crate::harness::RefOracle;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::grammar::{
    generate_grammar, render, Clause, GrammarConfig, GrammarDataset,
};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

/// Fixed seeds, as everywhere in the gate (ADR-008). Set `RR_GRAMMAR_SEED` to run one other.
fn seeds() -> Vec<u64> {
    match std::env::var("RR_GRAMMAR_SEED") {
        Ok(seed) => vec![parse_seed(&seed)],
        Err(_) => vec![0x6AA2_0025, 0x0BAD_5EED, 0x51CA_FE77],
    }
}

fn parse_seed(text: &str) -> u64 {
    let text = text.trim();
    let parsed = match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => text.parse(),
    };
    parsed.unwrap_or_else(|_| panic!("RR_GRAMMAR_SEED is not a number: {text:?}"))
}

fn corpus(seed: u64) -> GrammarDataset {
    generate_grammar(&GrammarConfig {
        seed,
        ..GrammarConfig::default()
    })
}

/// The generator does what it is for: every clause kind, bodies of any-of groups alone,
/// several groups in one body, and a negation that sits between two positive clauses. A
/// generator that quietly stopped writing one of these would leave every test below green.
#[test]
fn the_grammar_corpus_has_every_shape() {
    let data = corpus(seeds()[0]);
    let mut kinds = HashSet::new();
    let (mut groups_only, mut multi_group, mut negation_between, mut two_token_member) =
        (0usize, 0usize, 0usize, 0usize);
    for query in &data.queries {
        let positive = |clause: &Clause| {
            matches!(
                clause,
                Clause::Terms(_) | Clause::Phrase(_) | Clause::AnyOf(_)
            )
        };
        let groups = query
            .clauses
            .iter()
            .filter(|clause| matches!(clause, Clause::AnyOf(_)))
            .count();
        let bare = query
            .clauses
            .iter()
            .any(|clause| matches!(clause, Clause::Terms(_) | Clause::Phrase(_)));
        groups_only += usize::from(groups > 0 && !bare);
        multi_group += usize::from(groups >= 2);
        let first = query.clauses.iter().position(positive);
        let last = query.clauses.iter().rposition(positive);
        if let (Some(first), Some(last)) = (first, last) {
            negation_between +=
                usize::from(query.clauses[first..last].iter().any(|c| !positive(c)));
        }
        for clause in &query.clauses {
            kinds.insert(std::mem::discriminant(clause));
            if let Clause::AnyOf(members) | Clause::NotAnyOf(members) = clause {
                two_token_member += usize::from(members.iter().any(|m| m.len() == 2));
            }
        }
        assert!(
            query.clauses.iter().any(positive),
            "a query with no positive clause: {}",
            query.dsl
        );
    }
    assert_eq!(kinds.len(), 6, "a clause kind is never generated");
    for (what, count) in [
        ("any-of groups only", groups_only),
        ("two or more any-of groups", multi_group),
        ("a negation between positive clauses", negation_between),
        ("a two-token any-of member", two_token_member),
    ] {
        assert!(
            count * 10 >= data.queries.len(),
            "{what}: only {count} of {} queries",
            data.queries.len()
        );
    }
}

/// Engine against the independent reference on every title: the one built for each query,
/// the near-misses (a required token dropped, a phrase broken, a forbidden clause completed)
/// and random bags of tokens. Zero false negatives and zero false positives.
#[test]
fn grammar_corpus_differential() {
    for seed in seeds() {
        let data = corpus(seed);
        let oracle = RefOracle::build_default(&data.dsl());
        oracle.assert_matches(&data.titles(), &format!("grammar/{seed:#x}"));
    }
}

/// The same with the hot tier on (ADR-105): a cost placement, which the reference does not
/// know about and must not be able to see.
#[test]
fn grammar_corpus_differential_with_the_hot_tier_on() {
    let seed = seeds()[0];
    let data = corpus(seed);
    let oracle = RefOracle::build_default_with_config(
        &data.dsl(),
        EngineConfig {
            hot_anchor_threshold: 16,
            ..EngineConfig::default()
        },
    );
    oracle.assert_matches(&data.titles(), &format!("grammar+hot-tier/{seed:#x}"));
}

/// A title built to satisfy a query retrieves it. The title is made from the language rules
/// (every bare term, every phrase unbroken, one whole member of each group, and of each
/// forbidden phrase or two-token member at most one token), with no reference involved, so
/// this also holds where the engine and the reference could share a misreading. It is checked
/// on three engines: built in one batch, loaded in three phases across segments, and loaded
/// that way and then compacted.
#[test]
fn a_title_built_to_satisfy_a_query_retrieves_it() {
    for seed in seeds() {
        let data = corpus(seed);
        let queries = data.dsl();
        let norm = || Normalizer::default_vocab().expect("default vocabulary");

        let mut one_batch = Engine::new(norm());
        one_batch.build_from_queries(&queries);

        let third = queries.len() / 3;
        let in_phases = |compact: bool| {
            let mut engine = Engine::new(norm());
            engine.build_from_queries(&queries[..third]);
            engine.bulk_ingest(&queries[third..2 * third]);
            for (id, dsl) in &queries[2 * third..] {
                engine.insert_live(dsl, *id, 1);
            }
            if compact {
                engine.flush();
                engine.compact_all();
            }
            engine
        };

        for (how, engine) in [
            ("one batch", one_batch),
            ("three phases", in_phases(false)),
            ("three phases, compacted", in_phases(true)),
        ] {
            let mut scratch = MatchScratch::new();
            let mut out = Vec::new();
            let mut missed = Vec::new();
            for query in &data.queries {
                engine.match_title(&query.satisfying_title, &mut scratch, &mut out, true);
                if !out.contains(&query.id) {
                    missed.push(format!(
                        "  query {:?}\n  title {:?}",
                        query.dsl, query.satisfying_title
                    ));
                }
            }
            assert!(
                missed.is_empty(),
                "seed {seed:#x}, {how}: {} of {} queries were not retrieved by a title built \
                 to satisfy them. The first:\n{}",
                missed.len(),
                data.queries.len(),
                missed[0]
            );
        }
    }
}

/// Relations between a query and an edit of it that hold whatever the titles are, checked on
/// every title. They need no reference and no constructed title:
///
/// - adding a negation can only remove matches: `Q -w` ⊆ `Q`;
/// - adding a member to an any-of group can only add matches: `Q` ⊆ `Q` with a wider group;
/// - a phrase asks for more than its words: `"a b" …` ⊆ `a b …`.
///
/// Each edited query is stored beside its original, so the relation is read off one match
/// call. A title built to satisfy `Q`, with the new negation's word added, must match `Q`
/// and not `Q -w`: the subset is proper where it should be.
#[test]
fn an_edited_query_matches_a_subset_or_a_superset_as_its_edit_says() {
    let data = corpus(seeds()[0]);
    let n = data.queries.len() as u64;
    let (narrower, wider, unphrased) = (n, 2 * n, 3 * n);
    let extra = "zzextraword";
    let mut stored = data.dsl();
    let (mut widened, mut loosened) = (0usize, 0usize);
    for query in &data.queries {
        stored.push((query.id + narrower, format!("{} -{extra}", query.dsl)));
        let mut clauses = query.clauses.clone();
        if let Some(Clause::AnyOf(members)) = clauses
            .iter_mut()
            .find(|clause| matches!(clause, Clause::AnyOf(_)))
        {
            members.push(vec![extra.to_string()]);
            stored.push((query.id + wider, render(&clauses)));
            widened += 1;
        }
        let mut clauses = query.clauses.clone();
        if let Some(clause) = clauses
            .iter_mut()
            .find(|clause| matches!(clause, Clause::Phrase(_)))
        {
            if let Clause::Phrase(tokens) = clause.clone() {
                *clause = Clause::Terms(tokens);
            }
            stored.push((query.id + unphrased, render(&clauses)));
            loosened += 1;
        }
    }
    assert!(
        widened * 4 >= data.queries.len() && loosened * 10 >= data.queries.len(),
        "too few edited queries to say anything: {widened} widened, {loosened} unphrased"
    );
    let mut engine = Engine::new(Normalizer::default_vocab().expect("default vocabulary"));
    engine.build_from_queries(&stored);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut titles = data.titles();
    let with_the_new_word: Vec<String> = data
        .queries
        .iter()
        .map(|query| format!("{} {extra}", query.satisfying_title))
        .collect();
    titles.extend(with_the_new_word.iter().cloned());
    for title in &titles {
        engine.match_title(title, &mut scratch, &mut out, true);
        let matched: HashSet<u64> = out.iter().copied().collect();
        for &id in &matched {
            // An edit that narrows: its match implies the original's.
            if (narrower..wider).contains(&id) {
                assert!(
                    matched.contains(&(id - narrower)),
                    "{title:?} matches a query with one more negation and not the query"
                );
            }
            // An edit that widens: the original's match implies its own.
            if id < n {
                for edited in [id + wider, id + unphrased] {
                    let stored = stored.iter().any(|(stored, _)| *stored == edited);
                    assert!(
                        !stored || matched.contains(&edited),
                        "{title:?} matches query {id} and not its wider edit {edited}"
                    );
                }
            }
        }
    }
    for (query, title) in data.queries.iter().zip(&with_the_new_word) {
        engine.match_title(title, &mut scratch, &mut out, true);
        assert!(
            out.contains(&query.id) && !out.contains(&(query.id + narrower)),
            "{title:?}: the query {:?} must match and its negated edit must not",
            query.dsl
        );
    }
}
