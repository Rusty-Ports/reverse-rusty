//! Grammar-random queries, and a title built to satisfy each one.
//!
//! [`generate`](super::generate) writes one shape of query: bare required terms with
//! single-term negations at the end. This generator writes every clause the DSL has, in any
//! order: runs of bare terms, quoted phrases, any-of groups with one- and two-token members,
//! and the negated form of each, including a negation between two positive runs and a body
//! made of any-of groups alone.
//!
//! For each query it also builds a title that satisfies it, from the language rules and not
//! from the engine: every bare term, every phrase in order and unbroken, one whole member of
//! each any-of group, and no forbidden clause complete. A forbidden phrase contributes one
//! of its tokens to that title, or all of them with a word between each two, and a forbidden
//! two-token member contributes one token, so that an engine which rejects on part of a
//! negated clause, or on a phrase's words out of place, fails. Such a title must retrieve its query: that is the
//! lossless-cover contract, stated for a title that is known to satisfy the query without
//! asking anything what "satisfy" means.
//!
//! Tokens come from a small hot pool and a large rare pool, so a corpus fills the top-64
//! frequency mask and has features on both sides of it. Token names depend on their index
//! only, so two corpora from different seeds share a vocabulary and one can be loaded after
//! the other to move frequencies.

use std::collections::HashSet;

use super::Rng;

/// How to generate a grammar corpus.
#[derive(Clone, Debug)]
pub struct GrammarConfig {
    pub seed: u64,
    pub num_queries: usize,
    /// Random titles, in addition to the one built for each query and the near-misses.
    pub num_random_titles: usize,
    /// Tokens that are used often. More than 64, so the frequency mask is contested.
    pub hot_tokens: usize,
    /// Tokens that are used rarely.
    pub rare_tokens: usize,
    /// How often a token position draws from the hot pool.
    pub hot_frac: f64,
    /// How many queries have no bare term and no phrase: any-of groups only (and negations).
    pub groups_only_frac: f64,
    /// First logical id.
    pub first_id: u64,
}

impl Default for GrammarConfig {
    fn default() -> Self {
        GrammarConfig {
            seed: 0x6AA2_0025,
            num_queries: 2_000,
            num_random_titles: 1_000,
            hot_tokens: 96,
            rare_tokens: 1_500,
            hot_frac: 0.4,
            groups_only_frac: 0.3,
            first_id: 0,
        }
    }
}

/// One clause of a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Clause {
    /// A run of bare required terms.
    Terms(Vec<String>),
    /// `"a b"`: the tokens in order, with nothing between them.
    Phrase(Vec<String>),
    /// `(a,b c)`: at least one member, and every token of that member.
    AnyOf(Vec<Vec<String>>),
    /// `-a`.
    NotTerm(String),
    /// `-"a b"`.
    NotPhrase(Vec<String>),
    /// `-(a,b c)`: no member complete.
    NotAnyOf(Vec<Vec<String>>),
}

impl Clause {
    fn render(&self) -> String {
        let group = |members: &[Vec<String>]| {
            let members: Vec<String> = members.iter().map(|member| member.join(" ")).collect();
            format!("({})", members.join(","))
        };
        match self {
            Clause::Terms(terms) => terms.join(" "),
            Clause::Phrase(tokens) => format!("\"{}\"", tokens.join(" ")),
            Clause::AnyOf(members) => group(members),
            Clause::NotTerm(term) => format!("-{term}"),
            Clause::NotPhrase(tokens) => format!("-\"{}\"", tokens.join(" ")),
            Clause::NotAnyOf(members) => format!("-{}", group(members)),
        }
    }
}

/// `clauses` as a query in the DSL.
#[must_use]
pub fn render(clauses: &[Clause]) -> String {
    clauses
        .iter()
        .map(Clause::render)
        .collect::<Vec<_>>()
        .join(" ")
}

/// A generated query.
#[derive(Clone, Debug)]
pub struct GrammarQuery {
    pub id: u64,
    pub clauses: Vec<Clause>,
    /// The query in the DSL.
    pub dsl: String,
    /// A title that satisfies the query (see the module documentation).
    pub satisfying_title: String,
}

/// A grammar corpus.
pub struct GrammarDataset {
    pub queries: Vec<GrammarQuery>,
    /// Titles that are one edit away from a satisfying title: a required token dropped, a
    /// phrase broken or reversed, a forbidden clause completed.
    pub near_misses: Vec<String>,
    /// Bags of tokens from the pools.
    pub random_titles: Vec<String>,
}

impl GrammarDataset {
    /// `(logical id, DSL)`, the form an engine is built from.
    #[must_use]
    pub fn dsl(&self) -> Vec<(u64, String)> {
        self.queries
            .iter()
            .map(|query| (query.id, query.dsl.clone()))
            .collect()
    }

    /// Every title: the one built for each query, the near-misses, and the random ones.
    #[must_use]
    pub fn titles(&self) -> Vec<String> {
        self.queries
            .iter()
            .map(|query| query.satisfying_title.clone())
            .chain(self.near_misses.iter().cloned())
            .chain(self.random_titles.iter().cloned())
            .collect()
    }
}

/// A letters-only word for index `i`, distinct for distinct `(prefix, i)`. No digits: a
/// number is typed by the normalizer and a word is not.
fn word(prefix: &str, mut i: usize) -> String {
    let mut out = String::from(prefix);
    for _ in 0..4 {
        out.push((b'a' + (i % 26) as u8) as char);
        i /= 26;
    }
    out
}

struct Pools {
    hot: Vec<String>,
    rare: Vec<String>,
    filler: Vec<String>,
}

impl Pools {
    fn new(cfg: &GrammarConfig) -> Self {
        Pools {
            hot: (0..cfg.hot_tokens.max(1)).map(|i| word("hx", i)).collect(),
            rare: (0..cfg.rare_tokens.max(1)).map(|i| word("rq", i)).collect(),
            // Never in a query: what a real title has around the words that matter.
            filler: (0..200).map(|i| word("fy", i)).collect(),
        }
    }

    fn token(&self, rng: &mut Rng, hot_frac: f64) -> &str {
        if rng.frac() < hot_frac {
            &self.hot[rng.skewed(self.hot.len(), 1.5)]
        } else {
            &self.rare[rng.below(self.rare.len())]
        }
    }
}

/// The tokens a query has used so far. A positive clause never takes a forbidden token and
/// a negated clause never takes a required one, so every query can be satisfied; and no two
/// negated clauses share a token, so one token of a forbidden phrase can be put in a title
/// without completing some other forbidden clause.
#[derive(Default)]
struct Used {
    positive: HashSet<String>,
    negative: HashSet<String>,
}

impl Used {
    fn positive(&mut self, rng: &mut Rng, pools: &Pools, hot_frac: f64) -> String {
        // Now and then a positive clause reuses a token of an earlier one: `a (a,b)`.
        if !self.positive.is_empty() && rng.frac() < 0.1 {
            let mut earlier: Vec<&String> = self.positive.iter().collect();
            earlier.sort();
            return earlier[rng.below(earlier.len())].clone();
        }
        loop {
            let token = pools.token(rng, hot_frac);
            if !self.negative.contains(token) {
                self.positive.insert(token.to_string());
                return token.to_string();
            }
        }
    }

    fn negative(&mut self, rng: &mut Rng, pools: &Pools, hot_frac: f64) -> String {
        loop {
            let token = pools.token(rng, hot_frac);
            if !self.positive.contains(token) && self.negative.insert(token.to_string()) {
                return token.to_string();
            }
        }
    }
}

fn shuffle<T>(rng: &mut Rng, items: &mut [T]) {
    for i in (1..items.len()).rev() {
        items.swap(i, rng.below(i + 1));
    }
}

fn gen_clauses(rng: &mut Rng, cfg: &GrammarConfig, pools: &Pools) -> Vec<Clause> {
    let mut used = Used::default();
    let hot = cfg.hot_frac;
    let mut clauses = Vec::new();
    let tokens = |rng: &mut Rng, used: &mut Used, n: usize, negative: bool| -> Vec<String> {
        (0..n)
            .map(|_| {
                if negative {
                    used.negative(rng, pools, hot)
                } else {
                    used.positive(rng, pools, hot)
                }
            })
            .collect()
    };
    let group = |rng: &mut Rng, used: &mut Used, negative: bool| -> Vec<Vec<String>> {
        (0..2 + rng.below(3))
            .map(|_| {
                let len = if rng.frac() < 0.3 { 2 } else { 1 };
                tokens(rng, used, len, negative)
            })
            .collect()
    };

    let groups_only = rng.frac() < cfg.groups_only_frac;
    if groups_only {
        for _ in 0..=rng.below(3) {
            clauses.push(Clause::AnyOf(group(rng, &mut used, false)));
        }
    } else {
        for _ in 0..=rng.below(2) {
            let run = 1 + rng.below(3);
            clauses.push(Clause::Terms(
                (0..run).map(|_| used.positive(rng, pools, hot)).collect(),
            ));
        }
        if rng.frac() < 0.3 {
            let len = 2 + rng.below(2);
            clauses.push(Clause::Phrase(
                (0..len).map(|_| used.positive(rng, pools, hot)).collect(),
            ));
        }
        for _ in 0..rng.below(3) {
            clauses.push(Clause::AnyOf(group(rng, &mut used, false)));
        }
    }
    for _ in 0..rng.below(4) {
        clauses.push(match rng.below(5) {
            0 => {
                let len = 2 + rng.below(2);
                Clause::NotPhrase((0..len).map(|_| used.negative(rng, pools, hot)).collect())
            }
            1 => Clause::NotAnyOf(group(rng, &mut used, true)),
            _ => Clause::NotTerm(used.negative(rng, pools, hot)),
        });
    }
    // Any order: a negation between two positive runs is a clause boundary (ADR-118).
    shuffle(rng, &mut clauses);
    clauses
}

/// A title that satisfies `clauses`.
fn satisfying_title(rng: &mut Rng, pools: &Pools, clauses: &[Clause]) -> String {
    let mut parts: Vec<Vec<String>> = Vec::new();
    for clause in clauses {
        match clause {
            Clause::Terms(terms) => {
                // The terms of a run in any order, each a part of its own.
                for term in terms {
                    parts.push(vec![term.clone()]);
                }
            }
            // In order and unbroken: one part.
            Clause::Phrase(tokens) => parts.push(tokens.clone()),
            // One member, whole. Its tokens need not be adjacent.
            Clause::AnyOf(members) => {
                for token in &members[rng.below(members.len())] {
                    parts.push(vec![token.clone()]);
                }
            }
            Clause::NotTerm(_) => {}
            // A forbidden phrase is its tokens in order with nothing between them. One of
            // them does not complete it, and neither do all of them with a word between
            // each two.
            Clause::NotPhrase(tokens) => match rng.below(3) {
                0 => parts.push(vec![tokens[rng.below(tokens.len())].clone()]),
                1 => {
                    let mut apart = Vec::new();
                    for token in tokens {
                        if !apart.is_empty() {
                            apart.push(pools.filler[rng.below(pools.filler.len())].clone());
                        }
                        apart.push(token.clone());
                    }
                    parts.push(apart);
                }
                _ => {}
            },
            // Nor does one token of a two-token member complete that member.
            Clause::NotAnyOf(members) => {
                for member in members.iter().filter(|member| member.len() > 1) {
                    if rng.frac() < 0.5 {
                        parts.push(vec![member[rng.below(member.len())].clone()]);
                    }
                }
            }
        }
    }
    shuffle(rng, &mut parts);
    let mut title: Vec<String> = Vec::new();
    for part in parts {
        if rng.frac() < 0.4 {
            title.push(pools.filler[rng.below(pools.filler.len())].clone());
        }
        title.extend(part);
    }
    if rng.frac() < 0.4 {
        title.push(pools.filler[rng.below(pools.filler.len())].clone());
    }
    title.join(" ")
}

/// A title one edit away from `title`, which satisfies `clauses`. Whether it still matches
/// is for a reference to say: dropping a token that an any-of group can do without leaves a
/// match, and completing a forbidden clause does not.
fn near_miss(rng: &mut Rng, pools: &Pools, clauses: &[Clause], title: &str) -> String {
    let mut tokens: Vec<String> = title.split(' ').map(str::to_string).collect();
    let clause = &clauses[rng.below(clauses.len())];
    match clause {
        // Complete what is forbidden.
        Clause::NotTerm(term) => tokens.push(term.clone()),
        Clause::NotPhrase(phrase) => tokens.extend(phrase.iter().cloned()),
        Clause::NotAnyOf(members) => {
            tokens.extend(members[rng.below(members.len())].iter().cloned());
        }
        // Break a phrase: put something in the middle of it, or turn it around.
        Clause::Phrase(phrase) => {
            let at = tokens.iter().position(|token| *token == phrase[0]);
            if let Some(at) = at {
                if rng.frac() < 0.5 && at + 1 < tokens.len() {
                    tokens.swap(at, at + 1);
                } else {
                    tokens.insert(at + 1, pools.filler[rng.below(pools.filler.len())].clone());
                }
            }
        }
        // Drop a token.
        Clause::Terms(_) | Clause::AnyOf(_) => {
            if tokens.len() > 1 {
                let at = rng.below(tokens.len());
                tokens.remove(at);
            }
        }
    }
    tokens.join(" ")
}

/// Generate a grammar corpus. Deterministic in `cfg`.
#[must_use]
pub fn generate_grammar(cfg: &GrammarConfig) -> GrammarDataset {
    let mut rng = Rng::new(cfg.seed);
    let pools = Pools::new(cfg);
    let mut queries = Vec::with_capacity(cfg.num_queries);
    let mut near_misses = Vec::with_capacity(cfg.num_queries);
    for n in 0..cfg.num_queries {
        let clauses = gen_clauses(&mut rng, cfg, &pools);
        let dsl = render(&clauses);
        let satisfying_title = satisfying_title(&mut rng, &pools, &clauses);
        near_misses.push(near_miss(&mut rng, &pools, &clauses, &satisfying_title));
        queries.push(GrammarQuery {
            id: cfg.first_id + n as u64,
            clauses,
            dsl,
            satisfying_title,
        });
    }
    let random_titles = (0..cfg.num_random_titles)
        .map(|_| {
            let len = 3 + rng.below(10);
            (0..len)
                .map(|_| {
                    if rng.frac() < 0.15 {
                        pools.filler[rng.below(pools.filler.len())].clone()
                    } else {
                        pools.token(&mut rng, 0.7).to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    GrammarDataset {
        queries,
        near_misses,
        random_titles,
    }
}
