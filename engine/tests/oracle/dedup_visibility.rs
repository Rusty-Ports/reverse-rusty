//! Dedup groups are partitioned by visibility (ADR-186).
//!
//! A group member rides its leader's postings and takes its class byte, so a
//! join must never change which reads see the member. Identical bodies CAN sit
//! on opposite sides of the opt-in boundary: a copy compiled before the first
//! mask finalize saw no top-64 feature at all, and a rebuild keeps a copy that
//! default reads returned visible whatever it would plan today (ADR-187). Every
//! leg here builds such a pair and checks that each copy keeps its own
//! visibility:
//!
//! 1. the memtable join, and the same join inside a rebuilt segment, in both
//!    directions;
//! 2. the grouped merge and the re-anchoring merge, in both segment orders;
//! 3. flush and a durable reopen (the adopted class byte is what gets stored);
//! 4. a duplicated any-of corpus with copies on both sides, dedup on ≡ off in
//!    both `include_broad` modes after every lifecycle step;
//! 5. no sharing is lost when a whole opt-in group re-plans visible together.

use crate::dedup::{assert_on_equals_off, tempdir};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

const VISIBLE_COPY: u64 = 1;
const OPT_IN_COPY: u64 = 2;

/// Filler that pins the frozen top-64 mask: each word in `hot` (10 uses) and
/// 70 `fz` fillers (4 uses) outrank everything else, so a word that is absent
/// here can never get a mask bit however often it is used later.
pub(crate) fn mask_corpus(hot: &[&str]) -> Vec<(u64, String)> {
    mask_corpus_scaled(hot, 1)
}

/// [`mask_corpus`] with every count multiplied by `scale`, for an engine whose
/// other words were already used a few times before the mask is finalized.
fn mask_corpus_scaled(hot: &[&str], scale: u64) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut id = 1_000_000u64;
    for k in 0..70u64 {
        for r in 0..4 * scale {
            out.push((id, format!("fz{k} pad{k}x{r}")));
            id += 1;
        }
    }
    for word in hot {
        for n in 0..10 * scale {
            out.push((id, format!("{word} {word}pad{n}")));
            id += 1;
        }
    }
    out
}

pub(crate) fn manual_cfg(dedup: bool) -> EngineConfig {
    EngineConfig {
        dedup_bodies: dedup,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

pub(crate) fn new_engine(cfg: EngineConfig) -> Engine {
    Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg)
}

/// `(default read, include_broad read)` for one title.
pub(crate) fn reads(eng: &Engine, title: &str) -> (HashSet<u64>, HashSet<u64>) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(title, &mut scratch, &mut out, false);
    let default: HashSet<u64> = out.iter().copied().collect();
    eng.match_title(title, &mut scratch, &mut out, true);
    (default, out.iter().copied().collect())
}

pub(crate) fn ids(list: &[u64]) -> HashSet<u64> {
    list.iter().copied().collect()
}

/// One copy of the body is opt-in and the other is default-visible.
fn assert_split(eng: &Engine, title: &str, ctx: &str) {
    let (default, broad) = reads(eng, title);
    assert_eq!(
        default,
        ids(&[VISIBLE_COPY]),
        "{ctx}: the default read must return exactly the default-visible copy"
    );
    assert_eq!(
        broad,
        ids(&[VISIBLE_COPY, OPT_IN_COPY]),
        "{ctx}: the include_broad read must return both copies"
    );
}

const EARLY_BODY: &str = "(hword,xword)";
const EARLY_TITLE: &str = "xword anything";

/// Compile before vs after the first mask finalize. The live copy is compiled
/// on an empty engine, where nothing is top-64 yet: class B. The bulk batch
/// then finalizes the mask with `hword` in it, so the `bulk_copies` compiled in
/// that batch are class C. Any-of groups are stored as raw feature ids, so all
/// the copies have equal bodies. The live copy stays in the memtable, which puts
/// it AFTER the bulk segment once flushed, unless `seal_live_copy_first` flushes
/// it into a segment of its own before the bulk batch.
fn early_engine(cfg: EngineConfig, bulk_copies: u64, seal_live_copy_first: bool) -> Engine {
    let mut eng = new_engine(cfg);
    eng.insert_live(EARLY_BODY, VISIBLE_COPY, 1);
    if seal_live_copy_first {
        eng.flush();
    }
    let mut batch = mask_corpus(&["hword"]);
    for copy in 0..bulk_copies {
        batch.push((OPT_IN_COPY + copy, EARLY_BODY.to_string()));
    }
    eng.bulk_ingest(&batch);
    eng
}

/// The re-anchoring merge, in both segment orders. The first copy in segment
/// order becomes the dest leader and re-plans class C. When that is the opt-in
/// bulk copy, the live copy must keep its own visible cover and not adopt the
/// leader's lane; when it is the live copy (which the guard keeps visible), the
/// opt-in copy must not be exposed by joining it.
#[test]
fn reanchoring_member_never_takes_its_leaders_visibility() {
    for live_copy_leads in [false, true] {
        let ctx = format!("live_copy_leads={live_copy_leads}");
        let mut eng = early_engine(
            EngineConfig {
                compaction_reanchor: true,
                ..manual_cfg(true)
            },
            1,
            live_copy_leads,
        );
        assert_split(&eng, EARLY_TITLE, &format!("{ctx}: before compaction"));
        eng.flush();
        eng.compact_all().expect("re-anchoring compaction ran");
        // One base segment plus the memtable.
        assert_eq!(eng.num_segments(), 2, "{ctx}: one merge covers both copies");
        assert_split(&eng, EARLY_TITLE, &format!("{ctx}: after the merge"));
    }
}

/// The mechanical grouped merge (re-anchoring off), in both segment orders. Two
/// opt-in copies in the bulk batch make that source carry a group, which
/// selects the grouped arm.
#[test]
fn grouped_merge_member_never_takes_its_leaders_visibility() {
    for live_copy_leads in [false, true] {
        let ctx = format!("live_copy_leads={live_copy_leads}");
        let mut eng = early_engine(manual_cfg(true), 2, live_copy_leads);
        assert!(
            eng.snapshot().dup_joined() > 0,
            "{ctx}: precondition: a source segment must carry a dedup group"
        );
        let expected = (
            ids(&[VISIBLE_COPY]),
            ids(&[VISIBLE_COPY, OPT_IN_COPY, OPT_IN_COPY + 1]),
        );
        assert_eq!(reads(&eng, EARLY_TITLE), expected, "{ctx}: before");
        eng.flush();
        eng.compact_all().expect("compaction ran");
        assert_eq!(eng.num_segments(), 2, "{ctx}: one merge covers every copy");
        assert_eq!(reads(&eng, EARLY_TITLE), expected, "{ctx}: after the merge");
    }
}

/// The memtable join in the visibility-ADDING direction: an opt-in copy that
/// joins a default-visible leader must stay out of the default read.
#[test]
fn opt_in_duplicate_is_not_exposed_by_a_visible_leader() {
    let build = |dedup: bool| {
        let mut eng = early_engine(manual_cfg(dedup), 0, false);
        eng.insert_live(EARLY_BODY, OPT_IN_COPY, 1);
        eng
    };
    let off = build(false);
    assert_split(&off, EARLY_TITLE, "dedup off");
    let on = build(true);
    assert_split(&on, EARLY_TITLE, "memtable");
    assert_on_equals_off(&on, &off, &[EARLY_TITLE.to_string()], "memtable");
}

/// The join inside a REBUILT segment. A rebuild compiles every live query into
/// one fresh segment in source order, keeping a query that default reads
/// returned visible and giving the rest the lane they plan today, so a
/// visible copy and an opt-in copy of one body meet in whichever order their
/// ids sort. Two bodies with opposite id orders cover both directions.
#[test]
fn rebuilt_duplicates_keep_their_own_visibility() {
    use reverse_rusty::vocab::Vocab;

    // (visible id, opt-in id, body, title): the visible copy has the lower id
    // for the first body and the higher id for the second.
    let pairs = [
        (10u64, 20u64, "(hword,xword)", "xword anything"),
        (40u64, 30u64, "(hword,yword)", "yword anything"),
    ];
    let dir = tempdir("visibility-rebuild");
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        ..manual_cfg(true)
    };
    let check = |eng: &Engine, ctx: &str| {
        for (visible, opt_in, _, title) in pairs {
            assert_eq!(
                reads(eng, title),
                (ids(&[visible]), ids(&[visible, opt_in])),
                "{ctx}: {title:?}"
            );
        }
    };
    {
        let mut eng = new_engine(cfg.clone());
        for (visible, _, body, _) in pairs {
            eng.insert_live(body, visible, 1); // nothing is top-64 yet: visible
        }
        eng.bulk_ingest(&mask_corpus(&["hword"]));
        for (_, opt_in, body, _) in pairs {
            eng.insert_live(body, opt_in, 1); // `hword` is top-64 now: opt-in
        }
        check(&eng, "before the rebuild");

        let mut vocab = Vocab::new();
        vocab.add_equivalence(&["unrelated", "unconnected"]);
        eng.set_vocab(vocab).expect("set_vocab");
        assert!(eng.recompile_stale_segments() > 0, "the corpus was rebuilt");
        assert!(
            eng.snapshot().dup_joined() == 0,
            "copies on opposite sides of the boundary must not share a group"
        );
        check(&eng, "after the rebuild");
    }
    // The rebuilt segment is the committed base: its class bytes are on disk.
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen");
    check(&eng, "after a durable reopen");
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

const GROUP_COPIES: u64 = 50;

/// `GROUP_COPIES` copies of one any-of body in a single group, stored class C,
/// re-anchored against a dictionary under which the body no longer has a top-64
/// member. No public write path can produce this any more (the any-of cover is
/// keyed to the frozen mask), so the stored class is set up with one dictionary
/// and the merge is run with another. Returns the merged class counts, how many
/// covers the merge re-derived, and how many rows it moved into the hot tier.
fn reanchor_a_class_c_group(theta: u32) -> ([u64; 5], usize, usize) {
    use reverse_rusty::compile::Extracted;
    use reverse_rusty::dict::{Dict, FeatureKind, NO_MASK_BIT};
    use reverse_rusty::segment::{CompileKnobs, Segment};

    let mut dict = Dict::new();
    // 64 features own the mask; `gword` is one of them and `yword` is not.
    let gword = dict.intern("gword", FeatureKind::Generic);
    for _ in 0..70 {
        dict.bump_freq(gword);
    }
    for i in 0..63 {
        let filler = dict.intern(&format!("filler{i}"), FeatureKind::Generic);
        for _ in 0..100 {
            dict.bump_freq(filler);
        }
    }
    let yword = dict.intern("yword", FeatureKind::Generic);
    dict.bump_freq(yword);
    dict.finalize_mask();
    assert_ne!(dict.mask_bit(gword), NO_MASK_BIT);

    let body = Extracted {
        anyof: vec![vec![gword, yword]],
        ..Extracted::default()
    };
    let knobs = CompileKnobs {
        accept_class_d: false,
        hot_anchor_threshold: 0,
        dedup_bodies: true,
        keep_visible: false,
    };
    let mut source = Segment::new();
    for id in 0..GROUP_COPIES {
        source.add_compiled(&body, &[], &dict, id, 1, knobs);
    }
    let mut stored = [0u64; 5];
    source.class_counts(&mut stored);
    assert_eq!(
        stored[2], GROUP_COPIES,
        "precondition: every copy is class C"
    );
    assert!(source.has_dup_groups(), "precondition: one shared group");

    // The same features with `gword`'s mask bit gone: the body now plans on the
    // always-visible side (class B, or class H once θ is at or below 70).
    let mut drifted = dict.clone();
    drifted.set_freq_and_mask(gword, 70, NO_MASK_BIT);
    let (merged, stats) = Segment::compact_from_reanchored(&[&source], &drifted, theta, usize::MAX);
    let mut classes = [0u64; 5];
    merged.class_counts(&mut classes);
    (classes, stats.reanchored, stats.hot_promoted)
}

/// A whole opt-in group whose body re-plans visible moves together under ONE
/// leader: partitioning by visibility must not cost sharing when no copy
/// actually differs from the others.
#[test]
fn a_group_that_replans_visible_together_still_shares_one_leader() {
    let (classes, reanchored, _) = reanchor_a_class_c_group(0);
    assert_eq!(classes[1], GROUP_COPIES, "broad→main moves the whole group");
    assert_eq!(
        reanchored, 1,
        "one leader re-derived a cover; every other copy joined it"
    );
}

/// The same re-plan with the hot tier on comes out class H. A class-C query
/// never moves into the hot tier (it would start appearing on default reads),
/// so the whole group keeps its broad cover.
#[test]
fn a_class_c_group_never_moves_into_the_hot_tier() {
    let (classes, reanchored, hot_promoted) = reanchor_a_class_c_group(30);
    assert_eq!((classes[2], classes[4]), (GROUP_COPIES, 0));
    assert_eq!((reanchored, hot_promoted), (0, 0));
}

/// A small deterministic generator (xorshift64*), so the drift corpus needs no
/// external seed plumbing.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const HOT_WORDS: [&str; 6] = ["hota", "hotb", "hotc", "hotd", "hote", "hotf"];
const COLD_WORDS: u64 = 12;

fn drift_word(rng: &mut Rng) -> String {
    let pick = rng.below(HOT_WORDS.len() as u64 + COLD_WORDS);
    match HOT_WORDS.get(pick as usize) {
        Some(hot) => (*hot).to_string(),
        None => format!("cold{}", pick - HOT_WORDS.len() as u64),
    }
}

/// One body of the duplicated corpus: its text, a title that satisfies it, and
/// the ids of its copies.
struct Body {
    text: String,
    title: String,
    copies: Vec<u64>,
}

/// Any-of-only bodies of one or two groups, re-inserted under fresh ids with
/// filler between the copies. `(ops, bodies)`: an op is one live insert.
fn duplicated_corpus(seed: u64) -> (Vec<(u64, String)>, Vec<Body>) {
    let mut rng = Rng(seed);
    let mut bodies = Vec::new();
    for _ in 0..24 {
        let groups = 1 + rng.below(2);
        let mut text = Vec::new();
        let mut title = Vec::new();
        for _ in 0..groups {
            let (a, b) = (drift_word(&mut rng), drift_word(&mut rng));
            text.push(format!("({a},{b})"));
            title.push(if rng.below(2) == 0 { a } else { b });
        }
        bodies.push(Body {
            text: text.join(" "),
            title: title.join(" "),
            copies: Vec::new(),
        });
    }
    let ops = (1..=900u64)
        .map(|id| {
            let text = if rng.below(3) == 0 {
                let body = &mut bodies[rng.below(24) as usize];
                body.copies.push(id);
                body.text.clone()
            } else {
                // Filler: moves one word's frequency and matches no title above.
                format!("{} drift{id}", drift_word(&mut rng))
            };
            (id, text)
        })
        .collect();
    (ops, bodies)
}

/// How many ops run before the mask is finalized. Every body copy among them
/// compiles default-visible; a later copy of a body whose groups all have a
/// top-64 member compiles opt-in.
const EARLY_OPS: usize = 150;

/// Run the first [`EARLY_OPS`] on the empty engine, then finalize the mask.
fn start_duplicated(eng: &mut Engine, ops: &[(u64, String)]) {
    for (id, text) in &ops[..EARLY_OPS] {
        eng.insert_live(text, *id, 1);
    }
    // Scaled so the words used above stay below every mask holder.
    eng.bulk_ingest(&mask_corpus_scaled(&HOT_WORDS, 10));
}

/// How many bodies have, in `eng`, both a default-visible and an opt-in copy.
fn split_bodies(eng: &Engine, bodies: &[Body]) -> usize {
    bodies
        .iter()
        .filter(|body| {
            let (default, _) = reads(eng, &body.title);
            let visible = body.copies.iter().filter(|id| default.contains(id)).count();
            visible != 0 && visible != body.copies.len()
        })
        .count()
}

#[test]
fn duplicated_corpus_is_dedup_invariant_in_both_read_modes() {
    let (ops, bodies) = duplicated_corpus(0xDED0_0105);
    let titles: Vec<String> = bodies.iter().map(|body| body.title.clone()).collect();
    let build = |dedup: bool| {
        let mut eng = new_engine(manual_cfg(dedup));
        start_duplicated(&mut eng, &ops);
        eng
    };
    let (mut on, mut off) = (build(true), build(false));
    assert_on_equals_off(&on, &off, &titles, "early");
    let rest = &ops[EARLY_OPS..];
    for (step, chunk) in rest.chunks(rest.len() / 3).enumerate() {
        for (id, text) in chunk {
            on.insert_live(text, *id, 1);
            off.insert_live(text, *id, 1);
        }
        assert_on_equals_off(&on, &off, &titles, &format!("memtable {step}"));
        on.flush();
        off.flush();
        assert_on_equals_off(&on, &off, &titles, &format!("flushed {step}"));
    }
    assert!(
        split_bodies(&off, &bodies) >= 3,
        "the corpus must hold bodies with copies on both sides of the opt-in \
         boundary, or this test proves nothing"
    );
    on.compact_all().expect("compaction ran");
    off.compact_all().expect("compaction ran");
    assert_on_equals_off(&on, &off, &titles, "compacted");
}

/// The same corpus through the re-anchoring merge. There is no dedup-off
/// reference for it, so this checks the guard directly: no row that a default
/// read returned before the merge may be missing after it, and the
/// `include_broad` result is unchanged.
#[test]
fn reanchoring_a_duplicated_corpus_never_hides_a_visible_row() {
    let (ops, bodies) = duplicated_corpus(0xDED0_0106);
    let titles: Vec<String> = bodies.iter().map(|body| body.title.clone()).collect();
    let mut eng = new_engine(EngineConfig {
        compaction_reanchor: true,
        ..manual_cfg(true)
    });
    start_duplicated(&mut eng, &ops);
    let rest = &ops[EARLY_OPS..];
    for chunk in rest.chunks(rest.len() / 3) {
        for (id, text) in chunk {
            eng.insert_live(text, *id, 1);
        }
        eng.flush();
    }
    assert!(split_bodies(&eng, &bodies) >= 3, "the corpus must be split");
    let before: Vec<_> = titles.iter().map(|title| reads(&eng, title)).collect();
    eng.compact_all().expect("re-anchoring compaction ran");
    for (title, (default_before, broad_before)) in titles.iter().zip(before) {
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
}
