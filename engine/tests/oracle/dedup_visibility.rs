//! Dedup groups are partitioned by visibility (ADR-106, amended).
//!
//! A group member rides its leader's postings and takes its class byte, so a
//! join must never change which reads see the member. Identical bodies CAN
//! plan on opposite sides of the opt-in boundary when the body has no required
//! feature: the any-of group that anchors it is chosen by live frequency, and
//! a body compiled before the first mask finalize sees no top-64 feature at
//! all. Every leg here builds such a pair and checks that each copy keeps the
//! visibility it would have with dedup off:
//!
//! 1. the memtable join, both directions (drift, and compile-before-finalize);
//! 2. the grouped merge and the re-anchoring merge;
//! 3. flush and a durable reopen (the adopted class byte is what gets stored);
//! 4. a drifting, duplicated any-of corpus, dedup on ≡ off in both
//!    `include_broad` modes after every lifecycle step;
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
fn mask_corpus(hot: &[&str]) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut id = 1_000_000u64;
    for k in 0..70u64 {
        for r in 0..4u64 {
            out.push((id, format!("fz{k} pad{k}x{r}")));
            id += 1;
        }
    }
    for word in hot {
        for n in 0..10u64 {
            out.push((id, format!("{word} {word}pad{n}")));
            id += 1;
        }
    }
    out
}

fn manual_cfg(dedup: bool) -> EngineConfig {
    EngineConfig {
        dedup_bodies: dedup,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    }
}

fn new_engine(cfg: EngineConfig) -> Engine {
    Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg)
}

/// `(default read, include_broad read)` for one title.
fn reads(eng: &Engine, title: &str) -> (HashSet<u64>, HashSet<u64>) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(title, &mut scratch, &mut out, false);
    let default: HashSet<u64> = out.iter().copied().collect();
    eng.match_title(title, &mut scratch, &mut out, true);
    (default, out.iter().copied().collect())
}

fn ids(list: &[u64]) -> HashSet<u64> {
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

const DRIFT_BODY: &str = "(hword,xword) (gword,yword)";
const DRIFT_TITLE: &str = "xword gword";

/// Frequency drift under a frozen mask. `hword` is top-64. The first copy is
/// inserted while `(hword,xword)` is the more selective group, so it anchors
/// there and is opt-in (class C). `xword` then overtakes `gword`, so the second
/// copy anchors on `(gword,yword)`, which has no top-64 member: class B.
fn drift_engine(cfg: EngineConfig) -> Engine {
    let mut eng = new_engine(cfg);
    eng.build_from_queries(&mask_corpus(&["hword"]));
    for i in 0..20u64 {
        eng.insert_live(&format!("gword gfill{i}"), 2_000 + i, 1);
    }
    eng.insert_live(DRIFT_BODY, OPT_IN_COPY, 1);
    for i in 0..40u64 {
        eng.insert_live(&format!("xword xfill{i}"), 3_000 + i, 1);
    }
    eng.insert_live(DRIFT_BODY, VISIBLE_COPY, 1);
    eng
}

#[test]
fn drifted_duplicate_never_hides_a_default_visible_member() {
    // Dedup off is the reference: it shows the two copies really did plan on
    // opposite sides of the opt-in boundary.
    let mut off = drift_engine(manual_cfg(false));
    assert_split(&off, DRIFT_TITLE, "dedup off");

    let mut on = drift_engine(manual_cfg(true));
    assert_split(&on, DRIFT_TITLE, "memtable");
    let titles = vec![DRIFT_TITLE.to_string(), "hword yword".to_string()];
    assert_on_equals_off(&on, &off, &titles, "memtable");

    on.flush();
    off.flush();
    assert_split(&on, DRIFT_TITLE, "after flush");
    assert_on_equals_off(&on, &off, &titles, "after flush");
}

#[test]
fn drifted_duplicate_stays_visible_across_a_durable_reopen() {
    let dir = tempdir("visibility-reopen");
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        ..manual_cfg(true)
    };
    {
        let mut eng = drift_engine(cfg.clone());
        // Flush writes the member's class byte and expands it into its
        // leader's lane: a wrongly adopted class would become permanent here.
        eng.flush();
        assert_split(&eng, DRIFT_TITLE, "durable flush");
    }
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen");
    assert_split(&eng, DRIFT_TITLE, "durable reopen");
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
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

const GROUP_COPIES: u64 = 50;

/// `GROUP_COPIES` class-C copies of the drift body in one group, then enough
/// drift that the body re-plans onto `(gword,yword)`, which has no top-64
/// member. `gword` ends at frequency 70, so the re-plan is class B with the hot
/// tier off and class H with `theta` at or below 70. Returns the engine after
/// a re-anchoring merge.
fn replanned_group(theta: u32) -> Engine {
    let mut eng = new_engine(EngineConfig {
        compaction_reanchor: true,
        hot_anchor_threshold: theta,
        ..manual_cfg(true)
    });
    eng.build_from_queries(&mask_corpus(&["hword"]));
    for i in 0..20u64 {
        eng.insert_live(&format!("gword gfill{i}"), 2_000 + i, 1);
    }
    // Every copy anchors on `(hword,xword)`: class C.
    let copies: Vec<(u64, String)> = (0..GROUP_COPIES)
        .map(|i| (10 + i, DRIFT_BODY.to_string()))
        .collect();
    eng.bulk_ingest(&copies);
    let (default, broad) = reads(&eng, DRIFT_TITLE);
    assert!(default.is_empty(), "precondition: every copy is opt-in");
    assert_eq!(broad.len(), GROUP_COPIES as usize);

    // `xword` overtakes `gword`: the body now anchors on `(gword,yword)`.
    for i in 0..200u64 {
        eng.insert_live(&format!("xword xfill{i}"), 3_000 + i, 1);
    }
    eng.flush();
    eng.compact_all().expect("re-anchoring compaction ran");
    eng
}

/// `(default hits, include_broad hits, posting entries the default read scanned)`.
fn group_reads(eng: &Engine) -> (usize, usize, u32) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let stats = eng.match_title(DRIFT_TITLE, &mut scratch, &mut out, false);
    let default = out.len();
    eng.match_title(DRIFT_TITLE, &mut scratch, &mut out, true);
    (default, out.len(), stats.postings_scanned)
}

/// A whole opt-in group whose body re-plans visible moves together and keeps
/// ONE leader: partitioning by visibility must not cost sharing when no copy
/// actually differs from the others.
#[test]
fn a_group_that_replans_visible_together_still_shares_one_leader() {
    let (default, broad, scanned) = group_reads(&replanned_group(0));
    assert_eq!(
        (default, broad),
        (GROUP_COPIES as usize, GROUP_COPIES as usize),
        "broad→main re-anchoring makes the whole group default-visible"
    );
    assert!(
        scanned <= 4,
        "the group must still be one posting entry, scanned {scanned}"
    );
}

/// The same drift with the hot tier on re-plans the body to class H. A class-C
/// query never moves into the hot tier (it would start appearing on default
/// reads), so the whole group keeps its broad cover. This is reachable only for
/// a body with no required feature, whose re-plan can change any-of group.
#[test]
fn a_class_c_group_never_moves_into_the_hot_tier() {
    let eng = replanned_group(30);
    assert_eq!(eng.class_counts()[4], 0, "nothing became class H");
    let (default, broad, _) = group_reads(&eng);
    assert_eq!((default, broad), (0, GROUP_COPIES as usize));
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

/// Any-of-only bodies of one or two groups, re-inserted under fresh ids with
/// frequency-moving filler between the copies, so copies of one body plan
/// under different frequencies. `(ops, titles)`: an op is one live insert.
fn drift_corpus(seed: u64) -> (Vec<(u64, String)>, Vec<String>) {
    let mut rng = Rng(seed);
    let mut bodies = Vec::new();
    let mut titles = Vec::new();
    for _ in 0..24 {
        let groups = 1 + rng.below(2);
        let mut body = Vec::new();
        let mut title = Vec::new();
        for _ in 0..groups {
            let (a, b) = (drift_word(&mut rng), drift_word(&mut rng));
            body.push(format!("({a},{b})"));
            title.push(if rng.below(2) == 0 { a } else { b });
        }
        bodies.push(body.join(" "));
        titles.push(title.join(" "));
    }
    let ops = (1..=900u64)
        .map(|id| {
            let text = if rng.below(3) == 0 {
                bodies[rng.below(bodies.len() as u64) as usize].clone()
            } else {
                // Filler: moves one word's frequency and matches no title above.
                format!("{} drift{id}", drift_word(&mut rng))
            };
            (id, text)
        })
        .collect();
    (ops, titles)
}

/// How many bodies have, in `eng`, both a default-visible and an opt-in copy.
fn split_bodies(eng: &Engine, titles: &[String]) -> usize {
    titles
        .iter()
        .filter(|title| {
            let (default, broad) = reads(eng, title);
            !default.is_empty() && default.len() < broad.len()
        })
        .count()
}

#[test]
fn drifting_duplicate_corpus_is_dedup_invariant_in_both_read_modes() {
    let (ops, titles) = drift_corpus(0xDED0_0105);
    let build = |dedup: bool| {
        let mut eng = new_engine(manual_cfg(dedup));
        eng.build_from_queries(&mask_corpus(&HOT_WORDS));
        eng
    };
    let (mut on, mut off) = (build(true), build(false));
    let third = ops.len() / 3;
    for (step, chunk) in ops.chunks(third).enumerate() {
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
        split_bodies(&off, &titles) >= 3,
        "the corpus must hold bodies whose copies planned on both sides of the \
         opt-in boundary, or this test proves nothing"
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
fn reanchoring_a_drifting_duplicate_corpus_never_hides_a_visible_row() {
    let (ops, titles) = drift_corpus(0xDED0_0106);
    let mut eng = new_engine(EngineConfig {
        compaction_reanchor: true,
        ..manual_cfg(true)
    });
    eng.build_from_queries(&mask_corpus(&HOT_WORDS));
    for chunk in ops.chunks(ops.len() / 3) {
        for (id, text) in chunk {
            eng.insert_live(text, *id, 1);
        }
        eng.flush();
    }
    assert!(split_bodies(&eng, &titles) >= 3, "the corpus must be split");
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
