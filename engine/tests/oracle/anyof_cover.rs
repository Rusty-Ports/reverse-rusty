//! Default visibility of a query whose only flat anchor would be top-64 (ADR-187).
//!
//! 1. An any-of group with no top-64 member anchors such a query, so adding a
//!    required term never makes it less visible than the group alone.
//! 2. Activating an alias never hides a query default reads returned: the rare
//!    term's alias group becomes the anchor, and where only top-64 anchors are
//!    left the rebuilt row keeps an always-probed cover.
//! 3. No rebuild hides a query merely because its anchor became top-64 after it
//!    was compiled.

use crate::dedup::tempdir;
use crate::dedup_visibility::{ids, manual_cfg, mask_corpus, new_engine, reads};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

const MIXED: u64 = 1;
const GROUP_ONLY: u64 = 2;
const HOT_ONLY: u64 = 3;

/// `hword` is top-64; `acme` and `zenith` never are.
fn mixed_queries() -> Vec<(u64, String)> {
    vec![
        (MIXED, "hword (acme, zenith)".to_string()),
        (GROUP_ONLY, "(acme, zenith)".to_string()),
        (HOT_ONLY, "hword".to_string()),
    ]
}

fn assert_mixed_reads(eng: &Engine, ctx: &str) {
    // The mixed query is exactly as visible as the broader group-only query.
    for title in ["hword acme", "zenith filler hword"] {
        assert_eq!(
            reads(eng, title),
            (
                ids(&[MIXED, GROUP_ONLY]),
                ids(&[MIXED, GROUP_ONLY, HOT_ONLY])
            ),
            "{ctx}: {title:?}"
        );
    }
    assert_eq!(
        reads(eng, "acme only"),
        (ids(&[GROUP_ONLY]), ids(&[GROUP_ONLY])),
        "{ctx}: the required term is still required"
    );
    assert_eq!(
        reads(eng, "hword only"),
        (ids(&[]), ids(&[HOT_ONLY])),
        "{ctx}: a query with only a top-64 anchor stays opt-in"
    );
}

#[test]
fn a_top64_required_term_with_a_selective_group_is_default_visible() {
    // Live inserts.
    let mut live = new_engine(manual_cfg(true));
    live.build_from_queries(&mask_corpus(&["hword"]));
    for (id, text) in mixed_queries() {
        live.insert_live(&text, id, 1);
    }
    assert_mixed_reads(&live, "memtable");
    live.flush();
    live.compact_all().expect("compaction ran");
    assert_mixed_reads(&live, "flushed and compacted");

    // The bulk path, with re-anchoring compaction.
    let mut bulk = new_engine(EngineConfig {
        compaction_reanchor: true,
        ..manual_cfg(true)
    });
    bulk.build_from_queries(&mask_corpus(&["hword"]));
    bulk.bulk_ingest(&mixed_queries());
    assert_mixed_reads(&bulk, "bulk");
    bulk.compact_all().expect("compaction ran");
    assert_mixed_reads(&bulk, "re-anchored");
}

fn alias(forms: &[&str]) -> Vocab {
    let mut vocab = Vocab::new();
    vocab.add_equivalence(forms);
    vocab
}

/// `acme hword` is anchored on the rare `acme`. Declaring `acme ≡ acm` moves
/// `acme` out of the required set into an any-of group, which leaves `hword`,
/// a top-64 feature, as the only required term. The group must take over as the
/// anchor: the alias can only add matches.
#[test]
fn an_alias_on_the_rare_term_never_hides_the_query() {
    let mut eng = new_engine(manual_cfg(true));
    eng.build_from_queries(&mask_corpus(&["hword"]));
    eng.insert_live("acme hword", 1, 1);
    assert_eq!(reads(&eng, "acme hword"), (ids(&[1]), ids(&[1])));
    assert_eq!(reads(&eng, "acm hword"), (ids(&[]), ids(&[])));

    eng.set_vocab(alias(&["acme", "acm"])).expect("set_vocab");
    eng.recompile_stale_segments();
    for title in ["acme hword", "acm hword"] {
        assert_eq!(reads(&eng, title), (ids(&[1]), ids(&[1])), "{title:?}");
    }
}

/// `hword hphone` is a pair of top-64 features: default-visible. Declaring
/// `hphone ≡ mobile` leaves `hword` required and a group that contains a top-64
/// member, so a fresh compile of that body is opt-in. The STORED query was
/// visible and must stay visible through the rebuild, a reopen and a merge.
#[test]
fn an_alias_that_leaves_only_top64_anchors_keeps_the_stored_query_visible() {
    let dir = tempdir("anyof-cover-alias");
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        compaction_reanchor: true,
        ..manual_cfg(true)
    };
    {
        let mut eng = new_engine(cfg.clone());
        eng.build_from_queries(&mask_corpus(&["hword", "hphone"]));
        eng.insert_live("hword hphone", 1, 1);
        assert_eq!(reads(&eng, "hword hphone"), (ids(&[1]), ids(&[1])));

        eng.set_vocab(alias(&["hphone", "mobile"]))
            .expect("set_vocab");
        eng.recompile_stale_segments();
        for title in ["hword hphone", "hword mobile"] {
            assert_eq!(reads(&eng, title), (ids(&[1]), ids(&[1])), "{title:?}");
        }

        // A NEW query with the same text takes the lane its own plan gives it,
        // and does not drag the stored one along (or get exposed by it).
        eng.insert_live("hword hphone", 2, 1);
        assert_eq!(reads(&eng, "hword mobile"), (ids(&[1]), ids(&[1, 2])));
        eng.flush();
        eng.compact_all().expect("re-anchoring compaction ran");
        assert_eq!(reads(&eng, "hword mobile"), (ids(&[1]), ids(&[1, 2])));
    }
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen");
    assert_eq!(reads(&eng, "hword mobile"), (ids(&[1]), ids(&[1, 2])));
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A query compiled before the first mask finalize whose only term later turns
/// top-64 sits in the main lane and is default-visible. Re-planned today it
/// would be opt-in, so a rebuild for an UNRELATED vocabulary change must keep
/// its always-probed cover.
#[test]
fn a_rebuild_never_hides_a_query_whose_anchor_became_top64() {
    let mut eng = new_engine(manual_cfg(true));
    eng.insert_live("rareword", 1, 1);
    eng.bulk_ingest(&mask_corpus(&["rareword"]));
    eng.insert_live("rareword", 2, 1); // compiled now: opt-in
    let expected = (ids(&[1]), ids(&[1, 2]));
    assert_eq!(reads(&eng, "rareword title"), expected, "before");

    eng.set_vocab(alias(&["unrelated", "unconnected"]))
        .expect("set_vocab");
    assert!(eng.recompile_stale_segments() > 0, "the corpus was rebuilt");
    assert_eq!(reads(&eng, "rareword title"), expected, "after the rebuild");
}

/// An id can have two live copies: a bulk batch re-ingests an id the memtable
/// still holds. Default reads return the id as long as ONE copy is visible, so
/// the rebuild, which keeps only the newest copy, must keep that id visible
/// even when the newest copy is the opt-in one.
#[test]
fn a_rebuild_keeps_an_id_visible_when_any_live_copy_was() {
    let mut eng = new_engine(manual_cfg(true));
    eng.insert_live("rareword", 1, 1); // visible copy, in the memtable
    let mut batch = mask_corpus(&["rareword"]);
    batch.push((1, "rareword".to_string())); // a second, opt-in copy of id 1
    eng.bulk_ingest(&batch);
    assert_eq!(reads(&eng, "rareword title"), (ids(&[1]), ids(&[1])));

    eng.set_vocab(alias(&["unrelated", "unconnected"]))
        .expect("set_vocab");
    assert!(eng.recompile_stale_segments() > 0, "the corpus was rebuilt");
    assert_eq!(reads(&eng, "rareword title"), (ids(&[1]), ids(&[1])));
}
