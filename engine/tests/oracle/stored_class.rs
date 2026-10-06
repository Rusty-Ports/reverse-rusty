//! A write reports the cost class its row was stored under.
//!
//! The class decides whether a default read returns the query, so a writer that is told it
//! can tell an opt-in query from one every read returns. What is reported is the class of
//! the stored row, which for a row that joined an identical body is its group's class and
//! not the class of the row's own plan.

use reverse_rusty::compile::CostClass;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{
    Engine, IngestItemStatus, InsertOutcome, MatchScratch, StoredRow, UpsertOutcome,
};

fn slot(class: CostClass) -> usize {
    match class {
        CostClass::A => 0,
        CostClass::B => 1,
        CostClass::C => 2,
        CostClass::D => 3,
        CostClass::H => 4,
    }
}

/// The class counts that one more row of `class` gives.
fn with_one_more(mut counts: [u64; 5], class: CostClass) -> [u64; 5] {
    counts[slot(class)] += 1;
    counts
}

fn insert(eng: &mut Engine, dsl: &str, id: u64) -> StoredRow {
    match eng.try_insert_live(dsl, id, 1).expect("insert") {
        InsertOutcome::Inserted(stored) => stored,
        InsertOutcome::RejectedClassD => panic!("{dsl:?} was rejected"),
    }
}

fn returned(eng: &Engine, title: &str, id: u64, include_broad: bool) -> bool {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(title, &mut s, &mut out, include_broad);
    out.contains(&id)
}

/// A first load of three terms: every one of them holds a top-64 bit from then on.
fn engine_with_a_mask(config: EngineConfig) -> Engine {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
    eng.build_from_queries(&[
        (1, "zzalpha zzbeta".to_string()),
        (2, "zzgamma".to_string()),
    ]);
    eng
}

#[test]
fn an_insert_reports_the_class_of_the_stored_row() {
    let mut eng = engine_with_a_mask(EngineConfig {
        accept_class_d: true,
        ..EngineConfig::default()
    });
    for (id, dsl, class, title) in [
        (10u64, "zzrare", CostClass::A, "zzrare"),
        (11, "zzalpha zzgamma", CostClass::B, "zzgamma zzalpha"),
        (12, "zzalpha", CostClass::C, "zzalpha"),
        (13, "-zzalpha", CostClass::D, "zzother"),
    ] {
        let before = eng.class_counts();
        let stored = insert(&mut eng, dsl, id);
        assert_eq!(stored.class, class, "{dsl:?}");
        assert_eq!(
            eng.class_counts(),
            with_one_more(before, class),
            "{dsl:?}: the reported class is the one the row was counted under"
        );
        assert_eq!(
            returned(&eng, title, id, false),
            stored.default_visible(),
            "{dsl:?}: a default read returns exactly the rows reported default-visible"
        );
        assert!(returned(&eng, title, id, true), "{dsl:?}: a broad read");
    }
}

#[test]
fn an_upsert_reports_the_class_of_the_new_version() {
    let mut eng = engine_with_a_mask(EngineConfig::default());
    let created = eng.try_upsert_live("zzalpha", 20, 1).expect("upsert");
    let UpsertOutcome::Created(stored) = created else {
        panic!("expected a creation, got {created:?}");
    };
    assert_eq!(stored.class, CostClass::C);
    assert!(!stored.default_visible());

    let updated = eng.try_upsert_live("zzrare", 20, 2).expect("upsert");
    let UpsertOutcome::Updated { stored, replaced } = updated else {
        panic!("expected a replacement, got {updated:?}");
    };
    assert_eq!(replaced, 1);
    assert_eq!(stored.class, CostClass::A);
    assert!(stored.default_visible());
    assert!(returned(&eng, "zzrare", 20, false));
}

#[test]
fn a_bulk_item_reports_the_class_of_the_stored_row() {
    let mut eng = engine_with_a_mask(EngineConfig::default());
    let before = eng.class_counts();
    let batch: Vec<(u64, String)> = vec![
        (30, "zzrare".to_string()),
        (31, "zzalpha zzbeta".to_string()),
        (32, "(((".to_string()),
        (33, "zzbeta".to_string()),
        (34, "-zzbeta".to_string()),
    ];
    let (report, items) = eng.try_bulk_ingest_detailed(&batch).expect("bulk ingest");
    assert_eq!(report.ingested, 3);
    let classes: Vec<Option<CostClass>> = items
        .iter()
        .map(|item| match item {
            IngestItemStatus::Ingested { class } => Some(*class),
            _ => None,
        })
        .collect();
    assert_eq!(
        classes,
        vec![
            Some(CostClass::A),
            Some(CostClass::B),
            None,
            Some(CostClass::C),
            None
        ]
    );
    let mut want = before;
    for class in classes.into_iter().flatten() {
        want = with_one_more(want, class);
    }
    assert_eq!(eng.class_counts(), want);
}

#[test]
fn a_duplicate_reports_the_class_of_the_group_it_joined() {
    // With a hot-anchor threshold of four, a query whose anchor four or more queries use
    // is stored in the hot tier (class H); below that it is class A.
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            hot_anchor_threshold: 4,
            dedup_bodies: true,
            ..EngineConfig::default()
        },
    );
    // The first copy is stored while its anchor is rare.
    assert_eq!(insert(&mut eng, "zzanchor", 1).class, CostClass::A);
    for id in 2..=8u64 {
        insert(&mut eng, &format!("zzanchor zzother{id}"), id);
    }
    // A different body on the same anchor is now planned into the hot tier.
    assert_eq!(insert(&mut eng, "zzanchor -zzneg", 50).class, CostClass::H);

    // An identical copy of the first body would be planned there too. It joins the first
    // copy's group instead and is stored under the group's class, which is what it reports.
    let before = eng.class_counts();
    let copy = insert(&mut eng, "zzanchor", 51);
    assert_eq!(copy.class, CostClass::A);
    assert_eq!(eng.class_counts(), with_one_more(before, CostClass::A));
    assert!(returned(&eng, "zzanchor", 51, false));
}
