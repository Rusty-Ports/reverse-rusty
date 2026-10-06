//! Populated-vocab differentials: generic attributes and multi-word phrases /
//! synonyms — the parts of the front end the default-vocab pass does not exercise. The engine
//! `Normalizer` and the reference `RefVocab` are built from the SAME generator constants
//! (`reverse_rusty::gen::{ENTITIES,BRANDS,BRAND_ALT,ATTRIBUTES}`), so they encode one
//! vocabulary in each side's own type. (Alias-mode two-view + equivalence expansion get their own
//! pass once this populated-vocabulary pass is green.)

use crate::harness::RefOracle;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig, Rng};
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::RefVocab;

/// The engine-side populated normalizer — identical in spirit to the in-tree oracle's `gen_vocab`
/// (multiword entity/brand phrases plus single-token brand, alternate-brand, and attribute
/// synonyms).
fn gen_engine_norm() -> Normalizer {
    use reverse_rusty::gen::{ATTRIBUTES, BRANDS, BRAND_ALT, ENTITIES};
    let mut b = NormalizerBuilder::new();
    for p in ENTITIES {
        let canon = format!("entity:{}", p.replace(' ', "_"));
        let toks: Vec<&str> = p.split(' ').collect();
        b.add_phrase(&toks, &canon, FeatureKind::Entity);
    }
    for brand in BRANDS {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        let toks: Vec<&str> = brand.split(' ').collect();
        if toks.len() > 1 {
            b.add_phrase(&toks, &canon, FeatureKind::Brand);
        } else {
            b.add_synonym(toks[0], &canon, FeatureKind::Brand);
        }
    }
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        b.add_synonym(alt, &canon, FeatureKind::Brand);
    }
    for ct in ATTRIBUTES {
        b.add_synonym(ct, &format!("attribute:{ct}"), FeatureKind::Category);
    }
    b.build().expect("gen vocab automaton")
}

/// The reference-side vocabulary built from the SAME generator constants — one source of truth,
/// expressed in `RefVocab`'s own type. Multiword brand/entity phrases are `Collapse` (the engine's
/// `add_phrase` default).
fn gen_ref_vocab() -> RefVocab {
    use reverse_rusty::gen::{ATTRIBUTES, BRANDS, BRAND_ALT, ENTITIES};
    let mut v = RefVocab::default_vocab();
    for p in ENTITIES {
        let canon = format!("entity:{}", p.replace(' ', "_"));
        v = v.phrase(p, &canon, PhraseMode::Collapse);
    }
    for brand in BRANDS {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        if brand.split(' ').count() > 1 {
            v = v.phrase(brand, &canon, PhraseMode::Collapse);
        } else {
            v = v.synonym(brand, &canon);
        }
    }
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        v = v.synonym(alt, &canon);
    }
    for ct in ATTRIBUTES {
        v = v.synonym(ct, &format!("attribute:{ct}"));
    }
    v
}

fn cfg(seed: u64) -> GenConfig {
    GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_entities: 3_000,
        num_collections: 1_200,
    }
}

#[test]
#[ignore = "probe: discovers the engine's alias canonical features; run with --ignored"]
fn probe_alias_features() {
    use reverse_rusty::normalize::{NormScratch, Side};
    use reverse_rusty::segment::Engine;
    let queries = vec![
        (1u64, "ny catalog".to_string()),
        (2u64, "new york inventory".to_string()),
    ];
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    eng.import_alias_synonyms("ny => new york\nnyc => new york city")
        .expect("aliases");
    let vocab = eng.vocab().expect("vocab").clone();
    eprintln!("equivalences:           {:?}", vocab.equivalences());
    eprintln!(
        "effective_equiv_groups: {:?}",
        vocab.effective_equivalence_groups()
    );
    let norm = vocab.to_normalizer().expect("norm");
    let mut lc = String::new();
    let mut sc = NormScratch::new();
    let emit_names =
        |norm: &Normalizer, lc: &mut String, sc: &mut NormScratch, t: &str, side, fa| {
            let mut v = Vec::new();
            norm.emit(t, lc, sc, side, fa, &mut |n, _k| v.push(n.to_string()));
            v
        };
    for probe in [
        "new york",
        "ny",
        "new york city",
        "nyc",
        "york",
        "new york city inventory",
        "ny catalog",
    ] {
        let q = emit_names(&norm, &mut lc, &mut sc, probe, Side::Query, false);
        let tn = emit_names(&norm, &mut lc, &mut sc, probe, Side::Title, false);
        let tp = emit_names(&norm, &mut lc, &mut sc, probe, Side::Title, true);
        eprintln!("{probe:?}\n  Q:    {q:?}\n  N(T): {tn:?}\n  T+:   {tp:?}");
    }
}

#[test]
fn populated_vocab_attributes_and_phrases() {
    let data = generate(&cfg(0x1234_5678));
    let oracle =
        RefOracle::build_with_normalizer(&data.queries, gen_engine_norm(), gen_ref_vocab());
    oracle.assert_matches(&data.titles, "populated/attributes+phrases");
}

/// The reference vocabulary mirroring `import_alias_synonyms("ny => new york\nnyc => new york
/// city")`: two `Alias`-mode multi-word phrases (entity `term:<tokens joined by _>`) + the
/// effective equivalence form-groups. (Canonical features confirmed by `probe_alias_features`.)
fn ny_ref_vocab() -> RefVocab {
    RefVocab::default_vocab()
        .phrase("new york", "term:new_york", PhraseMode::Alias)
        .phrase("new york city", "term:new_york_city", PhraseMode::Alias)
        .equivalence(&["new york", "ny"])
        .equivalence(&["new york city", "nyc"])
}

/// ADR-061 two-view differential: a query mix exercising bidirectional aliases, overlapping/nested
/// entities, forbidden-over-multi-word (the canonical `N(T)` view), component-token, and any-of —
/// engine vs. the independent reference, zero FN AND zero FP over every title. The engine is built
/// the same way the in-tree alias oracle builds it (`import_alias_synonyms`); the reference uses a
/// from-scratch two-view normalizer + equivalence expansion.
#[test]
fn multiword_alias_two_view_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny catalog".into()),
        (2, "new york inventory".into()),
        (3, "new york -catalog".into()),
        (4, "foo -\"new york\"".into()), // THE WALL: forbidden multi-word vs canonical N(T)
        (5, "york".into()),              // component-token query (title side additive)
        (6, "new york city subway".into()),
        (7, "(ny,chicago) closing".into()), // any-of with an alias form
        (8, "brooklyn".into()),
        (9, "\"new york\" office".into()), // quoted alias path keeps adjacency
        (10, "(new york,brooklyn) office".into()), // a form as a group member keeps its words
        (11, "office -(new york,chicago)".into()), // a negated form rejects the form alone
    ];
    let titles: Vec<String> = [
        "new york catalog opening day",
        "ny inventory annual sale",
        "new york city subway map",
        "foo new york city skyline", // q4 MATCHES: canonical reads `new york city`, not `new york`
        "foo new york state",        // q4 rejected: literal `new york` present
        "chicago closing run",
        "brooklyn bridge",
        "york peppermint pattie",
        "ny catalog near chicago",
        "new york city",
        "new  york catalog", // whitespace run: P(T) overlap scan still matches the alias
        "ny office",
        "new vintage york office",
        // The form's words apart and reordered: q2 and q6 spell a form out and keep them.
        "new vintage york inventory",
        "york inventory new",
        "city subway york new",
        "york office", // one word of the form is not the form
        "new office",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let oracle = RefOracle::build_with_alias_import(
        &queries,
        "ny => new york\nnyc => new york city",
        ny_ref_vocab(),
    );
    oracle.assert_matches(&titles, "alias/two-view");
}

/// A vocabulary in which a word of an alias form is also a synonym for the alias's entity.
/// The word alone then names the entity, as it did before the alias; the form's other word
/// alone does not, because a title carries a form only when it carries every word of it
/// (ADR-205).
#[test]
fn a_form_word_that_names_the_entity_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "wireless mouse".into()),
        (2, "mouse".into()),
        (3, "(wireless mouse,trackball) usb".into()),
        (4, "cordless mouse pad".into()),
        (5, "usb -(wireless mouse,trackball)".into()),
    ];
    let titles: Vec<String> = [
        "wireless mouse",
        "mouse",
        "wireless",
        "cordless mouse",
        "cordless optical mouse pad",
        "wireless optical mouse",
        "usb mouse",
        "usb cordless mouse",
        "usb trackball",
        "usb wireless",
        "mouse pad cordless",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_synonym(
        "wireless",
        "term:wireless_mouse",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    let ref_vocab = RefVocab::default_vocab()
        .synonym("wireless", "term:wireless_mouse")
        .phrase("wireless mouse", "term:wireless_mouse", PhraseMode::Alias)
        .phrase("cordless mouse", "term:cordless_mouse", PhraseMode::Alias)
        .equivalence(&["wireless mouse", "cordless mouse"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        vocab,
        "wireless mouse => cordless mouse",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/a-word-names-the-entity");
}

/// A word of an alias form that titles write through a synonym. The title carries the word
/// under the synonym's canonical, so `refurbished heavy unit` carries the form `refurb unit`
/// although it has neither the token `refurb` nor the two words together (ADR-205).
#[test]
fn a_form_word_written_through_a_synonym_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "refurb unit shelf".into()),
        (2, "ru shelf".into()),
        (3, "refurbished shelf".into()),
        (4, "shelf -(refurb unit,zzother)".into()),
        (5, "\"refurb unit\" shelf".into()),
    ];
    let titles: Vec<String> = [
        "refurbished heavy unit shelf",
        "refurb unit shelf",
        "unit shelf refurb",
        "ru shelf",
        "refurbished shelf",
        "unit shelf",
        "refurbished unit shelf",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_synonym(
        "refurb",
        "term:refurbished",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    let ref_vocab = RefVocab::default_vocab()
        .synonym("refurb", "term:refurbished")
        .phrase("refurb unit", "term:refurb_unit", PhraseMode::Alias)
        .equivalence(&["refurb unit", "ru"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        vocab,
        "ru => refurb unit",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/a-word-through-a-synonym");
}

/// Forms built on one another: `new york` is a declared phrase for the word `ny` and an
/// alias form, and `ny catalog` is an alias form whose first word is that entity. A title
/// with the words of `new york` apart carries `ny`, and through it `ny catalog` (ADR-205).
#[test]
fn a_form_built_on_another_forms_entity_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny catalog".into()),
        (2, "nycat".into()),
        (3, "new york catalog".into()),
        (4, "york".into()),
        (5, "sale -(ny catalog,zzother)".into()),
    ];
    let titles: Vec<String> = [
        "york new catalog",
        "new york catalog",
        "ny catalog",
        "nycat",
        "catalog york",
        "new york city catalog sale",
        "catalog sale ny",
        "ny catalog sale",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_phrase(
        &["new", "york"],
        "term:ny",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    let ref_vocab = RefVocab::default_vocab()
        .phrase("new york", "term:ny", PhraseMode::Alias)
        .phrase("ny catalog", "term:ny_catalog", PhraseMode::Alias)
        .equivalence(&["ny catalog", "nycat"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        vocab,
        "ny => new york\nnycat => ny catalog",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/forms-built-on-one-another");
}

/// Chained ordinary imports, with no declared phrase between them: `ny catalog` is a form
/// whose word `ny` a title can carry only through the equivalence `ny ≡ new york`, and
/// `pkg deal` is a form whose word `pkg` a title can write as `package` (ADR-205).
#[test]
fn a_form_word_carried_through_an_equivalence_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny catalog".into()),
        (2, "nycat".into()),
        (3, "new york catalog".into()),
        (4, "york".into()),
        (5, "sale -(ny catalog,zzother)".into()),
        (6, "pkg deal".into()),
        (7, "bargain".into()),
        (8, "package -bargain".into()),
        (9, "(nycat,bargain) sale".into()),
    ];
    let titles: Vec<String> = [
        "york new catalog",
        "new york catalog",
        "ny catalog",
        "nycat",
        "catalog york",
        "new york city catalog sale",
        "catalog sale ny",
        "ny catalog sale",
        "deal package",
        "package of the deal sale",
        "pkg deal",
        "package",
        "bargain package",
        "york package catalog deal new sale",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let ref_vocab = RefVocab::default_vocab()
        .phrase("new york", "term:new_york", PhraseMode::Alias)
        .phrase("ny catalog", "term:ny_catalog", PhraseMode::Alias)
        .phrase("pkg deal", "term:pkg_deal", PhraseMode::Alias)
        .equivalence(&["ny", "new york"])
        .equivalence(&["nycat", "ny catalog"])
        .equivalence(&["pkg", "package"])
        .equivalence(&["bargain", "pkg deal"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        reverse_rusty::vocab::Vocab::new(),
        "ny => new york\nnycat => ny catalog\npkg => package\nbargain => pkg deal",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/form-words-through-equivalences");
}

/// A form that contains an earlier alias: `new york catalog` over `new york ≡ big apple`, and
/// `north star lamp` over a declared collapse phrase with a declared equivalent. A title
/// carries the long form under any reading of its text (ADR-205).
#[test]
fn a_form_that_contains_a_phrase_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "new york catalog".into()),
        (2, "big apple catalog".into()),
        (3, "nyc".into()),
        (4, "catalog".into()),
        (5, "sale -(nyc,zzother)".into()),
        (6, "north star lamp".into()),
        (7, "nslamp".into()),
        (8, "polaris".into()),
        (9, "(nyc,nslamp) sale".into()),
        (10, "lamp -\"north star\"".into()),
    ];
    let titles: Vec<String> = [
        "big seasonal apple catalog",
        "apple big catalog",
        "new york catalog",
        "catalog york new",
        "big apple catalog sale",
        "big catalog",
        "nyc",
        "nyc sale",
        "lamp polaris",
        "lamp, north star",
        "star lamp north sale",
        "north star lamp",
        "nslamp sale",
        "lamp",
        "polaris big lamp apple catalog sale",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_phrase(
        &["north", "star"],
        "entity:north_star",
        reverse_rusty::dict::FeatureKind::Entity,
    );
    vocab.add_equivalence(&["north star", "polaris"]);
    let ref_vocab = RefVocab::default_vocab()
        .phrase("north star", "entity:north_star", PhraseMode::Collapse)
        .phrase("new york", "term:new_york", PhraseMode::Alias)
        .phrase("big apple", "term:big_apple", PhraseMode::Alias)
        .phrase(
            "new york catalog",
            "term:new_york_catalog",
            PhraseMode::Alias,
        )
        .phrase("north star lamp", "term:north_star_lamp", PhraseMode::Alias)
        .equivalence(&["north star", "polaris"])
        .equivalence(&["new york", "big apple"])
        .equivalence(&["nyc", "new york catalog"])
        .equivalence(&["nslamp", "north star lamp"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        vocab,
        "new york => big apple\nnyc => new york catalog\nnslamp => north star lamp",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/forms-that-contain-a-phrase");
}

/// A number its context types: after `#`, `1995` is a plain number and not a year. A title
/// carries the word `1995` of the form `1995 unit` as its own token whatever the context
/// made of it (ADR-205).
#[test]
fn a_form_word_typed_by_its_context_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "#1995 unit".into()),
        (2, "1995 unit".into()),
        (3, "unit1995".into()),
        (4, "unit -unit1995".into()),
    ];
    let titles: Vec<String> = [
        "unit #1995",
        "1995 unit",
        "unit 1995 edition",
        "#1995 unit",
        "unit1995",
        "unit",
        "1995",
        "#1995",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let ref_vocab = RefVocab::default_vocab()
        .phrase("1995 unit", "term:1995_unit", PhraseMode::Alias)
        .equivalence(&["unit1995", "1995 unit"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        reverse_rusty::vocab::Vocab::new(),
        "unit1995 => 1995 unit",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/a-word-typed-by-its-context");
}

/// A randomized at-scale alias corpus combining the overlapping forms (`ny` / `new york` /
/// `new york city` / `nyc` / component tokens) with fillers, negations (incl. forbidden phrases),
/// and any-of groups (incl. multi-word members) — so the two-view normalization, the overlap scan,
/// the canonical forbidden view, and equivalence widening all run over thousands of titles. Engine
/// vs. the independent reference, zero FN AND zero FP.
fn alias_scale_corpus(
    seed: u64,
    n_queries: usize,
    n_titles: usize,
) -> (Vec<(u64, String)>, Vec<String>) {
    let forms = [
        "ny",
        "new york",
        "new york city",
        "nyc",
        "york",
        "new",
        "city",
    ];
    let fillers = [
        "catalog",
        "inventory",
        "subway",
        "closing",
        "series",
        "bridge",
        "chicago",
        "new",
    ];
    let mut rng = Rng::new(seed);
    let pick = |rng: &mut Rng, xs: &[&str]| xs[rng.below(xs.len())].to_string();

    let mut queries = Vec::new();
    for i in 0..n_queries {
        let mut parts: Vec<String> = Vec::new();
        for _ in 0..=rng.below(2) {
            parts.push(pick(&mut rng, &forms));
        }
        if rng.frac() < 0.6 {
            parts.push(pick(&mut rng, &fillers));
        }
        if rng.frac() < 0.3 {
            let neg = if rng.frac() < 0.5 {
                pick(&mut rng, &forms)
            } else {
                pick(&mut rng, &fillers)
            };
            if neg.contains(' ') {
                parts.push(format!("-\"{neg}\""));
            } else {
                parts.push(format!("-{neg}"));
            }
        }
        if rng.frac() < 0.3 {
            parts.push(format!(
                "({},{})",
                pick(&mut rng, &forms),
                pick(&mut rng, &fillers)
            ));
        }
        queries.push((i as u64 + 1, parts.join(" ")));
    }

    let mut titles = Vec::new();
    for _ in 0..n_titles {
        let n = 2 + rng.below(5);
        let parts: Vec<String> = (0..n)
            .map(|_| {
                if rng.frac() < 0.5 {
                    pick(&mut rng, &forms)
                } else {
                    pick(&mut rng, &fillers)
                }
            })
            .collect();
        titles.push(parts.join(" "));
    }
    (queries, titles)
}

#[test]
fn multiword_alias_scale_differential() {
    let (queries, titles) = alias_scale_corpus(0x0A11_A5E2, 3_000, 1_500);
    let oracle = RefOracle::build_with_alias_import(
        &queries,
        "ny => new york\nnyc => new york city",
        ny_ref_vocab(),
    );
    oracle.assert_matches(&titles, "alias/scale");
}

/// OVERLAPPING equivalence declarations must merge into one transitive class (ADR-054 / codex
/// review): `ny ≡ new york` and `big apple ≡ new york` share `new york`, so the engine's
/// `resolve_equivalences` merges them to `{ny, new york, big apple}` — and the reference must too.
/// The load-bearing case is `ny catalog` reaching a `big apple catalog` title, which is possible ONLY
/// transitively (ny ≡ new york ≡ big apple). A reference that widened only the shared member would
/// reject it — a spurious divergence that the disjoint-group alias tests above cannot catch.
#[test]
fn transitive_equivalence_differential() {
    let queries: Vec<(u64, String)> =
        vec![(1, "ny catalog".into()), (2, "big apple inventory".into())];
    let titles: Vec<String> = [
        "new york catalog",  // q1 via ny ≡ new york
        "big apple catalog", // q1 via ny ≡ big apple — TRANSITIVE through new york
        "ny inventory",      // q2 via big apple ≡ ny — transitive
        "new york inventory",
        "chicago catalog", // no match
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let ref_vocab = RefVocab::default_vocab()
        .phrase("new york", "term:new_york", PhraseMode::Alias)
        .phrase("big apple", "term:big_apple", PhraseMode::Alias)
        .equivalence(&["new york", "ny"])
        .equivalence(&["big apple", "new york"]);

    let oracle = RefOracle::build_with_alias_import(
        &queries,
        "ny => new york\nbig apple => new york",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/transitive-equiv");
}
