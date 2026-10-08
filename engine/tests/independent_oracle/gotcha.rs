//! Hand-written spec gotcha table: `(query, [(title, expect_match)])` cases authored by hand from
//! the spec and asserted against BOTH the engine and the independent reference. A human-authored
//! expectation is the tiebreaker — if the reference disagrees it's a reference bug; if the engine
//! disagrees it's an engine bug; if they agree with each other but not the human, re-read the spec.
//!
//! These pin the exact boundaries most prone to engine-vs-spec drift: negation adjacency, the
//! `#`/`/` markers, caller-defined number context, diacritic folding, decimal tokens,
//! any-of, number-typing boundaries, and class-D drops.
//!
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

fn def_norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}
fn def_vocab() -> RefVocab {
    RefVocab::default_vocab()
}

fn context_norm() -> Normalizer {
    let mut b = NormalizerBuilder::new();
    b.set_number_context_words(&["model"]);
    b.build().expect("context normalizer")
}
fn context_vocab() -> RefVocab {
    RefVocab::default_vocab().number_context(&["model"])
}

/// Build a single-query engine + reference under the given vocab, and assert BOTH agree with the
/// hand-authored expectation for every `(title, expect_match)` case.
pub(super) fn check(
    make_norm: impl Fn() -> Normalizer,
    make_vocab: impl Fn() -> RefVocab,
    query: &str,
    cases: &[(&str, bool)],
) {
    let queries = vec![(1u64, query.to_string())];
    let mut eng = Engine::new(make_norm());
    eng.build_from_queries(&queries);
    let reference = RefMatcher::build(&queries, make_vocab());
    check_pair(&eng, &reference, query, cases);
}

pub(super) fn check_pair(
    eng: &Engine,
    reference: &RefMatcher,
    query: &str,
    cases: &[(&str, bool)],
) {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for &(title, expect) in cases {
        eng.match_title(title, &mut s, &mut out, true);
        let eng_match = out.contains(&1);
        let ref_match = reference.matches(title).contains(&1);
        assert_eq!(
            eng_match, expect,
            "ENGINE disagrees: query {query:?} vs title {title:?} (expected {expect})"
        );
        assert_eq!(
            ref_match, expect,
            "REFERENCE disagrees: query {query:?} vs title {title:?} (expected {expect})"
        );
        if expect {
            let detail = eng
                .explain_hit(1, title)
                .expect("a hand-authored truth must retain explainable source");
            assert!(
                detail.candidate,
                "CANDIDATE RECALL disagrees: query {query:?} vs title {title:?} was a semantic \
                 truth but its signature cover did not retrieve it"
            );
        }
    }
}

fn check_with_ny_alias(query: &str, cases: &[(&str, bool)]) {
    let queries = vec![(1u64, query.to_string())];
    let mut eng = Engine::new(def_norm());
    eng.build_from_queries(&queries);
    eng.import_alias_synonyms("ny => new york")
        .expect("install alias");
    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab()
            .phrase("new york", "term:new_york", PhraseMode::Alias)
            .equivalence(&["new york", "ny"]),
    );
    check_pair(&eng, &reference, query, cases);
}

#[test]
fn negation_adjacency() {
    // `-bar` negates; the title with `bar` is rejected, without it accepted.
    check(
        def_norm,
        def_vocab,
        "foo -bar",
        &[("foo baz", true), ("foo bar", false)],
    );
    // `foo - bar` is a PARSE ERROR -> the query is dropped -> it matches nothing (NOT `foo AND bar`).
    check(
        def_norm,
        def_vocab,
        "foo - bar",
        &[("foo bar", false), ("foo", false), ("foo baz", false)],
    );
    // A trailing dash is the same parse error -> dropped.
    check(
        def_norm,
        def_vocab,
        "product gamma -",
        &[("product gamma", false)],
    );
}

#[test]
fn clause_boundaries_are_semantic_not_a_global_positive_stream() {
    check_with_ny_alias(
        "new -used york",
        &[("new vintage york", true), ("new used york", false)],
    );
    check_with_ny_alias(
        "new -\"used item\" york",
        &[
            ("new vintage product york", true),
            ("new used item york", false),
        ],
    );
    check_with_ny_alias(
        "new -(used,damaged) york",
        &[("new vintage york", true), ("new damaged york", false)],
    );
    check_with_ny_alias(
        "new \"vintage\" york",
        &[
            ("new vintage product york", true),
            ("new product york", false),
        ],
    );
    check_with_ny_alias(
        "new (vintage,modern) york",
        &[
            ("new modern product york", true),
            ("new product york", false),
        ],
    );
}

#[test]
fn marked_number_and_serial() {
    // `#2` -> the `2` is an identifier number (generic `term:2`), not a year. A bare `2002` is a year,
    // so the `#2` query must NOT reach a `2002` title.
    check(
        def_norm,
        def_vocab,
        "widget #2",
        &[
            ("widget #2", true),
            ("widget 2002", false),
            ("widget", false),
        ],
    );
    // `/1999` is a serial (generic `term:1999`), distinct from the bare year `year:1999`.
    check(
        def_norm,
        def_vocab,
        "widget /1999",
        &[("widget /1999", true), ("widget 1999", false)],
    );
    // A marker token never becomes a feature.
    check(def_norm, def_vocab, "widget", &[("widget # 2", true)]);
}

#[test]
fn caller_defined_number_context() {
    // A number immediately after a declared context token remains generic.
    check(
        context_norm,
        context_vocab,
        "model 1995",
        &[("model 1995", true), ("widget 1995", false)],
    );
    check(
        context_norm,
        context_vocab,
        "1995 widget",
        &[("1995 widget", true), ("model 1995 widget", false)],
    );
}

#[test]
fn diacritic_fold() {
    check(
        def_norm,
        def_vocab,
        "cesky",
        &[
            ("Český", true),
            ("ČESKÝ", true),
            ("cesky", true),
            ("product gamma", false),
        ],
    );
    check(def_norm, def_vocab, "jalapeno", &[("Jalapeño new", true)]);
}

#[test]
fn decimal_stays_one_token() {
    // `.` is Keep, so `9.5` is a single token (not split into `9` and `5`).
    check(
        def_norm,
        def_vocab,
        "9.5",
        &[("widget 9.5", true), ("widget 9 5", false)],
    );
}

#[test]
fn any_of_groups() {
    // Single-token members: satisfied iff >=1 present.
    check(
        def_norm,
        def_vocab,
        "(red,blue) car",
        &[("red car", true), ("blue car", true), ("green car", false)],
    );
    // A multi-token member is a conjunction. A partial member must not pass just
    // because it carries the retrieval proxy.
    check(
        def_norm,
        def_vocab,
        "(north star,ns)",
        &[
            ("north star item", true),
            ("ns item", true),
            ("north item", false),
            ("star item", false),
            ("acme item", false),
        ],
    );
    // Distinct members can choose the same retrieval proxy; exact semantics must
    // still retain both complete conjunctions.
    check(
        def_norm,
        def_vocab,
        "(red shoe,red boot)",
        &[
            ("red shoe", true),
            ("red boot", true),
            ("red hat", false),
            ("shoe", false),
        ],
    );
    // Negation rejects a complete member, not each component independently.
    check(
        def_norm,
        def_vocab,
        "marker -(red shoe,boot)",
        &[
            ("marker red hat", true),
            ("marker shoe", true),
            ("marker red shoe", false),
            ("marker boot", false),
        ],
    );
}

#[test]
fn required_and_forbidden_phrases_are_ordered_contiguous_predicates() {
    check(
        def_norm,
        def_vocab,
        "\"red shoe\" marker",
        &[
            ("red shoe marker", true),
            ("red-shoe marker", true),
            ("red leather shoe marker", false),
            ("shoe red marker", false),
        ],
    );
    check(
        def_norm,
        def_vocab,
        "marker -\"for parts\"",
        &[
            ("marker for parts", false),
            ("marker for spare parts", true),
            ("marker parts for", true),
        ],
    );
}

#[test]
fn number_typing_boundaries() {
    // 1900..=2099 is a year; 1899 and 2100 are generic terms.
    check(
        def_norm,
        def_vocab,
        "1900",
        &[("item 1900", true), ("item 1899", false)],
    );
    check(
        def_norm,
        def_vocab,
        "2099",
        &[("item 2099", true), ("item 2100", false)],
    );
    // 1899 is a generic term — matches a 1899 title, not a 1900 one.
    check(
        def_norm,
        def_vocab,
        "1899",
        &[("item 1899", true), ("item 1900", false)],
    );
}

#[test]
fn class_d_queries_are_dropped() {
    // A forbidden-only (class-D) query is dropped at ingest -> it matches nothing.
    check(
        def_norm,
        def_vocab,
        "-manual",
        &[("manual item", false), ("item", false)],
    );
    // An empty / whitespace-only query parses to zero clauses -> dropped.
    check(def_norm, def_vocab, "   ", &[("anything at all", false)]);
}

fn phrase_norm() -> Normalizer {
    use reverse_rusty::dict::FeatureKind;
    let mut b = NormalizerBuilder::new();
    b.add_phrase(&["north", "star"], "brand:north_star", FeatureKind::Brand);
    b.add_phrase(&["star", "lamp"], "entity:star_lamp", FeatureKind::Entity);
    b.add_phrase(&["new", "york", "city"], "entity:nyc", FeatureKind::Entity);
    b.add_phrase(&["new", "york"], "entity:ny", FeatureKind::Entity);
    b.build().expect("phrase normalizer")
}
fn phrase_vocab() -> RefVocab {
    RefVocab::default_vocab()
        .phrase("north star", "brand:north_star", PhraseMode::Collapse)
        .phrase("star lamp", "entity:star_lamp", PhraseMode::Collapse)
        .phrase("new york city", "entity:nyc", PhraseMode::Collapse)
        .phrase("new york", "entity:ny", PhraseMode::Collapse)
}

/// A vocabulary phrase is its words as consecutive tokens. How many separators stand between
/// them does not matter: a comma and a space, a hyphen between spaces, two spaces and a tab
/// all end one token and start the next (ADR-218). The phrase was looked for in the cleaned
/// text with exactly one space between its words, and each separator had become a space of
/// its own, so `north, star` did not carry `north star` and an unquoted query for the phrase
/// missed the title. The quoted query always matched: it is here as the control.
#[test]
fn a_phrase_is_found_across_any_separators() {
    let carries = [
        ("north star", true),
        ("North-Star", true),
        ("north\tstar", true),
        ("north  star", true),
        ("north, star", true),
        ("north - star", true),
        ("the north   star lamp", true),
        ("  north ... star!", false), // `.` is kept: `...` is a token between the words
        ("north polar star", false),
        ("star north", false),
    ];
    for query in ["north star", "(north star,zzother)", "\"north star\""] {
        check(phrase_norm, phrase_vocab, query, &carries);
    }
    // The same on the query side: a member typed with two spaces is the phrase.
    check(
        phrase_norm,
        phrase_vocab,
        "(north  star,zzother)",
        &[
            ("north star", true),
            ("north, star", true),
            ("north", false),
        ],
    );
    // A forbidden term reads the title the same way: the phrase consumes its words wherever
    // it is found, so `-north` rejects neither spelling of a title that carries the phrase.
    check(
        phrase_norm,
        phrase_vocab,
        "star -north",
        &[("north star", false), ("north, star", false)],
    );
    check(
        phrase_norm,
        phrase_vocab,
        "lamp -north",
        &[
            ("north star lamp", true),
            ("north, star lamp", true),
            ("north lamp", false),
        ],
    );
}

/// Phrase selection is leftmost-longest over the occurrences that sit on token boundaries. A
/// pattern found inside a word, or one that ends inside a word, is not an occurrence and
/// hides nothing (ADR-218). The engine took such a match first and dropped it afterwards, by
/// which time it had hidden the valid phrase it overlapped: `new york cityscape` did not
/// carry `new york`.
#[test]
fn a_match_inside_a_word_hides_no_phrase() {
    check(
        phrase_norm,
        phrase_vocab,
        "new york",
        &[
            ("new york", true),
            ("new york cityscape", true),
            ("renew york", false),
            // The longer phrase is an occurrence here and wins: this title is the city.
            ("new york city", false),
        ],
    );
    check(
        phrase_norm,
        phrase_vocab,
        "star lamp",
        &[
            ("star lamp", true),
            ("xnorth star lamp", true),
            // `north star` starts first and takes `star`.
            ("north star lamp", false),
        ],
    );
}

/// A vocabulary may give the space itself a punctuation class. Kept, it is still the
/// separator: a title with two spaces between a phrase's words carries the phrase, and a
/// quoted query for a synonym of the phrase matches it (ADR-218).
#[test]
fn a_kept_space_is_still_a_separator() {
    use reverse_rusty::dict::FeatureKind;
    use reverse_rusty::normalize::PunctClass;
    let norm = || {
        let mut b = NormalizerBuilder::new();
        b.set_punct_class(' ', PunctClass::Keep);
        b.add_phrase(&["north", "star"], "brand:north_star", FeatureKind::Brand);
        b.add_synonym("ns", "brand:north_star", FeatureKind::Brand);
        b.build().expect("normalizer")
    };
    let vocab = || {
        let mut vocab = RefVocab::default_vocab()
            .phrase("north star", "brand:north_star", PhraseMode::Collapse)
            .synonym("ns", "brand:north_star");
        vocab
            .punct
            .set(' ', reverse_rusty_ref_matcher::clean::PunctClass::Keep);
        vocab
    };
    for query in ["\"ns\"", "ns", "north star", "\"north star\""] {
        check(
            norm,
            vocab,
            query,
            &[
                ("north star", true),
                ("north  star", true),
                ("ns", true),
                ("north", false),
            ],
        );
    }
}

/// Two statements of the normative front-end rules (`docs/design/normalization.md` §2.1)
/// that review showed the text had got wrong, pinned here so the text and both
/// implementations stay together.
#[test]
fn the_positive_phrase_graph_and_a_folded_space() {
    use reverse_rusty::dict::FeatureKind;
    use reverse_rusty::normalize::PunctClass;
    // A title's positive graph keeps the arc a token has by itself inside a phrase that
    // consumes it, with or without an alias in the vocabulary: `sale 1999` is the entity,
    // and a quoted `"1999"` still finds the year there.
    let offer_norm = || {
        let mut b = NormalizerBuilder::new();
        b.add_phrase(&["sale", "1999"], "entity:offer", FeatureKind::Entity);
        b.build().expect("normalizer")
    };
    let offer_vocab =
        || RefVocab::default_vocab().phrase("sale 1999", "entity:offer", PhraseMode::Collapse);
    check(
        offer_norm,
        offer_vocab,
        "\"1999\"",
        &[("sale 1999", true), ("1999", true), ("sale 2001", false)],
    );
    // The bare query reads the title's flat view, where the phrase has consumed the year.
    check(
        offer_norm,
        offer_vocab,
        "1999",
        &[("sale 1999", false), ("1999 sale", true)],
    );

    // A space that a vocabulary folds is deleted, like any folded character.
    let joined_norm = || {
        let mut b = NormalizerBuilder::new();
        b.set_punct_class(' ', PunctClass::Fold);
        b.build().expect("normalizer")
    };
    let joined_vocab = || {
        let mut vocab = RefVocab::default_vocab();
        vocab
            .punct
            .set(' ', reverse_rusty_ref_matcher::clean::PunctClass::Fold);
        vocab
    };
    check(
        joined_norm,
        joined_vocab,
        "northstar",
        &[("north star", true), ("northstar", true), ("north", false)],
    );
}
