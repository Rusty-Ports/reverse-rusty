//! Alias forms built on what earlier aliases made of their words (ADR-205).
//!
//! A query that spelled a form out asked, before the form existed, for whatever its words
//! compiled to then: a word or its equivalents, or an earlier alias that covers several of
//! the words. A title carries the form under any of those readings, so a later import
//! removes no match an earlier one gave.

use crate::alias_components::{assert_nothing_lost, matches_of, owned};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

#[test]
fn a_chain_of_imports_removes_no_match() {
    // Two ordinary imports and no declared phrase. After the first, `ny catalog` asks for
    // `ny` or its equivalent `new york`, and a title with `york new` carries that. The
    // second import makes `ny catalog` a form, whose word `ny` the title carries only
    // through the same equivalence.
    let mut eng = Engine::with_vocab(Vocab::new(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[
        (1, "ny catalog"),
        (2, "new york catalog"),
        (3, "nycat"),
    ]));
    let titles = [
        "york new catalog",
        "new york catalog",
        "ny catalog",
        "catalog york",
        "nycat",
    ];

    eng.import_alias_synonyms("ny => new york").expect("import");
    let first = matches_of(&mut eng, &titles);
    assert!(first[0].contains(&1) && first[0].contains(&2));
    assert!(!first[3].contains(&1));

    eng.import_alias_synonyms("nycat => ny catalog")
        .expect("import");
    let second = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &first, &second);
    assert!(
        second[0].contains(&3),
        "the title carries the second form, so it matches the alias's other form"
    );
    assert!(
        !second[3].contains(&1) && !second[3].contains(&3),
        "one word of the form is not the form"
    );

    // A third link, built on the second form's other name.
    eng.import_alias_synonyms("bigsale => nycat sale")
        .expect("import");
    let third = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &second, &third);
}

#[test]
fn a_word_counts_through_a_single_word_alias() {
    // `pkg` and `package` are one word to a query. `pkg deal` therefore matched a title
    // that says `package`, and still does once `pkg deal` is a form of its own.
    let mut eng = Engine::with_vocab(Vocab::new(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "pkg deal"), (2, "bargain"), (3, "deal")]));
    let titles = ["deal package", "package of the deal", "pkg deal", "package"];

    eng.import_alias_synonyms("pkg => package").expect("import");
    let before = matches_of(&mut eng, &titles);
    assert!(before[0].contains(&1) && before[1].contains(&1));
    assert!(!before[3].contains(&1));

    eng.import_alias_synonyms("bargain => pkg deal")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    assert!(after[0].contains(&2), "the title carries the form");
    assert!(!after[3].contains(&1) && !after[3].contains(&2));

    // A declared equivalence counts the same way as an imported one.
    let mut vocab = Vocab::new();
    vocab.add_equivalence(&["sofa", "couch"]);
    let mut eng = Engine::with_vocab(vocab, EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "sofa bed"), (2, "daybed")]));
    let titles = ["bed, couch", "sofa bed", "bed"];
    let before = matches_of(&mut eng, &titles);
    assert!(before[0].contains(&1));
    eng.import_alias_synonyms("daybed => sofa bed")
        .expect("import");
    let after = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &before, &after);
    assert!(after[0].contains(&2));
}

#[test]
fn a_form_that_contains_an_earlier_alias_removes_no_match() {
    // `new york catalog` was, to a query, the alias `new york` (or `big apple`) and the
    // word `catalog`. Once the three words are a form of their own, a title still carries
    // it under any of those readings.
    let mut eng = Engine::with_vocab(Vocab::new(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[
        (1, "new york catalog"),
        (2, "big apple catalog"),
        (3, "nyc"),
        (4, "catalog"),
    ]));
    let titles = [
        "big seasonal apple catalog",
        "apple big catalog",
        "new york catalog",
        "catalog york new",
        "big apple catalog",
        "big catalog",
        "nyc",
    ];

    eng.import_alias_synonyms("new york => big apple")
        .expect("import");
    let first = matches_of(&mut eng, &titles);
    assert!(first[0].contains(&1) && first[0].contains(&2));
    assert!(!first[5].contains(&1));

    eng.import_alias_synonyms("nyc => new york catalog")
        .expect("import");
    let second = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &first, &second);
    assert!(second[0].contains(&3) && second[3].contains(&3));
    assert!(!second[5].contains(&1) && !second[5].contains(&3));

    // The other order: the long form first, then the alias inside it.
    let mut eng = Engine::with_vocab(Vocab::new(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[
        (1, "new york catalog"),
        (2, "big apple catalog"),
        (3, "nyc"),
    ]));
    eng.import_alias_synonyms("nyc => new york catalog")
        .expect("import");
    let first = matches_of(&mut eng, &titles);
    eng.import_alias_synonyms("new york => big apple")
        .expect("import");
    let second = matches_of(&mut eng, &titles);
    assert_nothing_lost(&titles, &first, &second);
    assert!(second[0].contains(&1) && second[0].contains(&3));
}

/// A known limit, pinned (ADR-205, "What remains"). A new form that cuts through a phrase
/// an earlier parse used changes what the query asks for outside the form: `new york city`
/// was `new` and the alias `york city`; once `new york` is a form it is the alias `new york`
/// and the word `city`. A title that had `york city` only under its other name has no
/// `city`. Titles that have the words, together or apart, keep matching.
#[test]
fn a_form_that_cuts_through_an_earlier_phrase_can_still_narrow() {
    let mut eng = Engine::with_vocab(Vocab::new(), EngineConfig::default()).expect("engine");
    eng.build_from_queries(&owned(&[(1, "new york city")]));
    let titles = ["new yc", "new york city", "city new york", "york city new"];
    eng.import_alias_synonyms("yc => york city")
        .expect("import");
    let before = matches_of(&mut eng, &titles);
    assert!(before.iter().all(|matched| matched.contains(&1)));

    eng.import_alias_synonyms("ny => new york").expect("import");
    let after = matches_of(&mut eng, &titles);
    assert!(
        !after[0].contains(&1),
        "the limit no longer holds: update ADR-205 and its backlog item"
    );
    assert_nothing_lost(&titles[1..], &before[1..], &after[1..]);
}
