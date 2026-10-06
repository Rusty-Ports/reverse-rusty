//! Differentials for the forms a title carries by a reading of their text (ADR-205): the
//! engine against the independent reference matcher, with no false negative and no false
//! positive. Each vocabulary is one the rule had to be taught, and is declared on both sides
//! in that side's own type.

use crate::harness::RefOracle;
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::RefVocab;

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

/// Two phrases inside a form that overlap each other. A reading of the form uses one or the
/// other, with the words that are left; the two do not join (ADR-205).
#[test]
fn overlapping_phrases_inside_a_form_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "za zb zc zd".into()),
        (2, "zlong".into()),
        (3, "za zb".into()),
        (4, "zb zc zd".into()),
        (5, "zp zq".into()),
        (6, "zc -zlong".into()),
    ];
    let titles: Vec<String> = [
        "zp zc zd",
        "zd zc zp",
        "za zq",
        "zd zc zb za",
        "zp zq",
        "zq zp zc",
        "zp zd",
        "zlong",
        "za zb zc zd",
        "zq",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    let ref_vocab = RefVocab::default_vocab()
        .phrase("za zb", "term:za_zb", PhraseMode::Alias)
        .phrase("zb zc zd", "term:zb_zc_zd", PhraseMode::Alias)
        .phrase("za zb zc zd", "term:za_zb_zc_zd", PhraseMode::Alias)
        .equivalence(&["zp", "za zb"])
        .equivalence(&["zq", "zb zc zd"])
        .equivalence(&["zlong", "za zb zc zd"]);
    let oracle = RefOracle::build_with_vocab_and_alias_import(
        &queries,
        reverse_rusty::vocab::Vocab::new(),
        "zp => za zb\nzq => zb zc zd\nzlong => za zb zc zd",
        ref_vocab,
    );
    oracle.assert_matches(&titles, "alias/overlapping-phrases-inside-a-form");
}
