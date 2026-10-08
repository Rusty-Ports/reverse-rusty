use super::*;
use crate::semantic::{analyze, analyze_literal, resolve_equivalences, RefTitle};
use crate::vocab::{PhraseMode, RefVocab};

fn matches(vocab: &RefVocab, source: &str, title: &str) -> bool {
    analyze(&parse(source).unwrap(), vocab, &resolve_equivalences(vocab))
        .matches(&RefTitle::analyze(vocab, title))
}

#[test]
fn operator_table() {
    let cases = [
        ("laptop", false, Atom::Term("laptop".into())),
        (
            "\"running shoes\"",
            false,
            Atom::Phrase("running shoes".into()),
        ),
        (
            "(red,blue,green)",
            false,
            Atom::AnyOf(vec!["red".into(), "blue".into(), "green".into()]),
        ),
        ("-refurbished", true, Atom::Term("refurbished".into())),
        ("-\"for parts\"", true, Atom::Phrase("for parts".into())),
        (
            "-(used,open box,returned)",
            true,
            Atom::AnyOf(vec!["used".into(), "open box".into(), "returned".into()]),
        ),
    ];
    for (source, negated, atom) in cases {
        assert_eq!(parse(source).unwrap().clauses, [Clause { negated, atom }]);
    }
}

#[test]
fn groups_trim_drop_empty_members_and_replace_unicode_whitespace() {
    let ast = parse(" \t(, red\t\nshoe ,\u{a0}, boot\u{2003}box,, )\n").unwrap();
    assert_eq!(
        ast.clauses[0].atom,
        Atom::AnyOf(vec!["red  shoe".into(), "boot box".into()])
    );
    for source in ["()", "(, ,)"] {
        assert_eq!(parse(source), Err(ParseError::EmptyAnyOfGroup));
    }
    assert_eq!(parse("(red"), Err(ParseError::UnclosedGroup));
    let ast = parse("(a,(b,c))").unwrap();
    assert_eq!(
        ast.clauses,
        [
            Clause {
                negated: false,
                atom: Atom::AnyOf(vec!["a".into(), "(b".into(), "c".into()])
            },
            Clause {
                negated: false,
                atom: Atom::Term(")".into())
            },
        ]
    );
    // Quotes do not shield commas or the first closing parenthesis.
    let ast = parse("(\"a,b\",c)").unwrap();
    assert_eq!(
        ast.clauses[0].atom,
        Atom::AnyOf(vec!["\"a".into(), "b\"".into(), "c".into()])
    );
    let ast = parse("(\"a)b").unwrap();
    assert_eq!(ast.clauses[0].atom, Atom::AnyOf(vec!["\"a".into()]));
    assert_eq!(ast.clauses[1].atom, Atom::Term("b".into()));
}

#[test]
fn quotes_trim_but_preserve_interior_and_have_no_escape_syntax() {
    let ast = parse("\" \u{a0}north\t  star \n\" \"\"").unwrap();
    assert_eq!(ast.clauses[0].atom, Atom::Phrase("north\t  star".into()));
    assert_eq!(ast.clauses[1].atom, Atom::Phrase(String::new()));
    assert_eq!(parse("\"open"), Err(ParseError::UnclosedQuote));
    assert_eq!(
        parse(r#""a\"b"#).unwrap().clauses[0].atom,
        Atom::Phrase("a\\".into())
    );
}

#[test]
fn bare_terms_and_adjacent_delimiters() {
    let ast = parse("wi-fi a,b) a(b,c)\"d\"e\u{2003}f").unwrap();
    let atoms: Vec<_> = ast.clauses.into_iter().map(|clause| clause.atom).collect();
    assert_eq!(
        atoms,
        [
            Atom::Term("wi-fi".into()),
            Atom::Term("a,b)".into()),
            Atom::Term("a".into()),
            Atom::AnyOf(vec!["b".into(), "c".into()]),
            Atom::Phrase("d".into()),
            Atom::Term("e".into()),
            Atom::Term("f".into()),
        ]
    );
    for source in ["", " \t\u{a0}\n"] {
        assert!(parse(source).unwrap().clauses.is_empty());
    }
}

#[test]
fn negation_must_be_immediate_and_is_only_one_prefix() {
    for source in ["- used", "used -", "-", "-\tused", "-\u{a0}used"] {
        assert_eq!(parse(source), Err(ParseError::TrailingDash));
    }
    let ast = parse("--used -- (-used,a-b)").unwrap();
    assert_eq!(
        ast.clauses[0],
        Clause {
            negated: true,
            atom: Atom::Term("-used".into())
        }
    );
    assert_eq!(
        ast.clauses[1],
        Clause {
            negated: true,
            atom: Atom::Term("-".into())
        }
    );
    assert_eq!(
        ast.clauses[2].atom,
        Atom::AnyOf(vec!["-used".into(), "a-b".into()])
    );
}

#[test]
fn inclusive_byte_clause_and_retained_member_limits() {
    assert!(parse(&"x".repeat(MAX_QUERY_LENGTH)).is_ok());
    assert_eq!(
        parse(&"x".repeat(MAX_QUERY_LENGTH + 1)),
        Err(ParseError::QueryTooLong)
    );
    assert!(parse(&"é".repeat(MAX_QUERY_LENGTH / 2)).is_ok());
    assert_eq!(
        parse(&"é".repeat(MAX_QUERY_LENGTH / 2 + 1)),
        Err(ParseError::QueryTooLong)
    );
    for clause in ["x ", "\"\" "] {
        assert!(parse(&clause.repeat(MAX_CLAUSES)).is_ok());
        assert_eq!(
            parse(&clause.repeat(MAX_CLAUSES + 1)),
            Err(ParseError::TooManyClauses)
        );
    }
    let members = vec!["x"; MAX_ANY_OF_SIZE].join(",");
    assert!(parse(&format!("(,,{members},,)")).is_ok());
    assert_eq!(
        parse(&format!("({members},x)")),
        Err(ParseError::AnyOfGroupTooLarge)
    );
    assert!(parse(&format!("({members})")).unwrap().clauses.len() == 1);
}

#[test]
fn error_precedence_reports_length_then_clause_syntax_then_clause_count() {
    assert_eq!(
        parse(&format!("({}", "x".repeat(MAX_QUERY_LENGTH))),
        Err(ParseError::QueryTooLong)
    );
    let full = "x ".repeat(MAX_CLAUSES);
    assert_eq!(parse(&format!("{full}-")), Err(ParseError::TrailingDash));
    assert_eq!(parse(&format!("{full}(")), Err(ParseError::UnclosedGroup));
    let members = vec!["x"; MAX_ANY_OF_SIZE + 1].join(",");
    assert_eq!(
        parse(&format!("{full}({members})")),
        Err(ParseError::AnyOfGroupTooLarge)
    );
}

#[test]
fn complete_any_of_members_follow_the_example_table() {
    let vocab = RefVocab::default();
    for (title, positive, negative) in [
        ("red shoe marker", true, false),
        ("boot marker", true, false),
        ("red hat marker", false, true),
        ("shoe marker", false, true),
    ] {
        assert_eq!(
            matches(&vocab, "(red shoe,boot) marker", title),
            positive,
            "{title}"
        );
        assert_eq!(
            matches(&vocab, "marker -(red shoe,boot)", title),
            negative,
            "{title}"
        );
    }
    assert!(!matches(&vocab, "(red shoe,boot) marker", "boot"));
    assert!(matches(
        &vocab,
        "(red shoe,boot) marker",
        "shoe old red marker"
    ));
}

#[test]
fn quoted_phrase_example_table_and_normalization_examples() {
    let vocab = RefVocab::default();
    for (source, title, expected) in [
        ("\"red shoe\"", "red shoe", true),
        ("\"red shoe\"", "red-shoe", true),
        ("\"red shoe\"", "red leather shoe", false),
        ("\"red shoe\"", "shoe red", false),
        ("item -\"for parts\"", "item for parts", false),
        ("item -\"for parts\"", "item for spare parts", true),
        ("\"north  star\"", "north star", true),
        ("\"north star\"", "north  star", true),
        ("\"CAFÉ shoe\"", "cafe-shoe", true),
        ("\"1999 shoe\"", "#1999 shoe", false),
        ("#1999", "1999", false),
        ("1999", "/1999", false),
        ("#1999", "/1999", true),
    ] {
        assert_eq!(
            matches(&vocab, source, title),
            expected,
            "{source}: {title}"
        );
    }
    assert!(!matches(&vocab.fold_punct('-'), "\"red shoe\"", "red-shoe"));
}

#[test]
fn positive_runs_and_complete_bare_negation_follow_parsing_rules() {
    let vocab = RefVocab::default()
        .phrase("new york", "entity:ny", PhraseMode::Collapse)
        .synonym("ny", "entity:ny");
    assert!(matches(&vocab, "new york", "ny"));
    for boundary in [
        "-used",
        "-\"used item\"",
        "-(used,broken)",
        "\"\"",
        "(!!!)",
        "\"marker\"",
        "(marker,other)",
    ] {
        let source = format!("new {boundary} york");
        assert!(!matches(&vocab, &source, "ny marker"), "{source}");
        assert!(matches(&vocab, &source, "new marker york"), "{source}");
    }
    let vocab = RefVocab::default();
    assert!(!matches(&vocab, "wi-fi", "wi"));
    assert!(matches(&vocab, "wi-fi", "fi wi"));
    assert!(matches(&vocab, "item -wi-fi", "item wi"));
    assert!(!matches(&vocab, "item -wi-fi", "item fi wi"));
    assert!(!matches(&vocab, "item -wi-fi", "item wi-fi"));
}

#[test]
fn empty_analysis_is_dropped_after_syntax_validation() {
    let vocab = RefVocab::default();
    for source in ["", "\"\"", "!!!", "(!!!,???)", "-!!!", "-\" \"", "-(!!!)"] {
        assert!(
            analyze_literal(&parse(source).unwrap(), &vocab).is_empty(),
            "{source}"
        );
    }
    assert!(matches(&vocab, "item (!!!,???)", "item"));
    assert!(matches(&vocab, "(!!!,item)", "item"));
    assert!(!matches(&vocab, "(!!!,item)", "other"));
    assert!(matches(&vocab, "item -(!!!,broken)", "item"));
    assert!(!matches(&vocab, "item -(!!!,broken)", "item broken"));
    assert_eq!(parse("(, ,)"), Err(ParseError::EmptyAnyOfGroup));
}

#[test]
fn combined_operator_examples() {
    let vocab = RefVocab::default();
    assert!(matches(
        &vocab,
        "vintage leather jacket",
        "leather old jacket vintage"
    ));
    assert!(!matches(
        &vocab,
        "vintage leather jacket",
        "vintage leather"
    ));
    assert!(matches(
        &vocab,
        "(brown,tan,cognac) leather jacket",
        "tan leather jacket"
    ));
    assert!(!matches(
        &vocab,
        "vintage leather jacket -wallet -belt",
        "vintage leather jacket belt"
    ));
    let source =
        "vintage (leather,suede) \"bomber jacket\" (brown,tan,black) -womens -(replica,faux,vegan)";
    let title = "vintage suede bomber jacket black";
    assert!(matches(&vocab, source, title));
    for forbidden in ["womens", "replica", "faux", "vegan"] {
        assert!(!matches(&vocab, source, &format!("{title} {forbidden}")));
    }
    assert!(!matches(
        &vocab,
        source,
        "vintage suede jacket bomber black"
    ));
}

#[test]
fn configured_worked_example() {
    let vocab = RefVocab::default()
        .phrase("north star", "brand:north_star", PhraseMode::Collapse)
        .phrase(
            "wireless mouse",
            "entity:wireless_mouse",
            PhraseMode::Collapse,
        )
        .synonym("ns", "brand:north_star")
        .synonym("pkg", "term:package");
    let source = "2024 (north star,ns) wireless mouse (package,pkg) -(used,damaged) -refurbished";
    for title in [
        "2024 north star wireless mouse package",
        "2024 ns wireless mouse pkg",
    ] {
        assert!(matches(&vocab, source, title));
        for forbidden in ["used", "damaged", "refurbished"] {
            assert!(!matches(&vocab, source, &format!("{title} {forbidden}")));
        }
    }
    assert!(!matches(&vocab, source, "2024 ns wireless mouse"));
}
