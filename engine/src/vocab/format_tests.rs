//! ADR-184: the vocabulary is a durable document recorded in every manifest, so parsing must
//! refuse what this binary does not understand instead of silently dropping it.

use super::*;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use serde_json::{json, Value};

/// A document exercising every nested entry type, including alias feedback evidence.
fn full_document() -> Value {
    let mut vocab = Vocab::new();
    vocab
        .import_solr_aliases(
            "ny => new york",
            &Normalizer::default_vocab().expect("stock normalizer"),
            &Dict::new(),
        )
        .expect("alias fixture");
    let mut doc = serde_json::to_value(&vocab).expect("serialize");
    doc["synonyms"] = json!([{ "token": "tee", "canonical": "term:shirt", "kind": "generic" }]);
    doc["phrases"] = json!([
        { "tokens": ["usb", "hub"], "canonical": "entity:usb_hub", "kind": "entity", "additive": true }
    ]);
    doc["punctuation"] = json!([{ "ch": "'", "class": "fold" }]);
    doc["aliases"]["entries"][0]["feedback"] =
        json!({ "overlap": 0.9, "titles_a": 3, "titles_b": 4, "queries_sampled": 12 });
    doc
}

fn parse(doc: &Value) -> Result<Vocab, serde_json::Error> {
    Vocab::from_json(&doc.to_string())
}

#[test]
fn the_full_document_parses_and_round_trips() {
    let doc = full_document();
    let vocab = parse(&doc).expect("valid document");
    let again = parse(&serde_json::to_value(&vocab).expect("serialize")).expect("round trip");
    assert_eq!(
        vocab.to_json().expect("json"),
        again.to_json().expect("json"),
        "a written vocabulary must read back identically"
    );
}

#[test]
fn an_unknown_field_in_any_nested_entry_is_rejected() {
    let paths: [&[&str]; 6] = [
        &["synonyms", "0"],
        &["phrases", "0"],
        &["punctuation", "0"],
        &["aliases"],
        &["aliases", "entries", "0"],
        &["aliases", "entries", "0", "feedback"],
    ];
    for path in paths {
        let mut doc = full_document();
        let mut node = &mut doc;
        for key in path {
            node = match key.parse::<usize>() {
                Ok(index) => &mut node[index],
                Err(_) => &mut node[*key],
            };
        }
        node["from_a_newer_binary"] = json!(true);
        let Err(error) = parse(&doc) else {
            panic!("unknown field under {path:?} must be rejected");
        };
        let error = error.to_string();
        assert!(error.contains("unknown field"), "{path:?}: {error}");
    }
}

#[test]
fn only_format_version_one_is_accepted() {
    let mut doc = full_document();
    doc["format_version"] = json!(1);
    let vocab = parse(&doc).expect("format 1 is this binary's format");
    assert!(
        !vocab.to_json().expect("json").contains("format_version"),
        "format 1 is the implicit default and is never written"
    );
    for unsupported in [0, 2, 99] {
        doc["format_version"] = json!(unsupported);
        let Err(error) = parse(&doc) else {
            panic!("format_version {unsupported} must be rejected");
        };
        let error = error.to_string();
        assert!(
            error.contains("unsupported vocabulary format_version"),
            "{error}"
        );
    }
}
