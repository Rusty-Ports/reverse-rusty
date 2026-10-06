//! Feature-model fingerprint (ADR-184): a stable identity for everything in a
//! [`Normalizer`](super::Normalizer) that decides which features a text emits.
//!
//! Every compiled segment is a function of the normalizer it was compiled under, so the
//! manifests record this value and recovery refuses to serve a committed corpus under a
//! normalizer with a different one. The hash is computed once, when the builder freezes the
//! normalizer, over a **canonical** form of its data:
//!
//! - phrases `(pattern, feature, kind, mode)`, sorted — the automaton rejects duplicate
//!   patterns, so registration order carries no meaning;
//! - effective synonyms `(token, canonical, kind)`, sorted — the builder already applied its
//!   first-registration-wins rule;
//! - the full punctuation table (all ASCII classes plus sorted non-ASCII overrides);
//! - the number-context words, sorted and deduplicated (membership is all that matters).
//!
//! Derived state (both automata, `has_multiword_aliases`) is excluded because it is a pure
//! function of the hashed data. The alias word table (ADR-205) is excluded too, although it
//! also reads the vocabulary's equivalence groups, which are not hashed: it only adds to a
//! title's positive view, and no compiled row depends on it. The normalization *code* is not hashed: a change to it is a
//! compiler-semantics bump (ADR-118), which rebuilds every committed row from source.
//! Changing what this function hashes changes every recorded value, so it must ship with
//! such a bump as well.

use super::{PhraseEntry, PhraseMode, PunctClass, PunctTable};
use crate::dict::{kind_tag, FeatureKind};

/// Domain separator, so this hash can never collide by construction with another
/// FNV-1a identity the engine records (e.g. `Dict::fingerprint`).
const DOMAIN: &[u8] = b"rr-feature-model-v1";

fn mode_tag(mode: PhraseMode) -> u8 {
    match mode {
        PhraseMode::Collapse => 0,
        PhraseMode::Additive => 1,
        PhraseMode::Alias => 2,
    }
}

fn punct_tag(class: PunctClass) -> u8 {
    match class {
        PunctClass::Split => 0,
        PunctClass::Fold => 1,
        PunctClass::Keep => 2,
        PunctClass::Marker => 3,
    }
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn put_len(buf: &mut Vec<u8>, n: usize) {
    buf.extend_from_slice(&(n as u64).to_le_bytes());
}

pub(super) fn feature_model_fingerprint(
    patterns: &[String],
    entries: &[PhraseEntry],
    synonyms: &[(String, String, FeatureKind)],
    punct: &PunctTable,
    number_context: &[String],
) -> u64 {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(DOMAIN);

    let mut phrases: Vec<(&str, &str, u8, u8)> = patterns
        .iter()
        .zip(entries)
        .map(|(pattern, entry)| {
            (
                pattern.as_str(),
                entry.feature.as_str(),
                kind_tag(entry.kind),
                mode_tag(entry.mode),
            )
        })
        .collect();
    phrases.sort_unstable();
    put_len(&mut buf, phrases.len());
    for (pattern, feature, kind, mode) in phrases {
        put_str(&mut buf, pattern);
        put_str(&mut buf, feature);
        buf.push(kind);
        buf.push(mode);
    }

    let mut syns: Vec<(&str, &str, u8)> = synonyms
        .iter()
        .map(|(token, canon, kind)| (token.as_str(), canon.as_str(), kind_tag(*kind)))
        .collect();
    syns.sort_unstable();
    put_len(&mut buf, syns.len());
    for (token, canon, kind) in syns {
        put_str(&mut buf, token);
        put_str(&mut buf, canon);
        buf.push(kind);
    }

    buf.extend(punct.ascii.iter().map(|&class| punct_tag(class)));
    let mut non_ascii: Vec<(u32, u8)> = punct
        .non_ascii
        .iter()
        .map(|(&c, &class)| (u32::from(c), punct_tag(class)))
        .collect();
    non_ascii.sort_unstable();
    put_len(&mut buf, non_ascii.len());
    for (c, class) in non_ascii {
        buf.extend_from_slice(&c.to_le_bytes());
        buf.push(class);
    }

    let mut words: Vec<&str> = number_context.iter().map(String::as_str).collect();
    words.sort_unstable();
    words.dedup();
    put_len(&mut buf, words.len());
    for word in words {
        put_str(&mut buf, word);
    }

    crate::util::fnv1a64(&buf)
}

#[cfg(test)]
mod tests {
    use crate::dict::FeatureKind;
    use crate::normalize::{Normalizer, NormalizerBuilder, PunctClass};

    fn fp(b: NormalizerBuilder) -> u64 {
        b.build().expect("normalizer").fingerprint()
    }

    #[test]
    fn identical_vocabularies_share_a_fingerprint() {
        let a = Normalizer::default_vocab().unwrap().fingerprint();
        let b = Normalizer::default_vocab().unwrap().fingerprint();
        assert_eq!(a, b);
    }

    #[test]
    fn registration_order_does_not_change_the_fingerprint() {
        let a = NormalizerBuilder::new()
            .phrase(&["wireless", "mouse"], "entity:wm", FeatureKind::Entity)
            .phrase(&["usb", "hub"], "entity:uh", FeatureKind::Entity)
            .synonym("acme", "brand:acme", FeatureKind::Brand)
            .synonym("tee", "term:shirt", FeatureKind::Generic)
            .number_context_words(&["model", "series"]);
        let b = NormalizerBuilder::new()
            .synonym("tee", "term:shirt", FeatureKind::Generic)
            .phrase(&["usb", "hub"], "entity:uh", FeatureKind::Entity)
            .synonym("acme", "brand:acme", FeatureKind::Brand)
            .phrase(&["wireless", "mouse"], "entity:wm", FeatureKind::Entity)
            .number_context_words(&["series", "model", "model"]);
        assert_eq!(fp(a), fp(b));
    }

    #[test]
    fn every_emission_relevant_input_changes_the_fingerprint() {
        let base = || {
            NormalizerBuilder::new()
                .phrase(&["new", "york"], "term:new_york", FeatureKind::Generic)
                .synonym("tee", "term:shirt", FeatureKind::Generic)
        };
        let reference = fp(base());
        let variants = [
            ("stock", NormalizerBuilder::new()),
            (
                "extra phrase",
                base().phrase(&["usb", "hub"], "entity:uh", FeatureKind::Entity),
            ),
            (
                "extra synonym",
                base().synonym("ny", "term:ny", FeatureKind::Generic),
            ),
            ("punctuation", base().punct('\'', PunctClass::Fold)),
            (
                "non-ascii punctuation",
                base().punct('\u{2019}', PunctClass::Fold),
            ),
            ("number context", base().number_context_words(&["model"])),
            (
                "phrase kind",
                NormalizerBuilder::new()
                    .phrase(&["new", "york"], "term:new_york", FeatureKind::Entity)
                    .synonym("tee", "term:shirt", FeatureKind::Generic),
            ),
        ];
        for (label, variant) in variants {
            assert_ne!(
                fp(variant),
                reference,
                "{label} must change the fingerprint"
            );
        }

        let mut additive = NormalizerBuilder::new();
        additive.add_phrase_additive(&["new", "york"], "term:new_york", FeatureKind::Generic);
        additive.add_synonym("tee", "term:shirt", FeatureKind::Generic);
        assert_ne!(
            fp(additive),
            reference,
            "phrase mode must change the fingerprint"
        );

        let mut alias = base();
        alias.add_alias_form("big apple");
        assert_ne!(
            fp(alias),
            reference,
            "an alias phrase must change the fingerprint"
        );
    }
}
