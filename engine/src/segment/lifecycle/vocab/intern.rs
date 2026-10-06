//! One id per feature name across a rebuild that compiles read-only (ADR-204).

use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId};
use crate::error::ParseError;
use crate::normalize::Normalizer;
use crate::segment::LiveSourceDocument;

/// Intern into `dict` every feature name `norm` produces for the stored queries, and leave
/// everything `dict` already held as it was.
///
/// A rebuild compiles each stored query read-only, and a name that is not in the dictionary
/// then resolves to a synthetic id. Titles resolve it the same way, until an ordinary insert
/// that mentions the name interns it: from then on titles carry the dense id, and the
/// rebuilt queries, which hold the synthetic one, can no longer be matched through it. So
/// before the rebuild every such name is given the dense id an insert would give it, by
/// running the interning extractor over the same sources.
///
/// The extractor also counts each query's features. Existing features get their frequency
/// and mask bit back, so no stored query's anchor choice or visibility moves (the top-64
/// mask is assigned once, ADR-188). A new name keeps the count of the stored queries that
/// now carry it and has no mask bit.
///
/// `visit` sees each query's logical id and its compiled form, or the reason it no longer
/// parses, and may stop the pass with an error. The dictionary's existing features are
/// restored either way.
pub(in crate::segment::lifecycle) fn intern_live_names<E>(
    dict: &mut Dict,
    norm: &Normalizer,
    live: &[LiveSourceDocument],
    mut visit: impl FnMut(u64, Result<&Extracted, &ParseError>) -> Result<(), E>,
) -> Result<(), E> {
    let existing: Vec<(u32, u8)> = (0..dict.len())
        .map(|id| {
            let id = id as FeatureId;
            (dict.freq(id), dict.mask_bit(id))
        })
        .collect();
    let mut lc = String::new();
    let mut outcome = Ok(());
    for (logical, text, ..) in live {
        let compiled = crate::dsl::parse_for_recovery(text)
            .map(|ast| crate::compile::extract(&ast, norm, dict, &mut lc));
        if let Err(error) = visit(*logical, compiled.as_ref()) {
            outcome = Err(error);
            break;
        }
    }
    for (id, (freq, mask_bit)) in existing.into_iter().enumerate() {
        dict.set_freq_and_mask(id as FeatureId, freq, mask_bit);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::FeatureKind;

    fn document(logical: u64, text: &str) -> LiveSourceDocument {
        (
            logical,
            text.to_string(),
            1,
            0,
            Vec::new(),
            Vec::new(),
            crate::rank::RankValues::default(),
            crate::ownership::QueryPlacement::standalone(),
        )
    }

    fn dictionary() -> Dict {
        let mut dict = Dict::new();
        let alpha = dict.intern("term:alpha", FeatureKind::Generic);
        for _ in 0..5 {
            dict.bump_freq(alpha);
        }
        dict.finalize_mask();
        dict
    }

    #[test]
    fn new_names_are_interned_and_existing_features_are_untouched() {
        let norm = Normalizer::default_vocab().expect("normalizer");
        let mut dict = dictionary();
        let alpha = dict.get("term:alpha").expect("alpha");
        let (freq, mask) = (dict.freq(alpha), dict.mask_bit(alpha));
        let live = [document(1, "alpha beta"), document(2, "beta gamma")];

        let outcome: Result<(), ()> = intern_live_names(&mut dict, &norm, &live, |_, compiled| {
            compiled.map(|_| ()).map_err(|_| ())
        });

        assert_eq!(outcome, Ok(()));
        assert_eq!(dict.get("term:alpha"), Some(alpha));
        assert_eq!((dict.freq(alpha), dict.mask_bit(alpha)), (freq, mask));
        let beta = dict.get("term:beta").expect("beta is interned");
        assert_eq!(
            dict.freq(beta),
            2,
            "a new name counts the queries that carry it"
        );
        assert!(
            !crate::compile::is_hot(&dict, beta),
            "and holds no mask bit"
        );
        assert!(dict.get("term:gamma").is_some());
    }

    #[test]
    fn the_visitor_sees_a_query_that_no_longer_parses_and_can_stop_the_pass() {
        let norm = Normalizer::default_vocab().expect("normalizer");
        let mut dict = dictionary();
        let alpha = dict.get("term:alpha").expect("alpha");
        let freq = dict.freq(alpha);
        let live = [
            document(1, "alpha beta"),
            document(2, "(unclosed"),
            document(3, "gamma"),
        ];

        let outcome = intern_live_names(&mut dict, &norm, &live, |logical, compiled| {
            compiled.map(|_| ()).map_err(|_| logical)
        });

        assert_eq!(outcome, Err(2), "the failing query is reported");
        assert!(dict.get("term:gamma").is_none(), "the pass stopped there");
        assert_eq!(
            dict.freq(alpha),
            freq,
            "existing features are restored even then"
        );
    }
}
