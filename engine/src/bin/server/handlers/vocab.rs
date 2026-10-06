//! Vocabulary management (`_vocab`, `_vocab/learn[/_and_apply]`).

mod learn;
mod learn_apply;
mod read;
mod write;
pub(crate) use learn::{
    execute_vocab_learn, learn_vocab, vocab_learn_method_not_allowed, VocabLearnTransport,
    VOCAB_LEARN_BODY_LIMIT,
};
pub(crate) use learn_apply::{
    acquire_vocab_learn_apply_permit, finish_vocab_learn_apply_response, learn_and_apply_vocab,
    vocab_learn_apply_error_response, vocab_learn_apply_method_not_allowed,
    vocab_learn_apply_success, VocabLearnApplyTransport, VOCAB_LEARN_APPLY_BODY_LIMIT,
};
pub(crate) use read::{
    acquire_vocab_read_permit, finish_vocab_worker, get_vocab, serialize_vocab,
    vocab_method_not_allowed, VocabReadTransport, VOCAB_READ_BODY_LIMIT,
};
pub(crate) use write::{
    acquire_vocab_write_permit, finish_vocab_write_response, put_vocab, vocab_write_error_response,
    vocab_write_success, VocabWriteTransport, VOCAB_WRITE_BODY_LIMIT,
};

#[cfg(test)]
mod learn_apply_tests;
#[cfg(test)]
mod learn_tests;
#[cfg(test)]
mod write_tests;

pub(crate) fn default_min_count() -> usize {
    2
}

/// Build a [`CorpusLearnConfig`](reverse_rusty::vocab::CorpusLearnConfig) from the
/// shared learn-endpoint params, falling back to the engine defaults for any absent
/// NPMI knob (so `CorpusLearnConfig::default()` stays the single source of truth).
pub(crate) fn build_corpus_config(
    min_count: usize,
    corpus_phrases: bool,
    npmi_tau: Option<f64>,
    npmi_min_count: Option<usize>,
    npmi_iterations: Option<usize>,
    anyof_mode: reverse_rusty::vocab::AnyOfLearnMode,
) -> reverse_rusty::vocab::CorpusLearnConfig {
    let d = reverse_rusty::vocab::CorpusLearnConfig::default();
    reverse_rusty::vocab::CorpusLearnConfig {
        anyof_min_count: min_count,
        corpus_phrases,
        npmi_tau: npmi_tau.unwrap_or(d.npmi_tau),
        npmi_min_count: npmi_min_count.unwrap_or(d.npmi_min_count),
        npmi_iterations: npmi_iterations.unwrap_or(d.npmi_iterations),
        anyof_mode,
    }
}

/// `anyof_mode` as a request control: how what the any-of groups teach is applied.
#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnyOfModeParam {
    Expansion,
    Collapse,
}

/// The any-of mode a learn request asks for (ADR-202): `anyof_mode`, or the older
/// `learn_equivalences` boolean it replaces (`true` is expansion, `false` is collapse), or
/// expansion when the request names neither. A request that sends both and has them
/// disagree is refused, not guessed at.
pub(crate) fn resolve_anyof_mode(
    anyof_mode: Option<AnyOfModeParam>,
    learn_equivalences: Option<bool>,
) -> Result<reverse_rusty::vocab::AnyOfLearnMode, String> {
    use reverse_rusty::vocab::AnyOfLearnMode;
    let named = anyof_mode.map(|mode| match mode {
        AnyOfModeParam::Expansion => AnyOfLearnMode::Expansion,
        AnyOfModeParam::Collapse => AnyOfLearnMode::Collapse,
    });
    let from_flag = learn_equivalences.map(|expansion| {
        if expansion {
            AnyOfLearnMode::Expansion
        } else {
            AnyOfLearnMode::Collapse
        }
    });
    match (named, from_flag) {
        (Some(named), Some(flag)) if named != flag => Err(
            "`anyof_mode` and `learn_equivalences` disagree; send `anyof_mode` alone".to_string(),
        ),
        (Some(mode), _) | (None, Some(mode)) => Ok(mode),
        (None, None) => Ok(AnyOfLearnMode::default()),
    }
}

#[cfg(test)]
mod read_tests;

#[cfg(test)]
mod anyof_mode_tests {
    use super::{resolve_anyof_mode, AnyOfModeParam};
    use reverse_rusty::vocab::AnyOfLearnMode;

    #[test]
    fn a_request_that_names_no_mode_learns_by_expansion() {
        assert_eq!(
            resolve_anyof_mode(None, None),
            Ok(AnyOfLearnMode::Expansion)
        );
    }

    #[test]
    fn either_control_selects_the_mode_and_a_disagreement_is_refused() {
        use AnyOfModeParam::{Collapse, Expansion};
        assert_eq!(
            resolve_anyof_mode(Some(Collapse), None),
            Ok(AnyOfLearnMode::Collapse)
        );
        assert_eq!(
            resolve_anyof_mode(Some(Expansion), None),
            Ok(AnyOfLearnMode::Expansion)
        );
        // The older boolean: true asked for equivalences, false for collapse synonyms.
        assert_eq!(
            resolve_anyof_mode(None, Some(true)),
            Ok(AnyOfLearnMode::Expansion)
        );
        assert_eq!(
            resolve_anyof_mode(None, Some(false)),
            Ok(AnyOfLearnMode::Collapse)
        );
        assert_eq!(
            resolve_anyof_mode(Some(Collapse), Some(false)),
            Ok(AnyOfLearnMode::Collapse)
        );
        assert!(resolve_anyof_mode(Some(Collapse), Some(true)).is_err());
        assert!(resolve_anyof_mode(Some(Expansion), Some(false)).is_err());
    }
}
