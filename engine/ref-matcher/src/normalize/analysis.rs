//! Direct token scans shared by feature sets and position graphs.

use super::RefPositionArc;
use crate::clean::clean_tokens;
use crate::features::Feature;
use crate::semantic::resolve_equivalences;
use crate::tables::YEARS;
use crate::vocab::{Phrase, PhraseMode, RefVocab};
use std::cmp::Reverse;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Query,
    Title,
    KeepAll,
}

struct Occurrence<'a> {
    start: usize,
    end: usize,
    phrase: &'a Phrase,
}

impl Occurrence<'_> {
    fn arc(&self) -> RefPositionArc {
        arc(Feature::raw(&self.phrase.feature), self.start, self.end)
    }
}

fn occurrences<'a>(phrases: &'a [Phrase], tokens: &[String]) -> Vec<Occurrence<'a>> {
    let mut found = Vec::new();
    for start in 0..tokens.len() {
        for phrase in phrases {
            if !phrase.tokens.is_empty() && tokens[start..].starts_with(&phrase.tokens) {
                found.push(Occurrence {
                    start,
                    end: start + phrase.tokens.len(),
                    phrase,
                });
            }
        }
    }
    // The vocabulary has already discarded duplicate token sequences.
    found.sort_by_key(|item| (item.start, Reverse(item.end - item.start)));
    found
}

pub(super) fn all_phrase_arcs(phrases: &[Phrase], tokens: &[String]) -> Vec<RefPositionArc> {
    occurrences(phrases, tokens)
        .iter()
        .map(Occurrence::arc)
        .collect()
}

pub(super) fn emitted(
    vocab: &RefVocab,
    phrases: &[Phrase],
    tokens: &[String],
    mode: Mode,
) -> Vec<RefPositionArc> {
    let mut arcs = Vec::new();
    let mut consumed = vec![false; tokens.len()];
    let mut end = 0;
    for item in occurrences(phrases, tokens) {
        if item.start < end {
            continue;
        }
        end = item.end;
        arcs.push(item.arc());
        let consumes = mode != Mode::KeepAll
            && (item.phrase.mode == PhraseMode::Collapse
                || (item.phrase.mode == PhraseMode::Alias && mode == Mode::Query));
        if consumes {
            consumed[item.start..item.end].fill(true);
        }
    }
    for (position, token) in tokens.iter().enumerate() {
        if !consumed[position] {
            if let Some(feature) = token_feature(vocab, tokens, position, token) {
                arcs.push(arc(feature, position, position + 1));
            }
        }
    }
    arcs
}

pub(super) fn is_marker(token: &str) -> bool {
    matches!(token, "#" | "/")
}

fn token_feature(
    vocab: &RefVocab,
    tokens: &[String],
    position: usize,
    token: &str,
) -> Option<Feature> {
    if is_marker(token) {
        return None;
    }
    let digits = token.bytes().filter(u8::is_ascii_digit).count();
    let dots = token.bytes().filter(|&ch| ch == b'.').count();
    if digits > 0 && dots <= 1 && digits + dots == token.len() {
        let before = tokens[..position].last().map(String::as_str);
        let after = tokens.get(position + 1).map(String::as_str);
        let generic = matches!(before, Some("#" | "/"))
            || after == Some("/")
            || before.is_some_and(|word| {
                vocab
                    .number_context
                    .iter()
                    .any(|context| context.eq_ignore_ascii_case(word))
            });
        if !generic
            && digits == 4
            && dots == 0
            && token
                .parse::<u32>()
                .is_ok_and(|number| YEARS.contains(&number))
        {
            return Some(Feature::year(token));
        }
        return Some(Feature::term(token));
    }
    Some(
        vocab
            .synonym_for(token)
            .map_or_else(|| Feature::term(token), Feature::raw),
    )
}

pub(super) fn position(value: usize) -> u32 {
    u32::try_from(value).expect("token position must fit the public u32 graph interface")
}

pub(super) fn arc(feature: Feature, start: usize, end: usize) -> RefPositionArc {
    RefPositionArc {
        feature,
        start: position(start),
        end: position(end),
    }
}

pub(super) fn unique<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values.dedup();
    values
}

pub(super) fn features(arcs: &[RefPositionArc]) -> Vec<Feature> {
    unique(arcs.iter().map(|item| item.feature.clone()).collect())
}

pub(super) fn with_fallback(
    mut arcs: Vec<RefPositionArc>,
    tokens: &[String],
) -> Vec<RefPositionArc> {
    for (index, token) in tokens.iter().enumerate() {
        let start = position(index);
        if !arcs
            .iter()
            .any(|item| item.start <= start && start < item.end)
        {
            arcs.push(arc(Feature::term(token), index, index + 1));
        }
    }
    unique(arcs)
}

fn carried(feature: &Feature, available: &[Feature], equivalents: &[Vec<Feature>]) -> bool {
    available.contains(feature)
        || equivalents.iter().any(|class| {
            class.contains(feature) && class.iter().any(|member| available.contains(member))
        })
}

fn carried_in_pieces(
    form: &Phrase,
    vocab: &RefVocab,
    phrases: &[Phrase],
    available: &[Feature],
    equivalents: &[Vec<Feature>],
) -> bool {
    // Each reachable boundary describes a left-to-right partition of this form.
    let mut reachable = vec![false; form.tokens.len() + 1];
    reachable[0] = true;
    for start in 0..form.tokens.len() {
        if !reachable[start] {
            continue;
        }
        let token = &form.tokens[start];
        let alone = clean_tokens(token, &vocab.punct);
        if carried(&Feature::term(token), available, equivalents)
            || emitted(vocab, phrases, &alone, Mode::Title)
                .iter()
                .any(|item| carried(&item.feature, available, equivalents))
        {
            reachable[start + 1] = true;
        }
        for piece in phrases {
            if piece.tokens.len() >= 2
                && piece.tokens.len() < form.tokens.len()
                && form.tokens[start..].starts_with(&piece.tokens)
                && carried(&Feature::raw(&piece.feature), available, equivalents)
            {
                reachable[start + piece.tokens.len()] = true;
            }
        }
    }
    reachable[form.tokens.len()]
}

pub(super) fn add_aliases(vocab: &RefVocab, phrases: &[Phrase], available: &mut Vec<Feature>) {
    // Query analysis used by the existing equivalence resolver does not call title analysis.
    let equivalents = unique(resolve_equivalences(vocab).into_values().collect());
    loop {
        let old_len = available.len();
        for form in phrases {
            if form.mode == PhraseMode::Alias && !form.tokens.is_empty() {
                let feature = Feature::raw(&form.feature);
                if !available.contains(&feature)
                    && carried_in_pieces(form, vocab, phrases, available, &equivalents)
                {
                    available.push(feature);
                }
            }
        }
        if old_len == available.len() {
            break;
        }
    }
}
