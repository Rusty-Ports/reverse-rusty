//! Analysis: stages 3 to 5 of `docs/design/normalization.md` §2.1, the two views of a title,
//! and the graphs of quoted clauses.
//!
//! Written from that text alone (ADR-220).

mod analysis;

use crate::clean::clean_tokens;
use crate::features::Feature;
use crate::vocab::RefVocab;
use analysis::{arc, emitted, features, position, unique, with_fallback, Mode};

/// One arc of a title's graph: `feature`, from token position `start` to token position `end`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPositionArc {
    pub feature: Feature,
    pub start: u32,
    pub end: u32,
}

/// One edge of a quoted clause's graph. `alternatives` is sorted and has no duplicates. (The
/// caller may widen it by equivalence classes afterwards, and keeps it sorted.)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPhraseArc {
    pub start: u32,
    pub end: u32,
    pub alternatives: Vec<Feature>,
}

/// The graph of one quoted clause over `positions` token positions. A clause that analyzes to
/// nothing has no arcs, and the caller drops it.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPhraseGraph {
    pub positions: u32,
    pub arcs: Vec<RefPhraseArc>,
}

/// One title, analyzed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TitleViews {
    /// The canonical view `N(T)`: sorted, no duplicates.
    pub canonical_features: Vec<Feature>,
    /// The positive view `P(T)`: sorted, no duplicates.
    pub positive_features: Vec<Feature>,
    /// How many tokens the cleaned title has.
    pub positions: u32,
    /// The title's canonical graph.
    pub canonical_arcs: Vec<RefPositionArc>,
    /// The title's positive graph.
    pub positive_arcs: Vec<RefPositionArc>,
}

/// The features a piece of unquoted query text analyzes to. The caller passes a run of bare
/// terms joined by spaces, one member of a group, or one form of an equivalence group. Any
/// order; duplicates are allowed.
#[must_use]
pub fn query_features(vocab: &RefVocab, text: &str) -> Vec<Feature> {
    let tokens = clean_tokens(text, &vocab.punct);
    let phrases = vocab.phrases_in_force();
    emitted(vocab, &phrases, &tokens, Mode::Query)
        .into_iter()
        .map(|item| item.feature)
        .collect()
}

/// The two views and the two graphs of a title.
#[must_use]
pub fn title_views(vocab: &RefVocab, text: &str) -> TitleViews {
    let tokens = clean_tokens(text, &vocab.punct);
    let phrases = vocab.phrases_in_force();
    let canonical = emitted(vocab, &phrases, &tokens, Mode::Title);
    let unconsumed = emitted(vocab, &phrases, &tokens, Mode::KeepAll);
    let occurrences = analysis::all_phrase_arcs(&phrases, &tokens);
    let literal: Vec<_> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !analysis::is_marker(token))
        .map(|(index, token)| arc(Feature::term(token), index, index + 1))
        .collect();
    let canonical_features = features(&canonical);
    let mut positive_features = canonical_features.clone();
    if vocab.has_alias() {
        positive_features.extend(features(&unconsumed));
        positive_features.extend(features(&occurrences));
        positive_features.extend(features(&literal));
        positive_features = unique(positive_features);
        analysis::add_aliases(vocab, &phrases, &mut positive_features);
        positive_features = unique(positive_features);
    }
    let canonical_arcs = with_fallback(canonical, &tokens);
    let mut positive_arcs = canonical_arcs.clone();
    positive_arcs.extend(with_fallback(unconsumed, &tokens));
    positive_arcs.extend(literal);
    positive_arcs.extend(occurrences);
    TitleViews {
        canonical_features,
        positive_features,
        positions: position(tokens.len()),
        canonical_arcs,
        positive_arcs: unique(positive_arcs),
    }
}

/// The graph of a quoted clause's content, before any widening by equivalence classes.
#[must_use]
pub fn quoted_clause(vocab: &RefVocab, text: &str) -> RefPhraseGraph {
    let tokens = clean_tokens(text, &vocab.punct);
    let phrases = vocab.phrases_in_force();
    let emitted = with_fallback(emitted(vocab, &phrases, &tokens, Mode::Query), &tokens);
    let mut arcs: Vec<RefPhraseArc> = Vec::new();
    for item in emitted {
        if let Some(edge) = arcs
            .iter_mut()
            .find(|edge| edge.start == item.start && edge.end == item.end)
        {
            edge.alternatives.push(item.feature);
        } else {
            arcs.push(RefPhraseArc {
                start: item.start,
                end: item.end,
                alternatives: vec![item.feature],
            });
        }
    }
    for edge in &mut arcs {
        edge.alternatives.sort();
        edge.alternatives.dedup();
    }
    arcs.sort();
    RefPhraseGraph {
        positions: position(tokens.len()),
        arcs,
    }
}

/// Whether a quoted clause's graph matches a title's graph ("Matching" in §2.1). The caller
/// passes the positive graph for a required clause and the canonical graph for a forbidden
/// one.
#[must_use]
pub fn phrase_graph_matches(
    query: &RefPhraseGraph,
    title_positions: u32,
    title: &[RefPositionArc],
) -> bool {
    if query.positions == 0 || query.arcs.is_empty() {
        return false;
    }
    // Reachable pairs of query/title boundaries. Edge labels must agree; their
    // widths need not (a one-token synonym can carry a multi-token phrase).
    let mut reachable: Vec<_> = (0..title_positions).map(|start| (0, start)).collect();
    let mut next = 0;
    while let Some(&(query_at, title_at)) = reachable.get(next) {
        if query_at == query.positions {
            return true;
        }
        for edge in &query.arcs {
            if edge.start != query_at || edge.end <= query_at || edge.end > query.positions {
                continue;
            }
            for item in title {
                if item.start == title_at
                    && item.end > title_at
                    && item.end <= title_positions
                    && edge.alternatives.contains(&item.feature)
                {
                    let pair = (edge.end, item.end);
                    if !reachable.contains(&pair) {
                        reachable.push(pair);
                    }
                }
            }
        }
        next += 1;
    }
    false
}

#[cfg(test)]
mod tests;
