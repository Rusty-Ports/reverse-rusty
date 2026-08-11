//! Immutable per-segment tag summaries (ADR-174).
//!
//! A summary is the sorted union of every `TagId` stored in one sealed segment.
//! It can prove that a request tag predicate accepts no row in that segment when
//! any predicate group has no member in the union. It deliberately does not
//! encode cross-group correlation: inconclusive summaries fail open and the
//! ordinary per-row verifier remains authoritative.

use std::sync::Arc;

use crate::exact::TagPredicate;
use crate::tagdict::TagId;

#[derive(Clone, Debug)]
pub(crate) struct TagSummary {
    ids: Arc<[TagId]>,
}

impl TagSummary {
    /// Build an exact union from a segment's immutable tag blob. The blob is
    /// already per-row sorted, but values repeat across rows. Build a set first
    /// so a category-local segment's temporary state and sort cost scale with
    /// distinct tags rather than row count, then sort only the exact union. The
    /// source blob is still scanned once.
    pub(crate) fn build(tag_blob: &[TagId]) -> Self {
        let mut unique = crate::util::FastSet::<TagId>::default();
        // Insert manually: `collect`/`extend` may reserve from the source slice's
        // row-count size hint, defeating the distinct-cardinality memory bound.
        for &id in tag_blob {
            unique.insert(id);
        }
        let mut ids: Vec<TagId> = unique.into_iter().collect();
        ids.sort_unstable();
        Self { ids: ids.into() }
    }

    /// Whether this summary is compatible with at least one row satisfying
    /// `predicate`. `true` is deliberately inconclusive; `false` is a proof
    /// that the whole segment can be skipped.
    #[inline]
    pub(crate) fn may_match(&self, predicate: &TagPredicate) -> bool {
        predicate
            .groups()
            .iter()
            .all(|group| sorted_intersects(&self.ids, group))
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.ids.len() * std::mem::size_of::<TagId>()
    }
}

/// Integer-only intersection of two sorted slices. Predicate groups are
/// normally tiny, so binary-searching the summary avoids scanning a segment-
/// wide union. Empty on either side correctly proves no intersection.
#[inline]
fn sorted_intersects(a: &[TagId], b: &[TagId]) -> bool {
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    small.iter().any(|id| large.binary_search(id).is_ok())
}

#[cfg(test)]
mod tests {
    use super::TagSummary;
    use crate::exact::TagPredicate;

    #[test]
    fn proves_only_group_absence() {
        let summary = TagSummary::build(&[9, 3, 3, 7]);
        assert!(summary.may_match(&TagPredicate::empty()));
        assert!(summary.may_match(&TagPredicate::new(vec![vec![3, 4]])));
        assert!(summary.may_match(&TagPredicate::new(vec![vec![3], vec![7]])));
        assert!(!summary.may_match(&TagPredicate::new(vec![vec![4]])));
        assert!(!summary.may_match(&TagPredicate::new(vec![vec![3], vec![8]])));
        assert!(!summary.may_match(&TagPredicate::new(vec![Vec::new()])));
    }

    #[test]
    fn independent_group_presence_does_not_claim_row_correlation() {
        // The summary cannot know whether ids 3 and 7 occur on the same row.
        // It must fail open so exact per-row verification decides.
        let summary = TagSummary::build(&[3, 7]);
        assert!(summary.may_match(&TagPredicate::new(vec![vec![3], vec![7]])));
    }
}
