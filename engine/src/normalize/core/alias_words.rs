//! A title that carries every word of a multi-word alias form carries the form (ADR-205).
//!
//! A query that spells a form out (`wireless mouse`) is compiled to the form's entity, so
//! that the alias's other forms match it too. Before the alias existed the same query asked
//! for each word, and matched a title that had them apart or in another order. The title
//! side keeps that true: the positive view gets a form's entity whenever it holds all of the
//! form's words, wherever they stand. Stored queries are not touched, and the canonical view
//! that negation reads is not either.

use crate::dict::{Dict, FeatureId};

/// A form with more words than a mask holds bits is left to the adjacent match alone.
const MAX_WORDS: usize = 64;

/// The words of every multi-word alias form, keyed by the feature names a title may carry
/// each word under.
pub(in crate::normalize) struct AliasWords {
    /// Feature name -> every `(form, word)` the name stands for. The same string-keyed
    /// map the dictionary uses: one lookup per feature a title emits.
    index: crate::util::FastMap<String, Vec<(u32, u8)>>,
    /// Form -> its entity and the mask with one bit per word.
    forms: Vec<(String, u64)>,
}

impl AliasWords {
    /// `forms` gives, for each alias form, its entity and, for each of its words, the
    /// feature names a title carries the word under: whatever the word compiles to as a
    /// token of its own (itself, a synonym's canonical, a number typed or not).
    pub(in crate::normalize) fn new(forms: Vec<(String, Vec<Vec<String>>)>) -> Option<Self> {
        let mut index: crate::util::FastMap<String, Vec<(u32, u8)>> = crate::util::fast_map();
        let mut entities = Vec::new();
        for (entity, words) in forms {
            if words.is_empty() || words.len() > MAX_WORDS {
                continue;
            }
            let form = u32::try_from(entities.len()).ok()?;
            for (word, names) in words.iter().enumerate() {
                for name in names {
                    let slots = index.entry(name.clone()).or_default();
                    let slot = (form, word as u8);
                    if !slots.contains(&slot) {
                        slots.push(slot);
                    }
                }
            }
            let all = if words.len() == MAX_WORDS {
                u64::MAX
            } else {
                (1u64 << words.len()) - 1
            };
            entities.push((entity, all));
        }
        if entities.is_empty() {
            return None;
        }
        Some(Self {
            index,
            forms: entities,
        })
    }

    /// Note one feature name the title emitted. `seen` holds `(form, words seen)` for the
    /// forms touched so far; a title touches few.
    #[inline]
    pub(in crate::normalize) fn observe(&self, name: &str, seen: &mut Vec<(u32, u64)>) {
        let Some(slots) = self.index.get(name) else {
            return;
        };
        for &(form, word) in slots {
            let bit = 1u64 << word;
            match seen.iter_mut().find(|(touched, _)| *touched == form) {
                Some((_, mask)) => *mask |= bit,
                None => seen.push((form, bit)),
            }
        }
    }

    /// Append the entity of every form all of whose words were seen. An entity the
    /// dictionary has not interned resolves to its synthetic id, as everywhere on the title
    /// side (ADR-046).
    pub(in crate::normalize) fn complete_into(
        &self,
        seen: &[(u32, u64)],
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) {
        for &(form, mask) in seen {
            let (entity, all) = &self.forms[form as usize];
            if mask == *all {
                out.push(dict.get_or_synthetic(entity));
            }
        }
    }
}
