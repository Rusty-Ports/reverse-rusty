//! A title that carries every word of a multi-word alias form carries the form (ADR-205).
//!
//! A query that spells a form out (`wireless mouse`) is compiled to the form's entity, so
//! that the alias's other forms match it too. Before the alias existed the same query asked
//! for each word, and matched a title that had them apart or in another order. The title
//! side keeps that true: the positive view gets a form's entity whenever it holds all of the
//! form's words, wherever they stand. Stored queries are not touched, and the canonical view
//! that negation reads is not either.

use crate::dict::{Dict, FeatureId};
use crate::util::{fast_map, fnv1a64, FastMap};

/// The identity of a feature name here. Names are compared by this hash, so that a title's
/// names can be kept and searched without holding strings. A collision could only make a
/// title appear to carry a word it lacks, which adds a candidate and never removes one.
#[inline]
pub(in crate::normalize) fn name_hash(name: &str) -> u64 {
    fnv1a64(name.as_bytes())
}

struct Form {
    entity: String,
    /// One entry per word: the names a title may carry the word under, sorted.
    words: Vec<Vec<u64>>,
}

/// The words of every multi-word alias form.
pub(in crate::normalize) struct AliasWords {
    forms: Vec<Form>,
    /// Name -> the forms keyed on it. A form is listed under the names of ONE of its words,
    /// the word the fewest forms share, so a title visits a form only when it carries that
    /// word. Ten thousand forms `wireless <model>` cost a title that says `wireless`
    /// nothing; they are keyed on their model.
    keyed: FastMap<u64, Vec<u32>>,
}

impl AliasWords {
    /// `forms` gives, for each alias form, its entity and, for each of its words, the
    /// feature names a title carries the word under: `term:<word>`, which the positive view
    /// holds for every cleaned token, and whatever the word compiles to as a token of its
    /// own (a synonym's canonical, a typed number).
    pub(in crate::normalize) fn new(forms: Vec<(String, Vec<Vec<String>>)>) -> Option<Self> {
        let forms: Vec<Form> = forms
            .into_iter()
            .filter(|(_, words)| !words.is_empty())
            .map(|(entity, words)| Form {
                entity,
                words: words
                    .iter()
                    .map(|names| {
                        let mut hashes: Vec<u64> =
                            names.iter().map(|name| name_hash(name)).collect();
                        hashes.sort_unstable();
                        hashes.dedup();
                        hashes
                    })
                    .collect(),
            })
            .collect();
        if forms.is_empty() {
            return None;
        }
        // How many forms carry each name, counted once per form.
        let mut shared: FastMap<u64, u32> = fast_map();
        for form in &forms {
            let mut names: Vec<u64> = form.words.iter().flatten().copied().collect();
            names.sort_unstable();
            names.dedup();
            for name in names {
                *shared.entry(name).or_default() += 1;
            }
        }
        let mut keyed: FastMap<u64, Vec<u32>> = fast_map();
        for (index, form) in forms.iter().enumerate() {
            let index = u32::try_from(index).ok()?;
            let key = form.words.iter().min_by_key(|names| {
                names
                    .iter()
                    .map(|name| u64::from(shared.get(name).copied().unwrap_or(0)))
                    .sum::<u64>()
            })?;
            for &name in key {
                keyed.entry(name).or_default().push(index);
            }
        }
        Some(Self { forms, keyed })
    }

    /// Append the entity of every form all of whose words the title carries. `carried` is
    /// the title's complete positive view by name, in any order and possibly with repeats:
    /// a title has a few dozen names, and a form is examined only when one of them is the
    /// form's key, so a scan is cheaper than sorting. An entity the dictionary has not
    /// interned resolves to its synthetic id, as everywhere on the title side (ADR-046).
    pub(in crate::normalize) fn complete_into(
        &self,
        carried: &[u64],
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) {
        for name in carried {
            let Some(forms) = self.keyed.get(name) else {
                continue;
            };
            for &form in forms {
                let form = &self.forms[form as usize];
                let every_word = form
                    .words
                    .iter()
                    .all(|names| names.iter().any(|name| carried.contains(name)));
                if every_word {
                    out.push(dict.get_or_synthetic(&form.entity));
                }
            }
        }
    }

    /// How many forms a title that carries `name` has to look at.
    #[cfg(test)]
    pub(in crate::normalize) fn forms_keyed_on(&self, name: &str) -> usize {
        self.keyed.get(&name_hash(name)).map_or(0, Vec::len)
    }
}
