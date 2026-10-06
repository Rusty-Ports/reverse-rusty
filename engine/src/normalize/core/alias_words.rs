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
    /// Which entity this is, among the distinct entities of all forms. Two forms that name
    /// one entity share it.
    entity_slot: u32,
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
    /// Entity name -> its slot. A form the title carries puts its entity in the view, and
    /// that entity may be a word of another form.
    entity_slots: FastMap<u64, u32>,
    /// Entity slot -> the forms that have the entity among the names of one of their
    /// words: the only forms worth another look once the entity is in the view.
    dependents: Vec<Vec<u32>>,
}

impl AliasWords {
    /// `forms` gives, for each alias form, its entity and, for each of its words, the
    /// feature names a title carries the word under: `term:<word>`, which the positive view
    /// holds for every cleaned token, and whatever the word compiles to as a token of its
    /// own (a synonym's canonical, a typed number).
    pub(in crate::normalize) fn new(forms: Vec<(String, Vec<Vec<String>>)>) -> Option<Self> {
        // Entities are told apart by their names, exactly.
        let mut slot_of: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let mut entity_slots: FastMap<u64, u32> = fast_map();
        let forms: Vec<Form> = forms
            .into_iter()
            .filter(|(_, words)| !words.is_empty())
            .map(|(entity, words)| {
                let next = u32::try_from(slot_of.len()).unwrap_or(u32::MAX);
                let entity_slot = *slot_of.entry(entity.clone()).or_insert(next);
                entity_slots.insert(name_hash(&entity), entity_slot);
                Form {
                    entity,
                    entity_slot,
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
                }
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
        let mut dependents: Vec<Vec<u32>> = vec![Vec::new(); slot_of.len()];
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
            for name in form.words.iter().flatten() {
                if let Some(&slot) = entity_slots.get(name) {
                    let waiting = &mut dependents[slot as usize];
                    if !waiting.contains(&index) {
                        waiting.push(index);
                    }
                }
            }
        }
        Some(Self {
            forms,
            keyed,
            entity_slots,
            dependents,
        })
    }

    /// Append the entity of every form all of whose words the title carries, to a fixed
    /// point: a form the title carries puts its entity in the view, and that entity may be a
    /// word of another form. Returns how many times a form was examined.
    ///
    /// `carried` is the title's complete positive view by name, in any order and with
    /// repeats; it is left sorted and distinct. `in_view` and `completed` are scratch,
    /// empty between calls. An entity the dictionary has not interned resolves to its
    /// synthetic id, as everywhere on the title side (ADR-046).
    ///
    /// A form is examined when the title carries its key word, and again each time an
    /// entity that is one of its words enters the view, so the work follows the forms the
    /// title touches and not the number of forms.
    pub(in crate::normalize) fn complete_into(
        &self,
        carried: &mut Vec<u64>,
        in_view: &mut Vec<bool>,
        completed: &mut Vec<u32>,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) -> usize {
        // A title that repeats a word a thousand times carries it once.
        carried.sort_unstable();
        carried.dedup();
        if in_view.len() != self.dependents.len() {
            in_view.clear();
            in_view.resize(self.dependents.len(), false);
        }
        debug_assert!(completed.is_empty());
        let mut examined = 0usize;
        let mut examine = |form: u32, in_view: &mut Vec<bool>, completed: &mut Vec<u32>| {
            examined += 1;
            let candidate = &self.forms[form as usize];
            if in_view[candidate.entity_slot as usize] {
                return;
            }
            let has = |name: &u64| {
                carried.binary_search(name).is_ok()
                    || self
                        .entity_slots
                        .get(name)
                        .is_some_and(|&slot| in_view[slot as usize])
            };
            if candidate.words.iter().all(|names| names.iter().any(has)) {
                in_view[candidate.entity_slot as usize] = true;
                completed.push(form);
                out.push(dict.get_or_synthetic(&candidate.entity));
            }
        };
        for name in carried.iter() {
            if let Some(forms) = self.keyed.get(name) {
                for &form in forms {
                    examine(form, in_view, completed);
                }
            }
        }
        // Each entity that entered the view gives the forms waiting on it another look.
        let mut next = 0;
        while next < completed.len() {
            let slot = self.forms[completed[next] as usize].entity_slot;
            next += 1;
            for &form in &self.dependents[slot as usize] {
                examine(form, in_view, completed);
            }
        }
        for &form in completed.iter() {
            in_view[self.forms[form as usize].entity_slot as usize] = false;
        }
        completed.clear();
        examined
    }

    /// How many forms a title that carries `name` has to look at.
    #[cfg(test)]
    pub(in crate::normalize) fn forms_keyed_on(&self, name: &str) -> usize {
        self.keyed.get(&name_hash(name)).map_or(0, Vec::len)
    }
}
