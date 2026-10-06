//! A title that carries every word of a multi-word alias form carries the form (ADR-205).
//!
//! A query that spells a form out (`wireless mouse`) is compiled to the form's entity, so
//! that the alias's other forms match it too. Before the alias existed the same query asked
//! for each word, and matched a title that had them apart or in another order. The title
//! side keeps that true: the positive view gets a form's entity whenever it holds all of the
//! form's words, wherever they stand. Stored queries are not touched, and the canonical view
//! that negation reads is not either.

use crate::dict::{Dict, FeatureId};
use crate::util::{fast_map, fnv1a64, FastMap, FastSet};

/// The identity of a feature name here. Names are compared by this hash, so that a title's
/// names can be kept and searched without holding strings. A collision could only make a
/// title appear to carry a word it lacks, which adds a candidate and never removes one.
#[inline]
pub(in crate::normalize) fn name_hash(name: &str) -> u64 {
    fnv1a64(name.as_bytes())
}

/// One word of a form.
struct Word {
    /// The names a title may carry the word under, sorted.
    names: Vec<u64>,
    /// Whether one of those names is the entity of a form, so that the completion itself
    /// can put the word in the view.
    suppliable: bool,
}

struct Form {
    entity: String,
    entity_name: u64,
    words: Vec<Word>,
}

/// The words of every multi-word alias form.
pub(in crate::normalize) struct AliasWords {
    forms: Vec<Form>,
    /// Name -> the forms keyed on it. A form is listed under the names of ONE of its words,
    /// the word the fewest forms share, and is looked at only once that word is in the
    /// view. Ten thousand forms `wireless <model>` cost a title that says `wireless`
    /// nothing; they are keyed on their model.
    keyed: FastMap<u64, Vec<u32>>,
}

/// What the completion of one title remembers (ADR-205). Empty between titles. It grows
/// with what a title touches and never with the number of forms, so a scratch made for a
/// single title costs nothing until the title carries a form's key word.
#[derive(Debug, Default)]
pub(in crate::normalize) struct AliasScratch {
    /// Every feature name of the title's positive view, as [`name_hash`] values.
    pub(in crate::normalize) names: Vec<u64>,
    /// The entity names the completion has put in the view.
    entered: FastSet<u64>,
    /// The same names in the order they entered; each is visited once.
    queue: Vec<u64>,
    /// Form -> the word it was last found waiting on, or [`DONE`].
    looked: FastMap<u64, u32>,
    /// Name -> the forms found waiting on a word the view may yet get under that name.
    waiting: FastMap<u64, Vec<u32>>,
}

/// The form is in the view; nothing is left to look at.
const DONE: u32 = u32::MAX;

impl AliasWords {
    /// `forms` gives, for each alias form, its entity and, for each of its words, the
    /// feature names a title carries the word under: `term:<word>`, which the positive view
    /// holds for every cleaned token, and whatever the word compiles to as a token of its
    /// own (a synonym's canonical, a typed number).
    pub(in crate::normalize) fn new(forms: Vec<(String, Vec<Vec<String>>)>) -> Option<Self> {
        let forms: Vec<(String, Vec<Vec<u64>>)> = forms
            .into_iter()
            .filter(|(_, words)| !words.is_empty())
            .map(|(entity, words)| {
                let words = words
                    .iter()
                    .map(|names| {
                        let mut hashes: Vec<u64> =
                            names.iter().map(|name| name_hash(name)).collect();
                        hashes.sort_unstable();
                        hashes.dedup();
                        hashes
                    })
                    .collect();
                (entity, words)
            })
            .collect();
        if forms.is_empty() {
            return None;
        }
        let entities: FastSet<u64> = forms.iter().map(|(entity, _)| name_hash(entity)).collect();
        // How many forms carry each name, counted once per form.
        let mut shared: FastMap<u64, u32> = fast_map();
        for (_, words) in &forms {
            let mut names: Vec<u64> = words.iter().flatten().copied().collect();
            names.sort_unstable();
            names.dedup();
            for name in names {
                *shared.entry(name).or_default() += 1;
            }
        }
        let mut keyed: FastMap<u64, Vec<u32>> = fast_map();
        for (index, (_, words)) in forms.iter().enumerate() {
            let index = u32::try_from(index).ok()?;
            let key = words.iter().min_by_key(|names| {
                names
                    .iter()
                    .map(|name| u64::from(shared.get(name).copied().unwrap_or(0)))
                    .sum::<u64>()
            })?;
            for &name in key {
                keyed.entry(name).or_default().push(index);
            }
        }
        let forms = forms
            .into_iter()
            .map(|(entity, words)| Form {
                entity_name: name_hash(&entity),
                entity,
                words: words
                    .into_iter()
                    .map(|names| Word {
                        suppliable: names.iter().any(|name| entities.contains(name)),
                        names,
                    })
                    .collect(),
            })
            .collect();
        Some(Self { forms, keyed })
    }

    /// Append the entity of every form all of whose words the title carries, to a fixed
    /// point: a form the title carries puts its entity in the view, and that entity may be a
    /// word of another form. Returns how many times a form was examined.
    ///
    /// `scratch.names` is the title's complete positive view by name, in any order and with
    /// repeats; it is left sorted and distinct. The rest of the scratch is empty on entry
    /// and on return. An entity the dictionary has not interned resolves to its synthetic
    /// id, as everywhere on the title side (ADR-046). `out` may receive an entity twice.
    ///
    /// A name enters the view once, from the title or as the entity of a completed form,
    /// and there is one thing done with it: the forms keyed on it are examined, and so are
    /// the forms an earlier look found waiting on it. A form is therefore never examined
    /// before its key word is in the view, and the work follows the forms a title touches
    /// and not the number of forms.
    pub(in crate::normalize) fn complete_into(
        &self,
        scratch: &mut AliasScratch,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) -> usize {
        // A title that repeats a word a thousand times carries it once.
        scratch.names.sort_unstable();
        scratch.names.dedup();
        debug_assert!(scratch.is_clear());
        let mut examined = 0usize;
        for at in 0..scratch.names.len() {
            let name = scratch.names[at];
            self.enter(name, scratch, dict, out, &mut examined);
        }
        // Each entity that entered the view is a name like any other, in the order they
        // entered.
        let mut next = 0;
        while next < scratch.queue.len() {
            let name = scratch.queue[next];
            next += 1;
            self.enter(name, scratch, dict, out, &mut examined);
        }
        scratch.queue.clear();
        scratch.entered.clear();
        scratch.looked.clear();
        scratch.waiting.clear();
        examined
    }

    /// `name` is in the view: examine the forms keyed on it and the forms waiting on it.
    fn enter(
        &self,
        name: u64,
        scratch: &mut AliasScratch,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
        examined: &mut usize,
    ) {
        if let Some(forms) = self.keyed.get(&name) {
            for &form in forms {
                self.examine(form, scratch, dict, out, examined);
            }
        }
        if scratch.waiting.is_empty() {
            return;
        }
        if let Some(forms) = scratch.waiting.remove(&name) {
            for form in forms {
                self.examine(form, scratch, dict, out, examined);
            }
        }
    }

    /// Put the form's entity in the view if the view holds every word of the form. If it
    /// does not, and the word that is missing is one the completion could still supply,
    /// note the form under that word's names.
    fn examine(
        &self,
        form: u32,
        scratch: &mut AliasScratch,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
        examined: &mut usize,
    ) {
        *examined += 1;
        let looked = scratch.looked.get(&u64::from(form)).copied();
        if looked == Some(DONE) {
            return;
        }
        let candidate = &self.forms[form as usize];
        let has = |name: &u64| {
            scratch.names.binary_search(name).is_ok() || scratch.entered.contains(name)
        };
        let missing = candidate
            .words
            .iter()
            .position(|word| !word.names.iter().any(has));
        let Some(missing) = missing else {
            scratch.looked.insert(u64::from(form), DONE);
            out.push(dict.get_or_synthetic(&candidate.entity));
            // A name the title already carries has had its forms examined.
            let name = candidate.entity_name;
            if scratch.names.binary_search(&name).is_err() && scratch.entered.insert(name) {
                scratch.queue.push(name);
            }
            return;
        };
        let word = &candidate.words[missing];
        let at = u32::try_from(missing).unwrap_or(DONE - 1);
        // Words only ever enter the view, so the first missing word only moves forward:
        // a form is noted under each of its words at most once.
        if word.suppliable && looked != Some(at) {
            scratch.looked.insert(u64::from(form), at);
            for &name in &word.names {
                scratch.waiting.entry(name).or_default().push(form);
            }
        }
    }

    /// How many forms a title that carries `name` has to look at.
    #[cfg(test)]
    pub(in crate::normalize) fn forms_keyed_on(&self, name: &str) -> usize {
        self.keyed.get(&name_hash(name)).map_or(0, Vec::len)
    }
}

impl AliasScratch {
    /// Nothing is left from an earlier title.
    fn is_clear(&self) -> bool {
        self.entered.is_empty()
            && self.queue.is_empty()
            && self.looked.is_empty()
            && self.waiting.is_empty()
    }

    /// The memory held for the completion itself, in entries. A title that touches no form
    /// must leave it at zero.
    #[cfg(test)]
    pub(in crate::normalize) fn held(&self) -> usize {
        self.entered.capacity()
            + self.queue.capacity()
            + self.looked.capacity()
            + self.waiting.capacity()
    }
}
