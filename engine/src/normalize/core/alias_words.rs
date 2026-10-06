//! A title that carries every word of a multi-word alias form carries the form (ADR-205).
//!
//! A query that spells a form out (`wireless mouse`) is compiled to the form's entity, so
//! that the alias's other forms match it too. Before the alias existed the same query asked
//! for each word, and matched a title that had them apart or in another order. The title
//! side keeps that true: the positive view gets a form's entity whenever it holds some
//! parse of the form's text, wherever the pieces stand. A piece is a word, or a phrase the
//! vocabulary already had for several of the words, and it is held under its own name or
//! under one a query treats as the same. Stored queries are not touched, and the canonical
//! view that negation reads is not either.

use crate::dict::{name_hash, Dict, FeatureId};
use crate::util::{fast_map, FastMap, FastSet};

/// One way a query could have asked for a stretch of a form's text before the form
/// existed: a single word, or a phrase of the vocabulary that covers several of them.
pub(in crate::normalize) struct UnitSpec {
    /// The token positions the unit spans, `from..to`.
    pub(in crate::normalize) from: usize,
    pub(in crate::normalize) to: usize,
    /// The feature names a title may carry the unit under. For a word: `term:<word>`, which
    /// the positive view holds for every cleaned token, and whatever the word compiles to as
    /// a token of its own (a synonym's canonical, a typed number). For a phrase: its feature.
    pub(in crate::normalize) names: Vec<String>,
}

/// One multi-word alias form: its entity, how many tokens it has, and its units.
pub(in crate::normalize) struct FormSpec {
    pub(in crate::normalize) entity: String,
    pub(in crate::normalize) len: usize,
    pub(in crate::normalize) units: Vec<UnitSpec>,
}

struct Unit {
    from: u32,
    to: u32,
    /// Sorted.
    names: Vec<u64>,
}

struct Form {
    entity: String,
    entity_name: u64,
    len: u32,
    /// Sorted by `from`, so one pass over them finds every position the view reaches.
    units: Vec<Unit>,
}

/// The multi-word alias forms, as the parses of their text.
pub(in crate::normalize) struct AliasWords {
    forms: Vec<Form>,
    /// Name -> the forms keyed on it. Every parse of a form crosses each gap between two of
    /// its tokens with exactly one unit, so a title that carries the form carries a unit
    /// over any one gap. A form is listed under the names of the units over ONE gap, the gap
    /// whose names the fewest forms share, and is not looked at before one of them is in
    /// the view. Ten thousand forms `wireless <model>` cost a title that says `wireless`
    /// nothing; they are keyed on their model.
    keyed: FastMap<u64, Vec<u32>>,
    /// The names of every form's entity: what the completion itself can put in a view.
    entities: FastSet<u64>,
}

/// What the completion of one title remembers (ADR-205). Empty between titles. It grows
/// with what a title touches and never with the number of forms, so a scratch made for a
/// single title costs nothing until the title carries a form's key.
#[derive(Debug, Default)]
pub(in crate::normalize) struct AliasScratch {
    /// Every feature name of the title's positive view, as [`name_hash`] values.
    pub(in crate::normalize) names: Vec<u64>,
    /// The names the completion has put in the view: entities, and their equivalents.
    entered: FastSet<u64>,
    /// The same names in the order they entered; each is visited once.
    queue: Vec<u64>,
    /// The forms whose entity is in the view.
    done: FastSet<u64>,
    /// The (form, unit) pairs noted in `waiting`, so that each is noted once.
    noted: FastSet<u64>,
    /// Name -> the forms found waiting on a unit the view may yet get under that name.
    waiting: FastMap<u64, Vec<u32>>,
    /// Which token positions of the form under examination the view reaches.
    reach: Vec<bool>,
    /// The equivalence classes of the title's names, each by its first member, sorted.
    classes: Vec<u64>,
    /// The classes a completed form brought into the view, by their first member.
    widened: FastSet<u64>,
    /// How many names a completion went through to put them in the view.
    #[cfg(test)]
    pub(in crate::normalize) scanned: usize,
    /// Class (by its first member) -> whether one of its members is a form's entity, so
    /// that a class is looked through once per title however many pieces ask.
    supplies: FastMap<u64, bool>,
}

impl AliasWords {
    pub(in crate::normalize) fn new(specs: Vec<FormSpec>) -> Option<Self> {
        let forms: Vec<Form> = specs
            .into_iter()
            .filter(|spec| spec.len > 0)
            .filter_map(|spec| {
                let mut units: Vec<Unit> = spec
                    .units
                    .into_iter()
                    .filter(|unit| unit.from < unit.to && unit.to <= spec.len)
                    .map(|unit| {
                        let mut names: Vec<u64> =
                            unit.names.iter().map(|name| name_hash(name)).collect();
                        names.sort_unstable();
                        names.dedup();
                        Some(Unit {
                            from: u32::try_from(unit.from).ok()?,
                            to: u32::try_from(unit.to).ok()?,
                            names,
                        })
                    })
                    .collect::<Option<_>>()?;
                units.sort_by_key(|unit| (unit.from, unit.to));
                Some(Form {
                    entity_name: name_hash(&spec.entity),
                    entity: spec.entity,
                    len: u32::try_from(spec.len).ok()?,
                    units,
                })
            })
            .collect();
        if forms.is_empty() {
            return None;
        }
        // How many forms carry each name, counted once per form.
        let mut shared: FastMap<u64, u32> = fast_map();
        for form in &forms {
            let mut names: Vec<u64> = form
                .units
                .iter()
                .flat_map(|unit| unit.names.iter().copied())
                .collect();
            names.sort_unstable();
            names.dedup();
            for name in names {
                *shared.entry(name).or_default() += 1;
            }
        }
        let mut keyed: FastMap<u64, Vec<u32>> = fast_map();
        for (index, form) in forms.iter().enumerate() {
            let index = u32::try_from(index).ok()?;
            // What each gap costs: how widely shared the names of the units over it are.
            // One sweep, so a form of two thousand tokens is no harder than two thousand
            // forms of one.
            let mut change = vec![0i64; form.len as usize + 1];
            for unit in &form.units {
                let cost: i64 = unit
                    .names
                    .iter()
                    .map(|name| i64::from(shared.get(name).copied().unwrap_or(0)))
                    .sum();
                change[unit.from as usize] += cost;
                change[unit.to as usize] -= cost;
            }
            let (mut key_gap, mut least, mut running) = (0u32, i64::MAX, 0i64);
            for gap in 0..form.len {
                running += change[gap as usize];
                if running < least {
                    (key_gap, least) = (gap, running);
                }
            }
            let mut key: Vec<u64> = form
                .units
                .iter()
                .filter(|unit| unit.from <= key_gap && key_gap < unit.to)
                .flat_map(|unit| unit.names.iter().copied())
                .collect();
            key.sort_unstable();
            key.dedup();
            for name in key {
                keyed.entry(name).or_default().push(index);
            }
        }
        let entities = forms.iter().map(|form| form.entity_name).collect();
        Some(Self {
            forms,
            keyed,
            entities,
        })
    }

    /// Append the entity of every form the title carries, to a fixed point: a form the title
    /// carries puts its entity in the view, and that entity may be a unit of another form.
    /// Returns how many times a form was examined.
    ///
    /// A title carries a form when its view holds, for some way of cutting the form's text
    /// into words and phrases of the vocabulary, every piece: under one of the piece's own
    /// names, or under a name the dictionary's equivalences make a query take for it. Those
    /// are the ways a query could have asked for the text before the form existed.
    ///
    /// `scratch.names` is the title's complete positive view by name, in any order and with
    /// repeats; it is left sorted, distinct, and widened by the equivalents of its names.
    /// The rest of the scratch is empty on entry and on return. An entity the dictionary has
    /// not interned resolves to its synthetic id, as everywhere on the title side (ADR-046).
    /// `out` may receive an entity twice.
    ///
    /// A name enters the view once, from the title or from a completed form, and there is
    /// one thing done with it: the forms keyed on it are examined, and so are the forms an
    /// earlier look found waiting on it. A form is therefore never examined before a name of
    /// its key is in the view, and the work follows the forms a title touches and not the
    /// number of forms.
    pub(in crate::normalize) fn complete_into(
        &self,
        scratch: &mut AliasScratch,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) -> usize {
        debug_assert!(scratch.is_clear());
        // A title that repeats a word a thousand times carries it once.
        scratch.names.sort_unstable();
        scratch.names.dedup();
        // A title that carries a name carries, for this rule, every name a query treats as
        // the same.
        if dict.has_equivalent_names() {
            // Each class once, however many of its members the title carries: a class is
            // told by its first member.
            let AliasScratch { names, classes, .. } = scratch;
            classes.extend(
                names
                    .iter()
                    .filter_map(|&name| dict.equivalent_names(name)?.first().copied()),
            );
            if !classes.is_empty() {
                classes.sort_unstable();
                classes.dedup();
                for &first in classes.iter() {
                    if let Some(class) = dict.equivalent_names(first) {
                        names.extend_from_slice(class);
                    }
                }
                names.sort_unstable();
                names.dedup();
            }
        }
        let mut examined = 0usize;
        for at in 0..scratch.names.len() {
            let name = scratch.names[at];
            self.enter(name, scratch, dict, out, &mut examined);
        }
        // Each name that entered the view is a name like any other, in the order they
        // entered.
        let mut next = 0;
        while next < scratch.queue.len() {
            let name = scratch.queue[next];
            next += 1;
            self.enter(name, scratch, dict, out, &mut examined);
        }
        scratch.queue.clear();
        scratch.entered.clear();
        scratch.done.clear();
        scratch.noted.clear();
        scratch.waiting.clear();
        scratch.supplies.clear();
        scratch.classes.clear();
        scratch.widened.clear();
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

    /// Put the form's entity in the view if the view holds some parse of the form. If it
    /// does not, note the form under what the completion could still supply to take the
    /// view further into it.
    fn examine(
        &self,
        form: u32,
        scratch: &mut AliasScratch,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
        examined: &mut usize,
    ) {
        *examined += 1;
        let AliasScratch {
            names,
            entered,
            queue,
            done,
            noted,
            waiting,
            reach,
            classes,
            widened,
            supplies,
            #[cfg(test)]
            scanned,
        } = scratch;
        if done.contains(&u64::from(form)) {
            return;
        }
        let candidate = &self.forms[form as usize];
        let has = |name: &u64| names.binary_search(name).is_ok() || entered.contains(name);
        // How far into the form the view reads. The units are in order of where they start,
        // so the walk stops at the first one that starts beyond what has been reached: a
        // long form of which the title has little costs little.
        reach.clear();
        reach.resize(candidate.len as usize + 1, false);
        reach[0] = true;
        let mut furthest = 0u32;
        for unit in &candidate.units {
            if unit.from > furthest {
                break;
            }
            if reach[unit.from as usize] && !reach[unit.to as usize] && unit.names.iter().any(has) {
                reach[unit.to as usize] = true;
                furthest = furthest.max(unit.to);
            }
        }
        if reach[candidate.len as usize] {
            done.insert(u64::from(form));
            out.push(dict.get_or_synthetic(&candidate.entity));
            // The entity is in the view, and with it every name a query takes for it. A
            // name the title already carries has had its forms examined, and a class is
            // gone through once for a title, however many forms complete into it.
            let entity = [candidate.entity_name];
            let members = match dict.equivalent_names(candidate.entity_name) {
                Some(class) => {
                    let first = class.first().copied().unwrap_or(candidate.entity_name);
                    if classes.binary_search(&first).is_ok() || !widened.insert(first) {
                        return;
                    }
                    class
                }
                None => &entity,
            };
            for &name in members {
                #[cfg(test)]
                {
                    *scanned += 1;
                }
                if names.binary_search(&name).is_err() && entered.insert(name) {
                    queue.push(name);
                }
            }
            return;
        }
        // Only a piece that starts no further than the view has got to can take it further.
        // Note the form under each such piece that is missing and that the completion could
        // still supply, once. When one arrives the form is looked at again, from wherever
        // the view has got to by then.
        for (at, unit) in candidate.units.iter().enumerate() {
            if unit.from > furthest {
                break;
            }
            if reach[unit.to as usize] {
                continue;
            }
            let pair = (u64::from(form) << 32) | at as u64;
            if noted.contains(&pair) {
                continue;
            }
            let mut any = false;
            for &name in &unit.names {
                if self.can_supply(name, dict, supplies) {
                    waiting.entry(name).or_default().push(form);
                    any = true;
                }
            }
            if any {
                noted.insert(pair);
            }
        }
    }

    /// Whether the completion could put `name` in a view: it is a form's entity, or a query
    /// takes it for one.
    fn can_supply(&self, name: u64, dict: &Dict, supplies: &mut FastMap<u64, bool>) -> bool {
        if self.entities.contains(&name) {
            return true;
        }
        let Some(class) = dict.equivalent_names(name) else {
            return false;
        };
        let Some(&first) = class.first() else {
            return false;
        };
        *supplies
            .entry(first)
            .or_insert_with(|| class.iter().any(|same| self.entities.contains(same)))
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
            && self.done.is_empty()
            && self.noted.is_empty()
            && self.waiting.is_empty()
            && self.supplies.is_empty()
            && self.classes.is_empty()
            && self.widened.is_empty()
    }

    /// The memory held for what a title's completion remembers, in entries. A title that
    /// touches no form, or only forms that lack a piece nothing can supply, must leave it at
    /// zero. (`reach` and `classes` are not counted: they are as long as one form and as the
    /// title's own names.)
    #[cfg(test)]
    pub(in crate::normalize) fn held(&self) -> usize {
        self.entered.capacity()
            + self.queue.capacity()
            + self.done.capacity()
            + self.noted.capacity()
            + self.waiting.capacity()
            + self.supplies.capacity()
            + self.widened.capacity()
    }
}
