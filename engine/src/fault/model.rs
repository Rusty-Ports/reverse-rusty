//! What a store may hold after writes that were acknowledged and writes that failed: the
//! model a crash matrix checks a reopened store against (ADR-221).

use std::collections::{BTreeMap, BTreeSet};

/// Every text a matrix gives a query. A query is plain words, so a title holds it when it
/// has all of its words.
pub(crate) const TEXTS: [&str; 7] = [
    "package adapter",
    "vintage lamp",
    "brass compass",
    "package charger",
    "copper kettle",
    "brass sextant",
    "silver spoon",
];

fn holds(title: &str, query: &str) -> bool {
    query
        .split_whitespace()
        .all(|word| title.split_whitespace().any(|have| have == word))
}

/// What a store may hold for each id. A state is the text the id matches, or `None` for no
/// row. After an acknowledged write there is one state. After a write that failed there are
/// two or more: the state before it and the state it asked for, because a failed write may
/// or may not have been applied. Losing the row is not among them unless a remove was tried.
#[derive(Clone, Default)]
pub(crate) struct Acknowledged(BTreeMap<u64, Vec<Option<String>>>);

impl Acknowledged {
    pub(crate) fn of(corpus: &[(u64, String)]) -> Self {
        Self(
            corpus
                .iter()
                .map(|(id, text)| (*id, vec![Some(text.clone())]))
                .collect(),
        )
    }

    /// Record a write to `id` that asked for `asked` (`None`: a remove) and its outcome.
    pub(crate) fn note<T, E>(&mut self, id: u64, outcome: &Result<T, E>, asked: Option<&str>) {
        let asked = asked.map(str::to_string);
        let allowed = self.0.entry(id).or_insert_with(|| vec![None]);
        if outcome.is_ok() {
            *allowed = vec![asked];
        } else if !allowed.contains(&asked) {
            allowed.push(asked);
        }
    }

    /// The store, which answers a title with `matched`, holds for every id one of the states
    /// allowed for it. Returns those states, one for each id: what the store holds, and so
    /// what it must go on holding.
    pub(crate) fn settle(
        &self,
        matched: impl Fn(&str) -> Result<Vec<u64>, String>,
    ) -> Result<Self, String> {
        let mut answers = Vec::new();
        for title in TEXTS {
            let ids: BTreeSet<u64> = matched(title)
                .map_err(|e| format!("matching {title:?}: {e}"))?
                .into_iter()
                .collect();
            answers.push(ids);
        }
        let mut held = BTreeMap::new();
        for (id, allowed) in &self.0 {
            let is_held = |state: &Option<String>| {
                TEXTS.iter().zip(&answers).all(|(title, ids)| {
                    ids.contains(id) == state.as_deref().is_some_and(|query| holds(title, query))
                })
            };
            let Some(state) = allowed.iter().find(|state| is_held(state)) else {
                let titles: Vec<&str> = TEXTS
                    .iter()
                    .zip(&answers)
                    .filter(|(_, ids)| ids.contains(id))
                    .map(|(title, _)| *title)
                    .collect();
                return Err(format!(
                    "id {id} matches the titles {titles:?}, which is none of its allowed \
                     states {allowed:?}"
                ));
            };
            held.insert(*id, vec![state.clone()]);
        }
        Ok(Self(held))
    }

    /// How many ids hold a row, in a model that has been settled.
    pub(crate) fn live(&self) -> usize {
        self.0
            .values()
            .filter(|allowed| matches!(allowed.as_slice(), [Some(_)]))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store<'a>(rows: &'a [(u64, &'a str)]) -> impl Fn(&str) -> Result<Vec<u64>, String> + 'a {
        move |title| {
            Ok(rows
                .iter()
                .filter(|(_, query)| holds(title, query))
                .map(|(id, _)| *id)
                .collect())
        }
    }

    #[test]
    fn an_acknowledged_write_allows_one_state() {
        let mut model = Acknowledged::of(&[(1, TEXTS[0].to_string())]);
        model.note(1, &Ok::<(), ()>(()), Some(TEXTS[3]));
        assert!(model.settle(store(&[(1, TEXTS[3])])).is_ok());
        assert!(
            model.settle(store(&[(1, TEXTS[0])])).is_err(),
            "the old text"
        );
        assert!(model.settle(store(&[])).is_err(), "the row is gone");
    }

    #[test]
    fn a_failed_write_allows_the_state_before_it_or_the_one_it_asked_for_and_no_other() {
        let mut model = Acknowledged::of(&[(1, TEXTS[0].to_string())]);
        model.note(1, &Err::<(), ()>(()), Some(TEXTS[3]));
        assert!(model.settle(store(&[(1, TEXTS[0])])).is_ok());
        assert!(model.settle(store(&[(1, TEXTS[3])])).is_ok());
        assert!(
            model.settle(store(&[])).is_err(),
            "a failed upsert is not a remove"
        );
        assert!(model.settle(store(&[(1, TEXTS[1])])).is_err());

        let mut never_added = Acknowledged::default();
        never_added.note(7, &Err::<(), ()>(()), Some(TEXTS[4]));
        assert!(never_added.settle(store(&[])).is_ok());
        assert!(never_added.settle(store(&[(7, TEXTS[4])])).is_ok());
    }

    #[test]
    fn what_a_store_holds_is_what_it_must_go_on_holding() {
        let mut model = Acknowledged::of(&[(1, TEXTS[0].to_string())]);
        model.note(1, &Err::<(), ()>(()), None);
        let settled = model.settle(store(&[])).expect("removed is allowed");
        assert!(settled.settle(store(&[(1, TEXTS[0])])).is_err());
    }
}
