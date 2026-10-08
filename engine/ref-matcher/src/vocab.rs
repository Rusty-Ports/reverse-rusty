//! [`RefVocab`] — the reference's OWN plain-data vocabulary.
//!
//! This is deliberately a separate type from `reverse_rusty::vocab::Vocab`: the reference must not
//! depend on the engine. The differential harness builds BOTH a `Vocab` (for the engine) and a
//! `RefVocab` (for the reference) from one neutral description, so the same phrases / synonyms /
//! aliases / equivalences drive both sides while only the normalization *logic* differs.
//!
//! A vocabulary is a description, and declaring it does no analysis. What its declarations
//! amount to under its punctuation classes is asked for when a text is analyzed
//! ([`RefVocab::phrases_in_force`], [`RefVocab::synonym_for`]), so the order in which a
//! vocabulary is declared changes nothing (`docs/design/normalization.md` §2.1, "Phrases").

use crate::clean::{clean_tokens, PunctClass, PunctTable};

/// How a registered phrase treats its component tokens (`docs/design/normalization.md` §2.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PhraseMode {
    /// Consume the components — only the entity feature survives (manual multiword phrases).
    Collapse,
    /// Emit the entity feature AND keep the components (corpus-learned phrases, ADR-053).
    Additive,
    /// Asymmetric (ADR-061): collapse on the query side, additive on the title side.
    Alias,
}

/// A phrase as declared: a form, the feature it emits, and a mode.
#[derive(Clone, Debug)]
pub struct RefPhrase {
    /// The declared form (e.g. `"north star"`). Its tokens are this text cleaned and cut under
    /// the vocabulary's punctuation classes as they stand when a text is analyzed.
    pub form: String,
    /// The canonical entity feature emitted (e.g. `term:north_star`), used verbatim.
    pub feature: String,
    pub mode: PhraseMode,
}

/// A phrase in force: the tokens it occurs as, the feature it emits, and its mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phrase {
    /// One or more tokens (e.g. `["north","star"]`).
    pub tokens: Vec<String>,
    pub feature: String,
    pub mode: PhraseMode,
}

/// A single-token synonym: `token` -> a canonical feature
/// (e.g. `refurb` -> `term:refurbished`).
#[derive(Clone, Debug)]
pub struct RefSynonym {
    pub token: String,
    pub canonical: String,
}

/// The reference vocabulary. Construct with [`RefVocab::default_vocab`] then the builder methods,
/// or set the public fields directly from the harness.
#[derive(Clone, Debug)]
pub struct RefVocab {
    pub phrases: Vec<RefPhrase>,
    pub synonyms: Vec<RefSynonym>,
    /// A number immediately after one of these tokens is demoted to a generic term.
    /// Empty by default, making number typing position-insensitive.
    pub number_context: Vec<String>,
    /// Equivalence groups as **forms** (surface strings). During semantic analysis a positive
    /// feature requirement that resolves from one form is widened to alternatives over the
    /// complete group (ADR-054).
    pub equivalences: Vec<Vec<String>>,
    pub punct: PunctTable,
}

impl RefVocab {
    /// The empty default vocabulary: no phrases, synonyms, number contexts, or equivalences,
    /// plus the default punctuation table.
    #[must_use]
    pub fn default_vocab() -> Self {
        RefVocab {
            phrases: Vec::new(),
            synonyms: Vec::new(),
            number_context: Vec::new(),
            equivalences: Vec::new(),
            punct: PunctTable::new(),
        }
    }

    /// Declare a single-token synonym `token` -> `canonical`. The token is kept as declared.
    #[must_use]
    pub fn synonym(mut self, token: &str, canonical: &str) -> Self {
        self.synonyms.push(RefSynonym {
            token: token.to_string(),
            canonical: canonical.to_string(),
        });
        self
    }

    /// Declare a phrase by its form.
    #[must_use]
    pub fn phrase(mut self, form: &str, feature: &str, mode: PhraseMode) -> Self {
        self.phrases.push(RefPhrase {
            form: form.to_string(),
            feature: feature.to_string(),
            mode,
        });
        self
    }

    /// Register an equivalence group from its surface forms.
    #[must_use]
    pub fn equivalence(mut self, forms: &[&str]) -> Self {
        self.equivalences
            .push(forms.iter().map(|f| (*f).to_string()).collect());
        self
    }

    /// Reclassify a punctuation character (ADR-058), e.g. `'`/`-` as `Fold`.
    #[must_use]
    pub fn fold_punct(mut self, ch: char) -> Self {
        self.punct.set(ch, PunctClass::Fold);
        self
    }

    /// Set the number-context word list (ADR-069). Empty = parity mode.
    #[must_use]
    pub fn number_context(mut self, words: &[&str]) -> Self {
        self.number_context = words.iter().map(|w| w.to_ascii_lowercase()).collect();
        self
    }

    /// The phrases in force, in declaration order: each declared form cleaned and cut under
    /// the punctuation classes as they stand now. A form with no tokens is ignored, and of
    /// two forms with the same tokens the first stands (§2.1, "Phrases").
    #[must_use]
    pub fn phrases_in_force(&self) -> Vec<Phrase> {
        let mut in_force: Vec<Phrase> = Vec::new();
        for declared in &self.phrases {
            let tokens = clean_tokens(&declared.form, &self.punct);
            if tokens.is_empty() || in_force.iter().any(|phrase| phrase.tokens == tokens) {
                continue;
            }
            in_force.push(Phrase {
                tokens,
                feature: declared.feature.clone(),
                mode: declared.mode,
            });
        }
        in_force
    }

    /// The canonical feature name of the synonym for `token`, compared as declared. Of two
    /// synonyms for one token the first stands (§2.1, rule 3 of "Each remaining token").
    #[must_use]
    pub fn synonym_for(&self, token: &str) -> Option<&str> {
        self.synonyms
            .iter()
            .find(|synonym| synonym.token == token)
            .map(|synonym| synonym.canonical.as_str())
    }

    /// True if a phrase in force is in [`Alias`](PhraseMode::Alias) mode — a title then has a
    /// positive view `P(T)` wider than its canonical one (ADR-061; §2.1, "The two views of a
    /// title").
    #[must_use]
    pub fn has_alias(&self) -> bool {
        self.phrases_in_force()
            .iter()
            .any(|phrase| phrase.mode == PhraseMode::Alias)
    }
}

impl Default for RefVocab {
    fn default() -> Self {
        Self::default_vocab()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(vocab: &RefVocab) -> Vec<Vec<String>> {
        vocab
            .phrases_in_force()
            .into_iter()
            .map(|phrase| phrase.tokens)
            .collect()
    }

    #[test]
    fn a_form_is_cut_under_the_classes_as_they_finally_stand() {
        let before = RefVocab::default_vocab().fold_punct('-').phrase(
            "wi-fi router",
            "entity:wr",
            PhraseMode::Collapse,
        );
        let after = RefVocab::default_vocab()
            .phrase("wi-fi router", "entity:wr", PhraseMode::Collapse)
            .fold_punct('-');
        assert_eq!(tokens(&before), [["wifi", "router"]]);
        assert_eq!(tokens(&after), tokens(&before));

        let mut later = after;
        later.punct.set('-', PunctClass::Split);
        assert_eq!(tokens(&later), [["wi", "fi", "router"]]);
    }

    #[test]
    fn the_first_of_two_declarations_stands() {
        let vocab = RefVocab::default_vocab()
            .phrase("north star", "brand:first", PhraseMode::Collapse)
            .phrase("North, STAR", "brand:second", PhraseMode::Alias)
            .phrase("!!!", "ignored", PhraseMode::Alias)
            .synonym("pkg", "term:first")
            .synonym("pkg", "term:second");
        let in_force = vocab.phrases_in_force();
        assert_eq!(in_force.len(), 1);
        assert_eq!(in_force[0].feature, "brand:first");
        assert_eq!(in_force[0].mode, PhraseMode::Collapse);
        assert!(
            !vocab.has_alias(),
            "the ignored declarations were the only aliases"
        );
        assert_eq!(vocab.synonym_for("pkg"), Some("term:first"));
    }

    #[test]
    fn a_synonym_token_is_compared_as_declared() {
        let vocab = RefVocab::default_vocab().synonym("PKG", "term:package");
        assert_eq!(vocab.synonym_for("pkg"), None);
        assert_eq!(vocab.synonym_for("PKG"), Some("term:package"));
    }
}
