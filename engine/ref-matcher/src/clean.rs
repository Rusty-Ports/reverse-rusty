//! Cleaning and tokens: stages 1 and 2 of `docs/design/normalization.md` §2.1.
//!
//! Written from that text alone (ADR-220).

use crate::tables::{fold_diacritic, KEEP, MARKERS};

/// What cleaning does with a character that is not an ASCII letter or digit after folding.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PunctClass {
    Split,
    Fold,
    Keep,
    Marker,
}

/// The punctuation classes of one vocabulary: the specification's defaults, and the single
/// characters the vocabulary classes otherwise.
#[derive(Clone, Debug, Default)]
pub struct PunctTable {
    overrides: Vec<(char, PunctClass)>,
}

impl PunctTable {
    /// The default classes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Give one character a class.
    pub fn set(&mut self, ch: char, class: PunctClass) {
        if let Some(entry) = self.overrides.iter_mut().find(|entry| entry.0 == ch) {
            entry.1 = class;
        } else {
            self.overrides.push((ch, class));
        }
    }

    /// The class of a character.
    #[must_use]
    pub fn class_of(&self, ch: char) -> PunctClass {
        if let Some((_, class)) = self.overrides.iter().find(|entry| entry.0 == ch) {
            *class
        } else if KEEP.contains(&ch) {
            PunctClass::Keep
        } else if MARKERS.contains(&ch) {
            PunctClass::Marker
        } else {
            PunctClass::Split
        }
    }
}

/// The tokens of `text`, cleaned under `punct`.
#[must_use]
pub fn clean_tokens(text: &str, punct: &PunctTable) -> Vec<String> {
    cleaned_text(text, punct)
        .split(' ')
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect()
}

fn cleaned_text(text: &str, punct: &PunctTable) -> String {
    let mut cleaned = String::new();
    for ch in text.chars().map(fold_diacritic) {
        if ch.is_ascii_alphanumeric() {
            cleaned.push(ch.to_ascii_lowercase());
            continue;
        }
        match punct.class_of(ch) {
            PunctClass::Fold => {}
            PunctClass::Keep if ch != ' ' => cleaned.push(ch),
            PunctClass::Split | PunctClass::Keep => separator(&mut cleaned),
            PunctClass::Marker => {
                separator(&mut cleaned);
                if ch != ' ' {
                    cleaned.push(ch);
                    separator(&mut cleaned);
                }
            }
        }
    }
    cleaned
}

fn separator(cleaned: &mut String) {
    if !cleaned.is_empty() && !cleaned.ends_with(' ') {
        cleaned.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separators_are_never_first_or_repeated_even_around_markers() {
        let punct = PunctTable::new();
        assert_eq!(cleaned_text(" ,\t\n", &punct), "");
        assert_eq!(cleaned_text(" , #  / a,, - b ", &punct), "# / a b ");
        for class in [PunctClass::Keep, PunctClass::Marker] {
            let mut punct = PunctTable::new();
            punct.set(' ', class);
            assert_eq!(cleaned_text("  a  #  b  ", &punct), "a # b ");
        }
    }

    #[test]
    fn the_complete_fold_table_and_ascii_case() {
        let rows = [
            ('a', "áàâäãåāąÁÀÂÄÃÅ"),
            ('e', "éèêëēėęÉÈÊË"),
            ('i', "íìîïīįÍÌÎÏ"),
            ('o', "óòôöõøōÓÒÔÖÕ"),
            ('u', "úùûüūÚÙÛÜ"),
            ('n', "ñńÑ"),
            ('c', "çćčÇĆČ"),
            ('s', "šśŠŚ"),
            ('z', "žźżŽŹŻ"),
            ('y', "ýÿÝ"),
            ('l', "łŁ"),
        ];
        for (letter, forms) in rows {
            for ch in forms.chars() {
                assert_eq!(
                    clean_tokens(&ch.to_string(), &PunctTable::new()),
                    [letter.to_string()]
                );
            }
        }
        assert_eq!(
            clean_tokens("Café AZ09", &PunctTable::new()),
            ["cafe", "az09"]
        );
        // Unlisted uppercase diacritics and decomposed combining marks do not fold.
        assert_eq!(
            clean_tokens("xĀy x中y x🙂y e\u{301}x", &PunctTable::new()),
            ["x", "y", "x", "y", "x", "y", "e", "x"]
        );
    }

    #[test]
    fn default_classes_and_merged_separators() {
        let punct = PunctTable::new();
        assert_eq!(punct.class_of('.'), PunctClass::Keep);
        for ch in ['#', '/'] {
            assert_eq!(punct.class_of(ch), PunctClass::Marker);
        }
        for ch in [' ', '\t', '-', '中'] {
            assert_eq!(punct.class_of(ch), PunctClass::Split);
        }
        for text in [
            "north, star",
            "north - star",
            "north star",
            "  north\t\nstar  ",
        ] {
            assert_eq!(clean_tokens(text, &punct), ["north", "star"]);
        }
        assert_eq!(
            clean_tokens(" #1999///9.5... ", &punct),
            ["#", "1999", "/", "/", "/", "9.5..."]
        );
        for text in ["", " ,\t\n🙂 "] {
            assert!(clean_tokens(text, &punct).is_empty());
        }
    }

    #[test]
    fn overrides_apply_after_folding_and_can_be_replaced() {
        let mut punct = PunctTable::new();
        for ch in ['\'', '-', '’'] {
            punct.set(ch, PunctClass::Fold);
        }
        for text in ["O'Brien", "O-Brien", "OBrien", "O’Brien"] {
            assert_eq!(clean_tokens(text, &punct), ["obrien"]);
        }
        punct.set('-', PunctClass::Keep);
        punct.set('@', PunctClass::Marker);
        punct.set('.', PunctClass::Split);
        punct.set('中', PunctClass::Keep);
        punct.set('A', PunctClass::Split);
        punct.set('é', PunctClass::Keep);
        punct.set('e', PunctClass::Fold);
        assert_eq!(clean_tokens("A-é@中.5", &punct), ["a-e", "@", "中", "5"]);
        punct.set('Ā', PunctClass::Keep);
        punct.set('🙂', PunctClass::Fold);
        assert_eq!(clean_tokens("xĀY x🙂y", &punct), ["xĀy", "xy"]);
    }

    #[test]
    fn only_literal_spaces_cut_the_cleaned_buffer() {
        for class in [PunctClass::Split, PunctClass::Keep, PunctClass::Marker] {
            let mut punct = PunctTable::new();
            punct.set(' ', class);
            assert_eq!(clean_tokens("  a  b  ", &punct), ["a", "b"]);
        }
        let mut punct = PunctTable::new();
        punct.set(' ', PunctClass::Fold);
        assert_eq!(clean_tokens(" a b ", &punct), ["ab"]);
        assert_eq!(clean_tokens(" a # b ", &punct), ["a", "#", "b"]);
        punct.set('\t', PunctClass::Keep);
        assert_eq!(clean_tokens("a\tb", &punct), ["a\tb"]);
        punct.set('\t', PunctClass::Marker);
        assert_eq!(clean_tokens("a\tb", &punct), ["a", "\t", "b"]);
    }
}
