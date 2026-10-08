//! The finite tables and limits of the front end, copied from the specification.
//!
//! Everything in this file is data that the specification enumerates
//! (`docs/design/normalization.md` §2.1, `docs/reference/dsl.md`): which characters fold to
//! which letter, which characters have a punctuation class other than `split`, which numbers
//! are years, and how long a query may be. A table has one right answer, so copying it adds no
//! shared *logic*, and it is the only kind of thing in this crate that is allowed to be the same
//! as the engine's (ADR-087, ADR-219). A reviewer checks this file against the specification
//! line by line. No other module of this crate is derived from engine code, except the ones
//! the crate documentation lists as ported.

/// A query may be this many bytes.
pub const MAX_QUERY_LENGTH: usize = 10_240;
/// A query may have this many clauses.
pub const MAX_CLAUSES: usize = 256;
/// An any-of group may have this many members.
pub const MAX_ANY_OF_SIZE: usize = 64;

/// A number with exactly four digits in this range is a year.
pub const YEARS: std::ops::RangeInclusive<u32> = 1900..=2099;

/// The characters whose default punctuation class is `keep`.
pub const KEEP: &[char] = &['.'];
/// The characters whose default punctuation class is `marker`. A marker is a token of its
/// own, emits nothing, and changes how a number next to it is typed.
pub const MARKERS: &[char] = &['#', '/'];

/// The diacritic fold: the letter a character is read as, or the character itself.
#[must_use]
pub fn fold_diacritic(ch: char) -> char {
    match ch {
        'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' | 'ą' | 'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' => {
            'a'
        }
        'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ė' | 'ę' | 'É' | 'È' | 'Ê' | 'Ë' => 'e',
        'í' | 'ì' | 'î' | 'ï' | 'ī' | 'į' | 'Í' | 'Ì' | 'Î' | 'Ï' => 'i',
        'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' | 'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' => 'o',
        'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'Ú' | 'Ù' | 'Û' | 'Ü' => 'u',
        'ñ' | 'ń' | 'Ñ' => 'n',
        'ç' | 'ć' | 'č' | 'Ç' | 'Ć' | 'Č' => 'c',
        'š' | 'ś' | 'Š' | 'Ś' => 's',
        'ž' | 'ź' | 'ż' | 'Ž' | 'Ź' | 'Ż' => 'z',
        'ý' | 'ÿ' | 'Ý' => 'y',
        'ł' | 'Ł' => 'l',
        other => other,
    }
}
