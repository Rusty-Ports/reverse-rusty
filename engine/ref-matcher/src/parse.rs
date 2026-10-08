//! Reading a query string into clauses: "Parsing rules" in `docs/reference/dsl.md`.
//!
//! Written from that text alone (ADR-220).

pub use crate::tables::{MAX_ANY_OF_SIZE, MAX_CLAUSES, MAX_QUERY_LENGTH};

/// What one clause holds, as text. Nothing here is analyzed yet.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Atom {
    /// A bare term, as written.
    Term(String),
    /// The content of a quoted clause, trimmed.
    Phrase(String),
    /// The members of a group: each trimmed, none empty.
    AnyOf(Vec<String>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Clause {
    pub negated: bool,
    pub atom: Atom,
}

/// A query's clauses, in the order they were written.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ast {
    pub clauses: Vec<Clause>,
}

/// Why a query string is rejected.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParseError {
    /// Longer than [`MAX_QUERY_LENGTH`] bytes.
    QueryTooLong,
    /// A `-` that is not followed at once by what it negates.
    TrailingDash,
    /// A group with no `)`.
    UnclosedGroup,
    /// A group with no member left.
    EmptyAnyOfGroup,
    /// A group with more than [`MAX_ANY_OF_SIZE`] members.
    AnyOfGroupTooLarge,
    /// A quoted clause with no closing `"`.
    UnclosedQuote,
    /// More than [`MAX_CLAUSES`] clauses.
    TooManyClauses,
}

/// Read a query string.
pub fn parse(input: &str) -> Result<Ast, ParseError> {
    if input.len() > MAX_QUERY_LENGTH {
        return Err(ParseError::QueryTooLong);
    }
    let mut rest = input;
    let mut clauses = Vec::new();
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            return Ok(Ast { clauses });
        }
        let negated = rest.starts_with('-');
        if negated {
            rest = &rest[1..];
            if rest.is_empty() || rest.starts_with(char::is_whitespace) {
                return Err(ParseError::TrailingDash);
            }
        }
        let atom = if let Some(content) = rest.strip_prefix('(') {
            let end = content.find(')').ok_or(ParseError::UnclosedGroup)?;
            let body = &content[..end];
            let members: Vec<_> = body
                .split(',')
                .map(str::trim)
                .filter(|member| !member.is_empty())
                .map(|member| {
                    member
                        .chars()
                        .map(|ch| if ch.is_whitespace() { ' ' } else { ch })
                        .collect()
                })
                .collect();
            if members.is_empty() {
                return Err(ParseError::EmptyAnyOfGroup);
            }
            if members.len() > MAX_ANY_OF_SIZE {
                return Err(ParseError::AnyOfGroupTooLarge);
            }
            rest = &content[end + 1..];
            Atom::AnyOf(members)
        } else if let Some(content) = rest.strip_prefix('"') {
            let end = content.find('"').ok_or(ParseError::UnclosedQuote)?;
            rest = &content[end + 1..];
            Atom::Phrase(content[..end].trim().to_owned())
        } else {
            let end = rest
                .find(|ch: char| ch.is_whitespace() || ch == '(' || ch == '"')
                .unwrap_or(rest.len());
            let word = rest[..end].to_owned();
            rest = &rest[end..];
            Atom::Term(word)
        };
        clauses.push(Clause { negated, atom });
        if clauses.len() > MAX_CLAUSES {
            return Err(ParseError::TooManyClauses);
        }
    }
}

#[cfg(test)]
mod tests;
