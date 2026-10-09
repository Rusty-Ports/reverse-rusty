//! A write the coordinator sends to several shards for one mutation, and what a shard hands
//! back when it has only been asked to start it (ADR-224).
//!
//! The coordinator applies a mutation to each shard it touches. Done one shard after the
//! other, a write costs a round trip for every remote shard. [`Shard::start_write`] lets a
//! shard that answers over a network send the request and return, so the coordinator starts
//! the write on every shard of a step and only then waits for the answers.

use super::{Extracted, PlacedWrite, ReplaceMode, ReplaceStatus, Shard, ShardError};
use crate::ownership::QueryPlacement;

/// One of the writes a mutation is made of, for one shard.
#[derive(Clone, Copy)]
pub(crate) enum FannedWrite<'a> {
    /// [`Shard::replace_placed`].
    Replace {
        write: &'a PlacedWrite<'a>,
        mode: ReplaceMode,
    },
    /// [`Shard::delete_by_logical_id`].
    Delete { logical: u64 },
    /// [`Shard::insert_extracted_with_placement`].
    Insert {
        ex: &'a Extracted,
        logical: u64,
        version: u32,
        text: &'a str,
        tags: &'a [(String, String)],
        placement: &'a QueryPlacement,
    },
}

/// What a shard answered to a [`FannedWrite`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Applied {
    Replaced(ReplaceStatus),
    /// How many live copies were tombstoned.
    Deleted(usize),
    /// The memtable-local id of the row, when the shard stored one.
    Inserted(Option<u32>),
}

impl FannedWrite<'_> {
    /// Apply the write to `shard` now, through the entry point it stands for.
    pub(crate) fn apply_to<S: Shard + ?Sized>(&self, shard: &S) -> Result<Applied, ShardError> {
        match *self {
            Self::Replace { write, mode } => {
                shard.replace_placed(write, mode).map(Applied::Replaced)
            }
            Self::Delete { logical } => shard.delete_by_logical_id(logical).map(Applied::Deleted),
            Self::Insert {
                ex,
                logical,
                version,
                text,
                tags,
                placement,
            } => shard
                .insert_extracted_with_placement(ex, logical, version, text, tags, placement)
                .map(Applied::Inserted),
        }
    }
}

/// A write that a shard has started. [`wait`](Self::wait) returns its answer.
///
/// A shard in this process has finished the write by the time it returns this. A remote
/// shard has sent the request, and `wait` blocks for the reply. It is not `Send`: the thread
/// that started a write is the one that waits for it.
pub(crate) struct Started<'a>(State<'a>);

enum State<'a> {
    Done(Result<Applied, ShardError>),
    Pending(Box<dyn FnOnce() -> Result<Applied, ShardError> + 'a>),
}

impl<'a> Started<'a> {
    pub(crate) fn done(answer: Result<Applied, ShardError>) -> Self {
        Self(State::Done(answer))
    }

    pub(crate) fn pending(finish: impl FnOnce() -> Result<Applied, ShardError> + 'a) -> Self {
        Self(State::Pending(Box::new(finish)))
    }

    /// Whether the write has finished already, so that waiting costs nothing.
    pub(crate) fn is_done(&self) -> bool {
        matches!(self.0, State::Done(_))
    }

    pub(crate) fn wait(self) -> Result<Applied, ShardError> {
        match self.0 {
            State::Done(answer) => answer,
            State::Pending(finish) => finish(),
        }
    }
}
