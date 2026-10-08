//! How far the log has been sealed into segments, and the one way a memtable is replaced
//! (ADR-223).
//!
//! The memtable holds the rows of the records appended since it was last replaced by an
//! empty one, and nothing else. So one sequence number says which records of the log are in
//! segments and which are in the memtable: the last record appended when the memtable was
//! last sealed. A commit records it, and recovery skips every record at or below it and
//! rebuilds the memtable from the rest, in the order they were written. Every position in
//! the rebuilt memtable is then the one it had.
//!
//! Only a seal moves it. A merge or a bulk load commits without one. Every seal goes through
//! [`Engine::take_memtable`], so the position cannot be forgotten by a new one.

use super::{Arc, Engine, Segment};

/// A memtable taken out of the engine to be sealed, with what puts it back.
pub(in crate::segment) struct TakenMemtable {
    pub(in crate::segment) rows: Arc<Segment>,
    sealed_through_before: Option<u64>,
}

impl Engine {
    /// Put an empty memtable in place of the current one and hand that one to the caller,
    /// who seals its rows into a segment. From here on every record the log holds is out of
    /// the memtable, and the next commit says so: it names every sealed segment, writing one
    /// that is still in memory to disk first (ADR-190).
    pub(in crate::segment) fn take_memtable(&mut self) -> TakenMemtable {
        let mut fresh = Segment::new();
        fresh.vocab_epoch = self.vocab_epoch;
        let rows = std::mem::replace(&mut self.memtable, Arc::new(fresh));
        let sealed_through_before = self
            .sealed_through
            .replace(self.wal.as_ref().map_or(0, crate::wal::Wal::last_seq));
        TakenMemtable {
            rows,
            sealed_through_before,
        }
    }

    /// Undo [`take_memtable`](Self::take_memtable): the rows were not sealed, and no record
    /// was appended since they were taken.
    pub(in crate::segment) fn put_memtable_back(&mut self, taken: TakenMemtable) {
        self.memtable = taken.rows;
        self.sealed_through = taken.sealed_through_before;
    }
}
