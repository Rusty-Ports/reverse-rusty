//! How long each part of a rebuild took.
//!
//! A resize and a vocabulary change both rebuild the cluster: read the live corpus, extract
//! and place every query again, build new shards beside the old ones, publish them, and
//! commit. The engine keeps the times of the last one, so that an operator, and the
//! benchmark that sizes a rebuild, can see where it went.

use std::time::{Duration, Instant};

/// The parts of one rebuild, timed, in the order they run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RebuildTimings {
    /// Live queries rebuilt.
    pub queries: usize,
    /// Reading the live corpus out of the shards being replaced.
    pub gather: Duration,
    /// Parsing and extracting every query again. A vocabulary change also builds the
    /// dictionary here.
    pub extract: Duration,
    /// Placing every query under the new ring.
    pub place: Duration,
    /// Building the new shards and loading them.
    pub build: Duration,
    /// Publishing the new layout. Searches run on the old one until this ends.
    pub publish: Duration,
    /// What follows the publication, with writes still waiting: the control state, readers
    /// of the old layout finishing, and the checkpoint. Zero until the rebuild has committed,
    /// and for a cluster with no data directory, which has no checkpoint.
    pub commit: Duration,
}

impl RebuildTimings {
    /// From the first read of the old shards to the end of the commit. Writes wait for all
    /// of it.
    #[must_use]
    pub fn total(&self) -> Duration {
        self.gather + self.extract + self.place + self.build + self.publish + self.commit
    }
}

/// A stopwatch that gives the time since it was last read.
pub(in crate::cluster::coordinator) struct Lap(Instant);

impl Lap {
    pub(in crate::cluster::coordinator) fn start() -> Self {
        Self(Instant::now())
    }

    pub(in crate::cluster::coordinator) fn lap(&mut self) -> Duration {
        let now = Instant::now();
        let since = now.duration_since(self.0);
        self.0 = now;
        since
    }
}
