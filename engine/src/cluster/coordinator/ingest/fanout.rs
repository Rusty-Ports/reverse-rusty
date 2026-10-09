//! One step of a mutation, under way on every shard it touches at once (ADR-224).

use crate::cluster::shard::{Applied, FannedWrite, Shard, ShardError};

/// Start `write` on each of `targets`, then wait for every answer, and return the answers in
/// the order of `targets`.
///
/// A shard in this process applies the write when it is started, so for an in-process
/// cluster this is the loop it replaces. A remote shard sends its request and returns, so the
/// step costs one round trip, not one for each shard.
///
/// The writes are started in ascending shard position whatever order `targets` is in. A
/// replicated shard holds its lock from the start of a write until its answer is waited for,
/// so one mutation holds several of those locks at once, and every mutation taking them in
/// the same order is what keeps two of them from waiting for each other.
///
/// Nothing here orders one step against another: a caller that needs a step finished on
/// every shard before the next one begins (install, then remove elsewhere) calls this twice.
pub(in crate::cluster::coordinator) fn on_each(
    shards: &[Box<dyn Shard>],
    targets: &[usize],
    write: &FannedWrite<'_>,
) -> Vec<(usize, Result<Applied, ShardError>)> {
    // A shard whose backing can be exchanged (a handoff) names the backing in place now.
    // It is kept until the answer has been waited for.
    let backing: Vec<_> = targets.iter().map(|&s| shards[s].write_target()).collect();
    let mut ascending: Vec<usize> = (0..targets.len()).collect();
    ascending.sort_unstable_by_key(|&at| targets[at]);
    let mut started: Vec<_> = ascending
        .into_iter()
        .map(|at| {
            let shard: &dyn Shard = match &backing[at] {
                Some(backing) => backing.as_ref().as_ref(),
                None => shards[targets[at]].as_ref(),
            };
            (at, shard.start_write(*write))
        })
        .collect();
    // Back into the order asked for, which is the order the answers are read in.
    started.sort_unstable_by_key(|(at, _)| *at);
    started
        .into_iter()
        .map(|(at, started)| (targets[at], started.wait()))
        .collect()
}

/// The answer to a replace, a delete or an insert, when the shard answered the write it was
/// sent. A shard that answers another one has broken the seam, and says so loudly.
pub(super) fn answered(shard: usize, answer: Applied, sent: &FannedWrite<'_>) -> ShardError {
    let sent = match sent {
        FannedWrite::Replace { .. } => "replace",
        FannedWrite::Delete { .. } => "delete",
        FannedWrite::Insert { .. } => "insert",
    };
    ShardError::Protocol(format!("shard {shard} answered a {sent} with {answer:?}"))
}
