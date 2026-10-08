//! The record of the log losses a store has accepted (ADR-216).
//!
//! A store whose commit record says a log existed refuses to open when the log is gone
//! (ADR-213): the writes acknowledged since the last flush or checkpoint were in it. An
//! operator with no backup can accept that loss and start with what the store still holds.
//!
//! Accepting is one explicit step, and it is written down before anything is changed:
//!
//! 1. The refusal names a **token** for this loss: the log, what the commit record says,
//!    and how many losses this directory has accepted before. Nothing else is accepted by
//!    it, so a setting that is left in place after the start accepts no later loss.
//! 2. A start that is given that token appends the loss to [`ACCEPTED_LOG_LOSSES_FILE`] in
//!    the data directory, as *pending*, and makes that durable.
//! 3. Only then is the empty log put in place, and the entry marked *applied*.
//!
//! Every later start can read the file, whatever happened in between: a start that accepts
//! the loss and then fails for another reason has still left the record, and the start
//! after it finds a log, opens, and marks the entry applied.

use std::io::{self, Write};
use std::path::Path;

/// The file, in a store's data directory, that lists the log losses it has accepted.
pub const ACCEPTED_LOG_LOSSES_FILE: &str = "log_loss.accepted";

const VERSION: &str = "v1";

/// One log loss a store has accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedLogLoss {
    /// The lost log's file name (`wal.log`, `cluster.log`).
    pub log: String,
    /// What the store's commit record said when the loss was accepted. It identifies the
    /// state the store was started from; it is not a position up to which writes survived.
    pub commit_record: String,
    /// When it was accepted, in seconds since the Unix epoch.
    pub accepted_at: u64,
    /// Whether the empty log has been put in place. An entry that is not applied belongs to
    /// a start that stopped between recording the loss and replacing the log.
    pub applied: bool,
}

/// The log losses the store in `dir` has accepted, oldest first. A directory with no such
/// file has accepted none. A file that cannot be read or understood is an error: it is
/// evidence, and it is not ignored.
pub fn accepted_log_losses(dir: &Path) -> io::Result<Vec<AcceptedLogLoss>> {
    let path = dir.join(ACCEPTED_LOG_LOSSES_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(unreadable(&path, &e.to_string())),
    };
    text.lines()
        .map(|line| parse(line).ok_or_else(|| unreadable(&path, &format!("line {line:?}"))))
        .collect()
}

fn unreadable(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "the record of accepted log losses, {}, cannot be read ({why}). It is evidence \
             of data loss and is not ignored: repair it or move it aside.",
            path.display()
        ),
    )
}

fn parse(line: &str) -> Option<AcceptedLogLoss> {
    let mut fields = line.split('\t');
    if fields.next()? != VERSION {
        return None;
    }
    let accepted_at = fields.next()?.parse().ok()?;
    let applied = match fields.next()? {
        "applied" => true,
        "pending" => false,
        _ => return None,
    };
    let log = fields.next()?.to_string();
    let commit_record = fields.next()?.to_string();
    fields.next().is_none().then_some(AcceptedLogLoss {
        log,
        commit_record,
        accepted_at,
        applied,
    })
}

/// Replace the file with `losses`, and have it on disk before this returns.
fn write(dir: &Path, losses: &[AcceptedLogLoss]) -> io::Result<()> {
    let path = dir.join(ACCEPTED_LOG_LOSSES_FILE);
    let tmp = dir.join(format!("{ACCEPTED_LOG_LOSSES_FILE}.tmp"));
    let mut file = std::fs::File::create(&tmp)?;
    for loss in losses {
        writeln!(
            file,
            "{VERSION}\t{}\t{}\t{}\t{}",
            loss.accepted_at,
            if loss.applied { "applied" } else { "pending" },
            loss.log,
            loss.commit_record
        )?;
    }
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, &path)?;
    std::fs::File::open(dir)?.sync_all()
}

/// What [`accept`] decided about a log that is missing.
pub(crate) enum Decision {
    /// The loss is recorded, durably, as pending. The caller now puts the empty log in
    /// place and calls [`settle`].
    Accepted,
    /// Not accepted. `token` is what accepting this loss takes; `given` is the token the
    /// store was started with, when it names some other loss.
    Refused {
        token: String,
        given: Option<String>,
    },
}

/// The token that accepts the loss of `log` under `commit_record` in a directory that has
/// already had `applied` losses accepted and carried out.
fn token(log: &str, commit_record: &str, applied: usize) -> String {
    format!("{log}:{commit_record}:{}", applied + 1)
}

/// Decide about a log that its owner's commit record says existed and that is not there.
///
/// `given` is the token the store was started with, if any. When it is the token of this
/// loss, the loss is recorded in `dir` before this returns, so that the record exists
/// before the caller changes anything. A start that stopped after recording and before
/// replacing the log left a pending entry; the same token accepts it again and adds
/// nothing.
pub(crate) fn accept(
    dir: &Path,
    log: &str,
    commit_record: &str,
    given: Option<&str>,
) -> io::Result<Decision> {
    let mut losses = accepted_log_losses(dir)?;
    let applied = losses.iter().filter(|loss| loss.applied).count();
    let token = token(log, commit_record, applied);
    if given != Some(token.as_str()) {
        return Ok(Decision::Refused {
            token,
            given: given.map(str::to_string),
        });
    }
    let unfinished = |loss: &AcceptedLogLoss| {
        !loss.applied && loss.log == log && loss.commit_record == commit_record
    };
    if losses.iter().any(unfinished) {
        return Ok(Decision::Accepted);
    }
    losses.push(AcceptedLogLoss {
        log: log.to_string(),
        commit_record: commit_record.to_string(),
        accepted_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs()),
        applied: false,
    });
    write(dir, &losses)?;
    Ok(Decision::Accepted)
}

/// `log` is in place in `dir`: mark every pending loss of it applied. Called by every open
/// that finds or creates its log, so an acceptance that was interrupted after the log was
/// replaced is completed by the next start, with or without the token. Does nothing, and
/// writes nothing, for a store that has no pending entry.
pub(crate) fn settle(dir: &Path, log: &str) -> io::Result<()> {
    let mut losses = accepted_log_losses(dir)?;
    let mut changed = false;
    for loss in losses
        .iter_mut()
        .filter(|loss| !loss.applied && loss.log == log)
    {
        loss.applied = true;
        changed = true;
    }
    if changed {
        write(dir, &losses)?;
    }
    Ok(())
}

/// What a refusal tells the operator about accepting the loss.
pub(crate) fn how_to_accept(decision_token: &str, given: Option<&str>) -> String {
    let other = given.map_or_else(String::new, |given| {
        format!(" (It was started with {given:?}, which names a different loss.)")
    });
    format!(
        "To start without those writes instead, start once with accept_lost_log set to \
         {decision_token:?} (the server's --accept-lost-log). It accepts this loss and no \
         other, and the loss is recorded in {ACCEPTED_LOG_LOSSES_FILE}.{other}"
    )
}
