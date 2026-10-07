//! Durable on-disk persistence for the openraft control-plane backend
//! (`control_raft.rs`), clustering build-path step 5e / ADR-041.
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane), §10 step 5e.
//!
//! ADR-038 shipped the openraft backend with an **in-memory** log/state store — enough to
//! prove consensus convergence, but a manager node lost everything on restart. This module is
//! the byte-level durable substrate that lets [`RaftControlPlane`](super::control_raft) survive a
//! restart and rejoin the quorum. Two shapes:
//!
//! - a **CRC-framed append-only record log** ([`append_record`] / [`read_records`] /
//!   [`rewrite_records`]) — the Raft log entries, reusing the same forward-scan / torn-tail
//!   recovery shape as [`clog`](super::clog) / `wal.rs` (a crash mid-append drops the last partial
//!   frame, never corrupts an acknowledged prefix); and
//! - **atomic single-value files** ([`write_value`] / [`read_value`]) — the Raft hard state that
//!   must survive a crash whole: the **vote** (election safety), the **committed** log id (so a
//!   restart re-applies committed-but-un-snapshotted entries), the **last-purged** log id, and the
//!   **state-machine snapshot** (so the log can be compacted + the SM rebuilt). Written tmp +
//!   fsync + rename + parent-fsync, so a reader never sees a torn value.
//!
//! What openraft requires durable (0.9.24, from its storage FAQ) and where it lives:
//!
//! - `save_vote` MUST be durable before returning → [`RaftPaths::vote`] (fsync each write).
//! - `append` MUST be durable before the flush callback → the record log (fsync if asked).
//! - `save_committed` makes a restart re-apply `(snapshot.last, committed]` → [`RaftPaths::committed`].
//! - a snapshot lets `purge` compact the log + rebuilds the SM on restart → [`RaftPaths::snapshot`].
//!
//! The state machine itself is NOT persisted per-apply — it is rebuilt from the snapshot + the
//! durable log replayed up to `committed`, exactly as openraft prescribes.
//!
//! All of this is `distributed`-gated (openraft only); serialization is `serde_json` (the same
//! codec the gRPC `RaftNetwork` already uses — the control plane is low-rate, so JSON overhead
//! is irrelevant and the files stay debuggable). CRC via the core [`crate::storage::crc32`].

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage::framed_log::{
    header_was_interrupted, publish_empty_log, repair_tail, scan_records, write_frame, LogAppender,
};

/// Header of the record log: magic + format version. V4 is a one-way compatibility fence: an old
/// binary knows only `RRRL` (or one of the unsupported move prototypes) and therefore rejects a log
/// once it may contain source-fence-bound durable move commands.
const LOG_MAGIC_V1: [u8; 4] = *b"RRRL"; // Reverse-Rusty Raft Log
const LOG_MAGIC_V4: [u8; 4] = *b"RRL4";
/// V5 adds remote-resize commands (ADR-180); an RRL4-only binary rejects it.
const LOG_MAGIC_V5: [u8; 4] = *b"RRL5";
const LOG_HEADER: usize = 8;

/// Ordered oldest to newest: a log only ever upgrades to a later format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum LogFormat {
    Legacy,
    DurableMoves,
    DurableResize,
}

impl LogFormat {
    fn header(self) -> ([u8; 4], u32) {
        match self {
            Self::Legacy => (LOG_MAGIC_V1, 1),
            Self::DurableMoves => (LOG_MAGIC_V4, 4),
            Self::DurableResize => (LOG_MAGIC_V5, 5),
        }
    }

    /// The eight bytes that open a log of this format.
    fn header_bytes(self) -> [u8; LOG_HEADER] {
        let (magic, version) = self.header();
        let mut header = [0u8; LOG_HEADER];
        header[..4].copy_from_slice(&magic);
        header[4..].copy_from_slice(&version.to_le_bytes());
        header
    }
}

fn parse_log_format(data: &[u8]) -> io::Result<LogFormat> {
    if data.len() < LOG_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft log: header is truncated",
        ));
    }
    let version =
        u32::from_le_bytes(data[4..8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "raft log: invalid version")
        })?);
    match (data[0..4].try_into().ok(), version) {
        (Some(LOG_MAGIC_V1), 1) => Ok(LogFormat::Legacy),
        (Some(LOG_MAGIC_V4), 4) => Ok(LogFormat::DurableMoves),
        (Some(LOG_MAGIC_V5), 5) => Ok(LogFormat::DurableResize),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft log: unsupported magic/version {version}"),
        )),
    }
}

/// The set of files the durable control-plane store keeps under one manager node's raft dir.
pub(super) struct RaftPaths {
    dir: PathBuf,
}

impl RaftPaths {
    pub(super) fn new(dir: PathBuf) -> Self {
        RaftPaths { dir }
    }
    /// The CRC-framed Raft log (entries).
    pub(super) fn log(&self) -> PathBuf {
        self.dir.join("raft-log.bin")
    }
    /// The persisted hard-state vote (election safety).
    pub(super) fn vote(&self) -> PathBuf {
        self.dir.join("raft-vote.json")
    }
    /// The persisted committed log id (so a restart re-applies committed entries).
    pub(super) fn committed(&self) -> PathBuf {
        self.dir.join("raft-committed.json")
    }
    /// The persisted last-purged log id (the log's lower bound after compaction).
    pub(super) fn purged(&self) -> PathBuf {
        self.dir.join("raft-purged.json")
    }
    /// The persisted state-machine snapshot (meta + serialized document).
    pub(super) fn snapshot(&self) -> PathBuf {
        self.dir.join("raft-snapshot.json")
    }
}

/// Validate the header and complete JSON frames, sync any safe tail repair, then
/// return an append handle. LogStore first validates the concrete Entry schema;
/// this helper also refuses CRC errors and invalid JSON before modifying bytes.
pub(super) fn ensure_log(path: &Path, format: LogFormat) -> io::Result<LogAppender> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let data = std::fs::read(path)?;
        let found = parse_log_format(&data)?;
        if found != format {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log: expected {format:?}, found {found:?}"),
            ));
        }
        let scan = scan_records(&data, LOG_HEADER, |body| {
            serde_json::from_slice::<serde_json::Value>(body)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })?;
        repair_tail(path, data.len(), scan.valid_len)?;
        Ok(LogAppender::new(
            std::fs::OpenOptions::new().append(true).open(path)?,
        ))
    } else {
        // Written beside the path and renamed in, so a crash or a full disk leaves no
        // file or a whole one.
        Ok(LogAppender::new(publish_empty_log(
            path,
            &format.header_bytes(),
        )?))
    }
}

/// Check a node's raft log against the rest of its raft state, before anything reads it.
///
/// A node that has a vote, a committed index, a purge point or a snapshot once had a log:
/// the log is created when the store is first opened, before any of those is written. If
/// that log is now missing, or shorter than its header, the node has lost entries it may
/// have acknowledged, and bringing it back with an empty log would break what Raft promises
/// its peers (it could vote again in a term it voted in, or accept a shorter history). The
/// library underneath does not detect this, so it is refused here (ADR-212, ADR-213). There
/// is no override: the node's data directory is restored from a snapshot. (Replacing the
/// node under a new identity is the textbook answer, and needs a control plane that can add
/// a member.)
///
/// A node with none of that state is a fresh node. A log shorter than its header is then
/// one whose creation was interrupted (releases before ADR-212 created the file and then
/// wrote the header); it is removed, and the node starts as the fresh node it is. A missing
/// log there is simply a node that has not started yet.
pub(super) fn check_log_against_other_state(paths: &RaftPaths) -> io::Result<()> {
    let log = paths.log();
    let other_state = [
        paths.vote(),
        paths.committed(),
        paths.purged(),
        paths.snapshot(),
    ];
    let other = other_state.iter().find(|path| path.exists());
    if !log.exists() {
        return match other {
            None => Ok(()),
            Some(found) => Err(crate::storage::framed_log::lost_log(
                &log,
                &format!(
                    "this node has other raft state ({}), written after it existed",
                    found.display()
                ),
                "It must not start with an empty log beside that state. Restore this node's \
                 data directory from a snapshot; until then leave it down, and the other \
                 control nodes keep their majority.",
            )),
        };
    }
    let headers = [
        LogFormat::Legacy.header_bytes(),
        LogFormat::DurableMoves.header_bytes(),
        LogFormat::DurableResize.header_bytes(),
    ];
    let headers: Vec<&[u8]> = headers.iter().map(<[u8; LOG_HEADER]>::as_slice).collect();
    if !header_was_interrupted(&log, &headers)? {
        return Ok(());
    }
    if let Some(found) = other {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "raft log: header is truncated, and this node has other raft state ({}); it \
                 will not start with an empty log",
                found.display()
            ),
        ));
    }
    std::fs::remove_file(&log)?;
    if let Some(parent) = log.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Append one serde record to an open append handle: `len u32 | crc u32 | json(body)`. fsync
/// (durable before return) when `fsync` is set, else flush to the OS page cache. The framing +
/// torn-tail recovery mirror [`clog`](super::clog) / `wal.rs`.
pub(super) fn append_record<T: Serialize>(
    file: &mut LogAppender,
    value: &T,
    fsync: bool,
) -> io::Result<()> {
    let body =
        serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    file.append(&body, fsync).map(|_| ())
}

/// Read every decoded record, oldest-first. Ignore only an incomplete final
/// write or zero padding; complete CRC/schema failures are InvalidData. Reading
/// never changes the file. A missing file reads as empty (a fresh node).
pub(super) fn read_records<T: DeserializeOwned>(path: &Path) -> io::Result<(Vec<T>, LogFormat)> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok((Vec::new(), LogFormat::Legacy));
        }
        Err(e) => return Err(e),
    };
    let format = parse_log_format(&data)?;
    let scan = scan_records(&data, LOG_HEADER, |body| {
        serde_json::from_slice::<T>(body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log: complete frame has incompatible payload: {e}"),
            )
        })
    })?;
    Ok((scan.records, format))
}

/// Atomically rewrite the log to exactly `records` (header + framed bodies) — the durable form of
/// `truncate` / `purge`, which drop a suffix / prefix. tmp + fsync + rename + parent-fsync, so a
/// crash mid-rewrite leaves the old (consistent) file in place.
pub(super) fn rewrite_records<T: Serialize>(
    path: &Path,
    records: &[T],
    fsync: bool,
    format: LogFormat,
) -> io::Result<()> {
    let tmp = path.with_extension("bin.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    let (magic, version) = format.header();
    f.write_all(&magic)?;
    f.write_all(&version.to_le_bytes())?;
    for value in records {
        let body =
            serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_frame(&mut f, &body)?;
    }
    if fsync {
        f.sync_all()?;
    }
    drop(f);
    std::fs::rename(&tmp, path)?;
    if fsync {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

/// Atomically write one serde value (vote / committed / purged / snapshot) — `json` to a tmp
/// file, fsync, rename over the target, fsync the parent dir, so a reader never sees a torn value.
pub(super) fn write_value<T: Serialize>(path: &Path, value: &T, fsync: bool) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body =
        serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("json.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&body)?;
    if fsync {
        f.sync_all()?;
    }
    drop(f);
    std::fs::rename(&tmp, path)?;
    if fsync {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

/// Read one serde value back; `Ok(None)` if the file is absent (a fresh node). A present but
/// unparseable value is a fail-loud error (never silently treated as absent, which would drop
/// hard state).
pub(super) fn read_value<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let v = serde_json::from_slice::<T>(&data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rr_ctrlstore_{}_{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn log_round_trips_and_appends_are_monotonic() {
        let dir = scratch("log");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path, LogFormat::Legacy).unwrap();
            append_record(&mut f, &(1u64, "a".to_string()), true).unwrap();
            append_record(&mut f, &(2u64, "b".to_string()), true).unwrap();
        }
        let (recs, format): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(format, LogFormat::Legacy);
        assert_eq!(recs, vec![(1, "a".into()), (2, "b".into())]);
        // Reopen + append keeps prior records.
        {
            let mut f = ensure_log(&path, LogFormat::Legacy).unwrap();
            append_record(&mut f, &(3u64, "c".to_string()), false).unwrap();
        }
        let (recs, _): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(recs.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_drops_a_torn_tail() {
        let dir = scratch("torn");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path, LogFormat::Legacy).unwrap();
            append_record(&mut f, &(1u64, "alpha".to_string()), true).unwrap();
            append_record(&mut f, &(2u64, "beta".to_string()), true).unwrap();
        }
        // Corrupt the tail with bytes that can't frame a valid record.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0x10, 0, 0, 0, 0xAA, 0xBB, 0xCC]).unwrap();
        }
        let (recs, _): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(recs.len(), 2, "the two whole records survive a torn tail");
        let mut file = ensure_log(&path, LogFormat::Legacy).unwrap();
        append_record(&mut file, &(3u64, "gamma".to_string()), true).unwrap();
        drop(file);
        let (recs, _): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(
            recs,
            vec![(1, "alpha".into()), (2, "beta".into()), (3, "gamma".into())]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crc_failure_or_complete_invalid_json_refuses_append_open_without_modification() {
        let dir = scratch("corrupt_frames");
        let path = dir.join("raft-log.bin");
        for bad_crc in [false, true] {
            let mut bytes = Vec::from(LOG_MAGIC_V1);
            bytes.extend_from_slice(&1u32.to_le_bytes());
            write_frame(&mut bytes, b"unknown invalid json").unwrap();
            if bad_crc {
                bytes[12] ^= 1;
            }
            write_frame(&mut bytes, b"[2,\"later valid record\"]").unwrap();
            bytes.extend_from_slice(&[0xaa; 3]);
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                read_records::<(u64, String)>(&path).err().unwrap().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                ensure_log(&path, LogFormat::Legacy).err().unwrap().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rewrite_drops_prefix_and_suffix() {
        let dir = scratch("rewrite");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path, LogFormat::Legacy).unwrap();
            for i in 1..=5u64 {
                append_record(&mut f, &(i, "x".to_string()), false).unwrap();
            }
        }
        // Keep only records 2..=4 (a purge of 1 + a truncate of 5).
        let kept: Vec<(u64, String)> = vec![(2, "x".into()), (3, "x".into()), (4, "x".into())];
        rewrite_records(&path, &kept, true, LogFormat::Legacy).unwrap();
        let (recs, _): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(recs, kept);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_move_log_header_is_a_one_way_compatibility_fence() {
        let dir = scratch("move_fence");
        let path = dir.join("raft-log.bin");
        rewrite_records(
            &path,
            &[(1u64, "legacy".to_string())],
            true,
            LogFormat::DurableMoves,
        )
        .unwrap();
        let (records, format): (Vec<(u64, String)>, _) = read_records(&path).unwrap();
        assert_eq!(records, vec![(1, "legacy".into())]);
        assert_eq!(format, LogFormat::DurableMoves);
        assert!(
            ensure_log(&path, LogFormat::Legacy).is_err(),
            "a legacy binary must reject the fenced log"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn predecessor_move_log_formats_are_rejected() {
        let dir = scratch("predecessor_moves_rejected");
        for (magic, version) in [(*b"RRL2", 2u32), (*b"RRL3", 3u32)] {
            let path = dir.join(format!("raft-log-{version}.bin"));
            let mut predecessor = Vec::from(magic);
            predecessor.extend_from_slice(&version.to_le_bytes());
            std::fs::write(&path, predecessor).unwrap();
            assert!(
                read_records::<(u64, String)>(&path).is_err(),
                "predecessor move format {version} must fail loud"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn valid_crc_with_incompatible_payload_fails_loud() {
        let dir = scratch("incompatible");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path, LogFormat::Legacy).unwrap();
            append_record(&mut f, &serde_json::json!({"new_variant": true}), true).unwrap();
        }
        assert!(
            read_records::<(u64, String)>(&path).is_err(),
            "schema incompatibility is not a torn tail"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn value_round_trips_and_absent_is_none() {
        let dir = scratch("value");
        let path = dir.join("raft-vote.json");
        assert!(read_value::<(u64, bool)>(&path).unwrap().is_none());
        write_value(&path, &(7u64, true), true).unwrap();
        assert_eq!(read_value::<(u64, bool)>(&path).unwrap(), Some((7, true)));
        // Overwrite is atomic + last-writer-wins.
        write_value(&path, &(9u64, false), true).unwrap();
        assert_eq!(read_value::<(u64, bool)>(&path).unwrap(), Some((9, false)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
