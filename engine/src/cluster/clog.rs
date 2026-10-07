//! `ClusterLog` — the coordinator's durable, ordered mutation log (the externalized
//! "log is the database" source of truth, clustering build-path step 3a / ADR-031).
//!
//! Design: docs/design/clustering-and-scaling.md §4.1 (durable log), §10 step 3.
//!
//! [`ClusterLog`] is to durability what [`Shard`](super::shard::Shard) is to a shard:
//! a sync, fallible, `Send + Sync` seam that abstracts the OPERATION, so the
//! single-node file backend ([`FileClusterLog`]) shipped here can later be swapped for a
//! Raft-backed one *without touching the coordinator* — `append` becomes a
//! quorum-commit, `replay` a committed-prefix read, `checkpoint` a snapshot install,
//! `epoch` the Raft term. A second backend ([`NullClusterLog`]) is the in-memory /
//! no-`data_dir` path and the fast test backend; running a churn script through both and
//! asserting identical results is the differential proof that coordinator behavior is
//! log-impl-independent.
//!
//! ## Why a separate file format (not the engine [`Wal`](crate::wal::Wal))
//! The engine WAL's tombstone is a *per-shard physical* `(seg_idx, local_id)`; the
//! coordinator mutates by *logical id*. The formats retain separate headers and
//! payload decoders, sharing only the cold-path framing, strict validation, safe
//! tail repair, and sticky append-failure guard (ADR-182).
//!
//! ## On-disk frame (mirrors `wal.rs`)
//! ```text
//!   header (once): magic "CMLG" (4) + format_version u32 (4)
//!   per record:    total_len u32 | crc32 u32 | seq u64 | op u8 | payload
//!     op ADD    (0): logical u64 | version u32 | dsl_len u32 | dsl [u8]
//!                    tag_count u32 | (klen u32|k|vlen u32|v)*
//!                    placement_generation u64 | num_shards u32 | mode u8
//!                    position_count u32 | positions [u32]           (v4, ADR-109)
//!     op REMOVE (1): logical u64
//!     op UPSERT (2): same payload as ADD
//! ```
//! v4 requires explicit placement identity for ADD/UPSERT and therefore rejects v1–v3 logs with
//! an actionable rebuild error; re-deriving ownership under a newer ring/generation would be unsafe.
//! Recovery validates all complete frames and refuses CRC errors or incompatible
//! payloads. Open removes and synchronizes only an incomplete final write or zero
//! padding before allowing append; its exact byte count survives until replay. The
//! checkpoint *cursor* (which records are already captured by a base snapshot) and the
//! *epoch* live in the coordinator manifest — the atomic commit point — not in this
//! file, so [`ClusterLog::replay`] takes the cursor as an argument and a checkpoint
//! simply truncates already-captured records.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use super::shard::ShardError;
use crate::storage::framed_log::{
    header_was_interrupted, publish_empty_log, repair_tail, scan_records, write_frame, FrameScan,
    LogAppender,
};

mod codec;

const CLOG_MAGIC: [u8; 4] = *b"CMLG";
// v2 (ADR-055): optional trailing tags. v3 (ADR-070): atomic UPSERT. v4 (ADR-109):
// mandatory write-time placement metadata for ADD/UPSERT. v1-v3 are now a migration fence:
// without their original placement identity a new binary cannot choose one emission owner safely.
const CLOG_VERSION: u32 = 4;
const CLOG_HEADER_SIZE: usize = 8; // magic + version

const OP_ADD: u8 = 0;
const OP_REMOVE: u8 = 1;
const OP_UPSERT: u8 = 2;

/// Opaque, ordered position in the log — the Raft log index later. New-typed so callers
/// can't do arithmetic on it; `LogPos(0)` is "before the first record".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub(crate) struct LogPos(pub u64);

/// One coordinator-visible cluster mutation. Logical-id + raw DSL is node-independent
/// and re-compilable against the manifest's frozen dict (the ADR-029 DSL-on-wire
/// invariant), so replaying it reproduces byte-identical placement → zero false
/// negatives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ClusterMutation {
    /// Add (or replace, by logical id) a query. `version` mirrors the engine's
    /// per-logical version carried on the write path.
    Add {
        logical: u64,
        version: u32,
        dsl: String,
        /// Raw `(key, value)` metadata tags (ADR-055), re-resolved to `TagId`s against the
        /// frozen shared tag space on apply/replay (the tags-on-wire analogue of raw DSL).
        /// Empty for an untagged query — the byte-identical pre-tag path.
        tags: Vec<(String, String)>,
        /// ADR-109 write-time placement identity. Persisted so coordinator and
        /// per-shard translog replay cannot re-materialize stale ownership.
        placement: crate::ownership::QueryPlacement,
    },
    /// Remove every live entry for a logical id (idempotent).
    Remove { logical: u64 },
    /// Atomically replace a query by logical id (ADR-070): tombstone every prior live
    /// copy AND insert the new version under ONE frame, so replay reproduces the whole
    /// replacement or none of it — never a remove without its re-add (the cluster
    /// analogue of the engine WAL's `Upsert`, ADR-067). Payload layout is identical
    /// to [`Add`](Self::Add).
    Upsert {
        logical: u64,
        version: u32,
        dsl: String,
        /// Raw `(key, value)` metadata tags for the NEW version (ADR-055 semantics).
        tags: Vec<(String, String)>,
        placement: crate::ownership::QueryPlacement,
    },
}

/// Result of replaying a log from a cursor — mirrors [`WalRecovery`](crate::wal::WalRecovery):
/// the ordered mutations to apply plus a torn-tail byte count.
pub(crate) struct ClusterReplay {
    pub entries: Vec<(LogPos, ClusterMutation)>,
    /// Exact bytes repaired on open plus any currently incomplete final write or
    /// zero padding. Complete corrupt or incompatible records instead fail replay.
    pub skipped_bytes: usize,
}

/// The durable, ordered source of truth the coordinator applies to its shards.
///
/// Sync + fallible (`Result<_, ShardError>`) + `Send + Sync`, exactly like
/// [`Shard`](super::shard::Shard). A `NullClusterLog` is infallible; a `FileClusterLog`
/// errors on I/O — surfacing that (rather than swallowing it) is load-bearing for the
/// rebuild-from-log contract.
pub(crate) trait ClusterLog: Send + Sync {
    /// Durably append one mutation and return its assigned position. MUST be durable
    /// before returning `Ok` (WAL-first: the coordinator applies to shards only after
    /// this succeeds). Raft: returns once the entry commits on a quorum.
    fn append(&self, m: &ClusterMutation) -> Result<LogPos, ShardError>;

    /// Replay every committed record strictly after `from`, oldest-first. Used by
    /// `ClusterEngine::open` (with the manifest's snapshot cursor) and, later, a
    /// follower catching up.
    fn replay(&self, from: LogPos) -> Result<ClusterReplay, ShardError>;

    /// The highest position appended so far (`LogPos(0)` if none).
    fn last_pos(&self) -> Result<LogPos, ShardError>;

    /// Drop every record at or before `up_to` (now captured by a base snapshot). The
    /// caller (coordinator) MUST have durably written the snapshot + manifest first —
    /// the manifest is the atomic commit point, so a crash before this truncation just
    /// replays an already-captured (idempotent) tail. The local checkpoint generation
    /// lives in the coordinator manifest, not in this log or the separate control-plane
    /// document, so this byte-log stays a pure ordered store.
    fn checkpoint(&self, up_to: LogPos) -> Result<(), ShardError>;

    /// Whether each append is fsynced before it returns, so an acknowledged write survives a
    /// power loss and not only a process crash. False for a log that persists nothing.
    /// Read by the shard node's metrics, which exist only in the distributed build.
    #[cfg(feature = "distributed")]
    fn syncs_each_write(&self) -> bool {
        false
    }

    /// Test-only fault injection: make subsequent `append`s fail. Default no-op (e.g.
    /// `NullClusterLog`); `FileClusterLog` revokes its write handle. Exposed on the trait
    /// so a coordinator test can break the log through a `Box<dyn ClusterLog>` and prove
    /// the WAL-first fail-closed contract.
    #[cfg(test)]
    fn break_writes_for_test(&self) {}
}

// ---- NullClusterLog: in-memory (no data_dir) + the fast test backend ----

/// A non-durable log: assigns monotonic positions in memory, but persists nothing and
/// replays empty. This is the behavior of an in-process cluster built without a
/// `data_dir` (byte-identical to the pre-ADR-031 cluster) and the fast backend the
/// durability oracle diffs the file backend against.
pub(crate) struct NullClusterLog {
    next_seq: AtomicU64,
}

impl NullClusterLog {
    pub(crate) fn new() -> Self {
        NullClusterLog {
            next_seq: AtomicU64::new(1),
        }
    }
}

impl ClusterLog for NullClusterLog {
    fn append(&self, _m: &ClusterMutation) -> Result<LogPos, ShardError> {
        Ok(LogPos(self.next_seq.fetch_add(1, Ordering::Relaxed)))
    }

    fn replay(&self, _from: LogPos) -> Result<ClusterReplay, ShardError> {
        Ok(ClusterReplay {
            entries: Vec::new(),
            skipped_bytes: 0,
        })
    }

    fn last_pos(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(
            self.next_seq.load(Ordering::Relaxed).saturating_sub(1),
        ))
    }

    fn checkpoint(&self, _up_to: LogPos) -> Result<(), ShardError> {
        Ok(())
    }
}

// ---- FileClusterLog: the durable single-node backend ----

/// Mutable file state behind the `&self` trait (the coordinator holds the log shared).
/// A `std::sync::Mutex` both gives interior mutability and enforces the single-writer
/// total order a Raft leader will also want.
struct FileState {
    file: LogAppender,
    path: PathBuf,
    /// Next position to assign — kept monotonic across checkpoints/reopens (seeded from
    /// the manifest's snapshot cursor as a floor, so a truncated-then-reopened log never
    /// reissues a position).
    next_seq: u64,
    repaired_tail_bytes: usize,
}

/// A durable, CRC-framed, append-only cluster log (the file backend of [`ClusterLog`]).
pub(crate) struct FileClusterLog {
    state: Mutex<FileState>,
    /// When true, every append `fsync`s before returning (survives power loss); when
    /// false, appends only reach the OS page cache (survives process crash). Mirrors
    /// the engine WAL's `fsync_each_write` policy.
    fsync_each_write: bool,
}

impl FileClusterLog {
    /// Open or create the log at `path`. `floor_pos` (the manifest's snapshot cursor)
    /// seeds the position counter so it stays monotonic even after a checkpoint
    /// truncated the file.
    pub(crate) fn open(path: &Path, fsync_each_write: bool, floor_pos: LogPos) -> io::Result<Self> {
        let (file, next_seq, repaired_tail_bytes) = if path.exists() {
            let scan = Self::read_entries(path)?;
            let max_seq = scan.records.iter().map(|(p, _)| p.0).max().unwrap_or(0);
            let next_seq = max_seq.max(floor_pos.0).checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "cluster log position exhausted")
            })?;
            repair_tail(path, scan.valid_len + scan.torn_bytes, scan.valid_len)?;
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            (file, next_seq, scan.torn_bytes)
        } else {
            let next_seq = floor_pos.0.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "cluster log position exhausted")
            })?;
            // Written beside the path and renamed in, so a crash or a full disk leaves no
            // file or a whole one, and the directory entry is durable before the first
            // append is acknowledged.
            let file = publish_empty_log(path, &Self::header(CLOG_VERSION))?;
            (file, next_seq, 0)
        };
        Ok(FileClusterLog {
            state: Mutex::new(FileState {
                file: LogAppender::new(file),
                path: path.to_path_buf(),
                next_seq,
                repaired_tail_bytes,
            }),
            fsync_each_write,
        })
    }

    /// The eight bytes that open a log of format `version`.
    fn header(version: u32) -> [u8; CLOG_HEADER_SIZE] {
        let mut header = [0u8; CLOG_HEADER_SIZE];
        header[..CLOG_MAGIC.len()].copy_from_slice(&CLOG_MAGIC);
        header[CLOG_MAGIC.len()..].copy_from_slice(&version.to_le_bytes());
        header
    }

    /// Replace a log whose creation was interrupted with the empty log it was going to be.
    ///
    /// Releases before this one created the file and then wrote its header. A crash, a power
    /// loss or a full disk between the two left a file shorter than its header, and every
    /// later start refused it ("clog too small"). Such a file never held a record, because
    /// the header is written and synced before the first append.
    ///
    /// [`open`](Self::open) does not do this itself, and still refuses a short file. Whether
    /// a short log is an interrupted creation or a log that has lost its content cannot be
    /// read from the file; it is known to the owner, who calls this only when nothing it
    /// holds says the log was ever whole. A short file that is not the start of a supported
    /// header is left as it is, for `open` to refuse.
    pub(crate) fn finish_interrupted_creation(path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let supported: Vec<[u8; CLOG_HEADER_SIZE]> = (1..=CLOG_VERSION).map(Self::header).collect();
        let supported: Vec<&[u8]> = supported
            .iter()
            .map(<[u8; CLOG_HEADER_SIZE]>::as_slice)
            .collect();
        if header_was_interrupted(path, &supported)? {
            drop(publish_empty_log(path, &Self::header(CLOG_VERSION))?);
        }
        Ok(())
    }

    /// Lock the file state, recovering a poisoned guard rather than panicking (a prior
    /// writer panic must not take down the cluster; the file is append-framed, so the
    /// on-disk state is still consistent up to the last whole record).
    fn lock(&self) -> std::sync::MutexGuard<'_, FileState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Encode one mutation's body: `seq | op | payload` (the CRC'd, length-framed part).
    fn encode_body(seq: u64, m: &ClusterMutation) -> Vec<u8> {
        // V4 ADD/UPSERT always carries the tag count and write-time placement.
        // Each tag is a length-prefixed key + value.
        fn encode_add_like(
            body: &mut Vec<u8>,
            op: u8,
            logical: u64,
            version: u32,
            dsl: &str,
            tags: &[(String, String)],
            placement: &crate::ownership::QueryPlacement,
        ) {
            let dsl_bytes = dsl.as_bytes();
            body.push(op);
            body.extend_from_slice(&logical.to_le_bytes());
            body.extend_from_slice(&version.to_le_bytes());
            body.extend_from_slice(&(dsl_bytes.len() as u32).to_le_bytes());
            body.extend_from_slice(dsl_bytes);
            body.extend_from_slice(&(tags.len() as u32).to_le_bytes());
            for (k, v) in tags {
                let kb = k.as_bytes();
                let vb = v.as_bytes();
                body.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                body.extend_from_slice(kb);
                body.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                body.extend_from_slice(vb);
            }
            body.extend_from_slice(&placement.generation().0.to_le_bytes());
            body.extend_from_slice(&placement.num_shards().to_le_bytes());
            body.push(placement.mode() as u8);
            body.extend_from_slice(&(placement.positions().len() as u32).to_le_bytes());
            for position in placement.positions() {
                body.extend_from_slice(&position.to_le_bytes());
            }
        }
        let mut body = Vec::new();
        body.extend_from_slice(&seq.to_le_bytes());
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => encode_add_like(&mut body, OP_ADD, *logical, *version, dsl, tags, placement),
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => encode_add_like(
                &mut body, OP_UPSERT, *logical, *version, dsl, tags, placement,
            ),
            ClusterMutation::Remove { logical } => {
                body.push(OP_REMOVE);
                body.extend_from_slice(&logical.to_le_bytes());
            }
        }
        body
    }

    /// Read every valid record from a log file. Returns positioned mutations plus the
    /// byte count of any trailing data that could not be parsed (torn tail).
    fn read_entries(path: &Path) -> io::Result<FrameScan<(LogPos, ClusterMutation)>> {
        let data = std::fs::read(path)?;
        if data.len() < CLOG_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "clog too small"));
        }
        if data[0..4] != CLOG_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad clog magic"));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid cluster-log version")
        })?);
        if version != CLOG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                if version < CLOG_VERSION {
                    format!(
                        "cluster log format v{version} predates ADR-109 ownership metadata; rebuild the cluster with this binary"
                    )
                } else {
                    format!("unsupported cluster log format v{version}")
                },
            ));
        }
        Self::parse_entries(&data)
    }

    fn parse_entries(data: &[u8]) -> io::Result<FrameScan<(LogPos, ClusterMutation)>> {
        let mut previous_seq = 0;
        let scan = scan_records(data, CLOG_HEADER_SIZE, |body| {
            let entry = codec::decode(body)?;
            if entry.0 .0 <= previous_seq {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cluster log positions must be strictly increasing",
                ));
            }
            previous_seq = entry.0 .0;
            Ok(entry)
        })?;
        Ok(scan)
    }
}

impl ClusterLog for FileClusterLog {
    fn append(&self, m: &ClusterMutation) -> Result<LogPos, ShardError> {
        let mut st = self.lock();
        let seq = st.next_seq;
        // An I/O failure can leave a complete record with an uncertain durability
        // outcome. Do not reissue its position if a checkpoint replaces this handle.
        st.next_seq = seq
            .checked_add(1)
            .ok_or_else(|| ShardError::Log("cluster log position exhausted".into()))?;
        let body = Self::encode_body(seq, m);
        st.file
            .append(&body, self.fsync_each_write)
            .map_err(|e| ShardError::Log(format!("append: {e}")))?;
        Ok(LogPos(seq))
    }

    fn replay(&self, from: LogPos) -> Result<ClusterReplay, ShardError> {
        let mut st = self.lock();
        let scan =
            Self::read_entries(&st.path).map_err(|e| ShardError::Log(format!("replay: {e}")))?;
        let skipped_bytes = scan.torn_bytes + std::mem::take(&mut st.repaired_tail_bytes);
        let entries = scan
            .records
            .into_iter()
            .filter(|(p, _)| *p > from)
            .collect();
        Ok(ClusterReplay {
            entries,
            skipped_bytes,
        })
    }

    fn last_pos(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(self.lock().next_seq.saturating_sub(1)))
    }

    #[cfg(feature = "distributed")]
    fn syncs_each_write(&self) -> bool {
        self.fsync_each_write
    }

    fn checkpoint(&self, up_to: LogPos) -> Result<(), ShardError> {
        let mut st = self.lock();
        // Rewrite the file keeping only records strictly after `up_to` (those not yet
        // captured by the base snapshot). Atomic via tmp + rename so a crash mid-rewrite
        // leaves the old (already-consistent) file in place.
        let scan = Self::read_entries(&st.path)
            .map_err(|e| ShardError::Log(format!("checkpoint read: {e}")))?;
        let kept: Vec<(LogPos, ClusterMutation)> = scan
            .records
            .into_iter()
            .filter(|(p, _)| *p > up_to)
            .collect();

        st.file.disable();

        let rewrite = (|| -> io::Result<()> {
            let tmp = st.path.with_extension("clog.tmp");
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&CLOG_MAGIC)?;
            f.write_all(&CLOG_VERSION.to_le_bytes())?;
            for (pos, m) in &kept {
                let body = Self::encode_body(pos.0, m);
                write_frame(&mut f, &body)?;
            }
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &st.path)?;
            if let Some(parent) = st.path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        rewrite.map_err(|e| ShardError::Log(format!("checkpoint rewrite: {e}")))?;

        // Re-open the appending handle on the rewritten file.
        st.file = LogAppender::new(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&st.path)
                .map_err(|e| ShardError::Log(format!("checkpoint reopen: {e}")))?,
        );
        Ok(())
    }

    #[cfg(test)]
    fn break_writes_for_test(&self) {
        FileClusterLog::break_writes_for_test(self);
    }
}

#[cfg(test)]
impl FileClusterLog {
    /// Test-only: swap the file handle for a read-only one so subsequent appends fail —
    /// the deterministic write-fault injection used by the durability oracle (mirrors
    /// `Wal::break_writes_for_test`).
    pub(crate) fn break_writes_for_test(&self) {
        let mut st = self.lock();
        st.file = LogAppender::new(
            std::fs::OpenOptions::new()
                .read(true)
                .open(&st.path)
                .expect("reopen clog read-only"),
        );
    }
}

#[cfg(test)]
mod tests;
