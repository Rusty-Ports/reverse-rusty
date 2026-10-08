use super::{
    LogAppender, Path, Wal, WalEntry, Write, OP_DELETE_LOGICAL, OP_FLUSH_CHECKPOINT, OP_INSERT,
    OP_INSERT_CLASS_D, OP_TOMBSTONE, OP_UPSERT, OP_UPSERT_CLASS_D, SOURCE_GENERATION_MAGIC,
    WAL_HEADER_SIZE, WAL_VERSION,
};
use crate::storage::framed_log::repair_tail;
use std::io;
use std::io::{Seek, SeekFrom};

impl Wal {
    /// Open or create a WAL file. If the file exists, scans it to find the next
    /// sequence number. If it doesn't exist, creates it with a header.
    ///
    /// `fsync_each_write` selects the per-append durability policy (see
    /// [`Wal::fsync_each_write`]).
    pub fn open(path: &Path, fsync_each_write: bool) -> io::Result<Self> {
        if path.exists() && !Self::header_was_interrupted(path)? {
            // Open existing, find the max sequence number and current pending count.
            let (scan, version) = Self::read_entries(path)?;
            let entries = scan.records;
            let next_seq = entries
                .iter()
                .map(WalEntry::seq)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "WAL sequence space exhausted")
                })?;
            // Pending = entries after the last checkpoint (same set recover() replays).
            let pending_entries = match entries
                .iter()
                .rposition(|e| matches!(e, WalEntry::FlushCheckpoint { .. }))
            {
                Some(idx) => (entries.len() - idx - 1) as u64,
                None => entries.len() as u64,
            };
            repair_tail(path, scan.valid_len + scan.torn_bytes, scan.valid_len)?;
            // Never write a header through O_APPEND: a legacy file may contain known
            // modern frames, and all subsequent writes must advertise the current format.
            if version < WAL_VERSION {
                let mut header = std::fs::OpenOptions::new().write(true).open(path)?;
                header.seek(SeekFrom::Start(4))?;
                header.write_all(&WAL_VERSION.to_le_bytes())?;
                header.sync_all()?;
            }
            let size_bytes = scan.valid_len as u64;
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            Ok(Wal {
                file: LogAppender::new(file),
                path: path.to_path_buf(),
                next_seq,
                fsync_each_write,
                size_bytes,
                pending_entries,
                repaired_tail_bytes: scan.torn_bytes,
            })
        } else {
            // A new log, or one whose header an interrupted creation or reset left
            // incomplete: such a file never held a record (see `header_was_interrupted`).
            let file = Self::publish_empty_log(path)?;
            Ok(Wal {
                file: LogAppender::new(file),
                path: path.to_path_buf(),
                next_seq: 1,
                fsync_each_write,
                size_bytes: WAL_HEADER_SIZE as u64,
                pending_entries: 0,
                repaired_tail_bytes: 0,
            })
        }
    }

    fn take_seq(&mut self) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq = seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("WAL sequence space exhausted"))?;
        Ok(seq)
    }

    pub(crate) fn take_repaired_tail_bytes(&mut self) -> usize {
        std::mem::take(&mut self.repaired_tail_bytes)
    }

    /// Append an Insert entry. Returns the sequence number assigned. `tags` are the
    /// query's `(key, value)` metadata pairs (ADR-049); pass `&[]` for an untagged insert.
    pub fn append_insert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT, logical, version, text, tags, None, None)
    }

    pub fn append_insert_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_INSERT,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Insert accepted under the class-D lane (WAL v5, ADR-068). Same
    /// payload as [`append_insert`](Self::append_insert); the op code is the
    /// per-frame accept marker, so replay can store it unconditionally while legacy
    /// op-0 frames (logged before classification by pre-v5 binaries) still replay
    /// under the old reject gate.
    pub fn append_insert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT_CLASS_D, logical, version, text, tags, None, None)
    }

    pub fn append_insert_class_d_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_INSERT_CLASS_D,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Upsert entry (WAL v4, ADR-067) — the atomic replace-by-id. Same
    /// payload as Insert; the op code is what tells recovery to tombstone the prior
    /// live copies of `logical` before inserting this version.
    pub fn append_upsert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT, logical, version, text, tags, None, None)
    }

    pub fn append_upsert_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_UPSERT,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Upsert accepted under the class-D lane (WAL v5, ADR-068) — see
    /// [`append_insert_class_d`](Self::append_insert_class_d).
    pub fn append_upsert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT_CLASS_D, logical, version, text, tags, None, None)
    }

    pub fn append_upsert_class_d_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_UPSERT_CLASS_D,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an engine-owned Insert carrying its source generation (WAL v7).
    /// This is the live Engine path; the compatibility helpers above deliberately
    /// retain their historical generation-less wire shape for direct callers.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_insert_with_source_generation(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: u64,
        class_d_accepted: bool,
    ) -> io::Result<u64> {
        let op = if class_d_accepted {
            OP_INSERT_CLASS_D
        } else {
            OP_INSERT
        };
        self.append_insert_like(
            op,
            logical,
            version,
            text,
            tags,
            priority,
            Some(source_generation),
        )
    }

    /// Append an engine-owned Upsert carrying its source generation (WAL v7).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_upsert_with_source_generation(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: u64,
        class_d_accepted: bool,
    ) -> io::Result<u64> {
        let op = if class_d_accepted {
            OP_UPSERT_CLASS_D
        } else {
            OP_UPSERT
        };
        self.append_insert_like(
            op,
            logical,
            version,
            text,
            tags,
            priority,
            Some(source_generation),
        )
    }

    /// Shared encoder for the insert-shaped ops. Generation-less compatibility
    /// calls retain the v6 optional-priority tail; engine-owned writes use the
    /// marked v7 generation tail.
    // Keep the payload fields explicit here so every compatibility helper chooses
    // its priority/source-generation wire shape at the call site.
    #[allow(clippy::too_many_arguments)]
    fn append_insert_like(
        &mut self,
        op: u8,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: Option<u64>,
    ) -> io::Result<u64> {
        if source_generation == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero WAL source generation",
            ));
        }
        let seq = self.take_seq()?;

        let text_bytes = text.as_bytes();
        // tag section: tag_count(2) + per tag key_len(2)+key + val_len(2)+value
        let mut tag_bytes = Vec::new();
        let tag_count = u16::try_from(tags.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many WAL tags"))?;
        tag_bytes.extend_from_slice(&tag_count.to_le_bytes());
        for (k, v) in tags {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            let key_len = u16::try_from(kb.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL tag key exceeds u16 length",
                )
            })?;
            let value_len = u16::try_from(vb.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL tag value exceeds u16 length",
                )
            })?;
            tag_bytes.extend_from_slice(&key_len.to_le_bytes());
            tag_bytes.extend_from_slice(kb);
            tag_bytes.extend_from_slice(&value_len.to_le_bytes());
            tag_bytes.extend_from_slice(vb);
        }
        // payload: logical(8) + version(4) + text_len(4) + text + tag section
        let extension_len = match source_generation {
            Some(_) => 4 + 8 + 1 + priority.map_or(0, |_| 8),
            None => priority.map_or(0, |_| 8),
        };
        let payload_len = 8 + 4 + 4 + text_bytes.len() + tag_bytes.len() + extension_len;
        // entry body: seq(8) + op(1) + payload
        let body_len = 8 + 1 + payload_len;

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(op);
        body.extend_from_slice(&logical.to_le_bytes());
        body.extend_from_slice(&version.to_le_bytes());
        let text_len = u32::try_from(text_bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "WAL text exceeds u32 length")
        })?;
        body.extend_from_slice(&text_len.to_le_bytes());
        body.extend_from_slice(text_bytes);
        body.extend_from_slice(&tag_bytes);
        if let Some(source_generation) = source_generation {
            body.extend_from_slice(&SOURCE_GENERATION_MAGIC);
            body.extend_from_slice(&source_generation.to_le_bytes());
            body.push(u8::from(priority.is_some()));
            if let Some(value) = priority {
                body.extend_from_slice(&value.to_le_bytes());
            }
        } else if let Some(value) = priority {
            body.extend_from_slice(&value.to_le_bytes());
        }

        self.file
            .append_at(&self.path, &body, self.fsync_each_write)?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a Tombstone entry.
    pub fn append_tombstone(&mut self, seg_idx: u32, local_id: u32) -> io::Result<u64> {
        let seq = self.take_seq()?;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_TOMBSTONE);
        body.extend_from_slice(&seg_idx.to_le_bytes());
        body.extend_from_slice(&local_id.to_le_bytes());

        self.file
            .append_at(&self.path, &body, self.fsync_each_write)?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a DeleteByLogical entry (WAL v3, ADR-066): the address-free
    /// "tombstone every live copy of `logical`" mutation logged by
    /// [`Engine::delete_by_logical_id`](crate::segment::Engine::delete_by_logical_id).
    /// One frame per delete, regardless of how many physical copies it removes.
    pub fn append_delete_logical(&mut self, logical: u64) -> io::Result<u64> {
        let seq = self.take_seq()?;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_DELETE_LOGICAL);
        body.extend_from_slice(&logical.to_le_bytes());

        self.file
            .append_at(&self.path, &body, self.fsync_each_write)?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a FlushCheckpoint entry. Indicates that all prior WAL entries
    /// have been materialized into sealed segments.
    pub fn append_flush_checkpoint(&mut self, segment_file: &str) -> io::Result<u64> {
        let seq = self.take_seq()?;

        let name_bytes = segment_file.as_bytes();
        let mut body = Vec::with_capacity(8 + 1 + 4 + name_bytes.len());
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_FLUSH_CHECKPOINT);
        let name_len = u32::try_from(name_bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL checkpoint name exceeds u32 length",
            )
        })?;
        body.extend_from_slice(&name_len.to_le_bytes());
        body.extend_from_slice(name_bytes);

        self.file.append_at(&self.path, &body, true)?; // fsync on checkpoint
        self.size_bytes += 8 + body.len() as u64; // length prefix + CRC + body
        self.pending_entries = 0; // checkpoint materializes all prior mutations
        Ok(seq)
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Current on-disk WAL size in bytes (header + framed entries).
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Number of un-checkpointed entries (mutations not yet in a sealed segment).
    pub fn pending_entries(&self) -> u64 {
        self.pending_entries
    }

    /// The sequence number of the last appended entry (0 if none yet). Sequence
    /// numbers stay monotonic across [`reset`](Self::reset), so this is a valid
    /// high-water mark for the manifest's `wal_seq_watermark` (ADR-066).
    pub fn last_seq(&self) -> u64 {
        self.next_seq - 1
    }

    /// Pin the next sequence number past `watermark` (ADR-066). `reset` keeps the
    /// sequence monotonic only in memory: reopening a reset (header-only) WAL file
    /// rescans it and restarts at 1, while the manifest keeps its old watermark —
    /// so without this, frames appended after the reopen would sort at or below
    /// the watermark and be wrongly skipped by the next recovery (a resurrected
    /// delete). [`Engine::open`](crate::segment::Engine::open) calls this with the
    /// recovered manifest's watermark.
    pub fn ensure_seq_after(&mut self, watermark: u64) {
        if self.next_seq <= watermark {
            self.next_seq = watermark.saturating_add(1);
        }
    }

    /// Reset the WAL to an empty log. Called after a successful compaction + manifest
    /// write when all data is in sealed segments.
    ///
    /// The log is replaced, never truncated in place: an empty log is written beside it
    /// and renamed over it, so a crash at any point leaves either the old log (whose
    /// records the manifest already covers) or the new one, and both open. Until the
    /// rename the old log and its handle are untouched, so a failure before it leaves the
    /// WAL as it was. From the rename on, the old handle addresses an unlinked file and is
    /// disabled first: if anything after it fails, appends are refused until a reopen
    /// rather than acknowledged into a file no restart will read.
    pub fn reset(&mut self) -> io::Result<()> {
        let replacement = Self::write_empty_log_beside(&self.path)?;
        self.file.disable();
        crate::storage::durable_rename(&replacement, &self.path)?;
        let file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        self.file = LogAppender::new(file);
        self.size_bytes = WAL_HEADER_SIZE as u64;
        self.pending_entries = 0;
        // Don't reset next_seq — keep it monotonic across resets
        Ok(())
    }

    /// Where an empty replacement is built before it is renamed over the log.
    pub(super) fn replacement_path(path: &Path) -> std::path::PathBuf {
        crate::storage::framed_log::replacement_path(path)
    }

    /// Write a complete, synced, header-only log beside `path` and return where it is.
    fn write_empty_log_beside(path: &Path) -> io::Result<std::path::PathBuf> {
        let replacement = Self::replacement_path(path);
        let mut file = std::fs::File::create(&replacement)?;
        file.write_all(&Self::header(WAL_VERSION))?;
        file.sync_all()?;
        Ok(replacement)
    }

    /// Put an empty log at `path` atomically and return an append handle on it.
    fn publish_empty_log(path: &Path) -> io::Result<std::fs::File> {
        crate::storage::framed_log::publish_empty_log(path, &Self::header(WAL_VERSION))
    }

    /// Test-only: swap the underlying file for a read-only handle so subsequent
    /// appends fail with an `io::Error`, simulating a disk-full / EIO / revoked
    /// permission fault on a live WAL (an open fd is not affected by `chmod`, so
    /// this is the deterministic way to inject a write fault).
    #[cfg(test)]
    pub(crate) fn break_writes_for_test(&mut self) {
        self.file = LogAppender::new(
            std::fs::OpenOptions::new()
                .read(true)
                .open(&self.path)
                .expect("reopen WAL read-only"),
        );
    }
}
