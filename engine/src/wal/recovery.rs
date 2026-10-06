use super::{
    Path, Wal, WalEntry, WalRecovery, OP_DELETE_LOGICAL, OP_FLUSH_CHECKPOINT, OP_INSERT,
    OP_INSERT_CLASS_D, OP_TOMBSTONE, OP_UPSERT, OP_UPSERT_CLASS_D, SOURCE_GENERATION_MAGIC,
    WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION,
};
use crate::storage::framed_log::{scan_records, FrameScan, RecordReader};
use std::io;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

impl Wal {
    /// Whether `path` is a log whose header was never completely written: shorter than a
    /// header, and every byte it does hold is the byte a header this reader supports has
    /// there.
    ///
    /// A log is created, and was once reset, by truncating the file and then writing the
    /// eight header bytes. A crash between the two left a file like this. It never held a
    /// record: a creation has none yet, and a reset runs only after the manifest covers
    /// every record the old log held. So it is an empty log, not a damaged one. A short
    /// file with any other content is not ours to reinterpret and stays an error.
    pub(super) fn header_was_interrupted(path: &Path) -> io::Result<bool> {
        let len = std::fs::metadata(path)?.len();
        if len >= WAL_HEADER_SIZE as u64 {
            return Ok(false);
        }
        let held = std::fs::read(path)?;
        // The version bytes changed between releases, so any supported header may have been
        // the one in progress. A prefix of no supported header (a later format's, or bytes
        // no header has) is refused like the full header would be.
        Ok((1..=WAL_VERSION).any(|version| Self::header(version).starts_with(&held)))
    }

    /// The eight bytes that open a log of format `version`.
    pub(super) fn header(version: u32) -> [u8; WAL_HEADER_SIZE] {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[..WAL_MAGIC.len()].copy_from_slice(&WAL_MAGIC);
        header[WAL_MAGIC.len()..].copy_from_slice(&version.to_le_bytes());
        header
    }

    /// Validate every complete record before permitting any repair or append.
    pub(super) fn read_entries(path: &Path) -> io::Result<(FrameScan<WalEntry>, u32)> {
        let data = std::fs::read(path)?;
        let header = data
            .get(..WAL_HEADER_SIZE)
            .ok_or_else(|| invalid("WAL too small"))?;
        let mut reader = RecordReader::new(header);
        if reader.take(4)? != WAL_MAGIC {
            return Err(invalid("bad WAL magic"));
        }
        let version = reader.u32()?;
        if version == 0 || version > WAL_VERSION {
            return Err(invalid(format!("unsupported WAL format v{version}")));
        }
        // Older headers may contain mixed later opcodes because legacy writers did
        // not update the header on reopen. All known shapes remain readable.
        let mut previous_seq = 0;
        let scan = scan_records(&data, WAL_HEADER_SIZE, |body| {
            let entry = decode_entry(body)?;
            if entry.seq() <= previous_seq {
                return Err(invalid("WAL sequence numbers must be strictly increasing"));
            }
            previous_seq = entry.seq();
            Ok(entry)
        })?;
        Ok((scan, version))
    }

    /// Return only records after the last materialized FlushCheckpoint. Unknown
    /// or malformed complete frames are errors, not an implicitly discarded tail.
    pub fn recover(path: &Path) -> io::Result<WalRecovery> {
        if Self::header_was_interrupted(path)? {
            return Ok(WalRecovery {
                entries: Vec::new(),
                skipped_bytes: 0,
            });
        }
        let (scan, _) = Self::read_entries(path)?;
        let all = scan.records;
        let last_checkpoint_idx = all
            .iter()
            .rposition(|e| matches!(e, WalEntry::FlushCheckpoint { .. }));
        let entries = match last_checkpoint_idx {
            Some(idx) => all[idx + 1..].to_vec(),
            None => all,
        };
        Ok(WalRecovery {
            entries,
            skipped_bytes: scan.torn_bytes,
        })
    }
}

fn decode_entry(body: &[u8]) -> io::Result<WalEntry> {
    let mut reader = RecordReader::new(body);
    let seq = reader.u64()?;
    let op = reader.u8()?;
    let entry = match op {
        OP_INSERT | OP_UPSERT | OP_INSERT_CLASS_D | OP_UPSERT_CLASS_D => {
            let logical = reader.u64()?;
            let version = reader.u32()?;
            let text_len = reader.u32()? as usize;
            let text = reader.string(text_len)?;
            let mut tags = Vec::new();
            // V1 ends at the text; V2+ carries a complete tag block, even when empty.
            if !reader.remaining().is_empty() {
                let count = reader.u16()?;
                for _ in 0..count {
                    let key_len = usize::from(reader.u16()?);
                    let key = reader.string(key_len)?;
                    let value_len = usize::from(reader.u16()?);
                    let value = reader.string(value_len)?;
                    tags.push((key, value));
                }
            }
            let (source_generation, priority) = match reader.remaining().len() {
                0 => (None, None),
                8 => (None, Some(reader.i64()?)), // V6 optional priority.
                13 | 21 => {
                    if reader.take(4)? != SOURCE_GENERATION_MAGIC {
                        return Err(invalid("unknown WAL payload extension"));
                    }
                    let generation = reader.u64()?;
                    if generation == 0 {
                        return Err(invalid("zero WAL source generation"));
                    }
                    let priority = match reader.u8()? {
                        0 => None,
                        1 => Some(reader.i64()?),
                        _ => return Err(invalid("invalid WAL priority-presence flag")),
                    };
                    (Some(generation), priority)
                }
                _ => return Err(invalid("malformed WAL payload extension")),
            };
            if op == OP_INSERT || op == OP_INSERT_CLASS_D {
                WalEntry::Insert {
                    seq,
                    logical,
                    version,
                    text,
                    tags,
                    priority,
                    source_generation,
                    class_d_accepted: op == OP_INSERT_CLASS_D,
                }
            } else {
                WalEntry::Upsert {
                    seq,
                    logical,
                    version,
                    text,
                    tags,
                    priority,
                    source_generation,
                    class_d_accepted: op == OP_UPSERT_CLASS_D,
                }
            }
        }
        OP_TOMBSTONE => WalEntry::Tombstone {
            seq,
            seg_idx: reader.u32()?,
            local_id: reader.u32()?,
        },
        OP_DELETE_LOGICAL => WalEntry::DeleteByLogical {
            seq,
            logical: reader.u64()?,
        },
        OP_FLUSH_CHECKPOINT => {
            let len = reader.u32()? as usize;
            WalEntry::FlushCheckpoint {
                seq,
                segment_file: reader.string(len)?,
            }
        }
        _ => return Err(invalid(format!("unknown WAL opcode {op}"))),
    };
    reader.finish()?;
    Ok(entry)
}
