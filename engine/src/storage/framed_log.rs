//! Shared cold-path framing for WAL, coordinator/translog, and Raft records (ADR-182).
//! Only incomplete final writes are repairable; complete invalid records fail loud.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use super::crc32;

pub(crate) struct FrameScan<T> {
    pub records: Vec<T>,
    /// Absolute end of the last completely decoded frame, including the file header.
    pub valid_len: usize,
    pub torn_bytes: usize,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn u32_at(data: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid("offset overflow"))?;
    let bytes = data
        .get(offset..end)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| invalid("truncated frame header"))?;
    Ok(u32::from_le_bytes(bytes))
}

/// A damaged length prefix can hide later acknowledged records. Refuse repair when a
/// complete CRC-valid record exists behind it. Bound CRC work to keep hostile length
/// patterns linear in suffix size; an exhausted budget is ambiguous and also refused.
fn validate_torn_suffix(tail: &[u8]) -> io::Result<()> {
    let mut budget = tail.len().saturating_mul(4).max(64);
    for offset in 1..tail.len().saturating_sub(7) {
        let len = u32_at(tail, offset)? as usize;
        if len == 0 {
            continue;
        }
        let start = offset + 8;
        let Some(end) = start.checked_add(len).filter(|&end| end <= tail.len()) else {
            continue;
        };
        if len > budget {
            return Err(invalid(
                "ambiguous log tail exceeds repair validation budget",
            ));
        }
        budget -= len;
        if crc32(&tail[start..end]) == u32_at(tail, offset + 4)? {
            return Err(invalid(
                "damaged log prefix precedes a complete record; refusing repair",
            ));
        }
    }
    Ok(())
}

pub(crate) fn scan_records<T>(
    data: &[u8],
    header_len: usize,
    mut decode: impl FnMut(&[u8]) -> io::Result<T>,
) -> io::Result<FrameScan<T>> {
    if header_len > data.len() {
        return Err(invalid("truncated log header"));
    }
    let mut cursor = header_len;
    let mut records = Vec::new();
    while cursor < data.len() {
        let tail = &data[cursor..];
        // Zero padding cannot represent a record in any of these formats.
        if tail.iter().all(|&byte| byte == 0) {
            break;
        }
        if tail.len() < 8 {
            break;
        }
        let len = u32_at(tail, 0)? as usize;
        let Some(end) = 8usize.checked_add(len).filter(|&end| end <= tail.len()) else {
            validate_torn_suffix(tail)?;
            break;
        };
        let body = &tail[8..end];
        if crc32(body) != u32_at(tail, 4)? {
            return Err(invalid(format!("log CRC mismatch at byte {cursor}")));
        }
        let record = decode(body).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("invalid log record at byte {cursor}: {e}"),
            )
        })?;
        records.push(record);
        cursor += end;
    }
    Ok(FrameScan {
        records,
        valid_len: cursor,
        torn_bytes: data.len() - cursor,
    })
}

/// Called only after the header and all complete records have passed validation.
/// Synchronize the cut before any later append can be acknowledged behind the old tail.
pub(crate) fn repair_tail(path: &Path, scanned_len: usize, valid_len: usize) -> io::Result<()> {
    if valid_len > scanned_len {
        return Err(invalid("invalid log repair boundary"));
    }
    if valid_len == scanned_len {
        return Ok(());
    }
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    if file.metadata()?.len() != scanned_len as u64 {
        return Err(invalid("log length changed during recovery validation"));
    }
    file.set_len(valid_len as u64)?;
    file.sync_all()
}

/// Encode the whole frame before writing; every log uses this one write_all.
pub(crate) fn write_frame(writer: &mut impl Write, body: &[u8]) -> io::Result<usize> {
    let len = u32::try_from(body.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "log record exceeds u32 length")
    })?;
    let capacity = body
        .len()
        .checked_add(8)
        .ok_or_else(|| invalid("frame size overflow"))?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(capacity)
        .map_err(io::Error::other)?;
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32(body).to_le_bytes());
    frame.extend_from_slice(body);
    writer.write_all(&frame)?;
    Ok(frame.len())
}

/// A failed write/flush/sync may leave a partial or ambiguously durable frame. The
/// same handle must never append a later acknowledged frame behind it.
pub(crate) struct LogAppender<W = File> {
    writer: W,
    failed: bool,
}

impl<W: Write> LogAppender<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            failed: false,
        }
    }

    /// A replacement may have published a new inode before a later sync/open fails.
    /// Disable the old handle before replacing its file so it cannot acknowledge
    /// writes to a now-unlinked inode on any failure path.
    pub(crate) fn disable(&mut self) {
        self.failed = true;
    }

    fn healthy(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other(
                "log append disabled after I/O failure; reopen the log",
            ))
        } else {
            Ok(())
        }
    }

    fn append_with(
        &mut self,
        body: &[u8],
        finish: impl FnOnce(&mut W) -> io::Result<()>,
    ) -> io::Result<usize> {
        self.healthy()?;
        let result = write_frame(&mut self.writer, body)
            .and_then(|len| finish(&mut self.writer).map(|()| len));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl LogAppender {
    pub(crate) fn append(&mut self, body: &[u8], fsync: bool) -> io::Result<usize> {
        self.append_with(
            body,
            |file| {
                if fsync {
                    file.sync_all()
                } else {
                    file.flush()
                }
            },
        )
    }

    pub(crate) fn sync_all(&mut self) -> io::Result<()> {
        self.healthy()?;
        let result = self.writer.sync_all();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

/// Strict bounds-checked decoder for complete CRC-valid mutation bodies.
pub(crate) struct RecordReader<'a> {
    remaining: &'a [u8],
}

impl<'a> RecordReader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { remaining: data }
    }
    pub(crate) fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    pub(crate) fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let value = self
            .remaining
            .get(..len)
            .ok_or_else(|| invalid("short record payload"))?;
        self.remaining = &self.remaining[len..];
        Ok(value)
    }
    pub(crate) fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> io::Result<u16> {
        let b = self
            .take(2)?
            .try_into()
            .map_err(|_| invalid("invalid u16"))?;
        Ok(u16::from_le_bytes(b))
    }
    pub(crate) fn u32(&mut self) -> io::Result<u32> {
        let b = self
            .take(4)?
            .try_into()
            .map_err(|_| invalid("invalid u32"))?;
        Ok(u32::from_le_bytes(b))
    }
    pub(crate) fn u64(&mut self) -> io::Result<u64> {
        let b = self
            .take(8)?
            .try_into()
            .map_err(|_| invalid("invalid u64"))?;
        Ok(u64::from_le_bytes(b))
    }
    pub(crate) fn i64(&mut self) -> io::Result<i64> {
        let b = self
            .take(8)?
            .try_into()
            .map_err(|_| invalid("invalid i64"))?;
        Ok(i64::from_le_bytes(b))
    }
    pub(crate) fn string(&mut self, len: usize) -> io::Result<String> {
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    pub(crate) fn finish(self) -> io::Result<()> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(invalid("trailing record payload"))
        }
    }
}

#[cfg(test)]
mod tests;
