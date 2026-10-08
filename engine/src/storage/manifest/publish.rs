//! CRC sealing and atomic publication shared by both manifest codecs.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use super::super::{crc32, publish_by_rename, write_u32, Published};

/// Publish one encoded manifest body as the sole durable commit point.
///
/// The codec is responsible only for writing its body. This boundary preserves
/// the required ordering: body fsync, read-back CRC, CRC fsync, then rename plus
/// parent-directory fsync.
pub(super) fn publish_with_crc(
    path: &Path,
    tmp: &Path,
    encode_body: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    match publish_with_crc_reporting(path, tmp, encode_body)? {
        Published::Synced => Ok(()),
        Published::NotSynced(error) => Err(error),
    }
}

/// [`publish_with_crc`], saying whether a failure came before the rename (`Err`: the old
/// manifest is in place) or after it (`Ok(Published::NotSynced)`: the new one is).
pub(super) fn publish_with_crc_reporting(
    path: &Path,
    tmp: &Path,
    encode_body: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<Published> {
    crate::fault::step("create", tmp)?;
    let mut file = File::create(tmp)?;
    encode_body(&mut file)?;
    crate::fault::sync(&file, tmp)?;
    drop(file);

    let content = std::fs::read(tmp)?;
    let crc = crc32(&content);
    let mut file = OpenOptions::new().append(true).open(tmp)?;
    write_u32(&mut file, crc)?;
    crate::fault::sync(&file, tmp)?;
    drop(file);

    publish_by_rename(tmp, path)
}

/// Publish `bytes`, which are the manifest at `path`, again: a new file, a new rename, a new
/// directory sync (ADR-222).
///
/// The publication that put the manifest there may have ended at a directory sync that
/// failed, in this process or in the one before it. A restart does not clear what the
/// kernel holds about that failure, and a sync issued now over the same state can report
/// success without having written it. A publication issued again does not depend on it:
/// when this returns `Ok`, the manifest a power loss leaves has these bytes.
pub(super) fn publish_again(path: &Path, tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    crate::fault::step("create", tmp)?;
    let mut file = File::create(tmp)?;
    file.write_all(bytes)?;
    crate::fault::sync(&file, tmp)?;
    drop(file);
    crate::storage::durable_rename(tmp, path)
}
