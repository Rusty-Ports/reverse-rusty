//! CRC sealing and atomic publication shared by both manifest codecs.

use std::fs::{File, OpenOptions};
use std::io;
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
