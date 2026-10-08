//! Persistence + on-disk formats. The concrete codecs live in submodules; this
//! root holds the shared low-level binary primitives (CRC-32, atomic rename, and
//! little-endian scalar read/write) and re-exports the public API so callers keep
//! using `crate::storage::{…}` unchanged.
//!
//! - [`segment`] — the `.seg` segment file format (`write_segment` + the mmap-backed
//!   `MmapSegment` read view, ADR-012)
//! - [`dict`] — feature-dictionary (de)serialization (stored inside the manifests)
//! - [`manifest`] — the engine manifest codec plus the coordinator codec,
//!   shared CRC-sealed atomic publisher, and format/recovery tests in submodules
//! - [`sources`] — the per-query source-text store (`SourceStore`, ADR-020 Item 1)
//! - [`backup`] — manifest-driven atomic directory snapshot (ADR-079); restore is
//!   the existing `Engine::open` / `ClusterEngine::open`
//!
//! All multi-byte values are little-endian; integrity is a trailing CRC-32 plus
//! write-to-tmp + atomic rename (`durable_rename`).

use std::io::{self, Write};
use std::path::{Component, Path};

mod backup;
mod dict;
pub(crate) mod framed_log;
pub(crate) mod log_loss;
mod manifest;
mod segment;
mod sources;
mod tagdict;

pub use backup::{
    copy_cluster_dir, copy_engine_dir, verify_backup, verify_cluster_backup, BackupError,
};
pub use dict::{deserialize_dict, serialize_dict};
pub use log_loss::{accepted_log_losses, AcceptedLogLoss, ACCEPTED_LOG_LOSSES_FILE};
pub(crate) use manifest::write_manifest_reporting;
pub use manifest::{
    read_cluster_manifest, read_manifest, write_cluster_manifest, write_manifest, ClusterManifest,
    Manifest,
};
pub(crate) use segment::CURRENT_COMPILER_SEMANTICS_VERSION;
pub use segment::{write_segment, MmapSegment};
pub use sources::{load_query_sources, LazyBase, SourceStore, StoredSource};
pub use tagdict::{deserialize_tagdict, serialize_tagdict};

/// Validate an untrusted durable sidecar basename before joining it beneath a
/// data directory. Commit documents may select generations, but never paths.
pub(crate) fn validate_sidecar_basename(name: &str) -> io::Result<()> {
    let mut components = Path::new(name).components();
    let safe = matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
        && !name.as_bytes().contains(&0)
        && name.len() <= 255;
    if safe {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid sidecar filename {name:?}: expected one safe basename"),
        ))
    }
}

// ---- shared low-level binary primitives (used by the codec submodules) ----

/// CRC-32 (the standard reflected polynomial, as in zlib and Ethernet) of `data`. Every
/// durable file is checked with it: the manifest, segments, the source sidecar, the WAL, the
/// translog and the control store.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update_slice(data);
    crc.finish()
}

/// The reflected CRC-32 polynomial.
const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Lookup tables for eight bytes at a time. `CRC32_TABLES[0]` is the classic one-byte
/// table; `CRC32_TABLES[k][b]` is the effect of byte `b` followed by `k` zero bytes.
const CRC32_TABLES: [[u32; 256]; 8] = crc32_tables();

const fn crc32_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32_POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][byte] = crc;
        byte += 1;
    }
    let mut table = 1;
    while table < 8 {
        let mut byte = 0;
        while byte < 256 {
            let previous = tables[table - 1][byte];
            tables[table][byte] = (previous >> 8) ^ tables[0][(previous & 0xFF) as usize];
            byte += 1;
        }
        table += 1;
    }
    tables
}

/// Incremental form lets recovery recognize a complete payload even when its
/// length is damaged and an incomplete/padded suffix follows it.
struct Crc32(u32);

impl Crc32 {
    fn new() -> Self {
        Self(0xFFFF_FFFF)
    }

    #[inline]
    fn update(&mut self, byte: u8) {
        self.0 = CRC32_TABLES[0][((self.0 ^ u32::from(byte)) & 0xFF) as usize] ^ (self.0 >> 8);
    }

    /// The same result as [`update`](Self::update) on each byte in turn, eight bytes per step.
    fn update_slice(&mut self, data: &[u8]) {
        let mut chunks = data.chunks_exact(8);
        for chunk in &mut chunks {
            let low = self.0 ^ u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            self.0 = CRC32_TABLES[7][(low & 0xFF) as usize]
                ^ CRC32_TABLES[6][((low >> 8) & 0xFF) as usize]
                ^ CRC32_TABLES[5][((low >> 16) & 0xFF) as usize]
                ^ CRC32_TABLES[4][(low >> 24) as usize]
                ^ CRC32_TABLES[3][usize::from(chunk[4])]
                ^ CRC32_TABLES[2][usize::from(chunk[5])]
                ^ CRC32_TABLES[1][usize::from(chunk[6])]
                ^ CRC32_TABLES[0][usize::from(chunk[7])];
        }
        for &byte in chunks.remainder() {
            self.update(byte);
        }
    }

    fn finish(&self) -> u32 {
        !self.0
    }
}

/// What a rename that publishes a file achieved.
#[derive(Debug)]
pub(crate) enum Published {
    /// The file is in place and the directory was synced.
    Synced,
    /// The file is in place, and the directory sync after the rename failed. Every reader
    /// from now on reads the new file, a restart included. Whether it survives a power
    /// loss is not known: the file it replaced may be the one that comes back (ADR-222).
    NotSynced(io::Error),
}

/// Rename `from` over `to` and sync the directory, saying which of the two a failure was.
/// `Err` is a rename that did not happen: `to` is as it was.
pub(crate) fn publish_by_rename(from: &Path, to: &Path) -> io::Result<Published> {
    crate::fault::step("rename", to)?;
    if let Err(error) = std::fs::rename(from, to) {
        return if rename_took_effect(from, to) {
            Ok(Published::NotSynced(error))
        } else {
            Err(error)
        };
    }
    Ok(match crate::fault::sync_dir_of(to) {
        Ok(()) => Published::Synced,
        Err(error) => Published::NotSynced(error),
    })
}

/// Whether a rename of `from` over `to` that reported an error happened all the same. A
/// rename can take effect and still report an error (a network filesystem that loses the
/// reply); the source is gone exactly when it did. Anything that cannot be told is a rename
/// that did not happen only when the source is seen to be there.
fn rename_took_effect(from: &Path, to: &Path) -> bool {
    matches!(from.try_exists(), Ok(false)) && to.exists()
}

/// Atomic rename with parent-directory fsync for crash durability. An error does not say
/// whether the rename happened; a caller that must know uses [`publish_by_rename`].
pub(crate) fn durable_rename(from: &Path, to: &Path) -> io::Result<()> {
    match publish_by_rename(from, to)? {
        Published::Synced => Ok(()),
        Published::NotSynced(error) => Err(error),
    }
}

/// The directory whose entry names `path`. A bare file name has an empty parent, which
/// cannot be opened; the entry is in the working directory.
pub(crate) fn directory_of(path: &Path) -> Option<&Path> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        return Some(Path::new("."));
    }
    Some(parent)
}

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn read_u16_at(data: &[u8], off: usize) -> io::Result<u16> {
    let b: [u8; 2] = data
        .get(off..off + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u16"))?;
    Ok(u16::from_le_bytes(b))
}

fn read_u32_at(data: &[u8], off: usize) -> io::Result<u32> {
    let b: [u8; 4] = data
        .get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u32"))?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_at(data: &[u8], off: usize) -> io::Result<u64> {
    let b: [u8; 8] = data
        .get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u64"))?;
    Ok(u64::from_le_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::{
        crc32, directory_of, publish_by_rename, rename_took_effect, Crc32, Path, Published,
        CRC32_POLYNOMIAL,
    };

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rr_publish_by_rename_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        dir
    }

    /// The three outcomes of a publication are told apart: not renamed, renamed and
    /// synced, renamed and not synced.
    #[test]
    fn a_publication_says_whether_the_rename_happened() {
        let dir = scratch("outcomes");
        let (tmp, file) = (dir.join("manifest.tmp"), dir.join("manifest.bin"));
        std::fs::write(&file, b"old").expect("the old file");
        let rename = crate::fault::Step {
            name: "rename",
            path: "manifest.bin".into(),
        };
        let sync = crate::fault::Step {
            name: "sync_dir",
            path: "manifest.bin".into(),
        };
        let scope = crate::fault::Scope::open(&dir);

        std::fs::write(&tmp, b"new").expect("the replacement");
        scope.fail(&rename, 0);
        assert!(publish_by_rename(&tmp, &file).is_err(), "not renamed");
        assert_eq!(std::fs::read(&file).expect("read"), b"old");
        assert!(tmp.exists(), "the replacement is still beside it");

        scope.fail(&sync, 0);
        assert!(matches!(
            publish_by_rename(&tmp, &file),
            Ok(Published::NotSynced(_))
        ));
        assert_eq!(std::fs::read(&file).expect("read"), b"new");

        std::fs::write(&tmp, b"newer").expect("the next replacement");
        assert!(matches!(
            publish_by_rename(&tmp, &file),
            Ok(Published::Synced)
        ));
        assert_eq!(std::fs::read(&file).expect("read"), b"newer");
        drop(scope);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A rename that reported an error happened when its source is gone and its target is
    /// there, and did not when the source is still there.
    #[test]
    fn a_rename_that_reported_an_error_is_judged_by_what_is_on_disk() {
        let dir = scratch("took_effect");
        let (from, to) = (dir.join("a.tmp"), dir.join("a"));
        std::fs::write(&from, b"x").expect("source");
        assert!(!rename_took_effect(&from, &to), "nothing moved");
        std::fs::write(&to, b"y").expect("target");
        assert!(!rename_took_effect(&from, &to), "the source is still there");
        std::fs::remove_file(&from).expect("the source is gone");
        assert!(rename_took_effect(&from, &to));
        std::fs::remove_file(&to).expect("and the target too");
        assert!(!rename_took_effect(&from, &to), "neither is there");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The definition, one bit at a time: what every file on disk was checksummed with
    /// before the tables, and what the tables must reproduce exactly.
    fn bitwise_crc32(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &byte in data {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ CRC32_POLYNOMIAL
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    #[test]
    fn crc32_is_the_standard_checksum() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    /// Checksums written by earlier releases must still verify, so the table-driven
    /// checksum has to equal the bitwise one for every length and alignment, including the
    /// tail shorter than eight bytes.
    #[test]
    fn the_tables_reproduce_the_bitwise_checksum() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut bytes = Vec::with_capacity(4_100);
        for _ in 0..4_100 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            bytes.push((state >> 56) as u8);
        }
        for len in (0..=300).chain([1_023, 1_024, 1_025, 4_096, 4_099, 4_100]) {
            for start in 0..(4_100 - len).min(9) {
                let data = &bytes[start..start + len];
                assert_eq!(crc32(data), bitwise_crc32(data), "len {len} start {start}");
            }
        }
    }

    /// Not a gate: prints the throughput of the bitwise definition and of the tables.
    /// `cargo test --release --lib storage::tests::crc32_throughput -- --ignored --nocapture`
    #[test]
    #[ignore = "a measurement, run by hand in release mode"]
    fn crc32_throughput() {
        let data: Vec<u8> = (0..64 * 1024 * 1024u32)
            .map(|n| (n * 31 + 7) as u8)
            .collect();
        let megabytes = data.len() as f64 / (1024.0 * 1024.0);
        let started = std::time::Instant::now();
        let reference = bitwise_crc32(&data);
        let bitwise = megabytes / started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        let tabled = crc32(&data);
        let tables = megabytes / started.elapsed().as_secs_f64();
        assert_eq!(reference, tabled);
        println!("bitwise {bitwise:.0} MB/s, tables {tables:.0} MB/s");
    }

    /// Recovery feeds bytes one at a time and checks the running value; that path and the
    /// whole-slice path must agree at every prefix.
    #[test]
    fn one_byte_at_a_time_agrees_with_the_slice_at_every_prefix() {
        let data: Vec<u8> = (0..200u32).map(|n| (n * 37 + 11) as u8).collect();
        let mut running = Crc32::new();
        for (index, &byte) in data.iter().enumerate() {
            running.update(byte);
            assert_eq!(running.finish(), crc32(&data[..=index]), "prefix {index}");
        }
    }

    /// `Path::parent` of a bare file name is the empty path, which cannot be opened. The
    /// rename had already happened by then, so the caller saw an error for a file that was
    /// in place.
    #[test]
    fn a_bare_file_name_is_synced_through_the_working_directory() {
        assert_eq!(directory_of(Path::new("wal.log")), Some(Path::new(".")));
        assert_eq!(
            directory_of(Path::new("data/wal.log")),
            Some(Path::new("data"))
        );
        assert_eq!(
            directory_of(Path::new("/var/lib/rr/wal.log")),
            Some(Path::new("/var/lib/rr"))
        );
        assert_eq!(directory_of(Path::new("/")), None);
    }
}
