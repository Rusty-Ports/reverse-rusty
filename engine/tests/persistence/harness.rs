//! Shared helpers for the persistence test suite: temp-dir setup, the sample
//! query corpus, and the serialize→mmap→match round-trip helper.

use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn test_dir(name: &str) -> PathBuf {
    // Per-invocation unique suffix: the persistence suite runs alongside the other
    // test binaries (cargo schedules them concurrently), and `backup_to` fails loud
    // on a pre-existing dest. A fixed path that relied on a best-effort `remove_dir_all`
    // succeeding raced under that load (stale subdir → `DestExists`), so derive a
    // collision-free directory from pid + a process-local counter instead.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_test_{name}_{}_{unique}",
        std::process::id()
    ));
    // Clean up any residue from a previous run that happened to collide.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn make_norm() -> Normalizer {
    Normalizer::default_vocab().unwrap()
}

pub(crate) fn sample_queries() -> Vec<(u64, String)> {
    vec![
        (1, "wireless mouse 1986 vertex".into()),
        (2, "mechanical keyboard new".into()),
        (3, "noise cancelling headphones pro".into()),
        (4, "air purifier 2011 acme update".into()),
        (5, "product kappa summit chrome premium".into()),
        (6, "portable scanner product alpha new".into()),
        (7, "usb hub contoso silver".into()),
        (8, "smart speaker premium".into()),
        (9, "desk lamp acme chrome".into()),
        (10, "action camera contoso new".into()),
    ]
}

/// Helper: match a title and return sorted logical IDs.
pub(crate) fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

/// Where a durable write can be made to fail.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum StorageFailure {
    /// `segments/` is read-only: the segment file cannot be written.
    SegmentWrite,
    /// The data directory itself is read-only: the segment is written, but the
    /// source sidecar and manifest that would commit it cannot be.
    Commit,
}

#[cfg(unix)]
impl StorageFailure {
    fn blocked_dir(self, dir: &std::path::Path) -> std::path::PathBuf {
        match self {
            StorageFailure::SegmentWrite => dir.join("segments"),
            StorageFailure::Commit => dir.to_path_buf(),
        }
    }

    /// Make the directory read-only; returns the permissions to restore.
    pub(crate) fn block(self, dir: &std::path::Path) -> std::fs::Permissions {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(dir.join("segments")).expect("segments dir");
        let blocked = self.blocked_dir(dir);
        let original = std::fs::metadata(&blocked).unwrap().permissions();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o555)).unwrap();
        original
    }

    pub(crate) fn unblock(self, dir: &std::path::Path, original: std::fs::Permissions) {
        std::fs::set_permissions(self.blocked_dir(dir), original).unwrap();
    }
}
