//! Counters for complete writes of the stored-document sidecar (ADR-200).

use prometheus::{Counter, IntCounter, Opts, Registry};

/// One sidecar write: every stored document, whatever the size of the change behind it.
/// `flush_time_seconds_total` stops before this write, so it is counted on its own.
#[derive(Clone)]
pub(crate) struct SourceCommitMetrics {
    commits: IntCounter,
    bytes: IntCounter,
    seconds: Counter,
}

impl SourceCommitMetrics {
    pub(super) fn register(registry: &Registry) -> Self {
        let commits = IntCounter::with_opts(Opts::new(
            "source_commits_total",
            "Complete writes of the query-source corpus to a sidecar file",
        ))
        .unwrap();
        let bytes = IntCounter::with_opts(Opts::new(
            "source_commit_bytes_total",
            "Cumulative bytes of query-source sidecar files written",
        ))
        .unwrap();
        let seconds = Counter::with_opts(Opts::new(
            "source_commit_time_seconds_total",
            "Cumulative wall-clock seconds spent writing query-source sidecar files",
        ))
        .unwrap();
        registry.register(Box::new(commits.clone())).unwrap();
        registry.register(Box::new(bytes.clone())).unwrap();
        registry.register(Box::new(seconds.clone())).unwrap();
        Self {
            commits,
            bytes,
            seconds,
        }
    }

    pub(super) fn observe(&self, bytes: u64, duration_secs: f64) {
        self.commits.inc();
        self.bytes.inc_by(bytes);
        self.seconds.inc_by(duration_secs);
    }
}
