//! `shardserver` command line.
//!
//! One function turns the arguments into a [`ShardServerArgs`], so the flags that decide a
//! shard node's durability and storage behaviour can be tested without starting a node.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use reverse_rusty::cluster::{
    DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS, DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
    DEFAULT_MAX_GRPC_REQUEST_BYTES, DEFAULT_MAX_GRPC_RESULT_BYTES,
};
use reverse_rusty::config::EngineConfig;

pub(crate) struct ShardServerArgs {
    /// Start dict-less and wait for a coordinator's `AdoptDict` (ADR-034).
    pub(crate) pending: bool,
    /// `--data-dir <path>` makes the node DURABLE: its shards persist segments there, so it
    /// can serve `FetchSegments` and be a recovering replica (ADR-035/036).
    pub(crate) data_dir: Option<PathBuf>,
    /// The first positional argument; later positionals are ignored.
    pub(crate) addr: Option<String>,
    pub(crate) tls_cert: Option<PathBuf>,
    pub(crate) tls_key: Option<PathBuf>,
    pub(crate) tls_ca: Option<PathBuf>,
    pub(crate) tls_domain: Option<String>,
    pub(crate) token: Option<String>,
    /// Optional SEPARATE plaintext port for the gRPC health service (k8s probes, ADR-084).
    pub(crate) health_addr: Option<SocketAddr>,
    /// Optional SEPARATE plaintext port for the Prometheus `/_metrics` endpoint (ADR-091).
    pub(crate) metrics_addr: Option<SocketAddr>,
    /// Immutable CPU ranking models used by native ranked RPCs. Every remote coordinator
    /// request carries name + semantic fingerprint, so a missing or different local file
    /// fails before scoring.
    pub(crate) ranking_profiles_file: Option<PathBuf>,
    /// Exact protobuf bound for every result-bearing unary reply and each FetchMatches stream
    /// item. The builder enforces the hard 4 MiB ceiling.
    pub(crate) max_grpc_result_bytes: usize,
    /// The largest inbound request this node decodes (ADR-193). It bounds the dictionary a
    /// coordinator can ship; bulk ingest arrives in smaller requests whatever this is.
    pub(crate) max_grpc_request_bytes: usize,
    /// Node-local backpressure bounds for ADR-114 exhaustive streams. These are independent
    /// of the coordinator's HTTP admission because direct mesh callers and multiple
    /// coordinators share this process.
    pub(crate) max_concurrent_exhaustive_streams: usize,
    pub(crate) max_exhaustive_stream_duration: Duration,
    /// The configuration every shard slot on this node is built with (ADR-192). A remote
    /// coordinator ships a dictionary, never engine configuration, so the settings that
    /// belong to the process holding the disk are set here.
    pub(crate) engine: EngineConfig,
}

/// The value after `flag`, or an error naming what it expects.
fn value<'a>(args: &'a [String], at: usize, flag: &str, expects: &str) -> Result<&'a str, String> {
    args.get(at + 1)
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} requires {expects}"))
}

fn parsed<T: std::str::FromStr>(
    args: &[String],
    at: usize,
    flag: &str,
    expects: &str,
) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    let raw = value(args, at, flag, expects)?;
    raw.parse()
        .map_err(|error| format!("{flag} {raw}: {error} (expected {expects})"))
}

pub(crate) fn parse(args: &[String]) -> Result<ShardServerArgs, String> {
    let mut out = ShardServerArgs {
        pending: args.iter().any(|a| a == "--pending"),
        data_dir: None,
        addr: None,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        tls_domain: None,
        token: None,
        health_addr: None,
        metrics_addr: None,
        ranking_profiles_file: None,
        max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        max_grpc_request_bytes: DEFAULT_MAX_GRPC_REQUEST_BYTES,
        max_concurrent_exhaustive_streams: DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
        max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        engine: EngineConfig {
            // Presence flag, like `--pending`: start although the translog is gone (ADR-213).
            accept_lost_log: args.iter().any(|a| a == "--accept-lost-log"),
            ..EngineConfig::default()
        },
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                out.data_dir = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--health-addr" => {
                if let Some(v) = args.get(i + 1) {
                    out.health_addr =
                        Some(v.parse().map_err(|e| format!("--health-addr {v}: {e}"))?);
                }
                i += 1;
            }
            "--metrics-addr" => {
                if let Some(v) = args.get(i + 1) {
                    out.metrics_addr =
                        Some(v.parse().map_err(|e| format!("--metrics-addr {v}: {e}"))?);
                }
                i += 1;
            }
            "--ranking-profiles-file" => {
                out.ranking_profiles_file = Some(PathBuf::from(
                    args.get(i + 1)
                        .ok_or("--ranking-profiles-file requires a path")?,
                ));
                i += 1;
            }
            // The hot-anchor threshold θ (class H, ADR-105). Cost-only: this node CLASSIFIES
            // the queries the coordinator places on it, so θ decides whether a fat-anchored
            // query lands in the always-probed realtime lane (θ=0) or the columnar hot tier.
            // Run the SAME value as the coordinator; divergence can never drop a match (both
            // lanes are always visible), it only decides which node re-inherits the
            // un-quarantined scans.
            "--hot-anchor-threshold" => {
                if let Some(v) = args.get(i + 1) {
                    out.engine.hot_anchor_threshold = v
                        .parse()
                        .map_err(|e| format!("--hot-anchor-threshold {v}: {e}"))?;
                }
                i += 1;
            }
            // Default-on exact sealed-segment tag summaries (ADR-174). A result-preserving
            // read-path optimization that can be disabled at startup.
            "--tag-segment-skipping" => {
                out.engine.tag_segment_skipping =
                    parsed(args, i, "--tag-segment-skipping", "true or false")?;
                i += 1;
            }
            // fsync the shard translog on every write, so an acknowledged write survives a
            // power loss and not only a process crash. Off by default: the translog is then
            // synced at flush checkpoints.
            "--wal-sync-on-write" => {
                out.engine.wal_sync_on_write =
                    parsed(args, i, "--wal-sync-on-write", "true or false")?;
                i += 1;
            }
            "--max-segments" => {
                out.engine.max_segments = parsed(args, i, "--max-segments", "a count")?;
                i += 1;
            }
            "--memtable-flush-threshold" => {
                out.engine.memtable_flush_threshold =
                    parsed(args, i, "--memtable-flush-threshold", "a count")?;
                i += 1;
            }
            // false keeps query source text on disk and reads it on demand (ADR-020).
            "--retain-source" => {
                out.engine.retain_source = parsed(args, i, "--retain-source", "true or false")?;
                i += 1;
            }
            // The broad-lane kill switches (ADR-026): false falls back to the inline per-title
            // probe, or to verifying pure-anchor queries, with identical results.
            "--broad-columnar" => {
                out.engine.broad_columnar = parsed(args, i, "--broad-columnar", "true or false")?;
                i += 1;
            }
            "--broad-materialize" => {
                out.engine.broad_materialize =
                    parsed(args, i, "--broad-materialize", "true or false")?;
                i += 1;
            }
            "--max-grpc-result-bytes" => {
                if let Some(v) = args.get(i + 1) {
                    out.max_grpc_result_bytes = v
                        .parse()
                        .map_err(|e| format!("--max-grpc-result-bytes {v}: {e}"))?;
                }
                i += 1;
            }
            "--max-grpc-request-bytes" => {
                out.max_grpc_request_bytes =
                    parsed(args, i, "--max-grpc-request-bytes", "a byte count")?;
                i += 1;
            }
            "--max-concurrent-exhaustive-streams" => {
                if let Some(v) = args.get(i + 1) {
                    out.max_concurrent_exhaustive_streams = v
                        .parse()
                        .map_err(|e| format!("--max-concurrent-exhaustive-streams {v}: {e}"))?;
                }
                i += 1;
            }
            "--max-exhaustive-stream-secs" => {
                if let Some(v) = args.get(i + 1) {
                    let seconds = v
                        .parse()
                        .map_err(|e| format!("--max-exhaustive-stream-secs {v}: {e}"))?;
                    out.max_exhaustive_stream_duration = Duration::from_secs(seconds);
                }
                i += 1;
            }
            "--tls-cert" => {
                out.tls_cert = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-key" => {
                out.tls_key = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-ca" => {
                out.tls_ca = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-domain" => {
                out.tls_domain = args.get(i + 1).cloned();
                i += 1;
            }
            "--cluster-token" => {
                out.token = args.get(i + 1).cloned();
                i += 1;
            }
            // First positional arg = ADDR; later positionals are ignored.
            a if !a.starts_with("--") && out.addr.is_none() => {
                out.addr = Some(a.to_string());
            }
            _ => {}
        }
        i += 1;
    }
    let problems = out.engine.validate();
    if !problems.is_empty() {
        return Err(format!("invalid engine config: {}", problems.join("; ")));
    }
    Ok(out)
}

/// The settings an operator needs to see at startup to know what this node promises.
pub(crate) fn engine_banner(engine: &EngineConfig, durable: bool) -> String {
    let fsync = match (durable, engine.wal_sync_on_write) {
        (false, _) => "no data dir (nothing survives a restart)",
        (true, true) => "fsync on every write (survives power loss)",
        (true, false) => "fsync at flush checkpoints (survives a process crash)",
    };
    format!(
        "translog {fsync}; retain-source {}; max-segments {}; memtable-flush-threshold {}",
        engine.retain_source, engine.max_segments, engine.memtable_flush_threshold
    )
}

#[cfg(test)]
mod tests {
    use super::{engine_banner, parse};
    use reverse_rusty::config::EngineConfig;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_lost_translog_is_accepted_only_when_asked() {
        let default = parse(&args(&["0.0.0.0:50051", "--data-dir", "/data"])).expect("valid");
        assert!(!default.engine.accept_lost_log);
        let asked = parse(&args(&[
            "0.0.0.0:50051",
            "--data-dir",
            "/data",
            "--accept-lost-log",
        ]))
        .expect("valid");
        assert!(asked.engine.accept_lost_log);
        assert_eq!(asked.addr.as_deref(), Some("0.0.0.0:50051"));
    }

    #[test]
    fn engine_flags_reach_the_engine_config() {
        let parsed = parse(&args(&[
            "0.0.0.0:50051",
            "--pending",
            "--data-dir",
            "/data",
            "--wal-sync-on-write",
            "true",
            "--max-segments",
            "4",
            "--memtable-flush-threshold",
            "500",
            "--retain-source",
            "false",
            "--hot-anchor-threshold",
            "7",
            "--tag-segment-skipping",
            "false",
            "--broad-columnar",
            "false",
            "--broad-materialize",
            "false",
        ]))
        .expect("valid arguments");
        assert!(parsed.pending);
        assert_eq!(parsed.addr.as_deref(), Some("0.0.0.0:50051"));
        assert_eq!(
            parsed.data_dir.as_deref(),
            Some(std::path::Path::new("/data"))
        );
        let engine = parsed.engine;
        assert!(engine.wal_sync_on_write);
        assert_eq!(engine.max_segments, 4);
        assert_eq!(engine.memtable_flush_threshold, 500);
        assert!(!engine.retain_source);
        assert_eq!(engine.hot_anchor_threshold, 7);
        assert!(!engine.tag_segment_skipping);
        assert!(!engine.broad_columnar);
        assert!(!engine.broad_materialize);
    }

    #[test]
    fn the_request_limit_is_a_flag_with_a_default() {
        use reverse_rusty::cluster::DEFAULT_MAX_GRPC_REQUEST_BYTES;
        let parsed = parse(&args(&["--pending"])).expect("valid arguments");
        assert_eq!(
            parsed.max_grpc_request_bytes,
            DEFAULT_MAX_GRPC_REQUEST_BYTES
        );
        let parsed = parse(&args(&["--max-grpc-request-bytes", "134217728"])).expect("valid");
        assert_eq!(parsed.max_grpc_request_bytes, 134_217_728);
        for bad in [
            &["--max-grpc-request-bytes"][..],
            &["--max-grpc-request-bytes", "64MiB"],
        ] {
            let error = parse(&args(bad)).err();
            assert!(
                error
                    .as_deref()
                    .is_some_and(|e| e.contains("--max-grpc-request-bytes")),
                "{bad:?}: {error:?}"
            );
        }
    }

    #[test]
    fn no_engine_flag_means_the_engine_defaults() {
        let parsed = parse(&args(&["--pending"])).expect("valid arguments");
        let (engine, defaults) = (parsed.engine, EngineConfig::default());
        assert_eq!(engine.wal_sync_on_write, defaults.wal_sync_on_write);
        assert_eq!(engine.max_segments, defaults.max_segments);
        assert_eq!(
            engine.memtable_flush_threshold,
            defaults.memtable_flush_threshold
        );
        assert_eq!(engine.retain_source, defaults.retain_source);
        assert_eq!(engine.broad_columnar, defaults.broad_columnar);
        assert_eq!(engine.broad_materialize, defaults.broad_materialize);
    }

    #[test]
    fn a_durability_flag_that_cannot_be_read_is_an_error() {
        for bad in [
            &["--wal-sync-on-write"][..],
            &["--wal-sync-on-write", "yes"],
            &["--retain-source", "0"],
            &["--broad-columnar"],
            &["--broad-materialize", "off"],
            &["--max-segments", "many"],
            &["--memtable-flush-threshold"],
        ] {
            let error = parse(&args(bad)).err();
            assert!(
                error.as_deref().is_some_and(|e| e.contains(bad[0])),
                "{bad:?} must be refused and named, got {error:?}"
            );
        }
    }

    #[test]
    fn values_the_engine_rejects_are_an_error() {
        for bad in [
            &["--max-segments", "0"][..],
            &["--memtable-flush-threshold", "0"],
        ] {
            let error = parse(&args(bad)).err();
            assert!(
                error
                    .as_deref()
                    .is_some_and(|e| e.contains("invalid engine config")),
                "{bad:?}: {error:?}"
            );
        }
    }

    #[test]
    fn the_banner_states_what_survives() {
        let mut engine = EngineConfig::default();
        assert!(engine_banner(&engine, true).contains("survives a process crash"));
        engine.wal_sync_on_write = true;
        assert!(engine_banner(&engine, true).contains("survives power loss"));
        assert!(engine_banner(&engine, false).contains("nothing survives a restart"));
    }
}
