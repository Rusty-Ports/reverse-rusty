//! Reverse Rusty HTTP server — Elasticsearch-inspired REST API.
//!
//! Endpoints:
//!   PUT  /_doc/{id}          Register a query (body: {"query": "..."})
//!   GET  /_doc/{id}          Retrieve query source, version, and canonical tags
//!   HEAD /_doc/{id}          Bodyless existence check
//!   DELETE /_doc/{id}        Remove a stored query
//!   GET|POST /_search        Percolate title(s) (body: {"document": {"title": "..."}} or "documents")
//!   POST /_mpercolate        Batch percolate (body: {"documents":[...]}, responses[] envelope)
//!   POST /_bulk              NDJSON bulk ingest ({action}\n{source}\n...)
//!   GET/POST /_flush         Flush memtable to immutable segment
//!   POST /_compact           Force compaction
//!   POST /_forcemerge        ES/OpenSearch-familiar force merge
//!   POST /_backup            Snapshot durable state to a dir (body: {"dest":"..."})
//!   GET  /_stats             JSON metrics snapshot
//!   GET  /_cat/stats         Human-readable metrics
//!   GET  /_cat/segments      Per-segment LSM detail (text table; ?format=json)
//!   GET/HEAD /_health        Native readiness
//!   GET/HEAD /_metrics       Prometheus text exposition format
//!   GET/HEAD /_vocab         Current vocabulary as JSON / bodyless metadata
//!   PUT  /_vocab             Replace vocabulary (body: Vocab JSON)
//!   POST /_vocab/learn       Learn synonyms from raw query text (returns them)
//!   POST /_vocab/learn_and_apply  Learn synonyms from stored queries + apply (?min_count=N)
//!   GET/HEAD /_settings      Engine settings as JSON (?include_defaults=true)
//!   PUT  /_settings          Update dynamic settings (body: flat JSON, e.g. {"max_segments":16})
//!
//! Usage:
//!   cargo run --release --bin server -- [--port 9200] [--data-dir ./data] [--load-file queries.csv]
//!
//! The engine uses a snapshot-based concurrency model: a `Mutex<Engine>` for
//! serialized writes and an `ArcSwap<EngineSnapshot>` for lock-free reads.
//! Search and other read endpoints load the snapshot without any lock;
//! writes acquire the mutex, mutate the engine, then atomically publish a
//! new snapshot.
//!
//! Module layout (this file is the entry point — CLI parse, engine build, router
//! wiring, graceful shutdown):
//!   * [`cli`]     — command-line flags ([`cli::Cli`]).
//!   * [`auth`]    — opt-in bearer-token gate for mutating/admin endpoints (ADR-062).
//!   * [`metrics`] — Prometheus registry + the `EngineEvent` → counter bridge.
//!   * [`state`]   — [`state::AppState`] + the request-id / in-flight middleware.
//!   * [`dto`]     — response types shared across handlers (errors, `_source`).
//!   * [`handlers`] — the endpoint handlers, grouped by family (doc/search/admin/vocab),
//!     each owning its endpoint-specific request/response DTOs.

mod auth;
mod cli;
mod cluster_mode;
mod dto;
mod handlers;
mod jobs;
mod metrics;
mod pit;
mod preload;
mod resize_ops;
mod router;
mod state;
mod vocab_seed;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use clap::Parser;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::EngineEvent;
use reverse_rusty::loader;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;

use cli::Cli;
use metrics::PrometheusMetrics;
use state::AppState;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize structured logging.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match cli.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(false)
                .with_line_number(false)
                .with_env_filter(env_filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_target(false)
                .with_env_filter(env_filter)
                .init();
        }
    }

    info!(
        port = cli.port,
        data_dir = ?cli.data_dir,
        log_format = %cli.log_format,
        drain_timeout = cli.drain_timeout,
        "starting reverse-rusty server"
    );

    // Resolve bearer-token auth (ADR-062). Fail loud on an invalid config —
    // never fall back to silently serving open. The token itself is never
    // logged.
    let auth_config = match auth::AuthConfig::resolve(
        cli.auth_token.clone(),
        std::env::var("RR_AUTH_TOKEN"),
        cli.auth_protect_reads,
    ) {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "invalid auth configuration");
            std::process::exit(1);
        }
    };
    match &auth_config {
        Some(a) if a.protect_reads => {
            info!("bearer-token auth enabled (all requests except GET/HEAD /_health)");
        }
        Some(_) => info!("bearer-token auth enabled (mutating/admin endpoints)"),
        None => {
            if !cli.host.is_loopback() {
                warn!(
                    host = %cli.host,
                    "binding a non-loopback interface without auth: mutating endpoints are \
                     open to the network (set RR_AUTH_TOKEN/--auth-token or front with an \
                     authenticating reverse proxy)"
                );
            }
        }
    }

    let ranking_profiles_file = cli.ranking_profiles_file.clone().or_else(|| {
        std::env::var_os("RR_RANKING_PROFILES_FILE")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    });
    let rank_profiles = match &ranking_profiles_file {
        Some(path) => match reverse_rusty::RankProfiles::load_json(path) {
            Ok(profiles) => profiles,
            Err(error) => {
                error!(path = ?path, error = %error, "invalid ranking profile configuration");
                std::process::exit(1);
            }
        },
        None => reverse_rusty::RankProfiles::default(),
    };
    let mut profile_descriptions: Vec<_> = rank_profiles
        .names()
        .map(|name| {
            let fingerprint = rank_profiles.fingerprint(name).map_or(0, |value| value);
            format!("{name}@fnv1a64:{fingerprint:016x}")
        })
        .collect();
    profile_descriptions.sort_unstable();
    info!(
        path = ?ranking_profiles_file,
        profiles = ?profile_descriptions,
        "ranking profiles active"
    );
    let rank_profiles = Arc::new(rank_profiles);

    // Coordinator (cluster) mode: the same REST dialect over a ClusterEngine
    // (ADR-070). Everything below this branch is the single-node path.
    if cli.cluster {
        cluster_mode::run(cli, auth_config, rank_profiles).await;
        return;
    }
    if !cli.shard_endpoint.is_empty() {
        error!("--shard-endpoint requires --cluster (coordinator mode)");
        std::process::exit(1);
    }

    // Build engine config from CLI flags.
    let config = EngineConfig {
        data_dir: cli.data_dir.clone(),
        ..cli.engine_config()
    };
    if cli.retain_source_saves_nothing() {
        warn!(
            "--retain-source false without --data-dir saves nothing: source text is kept on \
             disk only when there is a data directory"
        );
    }
    let problems = config.validate();
    if !problems.is_empty() {
        for p in &problems {
            error!(problem = %p, "invalid engine config");
        }
        std::process::exit(1);
    }
    if config.data_dir.is_none() {
        warn!("no --data-dir specified: engine is in-memory only, data will not survive restarts");
    }

    // Load vocab file if provided, otherwise use minimal (domain-free) normalizer.
    let vocab = if let Some(ref path) = cli.vocab_file {
        info!(path = ?path, "loading vocabulary from file");
        let v = reverse_rusty::vocab::Vocab::load_json(path).expect("failed to read vocab file");
        let aliases = v.alias_summary();
        info!(
            synonyms = v.synonyms().len(),
            phrases = v.phrases().len(),
            equivalences = v.equivalences().len(),
            aliases_active = aliases.active,
            aliases_candidate = aliases.candidate,
            aliases_rejected = aliases.rejected,
            "vocabulary loaded"
        );
        Some(v)
    } else {
        None
    };

    let mut engine = if let Some(data_dir) = cli.data_dir.as_ref() {
        // ADR-184: the manifest records the feature model, so --vocab-file only seeds a store.
        // A committed vocabulary is authoritative; `open_seeded` restores it (installing its
        // equivalences before the WAL tail replays), and a mismatched model fails loud.
        let seed_supplied = vocab.is_some();
        match Engine::open_seeded(vocab, config.clone()) {
            Ok((e, outcome)) => {
                info!(data_dir = ?data_dir, "recovered engine from persistence");
                vocab_seed::log_outcome(outcome, seed_supplied);
                e
            }
            Err(e) => {
                // Engine::open returns Ok for a genuinely empty/new data dir, so an
                // error here is real corruption, an I/O failure, or a feature-model
                // mismatch — never "no data". Refuse to start rather than silently
                // overwriting recoverable data with a fresh (empty) engine.
                error!(
                    data_dir = ?data_dir,
                    error = %e,
                    "failed to open existing data directory; refusing to start to \
                     avoid overwriting recoverable data"
                );
                std::process::exit(1);
            }
        }
    } else if let Some(v) = vocab {
        Engine::with_vocab(v, config).expect("failed to build engine from vocab")
    } else {
        let norm = Normalizer::default_vocab().expect("failed to build normalizer");
        Engine::with_config(norm, config)
    };

    // Create Prometheus metrics and wire the engine observer.
    let prom = PrometheusMetrics::new();
    let prom_for_observer = prom.clone();
    engine.set_observer(move |event: &EngineEvent| {
        // Increment Prometheus counters.
        prom_for_observer.observe_event(event);

        // Emit structured tracing events.
        match event {
            EngineEvent::Flush {
                entries,
                base_segments_after,
                ..
            } => {
                info!(
                    entries = entries,
                    base_segments_after = base_segments_after,
                    "engine.flush"
                );
            }
            EngineEvent::Ingest {
                ingested,
                rejected_parse,
                rejected_class_d,
                base_segments_after,
            } => {
                info!(
                    ingested = ingested,
                    rejected_parse = rejected_parse,
                    rejected_class_d = rejected_class_d,
                    base_segments_after = base_segments_after,
                    "engine.ingest"
                );
            }
            EngineEvent::Compaction {
                report,
                trigger,
                base_segments_after,
                ..
            } => {
                info!(
                    segments_merged = report.segments_merged,
                    entries_before = report.entries_before,
                    entries_after = report.entries_after,
                    tombstones_reclaimed = report.tombstones_reclaimed,
                    reanchored = report.reanchored,
                    trigger = ?trigger,
                    base_segments_after = base_segments_after,
                    "engine.compaction"
                );
            }
            EngineEvent::SegmentCleanupFailed { path, error } => {
                warn!(
                    path = ?path,
                    error = %error,
                    "engine.segment_cleanup_failed: leaked segment file on disk"
                );
            }
            EngineEvent::DurabilityFailure { op, detail, error } => {
                // Data-at-risk failures (lost/uncommitted match data) are errors
                // worth paging on; display-only and benign-recovery failures are
                // warnings. See DurabilityOp::is_data_at_risk.
                if op.is_data_at_risk() {
                    error!(
                        op = op.as_str(),
                        detail = %detail,
                        error = %error,
                        "engine.durability_failure: durability degraded"
                    );
                } else {
                    warn!(
                        op = op.as_str(),
                        detail = %detail,
                        error = %error,
                        "engine.durability_failure"
                    );
                }
            }
        }
    });

    // Pre-load queries from file if specified.
    if let Some(ref path) = cli.load_file {
        info!(path = ?path, "loading queries from file");
        let start = Instant::now();
        let result = loader::load_file(path).expect("failed to read query file");
        if !result.errors.is_empty() {
            warn!(
                error_count = result.errors.len(),
                first_error = %result.errors.first().map(std::string::ToString::to_string).unwrap_or_default(),
                "query file had load errors"
            );
        }
        if !result.queries.is_empty() {
            // All-or-nothing: if the initial load can't be durably persisted,
            // fail fast rather than silently serve an empty/non-durable engine.
            match preload::preload_queries(&mut engine, &result.queries) {
                Ok(preload::Preload::Loaded(report)) => {
                    let elapsed = start.elapsed();
                    info!(
                        ingested = report.ingested,
                        rejected_parse = report.rejected_parse,
                        rejected_class_d = report.rejected_class_d,
                        elapsed_ms = format!("{:.1}", elapsed.as_secs_f64() * 1000.0),
                        "query file loaded"
                    );
                }
                Ok(preload::Preload::SkippedPopulated { existing }) => warn!(
                    existing = existing,
                    "skipping --load-file: the reopened data directory is already populated"
                ),
                Err(e) => {
                    error!(error = %e, "initial query load could not be durably persisted; aborting startup");
                    std::process::exit(1);
                }
            }
        }
    }

    // Build rayon pool.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads.unwrap_or(0)) // 0 = default (physical cores)
        .build()
        .expect("failed to build rayon thread pool");

    let drain_timeout = cli.drain_timeout;
    let slow_threshold = cli.slow_query_threshold_ms;
    let initial_snapshot = Arc::new(engine.snapshot());
    let ranked_workers = pool.current_num_threads().max(1);
    let exhaustive_jobs = jobs::ExhaustiveJobs::new(
        jobs::ExhaustiveJobConfig {
            threads: cli.exhaustive_threads,
            max_concurrent: cli.max_concurrent_exhaustive_jobs,
            chunk_size: cli.exhaustive_chunk_size,
            channel_depth: cli.exhaustive_channel_depth,
            max_timeout: std::time::Duration::from_secs(cli.exhaustive_job_timeout_secs),
            max_retained: cli.max_retained_exhaustive_jobs,
        },
        prom.clone(),
    )
    .unwrap_or_else(|reason| {
        error!(%reason, "invalid exhaustive-job configuration");
        std::process::exit(1);
    });
    let state = Arc::new(AppState {
        engine: Mutex::new(engine),
        flush_serial: Mutex::new(()),
        write_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_QUEUED_WRITES,
        )),
        backup_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_BACKUPS,
        )),
        health_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        snapshot: ArcSwap::new(initial_snapshot),
        pool,
        search_permits: (cli.max_concurrent_searches > 0)
            .then(|| std::sync::Arc::new(tokio::sync::Semaphore::new(cli.max_concurrent_searches))),
        ranked_search_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(ranked_workers)),
        exhaustive_jobs,
        rank_profiles,
        max_ranked_enrichment_bytes: cli.max_ranked_enrichment_bytes,
        include_broad: cli.include_broad,
        prom,
        slow_query_threshold_ms: slow_threshold,
        auth: auth_config,
        feedback: parking_lot::Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: pit::PitTokens::generate(),
        pits: parking_lot::Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig {
            default_keep_alive: std::time::Duration::from_secs(cli.pit_default_keep_alive_secs),
            max_keep_alive: std::time::Duration::from_secs(cli.pit_max_keep_alive_secs),
            max_open: cli.max_open_pits,
        },
    });

    let app = router::build_router(Arc::clone(&state), router::MAX_IN_FLIGHT_REQUESTS);

    let addr = SocketAddr::new(cli.host, cli.port);
    info!(
        address = %addr,
        slow_query_threshold_ms = slow_threshold,
        endpoints = "GET /, GET/HEAD/PUT/DELETE /_doc/{id}, GET/POST /_search, POST /_mpercolate, POST /_bulk, GET/POST /_flush, POST /_compact, POST /_forcemerge, POST /_backup, GET /_stats, GET /_cat/stats, GET/HEAD /_health, GET/HEAD /_metrics, GET/HEAD/PUT /_vocab, POST /_vocab/learn, POST /_vocab/learn_and_apply, GET /_vocab/aliases, POST /_vocab/aliases/import, POST /_vocab/aliases/learn_and_apply, POST /_vocab/aliases/discover, POST /_vocab/aliases/discover_and_record, GET /_vocab/aliases/feedback, POST /_vocab/aliases/feedback/reset, POST /_vocab/aliases/validate_and_apply",
        "server listening"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");

    // Graceful shutdown with drain timeout enforcement.
    // 1. Wait for SIGINT/SIGTERM.
    // 2. Tell axum to stop accepting new connections and drain in-flight requests.
    // 3. If drain doesn't complete within `drain_timeout` seconds, force through.
    let signal_received = Arc::new(tokio::sync::Notify::new());
    let signal_received2 = Arc::clone(&signal_received);

    let graceful_shutdown = async move {
        shutdown_signal().await;
        signal_received2.notify_one();
    };

    let server_fut = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);

    // After signal fires, race the drain against the timeout.
    let drain_deadline = async {
        signal_received.notified().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(drain_timeout)).await;
        warn!(drain_timeout, "drain timeout exceeded, forcing shutdown");
    };

    tokio::select! {
        result = server_fut => {
            if let Err(e) = result {
                eprintln!("server error: {e}");
            }
        }
        () = drain_deadline => {
            // Drain took too long — fall through to cleanup.
        }
    }

    let cancelled_jobs = state.exhaustive_jobs.cancel_all();
    if cancelled_jobs > 0 {
        info!(
            cancelled_jobs,
            "cancelled exhaustive jobs before shutdown cleanup"
        );
    }

    info!(
        drain_timeout = drain_timeout,
        "connection drain complete, running shutdown sequence"
    );

    // A write whose client disconnected is still owned by its blocking worker (ADR-191). Take
    // every write permit first, so none of them lands after the final flush.
    let _writes_quiesced = crate::state::quiesce_writes(&state).await.ok();

    // Flush memtable and log final metrics.
    {
        let mut engine = state.engine.lock();
        let pre_metrics = engine.metrics();

        if pre_metrics.memtable_entries > 0 {
            info!(
                memtable_entries = pre_metrics.memtable_entries,
                "flushing memtable before shutdown"
            );
            engine.flush();
        }

        let final_metrics = engine.metrics();
        info!(
            total_queries = final_metrics.total_queries,
            base_segments = final_metrics.base_segments,
            dict_features = final_metrics.dict_features,
            exact_bytes = final_metrics.exact_bytes,
            index_bytes = final_metrics.index_bytes,
            filter_bytes = final_metrics.filter_bytes,
            tag_summary_bytes = final_metrics.tag_summary_bytes,
            "final engine state"
        );
    }

    info!("shutdown complete");
}

/// Wait for SIGINT (ctrl-c) or SIGTERM, then return. Shared with cluster mode.
pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("received SIGINT (ctrl-c)");
        }
        () = sigterm => {
            info!("received SIGTERM");
        }
    }
}
