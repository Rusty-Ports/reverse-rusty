//! Coordinator-mode startup (ADR-070): assemble a [`ClusterEngine`] from the CLI
//! (in-process build/reopen, or remote connect under the `distributed` feature),
//! wire the observer → Prometheus bridge, build the cluster router over the shared
//! middleware stack, serve, and run the durability shutdown sequence (flush +
//! checkpoint).
//!
//! Durability model by mode (recorded in ADR-070): an in-process `--data-dir`
//! cluster is the ADR-031/032 story (log-first writes, manifest commit at
//! checkpoint, attach-and-mmap reopen). A remote cluster's coordinator is
//! STATELESS — durability lives on the shard nodes (per-shard translog +
//! checkpoint sidecar, ADR-039); a coordinator restart reconnects to the same
//! endpoints and re-ships the deterministically re-minted dict.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use tracing::{error, info, warn};

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::events::EngineEvent;
use reverse_rusty::loader;
use reverse_rusty::normalize::Normalizer;

use crate::auth::AuthConfig;
use crate::cli::Cli;
use crate::metrics::PrometheusMetrics;
use crate::shutdown_signal;
use crate::state::{ClusterAppState, ClusterRebalanceTopology};

/// Remote-coordinator assembly (connect + control-plane attach + route-by-assignments), split out to
/// keep this file within the module-size budget (ADR-086).
#[cfg(feature = "distributed")]
mod remote_connect;

/// The unattended re-point reconcile loop (ADR-092), split out to keep this file within the
/// module-size budget. `distributed`-gated: it drives the data-moving reconcile.
#[cfg(feature = "distributed")]
mod reconcile_loop;
mod rpc_runtime;
pub(crate) mod shard_local_flags;

pub(crate) use rpc_runtime::cluster_rpc_handle;
pub(crate) mod resize_loop;

/// Run the server in coordinator mode. Mirrors `main`'s single-node flow: build
/// the cluster, wire observability, serve, shut down cleanly.
pub(crate) async fn run(
    cli: Cli,
    auth_config: Option<AuthConfig>,
    rank_profiles: Arc<reverse_rusty::RankProfiles>,
) {
    // Per-shard engine config from the same flags single-node mode maps; the
    // coordinator derives each shard's data dir itself (ADR-032), so data_dir
    // stays unset here.
    let per_shard = cli.engine_config();
    let problems = per_shard.validate();
    if !problems.is_empty() {
        for p in &problems {
            error!(problem = %p, "invalid engine config");
        }
        std::process::exit(1);
    }
    let remote_groups: Vec<String> = cli.shard_endpoint.clone();
    // A coordinator routing by committed assignments with ONLY --control-endpoint (no --shard-endpoint)
    // is a REMOTE cluster that resolves its shard endpoints from the durable quorum (ADR-086
    // resolve-only boot — the quorum must already be seeded). Otherwise remote mode is defined by the
    // presence of --shard-endpoint.
    let resolve_only =
        cli.route_by_assignments && remote_groups.is_empty() && !cli.control_endpoint.is_empty();
    let in_process = remote_groups.is_empty() && !resolve_only;
    // accept_class_d drives the cluster always-candidate lane (ADR-080): the coordinator places
    // class-D on the broad lane (replicated to every shard). The COORDINATOR is the SOLE gate — a
    // remote `ShardServer` is coordinator-gated storage (`LocalShard` forces accept_class_d on
    // every shard it builds, so it stores whatever the coordinator places), and therefore needs no
    // flag of its own. (An earlier warning here told operators to set a nonexistent `shardserver
    // --accept-class-d`, describing a drop that LocalShard makes impossible — see the
    // cluster_grpc_oracle class-D test, which proves a default-config shard still serves class-D.)
    if in_process
        && (cli.grpc_tls_ca.is_some()
            || cli.grpc_tls_domain.is_some()
            || cli.cluster_token.is_some())
    {
        error!(
            "--grpc-tls-ca/--grpc-tls-domain/--cluster-token apply to the gRPC mesh links              and require --shard-endpoint (remote mode)"
        );
        std::process::exit(1);
    }
    if !in_process && cli.data_dir.is_some() {
        error!(
            "--data-dir cannot be combined with --shard-endpoint: a remote coordinator \
             is stateless — durability lives on the shard nodes (shardserver --data-dir)"
        );
        std::process::exit(1);
    }
    if in_process && cli.data_dir.is_none() {
        warn!("no --data-dir specified: cluster is in-memory only, data will not survive restarts");
    }
    // Durability and storage flags configure whichever process holds a shard's disk. Against
    // remote shard nodes that is not this one (ADR-192).
    if !in_process {
        let flags = shard_local_flags::in_remote_mode(&per_shard);
        for inert in &flags.inert {
            warn!("{inert}");
        }
        if !flags.refused.is_empty() {
            for refused in &flags.refused {
                error!("{refused}");
            }
            std::process::exit(1);
        }
    }
    // The hot tier (ADR-105) is classified SHARD-SIDE in remote mode: each shardserver's
    // own θ decides whether a coordinator-placed query lands in its realtime lane or its
    // hot tier. Divergence is cost-only (both lanes always-visible; placement θ-invariant)
    // but silently defeats the quarantine — remind the operator of the contract.
    if !in_process && cli.hot_anchor_threshold != 0 {
        warn!(
            theta = cli.hot_anchor_threshold,
            "--hot-anchor-threshold in remote cluster mode: ensure every shardserver runs              the same --hot-anchor-threshold (divergence is cost-only, never correctness)"
        );
    }
    if !in_process && !cli.tag_segment_skipping {
        warn!(
            "--tag-segment-skipping=false in remote cluster mode: restart every shardserver with \
             --tag-segment-skipping false; mixed values preserve results but produce mixed cost and telemetry"
        );
    }
    // --control-endpoint attaches the coordinator to a durable control-plane quorum (ADR-083). It is
    // only meaningful for a REMOTE cluster: an in-process cluster owns the one logical node, so its
    // in-memory control plane already IS the source of truth. Fail loud rather than silently ignore.
    if in_process && !cli.control_endpoint.is_empty() {
        error!(
            "--control-endpoint requires --shard-endpoint (remote mode): an in-process cluster uses \
             its own in-memory control plane"
        );
        std::process::exit(1);
    }
    // --route-by-assignments makes the committed quorum the topology source of truth (ADR-086), so it
    // requires a control plane to read. With --shard-endpoint it seeds + routes; with only
    // --control-endpoint it resolves the topology from the quorum (resolve-only boot).
    if cli.route_by_assignments && cli.control_endpoint.is_empty() {
        error!(
            "--route-by-assignments requires --control-endpoint: the committed quorum is the \
             topology source of truth (ADR-086)"
        );
        std::process::exit(1);
    }
    // --reconcile-interval-secs runs the unattended reconciler (ADR-092), which re-points routing by
    // MOVING data to the committed map's owner. It is only safe + meaningful when the coordinator
    // actually ROUTES by that committed map — otherwise a converged map would not change routing.
    // Require resolve-only routing. A CLI-seeded assignment coordinator follows
    // the committed map live, but changing that map makes its next guarded
    // restart fail against the stale position-preserving endpoint list.
    if cli.reconcile_interval_secs.is_some() && !resolve_only {
        error!(
            "--reconcile-interval-secs requires resolve-only assignment routing: use \
             --route-by-assignments, --control-endpoint, the committed --shards count, and no \
             --shard-endpoint (ADR-092/086)"
        );
        std::process::exit(1);
    }

    // ADR-179: the opt-in governed resize loop. It drives the in-process blue/green resize, so a
    // remote topology is refused before any data is touched (remote resize is roadmap work).
    let autoscale_resize = cli.autoscale_resize_interval_secs.map(|secs| {
        if !in_process {
            error!(
                "--autoscale-resize-interval-secs requires an in-process cluster: remote \
                 shard-count changes are not implemented (ADR-179)"
            );
            std::process::exit(1);
        }
        let Some(split_corpus_threshold) = cli.autoscale_split_threshold.filter(|&t| t > 0) else {
            error!("--autoscale-resize-interval-secs requires --autoscale-split-threshold > 0");
            std::process::exit(1);
        };
        let governor = reverse_rusty::cluster::ResizeGovernorConfig {
            required_observations: cli.autoscale_resize_observations,
            cooldown: std::time::Duration::from_secs(cli.autoscale_resize_cooldown_secs),
            max_step: cli.autoscale_resize_max_step,
            max_shards: cli.autoscale_resize_max_shards,
            min_relief_percent: cli.autoscale_resize_min_relief_percent,
        };
        let problems = governor.validate();
        if !problems.is_empty() {
            error!(problems = %problems.join("; "), "invalid governed resize configuration");
            std::process::exit(1);
        }
        resize_loop::AutoscaleResizeConfig {
            interval: std::time::Duration::from_secs(secs.max(1)),
            split_corpus_threshold,
            governor,
        }
    });

    // The ring size: --shards for an in-process OR a resolve-only-boot cluster (validated against the
    // quorum's committed num_shards on attach), else the --shard-endpoint count.
    let num_shards = if remote_groups.is_empty() {
        cli.shards
    } else {
        remote_groups.len()
    };
    let cluster_config = ClusterConfig {
        num_shards,
        replication_factor: cli.replication_factor,
        per_shard,
        include_broad: cli.include_broad,
        data_dir: if in_process {
            cli.data_dir.clone()
        } else {
            None
        },
        wal_sync_on_write: cli.wal_sync_on_write,
        ..ClusterConfig::default()
    };

    // Mesh client security for the remote links (ADR-071), resolved fail-loud HERE so a
    // misconfiguration refuses startup. Kept as plain bytes — the typed ClientSecurity is
    // built inside the distributed-gated connect path.
    let mesh = MeshClientParts {
        ca: cli.grpc_tls_ca.as_ref().map(|p| {
            std::fs::read(p).unwrap_or_else(|e| {
                error!(path = ?p, error = %e, "cannot read --grpc-tls-ca");
                std::process::exit(1);
            })
        }),
        domain: cli.grpc_tls_domain.clone(),
        token: match crate::auth::AuthConfig::resolve(
            cli.cluster_token.clone(),
            std::env::var("RR_CLUSTER_TOKEN"),
            false,
        ) {
            Ok(t) => t.map(|a| a.token_bytes().to_vec()),
            Err(e) => {
                error!(error = %e, "invalid mesh cluster token");
                std::process::exit(1);
            }
        },
        connect_timeout_secs: cli.grpc_connect_timeout_secs,
        read_timeout_secs: cli.grpc_read_timeout_secs,
        write_timeout_secs: cli.grpc_write_timeout_secs,
        keepalive_secs: cli.grpc_keepalive_secs,
        read_retries: cli.grpc_read_retries,
    };
    if mesh.token.is_some() && mesh.ca.is_none() {
        warn!(
            "--cluster-token without --grpc-tls-ca: the mesh secret crosses the wire in              cleartext; configure mesh TLS (ADR-071)"
        );
    }

    // Vocabulary → normalizer (the same vocab-file flow as single-node mode).
    let vocab = cli.vocab_file.as_ref().map(|path| {
        info!(path = ?path, "loading vocabulary from file");
        reverse_rusty::vocab::Vocab::load_json(path).expect("failed to read vocab file")
    });
    let norm = match &vocab {
        Some(v) => v
            .to_normalizer()
            .expect("failed to build normalizer from vocab"),
        None => Normalizer::default_vocab().expect("failed to build normalizer"),
    };

    // Pre-load corpus (used by build / ingest; skipped on a populated reopen).
    let load_start = Instant::now();
    let queries: Vec<(u64, String)> = match &cli.load_file {
        Some(path) => {
            info!(path = ?path, "loading queries from file");
            let result = loader::load_file(path).expect("failed to read query file");
            if !result.errors.is_empty() {
                warn!(
                    error_count = result.errors.len(),
                    first_error = %result.errors.first().map(std::string::ToString::to_string).unwrap_or_default(),
                    "query file had load errors"
                );
            }
            result.queries
        }
        None => Vec::new(),
    };

    // Assemble the cluster OFF the runtime workers: build/open are plain sync work,
    // and the gRPC connect path's sync→async bridge must not run on a runtime
    // worker thread (it would nest `block_on`). Every cluster connection is bound to
    // the dedicated RPC runtime, never the HTTP one (see `rpc_runtime`).
    let handle = cluster_rpc_handle();
    let data_dir = cluster_config.data_dir.clone();
    let cfg = cluster_config.clone();
    let control_endpoints: Vec<String> = cli.control_endpoint.clone();
    let route_by_assignments = cli.route_by_assignments;
    let assemble = tokio::task::spawn_blocking(move || {
        assemble_cluster(
            in_process,
            &remote_groups,
            data_dir,
            &cfg,
            norm,
            vocab,
            &queries,
            &handle,
            mesh,
            &control_endpoints,
            route_by_assignments,
        )
    });
    let cluster = match assemble.await.expect("cluster assembly task panicked") {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to assemble cluster; refusing to start");
            std::process::exit(1);
        }
    };
    info!(
        shards = cluster.num_shards(),
        replication_factor = cluster.replication_factor(),
        durable = cluster.is_durable(),
        elapsed_ms = format!("{:.1}", load_start.elapsed().as_secs_f64() * 1000.0),
        "cluster assembled"
    );

    // Prometheus + the observer bridge (the cluster emits Ingest/DurabilityFailure
    // events through the same EngineEvent enum).
    let prom = PrometheusMetrics::new();
    let prom_for_observer = prom.clone();
    cluster.set_observer(Arc::new(move |event: &EngineEvent| {
        prom_for_observer.observe_event(event);
        if let EngineEvent::DurabilityFailure { op, detail, error } = event {
            if op.is_data_at_risk() {
                error!(op = op.as_str(), detail = %detail, error = %error,
                    "cluster.durability_failure: durability degraded");
            } else {
                warn!(op = op.as_str(), detail = %detail, error = %error,
                    "cluster.durability_failure");
            }
        }
    }));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads.unwrap_or(0))
        .build()
        .expect("failed to build rayon thread pool");
    let ranked_workers = pool.current_num_threads().max(1);
    let exhaustive_jobs = crate::jobs::ExhaustiveJobs::new(
        crate::jobs::ExhaustiveJobConfig {
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

    let state = Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        topology_guard: RwLock::new(()),
        write_serial: Mutex::new(()),
        write_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_QUEUED_CLUSTER_WRITES,
        )),
        read_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_QUEUED_CLUSTER_READS,
        )),
        flush_serial: Mutex::new(()),
        durability_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_DURABILITY_OPERATIONS,
        )),
        rebalance_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_REBALANCES,
        )),
        reconcile_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_RECONCILES,
        )),
        handoff_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_HANDOFFS,
        )),
        reassign_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_REASSIGNS,
        )),
        remote_resize_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        rebalance_topology: if in_process {
            ClusterRebalanceTopology::InProcess
        } else if resolve_only {
            // `assemble_cluster` returned only after the control-plane
            // resolve path selected these live backings without a stale CLI
            // topology that would reject the next restart.
            ClusterRebalanceTopology::ResolveOnlyRemote
        } else if route_by_assignments {
            // The live backings follow the committed map, but startup is still
            // guarded against the supplied position-preserving endpoint list.
            // Rebalance must wait until the deployment is resolve-only.
            ClusterRebalanceTopology::CliSeededAssignmentRemote
        } else {
            ClusterRebalanceTopology::StaticRemote
        },
        health_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        pool,
        search_permits: (cli.max_concurrent_searches > 0)
            .then(|| std::sync::Arc::new(tokio::sync::Semaphore::new(cli.max_concurrent_searches))),
        ranked_search_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(ranked_workers)),
        exhaustive_jobs,
        rank_profiles,
        max_ranked_enrichment_bytes: cli.max_ranked_enrichment_bytes,
        include_broad: cli.include_broad,
        prom,
        slow_query_threshold_ms: cli.slow_query_threshold_ms,
        auth: auth_config,
        pit_tokens: crate::pit::PitTokens::generate(),
        pit_config: reverse_rusty::PitConfig {
            default_keep_alive: std::time::Duration::from_secs(cli.pit_default_keep_alive_secs),
            max_keep_alive: std::time::Duration::from_secs(cli.pit_max_keep_alive_secs),
            max_open: cli.max_open_pits,
        },
        resize_operations: Arc::new(crate::resize_ops::ResizeOperations::new(
            autoscale_resize.is_some(),
        )),
    });

    let app =
        router::build_cluster_router(Arc::clone(&state), crate::router::RequestPools::serving());

    let addr = SocketAddr::new(cli.host, cli.port);
    info!(address = %addr, mode = "cluster", "server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");

    let signal_received = Arc::new(tokio::sync::Notify::new());
    let signal_received2 = Arc::clone(&signal_received);
    let graceful_shutdown = async move {
        shutdown_signal().await;
        signal_received2.notify_one();
    };
    let server_fut = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);
    let drain_timeout = cli.drain_timeout;
    let drain_deadline = async {
        signal_received.notified().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(drain_timeout)).await;
        warn!(drain_timeout, "drain timeout exceeded, forcing shutdown");
    };

    // ADR-092: the opt-in unattended reconcile loop (distributed-only — it drives the data-moving
    // reconcile). Spawned only when --reconcile-interval-secs is set (the guard above already required
    // --route-by-assignments); held so it can be aborted at the start of the shutdown sequence, before
    // the durability flush, so a pass never starts racing the checkpoint. Default (unset) ⇒ never
    // spawned ⇒ byte-identical.
    #[cfg(feature = "distributed")]
    let reconcile_task = cli.reconcile_interval_secs.map(|secs| {
        let cfg = reverse_rusty::cluster::ReconcileConfig {
            enabled: true,
            rf: cli.replication_factor,
            min_interval: std::time::Duration::from_secs(secs.max(1)),
            max_parallel_moves: cli.reconcile_max_parallel.max(1),
            gc_orphans: cli.reconcile_gc_orphans,
        };
        reconcile_loop::spawn_reconcile_loop(Arc::clone(&state), &cfg)
    });

    // ADR-179: the opt-in governed resize loop, aborted first at shutdown like the reconciler.
    let resize_task = autoscale_resize
        .clone()
        .map(|config| resize_loop::spawn_resize_loop(Arc::clone(&state), config));

    tokio::select! {
        result = server_fut => {
            if let Err(e) = result {
                error!(error = %e, "server error");
            }
        }
        () = drain_deadline => {}
    }

    let cancelled_jobs = state.exhaustive_jobs.cancel_all();
    if cancelled_jobs > 0 {
        info!(
            cancelled_jobs,
            "cancelled exhaustive jobs before cluster shutdown cleanup"
        );
    }

    // Stop the reconcile loop before the durability flush: a pass already on the blocking pool
    // finishes its current durable move safely (handoff tolerates concurrent flushes, ADR-044), but
    // no new pass starts racing the checkpoint.
    #[cfg(feature = "distributed")]
    if let Some(task) = reconcile_task {
        info!("stopping reconcile loop");
        task.abort();
    }
    if let Some(task) = resize_task {
        info!("stopping governed resize loop");
        task.abort();
    }

    // Workers and cluster writes may outlive their HTTP requests by design; join them and hold
    // their admission through durability cleanup (see `shutdown::quiesce_detached_work`).
    info!("connection drain complete, waiting for detached cluster work");
    let _detached_work_guards = shutdown::quiesce_detached_work(&state).await;
    info!("detached cluster work quiesced, running cluster shutdown sequence");

    // Durability shutdown: flush + checkpoint (the manifest commit), so reopen
    // attaches segments instead of replaying a long log tail. In-memory clusters
    // flush only (checkpoint is a no-op there anyway).
    {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        if let Err(e) = cluster.flush() {
            error!(error = %e, "shutdown flush failed");
        }
        if cluster.is_durable() {
            match cluster.checkpoint() {
                Ok(()) => info!(epoch = cluster.epoch(), "shutdown checkpoint committed"),
                Err(e) => error!(error = %e, "shutdown checkpoint failed"),
            }
        }
    }
    info!("shutdown complete");
}

mod assemble;
pub(crate) mod router;
pub(crate) mod shutdown;

use assemble::{assemble_cluster, MeshClientParts};
