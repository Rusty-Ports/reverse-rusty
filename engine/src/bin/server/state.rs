//! Shared server state and request-scoped middleware.
//!
//! [`AppState`] holds the single-node snapshot-based concurrency primitives: a
//! `Mutex<Engine>` for serialized writes and an `ArcSwap<EngineSnapshot>` for
//! lock-free reads. [`ClusterAppState`] is the coordinator-mode analogue (ADR-070):
//! an `RwLock<ClusterEngine>` whose READ side serves both percolates and ordinary
//! writes (cluster reads are `&self` lock-free; writes are `&self` and the cluster
//! orders them itself, by its log and a lock per logical id), while the WRITE side is
//! taken only by the `&mut self` blue/green vocabulary/resize paths. Ordinary writes
//! share `write_admission`; an operation that needs every write out of the way takes
//! it exclusively (ADR-206). Descriptor mutations and
//! topology movement coordinate separately through `topology_guard`.
//! [`RequestCtx`] is the seam that lets one auth / request-id middleware serve both
//! backends. [`request_id_middleware`] stamps an `x-request-id` header and tracks
//! the in-flight-request gauge via the RAII [`InFlightGuard`].

use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{extract::State, http::HeaderValue, middleware::Next, response::Response};
use parking_lot::{Mutex, RwLock};
use prometheus::IntGauge;
use tracing::Instrument;

use reverse_rusty::segment::{Engine, EngineSnapshot};
use reverse_rusty::vocab::AliasFeedback;

use crate::auth::AuthConfig;
use crate::metrics::PrometheusMetrics;

mod cluster_access;

pub(crate) use cluster_access::{ClusterLock, ClusterWrite, SearchPool};

/// Static winner-enrichment budget shared by local and coordinator v2 search.
pub(crate) const DEFAULT_MAX_RANKED_ENRICHMENT_BYTES: usize = 16 * 1024 * 1024;
/// Backups serialize behind the engine/cluster writer lock, so admitting more
/// than one blocking worker would only create a detached blocking-pool queue.
pub(crate) const MAX_CONCURRENT_BACKUPS: usize = 1;
/// Checkpoint and backup share the coordinator writer boundary, so admit one
/// durability operation at a time instead of queuing blocking workers.
pub(crate) const MAX_CONCURRENT_CLUSTER_DURABILITY_OPERATIONS: usize = 1;
/// Rebalance is a cluster-wide topology workflow. Admit one operator-triggered
/// pass at a time so duplicate requests cannot accumulate blocking workers or
/// launch competing whole-cluster plans. Conflict-aware parallelism within one
/// data-moving pass remains controlled by its `max_parallel` request field.
pub(crate) const MAX_CONCURRENT_CLUSTER_REBALANCES: usize = 1;
/// Reconcile and orphan GC are cluster-wide maintenance passes. Manual requests
/// and the unattended reconcile+GC loop share this single slot so duplicate
/// sweeps cannot accumulate and shutdown can join detached in-flight work.
pub(crate) const MAX_CONCURRENT_CLUSTER_RECONCILES: usize = 1;
/// Raw handoff is a long-running operator workflow. Admit one request at a
/// time so duplicate HTTP calls cannot accumulate dedicated workers; the
/// engine's endpoint ledger still coordinates it with automatic/reassign
/// movement outside this REST boundary.
pub(crate) const MAX_CONCURRENT_CLUSTER_HANDOFFS: usize = 1;
/// A single-position reassignment is independently long-running and may wait
/// on the endpoint move ledger. Admit one operator request per coordinator so
/// duplicate calls cannot accumulate detached workers; automatic reconcile
/// and rebalance retain their own conflict-aware engine scheduling.
pub(crate) const MAX_CONCURRENT_CLUSTER_REASSIGNS: usize = 1;
/// Cluster writes (PUT, DELETE, bulk, flush) admitted onto blocking threads at once. It is how
/// many writes can run at the same time (ADR-206), and it bounds how many blocking threads
/// writes can hold while a whole-cluster operation keeps them waiting, including writes whose
/// clients have disconnected.
pub(crate) const MAX_QUEUED_CLUSTER_WRITES: usize = 32;
/// Standalone writes (PUT, DELETE, bulk, flush) admitted onto blocking threads at once. They run
/// one at a time behind the engine mutex, so this bounds how many blocking threads queued writes
/// can hold, including writes whose clients have disconnected.
pub(crate) const MAX_QUEUED_WRITES: usize = 32;
/// Brief cluster-lock reads (GET/HEAD `_doc`, `GET /`, the v2 and job compile steps) admitted
/// onto blocking threads at once. They wait only while a vocabulary rebuild or resize holds or
/// is queued for the exclusive cluster lock; this bounds the threads parked behind one.
pub(crate) const MAX_QUEUED_CLUSTER_READS: usize = 64;
/// The health route stays open even when read auth is enabled. Bound all of
/// its requests independently before their bodies are buffered.
pub(crate) const MAX_CONCURRENT_HEALTH_REQUESTS: usize = 8;
/// Expensive administrative work shares one blocking slot per server. Stats
/// scans dominate read cost; vocabulary reads, learning, and corpus-wide
/// replacements also use it so large JSON snapshots and O(corpus) work cannot
/// fan out.
pub(crate) const MAX_CONCURRENT_STATS: usize = 1;

pub(crate) struct AppState {
    pub(crate) engine: Mutex<Engine>,
    /// Serializes explicit flush requests separately from ordinary writes so
    /// `wait_if_ongoing=false` can reject only a competing flush, matching the
    /// ES/OpenSearch control instead of conflating it with any writer.
    pub(crate) flush_serial: Mutex<()>,
    /// Admission for PUT, DELETE, bulk and flush (ADR-191). The permit is awaited on the
    /// async side and then owned by the blocking worker, so queued writers wait as futures
    /// and the engine mutex is only ever waited on from a blocking thread.
    pub(crate) write_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// One admitted backup at a time. The owned permit moves into the blocking
    /// closure so a disconnected request cannot release admission while its
    /// backup is still waiting on or holding the engine writer lock.
    pub(crate) backup_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Per-server admission for the intentionally unauthenticated health route.
    pub(crate) health_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Bounds corpus-wide stats and vocabulary read/learn/replacement work
    /// independently from search and backup. The permit is owned by the worker.
    pub(crate) stats_permits: std::sync::Arc<tokio::sync::Semaphore>,
    pub(crate) snapshot: ArcSwap<EngineSnapshot>,
    pub(crate) pool: rayon::ThreadPool,
    /// Bounded search concurrency (ADR-099): `Some` ⇒ every `/_search` /
    /// `/_mpercolate` acquires one permit before its `spawn_blocking` match work,
    /// and the permit is moved INTO the closure — released when the blocking work
    /// actually ends (not when an abandoned join handle drops at timeout), so the
    /// semaphore reflects true pool occupancy. `None` ⇒ unbounded (default).
    pub(crate) search_permits: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Always-bounded v2 ranked-search admission. Its default is the Rayon
    /// worker count and is deliberately independent from compatibility routes.
    pub(crate) ranked_search_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Separate pool/admission/registry for ADR-114 exhaustive jobs.
    pub(crate) exhaustive_jobs: Arc<crate::jobs::ExhaustiveJobs>,
    /// Startup-loaded, immutable CPU ranking profile registry (ADR-162).
    pub(crate) rank_profiles: Arc<reverse_rusty::RankProfiles>,
    pub(crate) max_ranked_enrichment_bytes: usize,
    pub(crate) include_broad: bool,
    pub(crate) prom: PrometheusMetrics,
    pub(crate) slow_query_threshold_ms: u64,
    /// Bearer-token auth (ADR-062). `None` ⇒ the gate is a pass-through.
    pub(crate) auth: Option<AuthConfig>,
    /// Match-feedback aggregator (ADR-103): tracked candidate pairs + bounded behavioral
    /// evidence. Fed post-match by the percolate handlers when `alias_feedback_capture` is on
    /// (default off ⇒ never touched); re-synced against the registry on every snapshot
    /// publish. Not persisted — a rolling operational signal.
    pub(crate) feedback: Mutex<AliasFeedback>,
    /// Per-process HMAC key for PIT/cursor tokens (ADR-113): a restart mints a
    /// new key, so every outstanding token fails closed as stale.
    pub(crate) pit_tokens: crate::pit::PitTokens,
    /// Open point-in-time snapshots: each entry pins one `Arc<EngineSnapshot>`
    /// for cursor pagination. In-memory only — dies with the process by design.
    pub(crate) pits: Mutex<reverse_rusty::PitRegistry<Arc<EngineSnapshot>>>,
    pub(crate) pit_config: reverse_rusty::PitConfig,
}

/// Why an admitted standalone write produced no result.
#[derive(Debug)]
pub(crate) enum WriteWorkerError {
    /// Write admission is closed: the server is shutting down.
    AdmissionClosed,
    /// The blocking worker panicked or was cancelled by a runtime shutdown.
    Worker(tokio::task::JoinError),
}

impl std::fmt::Display for WriteWorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteWorkerError::AdmissionClosed => f.write_str("write admission is closed"),
            WriteWorkerError::Worker(error) => write!(f, "write worker failed: {error}"),
        }
    }
}

/// Admit one standalone write onto a blocking thread (ADR-191). The permit is awaited, so a
/// request cancelled while it waits starts nothing; the caller moves the permit into the worker,
/// so the capacity stays taken until the write finishes, even if its client has disconnected.
pub(crate) async fn admit_write(
    state: &AppState,
) -> Result<tokio::sync::OwnedSemaphorePermit, WriteWorkerError> {
    Arc::clone(&state.write_permits)
        .acquire_owned()
        .await
        .map_err(|_| WriteWorkerError::AdmissionClosed)
}

/// Run a standalone mutation on a blocking thread, never on an async worker (ADR-191).
///
/// The engine mutex is held for a whole compaction, backup or vocabulary rebuild. A request
/// that waited for it on an async worker would park that worker, and as many waiting writes
/// as there are workers would stop the server from answering anything, `/_health` included.
/// Here the wait happens on a blocking thread and the queue ahead of it is a semaphore.
///
/// The snapshot is published by the worker, under the same lock as the write: the mutation
/// and its read view are one commit, and a request dropped after admission still publishes
/// what it wrote.
pub(crate) async fn run_engine_write<T: Send + 'static>(
    state: &Arc<AppState>,
    work: impl FnOnce(&mut Engine) -> T + Send + 'static,
) -> Result<T, WriteWorkerError> {
    let permit = admit_write(state).await?;
    let state = Arc::clone(state);
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut engine = state.engine.lock();
        let outcome = work(&mut engine);
        state.publish_snapshot_from_locked_engine(&engine);
        outcome
    })
    .await
    .map_err(WriteWorkerError::Worker)
}

/// Take every write permit: wait for each write that outlived its request, then keep
/// admission closed while the caller runs its shutdown flush, so no late write lands after it.
pub(crate) async fn quiesce_writes(
    state: &AppState,
) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
    let capacity = u32::try_from(MAX_QUEUED_WRITES).unwrap_or(u32::MAX);
    Arc::clone(&state.write_permits)
        .acquire_many_owned(capacity)
        .await
}

impl AppState {
    pub(crate) fn publish_snapshot(&self) {
        let engine = self.engine.lock();
        self.publish_snapshot_from_locked_engine(&engine);
    }

    /// Publish a snapshot derived from the already-locked state engine.
    ///
    /// Callers use this when a mutation and its lock-free read view must form
    /// one coherent commit instead of dropping and reacquiring `engine`.
    pub(crate) fn publish_snapshot_from_locked_engine(&self, engine: &Engine) {
        let snap = Arc::new(engine.snapshot());
        // Re-sync the feedback aggregator's tracked universe (ADR-103) on every publish — the
        // vocab epoch is NOT a sufficient dirty signal (the ADR-102 metadata-only install
        // records candidates without bumping it). Gated on the capture knob so the default-off
        // contract stays zero-work (codex review): with capture off, no lock, no registry
        // scan; flipping the knob on re-syncs at the next publish (the settings PUT publishes).
        {
            let cfg = engine.config();
            if cfg.alias_feedback_capture {
                let mut fb = self.feedback.lock();
                match snap.vocab() {
                    Some(v) => fb.sync_tracked(v.aliases(), cfg.alias_feedback_max_pairs),
                    None => fb.reset(),
                }
                // Publish while the feedback mutex is still held. The feedback-read worker
                // takes this mutex before loading `snapshot`, so it can observe either the
                // prior pair universe + prior snapshot or the new pair universe + new
                // snapshot, never the new evidence keys against stale query sources.
                self.snapshot.store(snap);
                return;
            }
        }
        self.snapshot.store(snap);
    }
}

/// Safety-relevant topology of the cluster-rebalance REST boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClusterRebalanceTopology {
    /// All physical shard positions are co-resident; the assignment map is
    /// advisory and may be updated without moving data.
    InProcess,
    /// gRPC shard backings follow static endpoint order rather than the
    /// committed assignment map. Map-driven movement is unsafe here.
    StaticRemote,
    /// gRPC shard backings were resolved from the committed assignment map,
    /// but startup also received a position-preserving CLI endpoint list. A
    /// changed map would make the next guarded restart fail, so rebalance must
    /// wait until the deployment switches to resolve-only routing.
    CliSeededAssignmentRemote,
    /// gRPC shard backings were resolved only from the committed assignment
    /// map. That map can safely select movement sources and remains the
    /// restart topology after assignments change.
    ResolveOnlyRemote,
}

/// Coordinator-mode state (ADR-070): the cluster analogue of [`AppState`].
pub(crate) struct ClusterAppState {
    /// Read lock for percolates AND ordinary writes (both `&self`); write lock only
    /// for the `&mut self` vocabulary rebuilds and resizes, and only through
    /// [`ClusterAppState::write_cluster`] (ADR-207). A read that returns ids only never
    /// waits for a write. A read that also returns sources or an explanation takes the
    /// cluster's mutation-frozen view, so it waits for the writes in flight and runs
    /// alone (`ClusterEngine::consistent_read_view`).
    pub(crate) cluster: ClusterLock,
    /// Excludes descriptor mutation from in-flight topology movement. Movement
    /// operations take a shared guard (so their own conflict-aware concurrency
    /// remains available); registration, deregistration, and resize take the
    /// exclusive side. Separate from `write_admission`, so ingestion keeps flowing.
    pub(crate) topology_guard: RwLock<()>,
    /// Admission for anything that changes the corpus (ADR-206).
    ///
    /// - **Shared:** an ordinary write (PUT, DELETE, a bulk batch). Writes run beside each
    ///   other; the cluster orders them itself (its log, and a lock per logical id held
    ///   across the append and the complete shard fan-out, ADR-177).
    /// - **Exclusive:** an operation that needs every write finished and none started:
    ///   flush, checkpoint, backup, a vocabulary change, resync, resize, an exhaustive job,
    ///   and shutdown.
    /// - **Shared, too:** a search that returns sources or an explanation. It is kept apart
    ///   from writes by the cluster's mutation-frozen view, not by this lock; it shares
    ///   admission so that nothing can queue for the cluster's write lock while it holds
    ///   the read lock (see `pool`). A read that returns ids only takes nothing.
    ///
    /// Take it before the `cluster` lock, never after.
    pub(crate) write_admission: RwLock<()>,
    /// Admission for cluster writes run on blocking threads (ADR-183). A permit is taken before
    /// the worker starts and held until it finishes, so a disconnected client can never leave more
    /// than [`MAX_QUEUED_CLUSTER_WRITES`] detached writers holding blocking threads.
    pub(crate) write_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Admission for brief cluster-lock reads that run on blocking threads (ADR-191).
    pub(crate) read_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Explicit-flush admission, separate from the general write serializer for
    /// the same `wait_if_ongoing` reason as [`AppState::flush_serial`].
    pub(crate) flush_serial: Mutex<()>,
    /// One admitted checkpoint or backup at a time. Both operations serialize
    /// behind `write_admission`; sharing this owned permit prevents disconnected
    /// requests from accumulating blocking workers behind the same durability
    /// boundary.
    pub(crate) durability_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// One operator-triggered cluster rebalance at a time. The owned permit is
    /// held by the off-runtime worker through final control-state attestation,
    /// including after an HTTP disconnect.
    pub(crate) rebalance_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Shared admission for manual reconcile/GC and the unattended
    /// reconcile+GC loop. The admitted worker owns the permit through its
    /// terminal report, including after an HTTP disconnect, and shutdown joins
    /// it before cleanup.
    pub(crate) reconcile_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// One operator-triggered raw handoff at a time. The admitted worker owns
    /// the permit through its terminal routing result, including after an HTTP
    /// disconnect, and shutdown quiesces it before durability cleanup.
    pub(crate) handoff_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// One operator-triggered move-and-commit reassignment at a time. The
    /// worker owns admission through its terminal durable outcome, including
    /// after an HTTP disconnect, and shutdown joins it before cleanup.
    pub(crate) reassign_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// One running remote resize (ADR-180). The worker takes it before returning the corpus
    /// administration slot that health probes share, and holds it through its terminal result,
    /// so shutdown joins the copy and cutover before cleanup.
    pub(crate) remote_resize_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Whether rebalance may commit an advisory map, move from the committed
    /// map, or must refuse because live routing has another authority.
    pub(crate) rebalance_topology: ClusterRebalanceTopology,
    /// Coordinator analogue of [`AppState::health_permits`].
    pub(crate) health_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Coordinator analogue of [`AppState::stats_permits`], including bounded
    /// vocabulary reads, learning, and blue/green replacements.
    pub(crate) stats_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// The search pool, behind its gate (ADR-207). Work enters it only through
    /// [`SearchPool::enter`], so a request holds the gate for as long as its work is in the
    /// pool, and whatever takes the cluster's write lock closes the gate first. Its workers
    /// take `cluster.read()` for each title they match and never find a writer queued.
    pub(crate) pool: SearchPool,
    /// Bounded search concurrency (ADR-099): `Some` ⇒ every `/_search` /
    /// `/_mpercolate` acquires one permit before its `spawn_blocking` match work,
    /// and the permit is moved INTO the closure — released when the blocking work
    /// actually ends (not when an abandoned join handle drops at timeout), so the
    /// semaphore reflects true pool occupancy. `None` ⇒ unbounded (default).
    pub(crate) search_permits: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Always-bounded v2 ranked-search admission, symmetric with local mode.
    pub(crate) ranked_search_permits: std::sync::Arc<tokio::sync::Semaphore>,
    pub(crate) exhaustive_jobs: Arc<crate::jobs::ExhaustiveJobs>,
    /// Coordinator copy of the immutable CPU ranking profile registry.
    pub(crate) rank_profiles: Arc<reverse_rusty::RankProfiles>,
    pub(crate) max_ranked_enrichment_bytes: usize,
    pub(crate) include_broad: bool,
    pub(crate) prom: PrometheusMetrics,
    pub(crate) slow_query_threshold_ms: u64,
    /// Bearer-token auth (ADR-062), identical to single-node mode.
    pub(crate) auth: Option<AuthConfig>,
    /// Per-process HMAC key for PIT/cursor tokens (ADR-113); the coordinator
    /// holds the registry itself (`ClusterEngine` pins per-shard snapshots).
    pub(crate) pit_tokens: crate::pit::PitTokens,
    /// Admission bounds handed to `ClusterEngine::open_pit` per call.
    pub(crate) pit_config: reverse_rusty::PitConfig,
    /// Retained resize operations and the latest autoscaler observation (ADR-179).
    pub(crate) resize_operations: Arc<crate::resize_ops::ResizeOperations>,
}

/// What the request-scoped middleware needs from either backend's state — the seam
/// that lets ONE auth middleware + request-id middleware serve single-node and
/// cluster mode (ADR-070) without duplicating them.
pub(crate) trait RequestCtx: Send + Sync + 'static {
    fn prom(&self) -> &PrometheusMetrics;
    fn auth(&self) -> Option<&AuthConfig>;
    fn health_permits(&self) -> &std::sync::Arc<tokio::sync::Semaphore>;
}

/// The scope `--include-broad` selects for requests that name none.
fn scope_for(include_broad: bool) -> reverse_rusty::QueryScope {
    if include_broad {
        reverse_rusty::QueryScope::WithBroad
    } else {
        reverse_rusty::QueryScope::Standard
    }
}

impl AppState {
    /// The scope a request is evaluated in when it names none: what `--include-broad` set
    /// for this process (ADR-201). Every surface resolves an omitted scope through this, so
    /// the flag means the same on the compatibility routes, the v2 routes and exhaustive
    /// jobs. A request that names a scope gets that scope.
    pub(crate) fn default_query_scope(&self) -> reverse_rusty::QueryScope {
        scope_for(self.include_broad)
    }
}

impl ClusterAppState {
    /// See [`AppState::default_query_scope`].
    pub(crate) fn default_query_scope(&self) -> reverse_rusty::QueryScope {
        scope_for(self.include_broad)
    }
}

impl RequestCtx for AppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }
    fn health_permits(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.health_permits
    }
}

impl RequestCtx for ClusterAppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }
    fn health_permits(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.health_permits
    }
}

/// A held search-concurrency permit (ADR-099): the semaphore permit plus the
/// `search_permits_in_use` gauge, both released/decremented together on drop. Moved
/// INTO the `spawn_blocking` closure so release tracks the blocking work's real end
/// (an abandoned join handle dropping at response-timeout does NOT release it), and
/// dropped correctly if the request is cancelled between acquire and spawn.
pub(crate) struct SearchPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    gauge: IntGauge,
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

/// Acquire a search permit if `--max-concurrent-searches` bounded the pool
/// (`None` config ⇒ `None` permit, unbounded). WAITS for a permit — the wait sits
/// inside the caller's `tokio::time::timeout` race, so a request that never gets one
/// 408s at its own deadline and its dropped acquire consumes nothing.
pub(crate) async fn acquire_search_permit(
    sem: Option<&std::sync::Arc<tokio::sync::Semaphore>>,
    gauge: &IntGauge,
) -> Option<SearchPermit> {
    match sem {
        None => None,
        Some(s) => {
            // `acquire_owned` errs only on a closed semaphore; ours is never closed.
            let permit = std::sync::Arc::clone(s).acquire_owned().await.ok()?;
            gauge.inc();
            Some(SearchPermit {
                _permit: permit,
                gauge: gauge.clone(),
            })
        }
    }
}

/// RAII guard for the in-flight request gauge: increments on construction and
/// decrements on drop, so every exit path of the request stays balanced.
struct InFlightGuard<'a>(&'a IntGauge);

impl<'a> InFlightGuard<'a> {
    fn new(gauge: &'a IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Adds a unique X-Request-Id header to every response, tracks the in-flight
/// request gauge, and includes the request ID in the tracing span for
/// correlation. Generic over the backend state ([`RequestCtx`]).
pub(crate) async fn request_id_middleware<S: RequestCtx>(
    State(state): State<Arc<S>>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let _in_flight = InFlightGuard::new(&state.prom().in_flight_requests);
    let request_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("request", request_id = %request_id);

    // `.instrument()` attaches the span to the future for the duration of the
    // await, rather than holding an `enter()` guard across the await point (which
    // would mis-attribute the span once the task yields — the canonical footgun).
    let mut response = next.run(request).instrument(span).await;
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}

#[cfg(test)]
pub(crate) mod test_support;
