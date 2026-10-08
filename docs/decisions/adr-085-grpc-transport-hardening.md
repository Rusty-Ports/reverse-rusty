# ADR-085: gRPC mesh-transport hardening — timeouts, keepalive, bounded read-retry, RPC metrics

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md)

**Status:** Accepted (2026-06-24)

**Context.** The distributed multi-node layers are built and oracle-proven on localhost but
explicitly **experimental — not hardened for real multi-machine deployment**
([threat model](../operations/threat-model.md)). The single most impactful correctness gap in that layer was that the
gRPC **client set no connect timeout, no per-RPC deadline, and no keepalive**:

- `configure_endpoint` (`src/cluster/security.rs`) built the channel with TLS only — no
  `connect_timeout`, no HTTP/2 keepalive — and it is the **shared** dial helper for the shard client
  (`RemoteShard`), the control-plane client (`RemoteControlPlane`), and the Raft peer network
  (`control_raft/network.rs`).
- Every RPC in `src/cluster/remote.rs` awaited with no deadline (e.g. `client.percolate(req).await`).
- The coordinator's percolate fan-out (`coordinator/matching.rs`) probes shards with
  `collect::<Result<_,_>>()?`. A remote shard that **accepts a connection then hangs** (GC pause,
  deadlock, half-open socket after a peer crash) blocked `RemoteShard::block_on` **forever**, hanging
  the whole percolate — the cardinal operational failure for a matcher whose contract is to answer.
- The servers (`ShardServer`/`ControlServer`) set no server-side HTTP/2 keepalive, so a dead client
  connection leaked server resources.
- The transport emitted **no metrics** — per-RPC latency / errors / timeouts were a black box.

The fix must respect the cardinal invariant: **a transport failure must fail loud, never silently
shrink a percolate's union** (that would be a false negative). A timeout turns a *hang* into a loud
`ShardError`; it must not turn a shard into "skipped."

**Decision.** Harden the mesh transport in four layers, all default-on with conservative values and
byte-identical on the in-process path.

- **Endpoint hardening (one place, both planes).** `configure_endpoint` now applies
  `connect_timeout` + `tcp_keepalive` + HTTP/2 `keep_alive_interval`/`keep_alive_timeout`/
  `keep_alive_while_idle`. Because it is the shared dial helper, the shard client, the control-plane
  client, and the Raft peer network all harden together (the same "one implementation so the two
  planes cannot drift" rule as the mesh-token machinery). Knobs live on a new `MeshTransport` carried
  by `ClientSecurity`; `ServerSecurity` gains the server-side keepalive pair. `ShardServer` and
  `ControlServer` set `http2_keepalive_interval`/`timeout` on the server builder.

- **One instrumented `call` seam in `RemoteShard`.** Rather than scatter timeout/retry/metric logic
  across ~15 RPC methods, every RPC funnels through `call(method, kind, mk)`:
  - **Per-call deadline via `tokio::time::timeout`**, NOT `Endpoint::timeout()` (channel-wide — it
    would also kill the legitimately long streaming recovery RPCs that share the channel) and NOT
    `tonic::Request::set_timeout` (does not exist in tonic 0.14). `mk` is a **factory** because a
    tonic call future is single-use — each attempt rebuilds it from a cloned client + request.
  - **Bounded fail-loud retry of IDEMPOTENT reads** (percolate, counts, fingerprint) on a transient
    error (gRPC `Unavailable`, or the transport-readiness `Unknown` tonic's generated client raises
    as "Service was not ready" when a channel can't reconnect) or a timeout, with exponential backoff
    (50ms→1s cap). **Writes never
    retry** — `ingest`/`insert`/`delete` are non-idempotent (a retry could double-apply); they fail
    loud and converge via the coordinator's durable log + `resync` partial-apply repair
    ([ADR-047](adr-047-remote-partial-apply-resync.md)).
  - **Long-running / streaming RPCs** (`FetchTranslog`, `RecoverFrom`) opt out of the per-call
    deadline (a real recovery is legitimately slow) and rely on connect-timeout + keepalive to notice
    a dead peer.
  - A timeout or exhausted-retry surfaces as `ShardError::Remote` and propagates through the fan-out's
    `collect::<Result<_,_>>()?` — **fail-closed, zero false negative.**

- **Lean transport metrics.** A new std-only `src/cluster/transport_metrics.rs` (`TransportMetrics`
  atomic per-method counters: calls / errors / timeouts / retries / summed latency) is shared (`Arc`)
  from the coordinator into every serving `RemoteShard`; `ClusterEngine::transport_metrics()` returns
  a `TransportMetricsSnapshot` (the pull-on-scrape pattern of `EngineMetrics`). The cluster-mode
  `GET /_metrics` handler refreshes Prometheus gauges (`transport_rpc_{calls,errors,timeouts,retries}`
  + `transport_rpc_latency_seconds`, labeled by `method`) from it on each scrape. The writer side is
  `distributed`-gated; the collector + snapshot are lean (cluster mode is not feature-gated), so an
  in-process cluster reports all-zero and is byte-identical.

- **Coordinator CLI tuning.** `--grpc-connect-timeout-secs` / `--grpc-read-timeout-secs` /
  `--grpc-write-timeout-secs` / `--grpc-keepalive-secs` / `--grpc-read-retries` (all optional, default
  to the always-on `MeshTransport` profile) tune the coordinator's client links — the hot-path
  percolate fan-out. Shard/control nodes use the defaults.

**Consequences / scope.**

- **Default-on, conservative** (connect 5s, keepalive 10s/20s, read deadline 10s, write deadline 30s,
  2 read retries). The in-process / RF=1 path never builds a `RemoteShard`, so behavior and metrics
  are byte-identical; the plaintext gRPC path gains keepalive (a safe behavior change). A timeout only
  ever fails a percolate loud — it never drops a shard from the union.
- **Recovery/handoff transient `RemoteShard`s** also share the collector (the handoff *target*
  becomes a serving shard once swapped into routing, so its metrics must aggregate), so the
  peer-recovery / catch-up / handoff RPCs (`recover_from`, `translog`, `retention_lease`,
  `fence`/`unfence`) are counted too. And because `fence` now carries a write-deadline, a fence that
  times out *after* the server applied it would have stranded the source write-fenced; the handoff's
  fence path now attempts the CAS-safe `unfence(new_gen)` cleanup on failure (lifts a real fence,
  no-op otherwise). Both were codex-review findings on this change.
- **Proven:** unit tests in `remote.rs` (`run_with_retry` timeout fires; transient retry recovers;
  non-transient + write paths do not retry; backoff bounds; `is_transient` only `Unavailable`) + a
  real-gRPC integration test (`cluster_grpc_oracle::transport`): metrics recorded on the happy path,
  and a percolate against a **downed** shard fails loud + fast with the read retried and the error
  counters reflecting it. The full distributed oracle stays green (zero-FN preserved).
- **Explicitly NOT done:** partial-results / graceful degradation on shard failure — it would shrink
  the union into a false negative; the fan-out deliberately stays fail-closed. Also deferred:
  distributed tracing / correlation IDs, control-plane RPC metrics, a per-call deadline on the long
  recovery RPCs (keepalive-guarded for now), and mTLS / per-node identity / cert hot-reload (the
  [ADR-071](adr-071-grpc-tls-auth.md) post-v1 items).
- **Later outcome — 2026-10-06:** the retry classifier covered `UNAVAILABLE` and tonic's
  "Service was not ready", but not a request written to a connection the peer had already closed.
  After a shard node restarts, the coordinator still holds its old connection until it notices the
  close, and a read issued in that window failed with `UNKNOWN` "transport error" (broken pipe)
  instead of being retried: the first read after a restart could error. A status whose cause is a
  lost connection (broken pipe, reset, aborted, not connected, unexpected end) is now transient
  too. Only idempotent reads retry, as before; a status sent by the server has no local cause and
  is never matched.
- **Later outcome — 2026-10-08:** that covered the lost connection the socket reports. Two more
  ways of losing a request with its connection reach the caller with no I/O error. hyper drops a
  request it had queued when the connection's task ended, which tonic reports as `CANCELLED`
  "operation was canceled: connection closed" with no cause; and a stream is reset under the
  request, or the I/O error surfaces inside HTTP/2, which tonic reports as `CANCELLED` or
  `UNKNOWN` "h2 protocol error: ..." with no cause. Which one a read gets after a shard goes
  down depends on which side notices first, so a read was sometimes not retried (seen as a gate
  failure on Linux: `transport_metrics_recorded_and_downed_shard_fails_loud`, zero retries).
  They are transient now, with hyper's "channel closed" and "connection closed before message
  completed" under tonic's "transport error". gRPC means these to be retried: a request that
  never reached the server's application is "always safe to retry" (gRFC A6), and grpc-go
  reports a lost connection as `UNAVAILABLE`. tonic splits it across three codes, and the
  classifier puts it back together. A timeout the server enforced (`CANCELLED` "Timeout
  expired"), its `INTERNAL` and its `RESOURCE_EXHAUSTED` are not retried, as before.
