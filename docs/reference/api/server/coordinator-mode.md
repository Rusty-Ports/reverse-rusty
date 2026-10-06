# Cluster (coordinator) mode — `--cluster` (ADR-070)

> [Server & shared behavior](../server.md) · [REST API hub](../../api.md)

The same binary also runs as a **cluster coordinator**: the same REST API is served over a
multi-shard [`ClusterEngine`](../../../design/clustering-and-scaling.md) instead of a single-node engine
(Distributed-v1 criterion 1, [ADR-065](../../../DECISIONS.md)). Auth (ADR-062), request-id middleware, and
Prometheus wiring are identical.

```bash
# In-process cluster: K shards in this process, durable under --data-dir.
cargo run --release --bin server -- --cluster --shards 8 --data-dir ./cluster-data \
  --load-file queries.csv

# Remote cluster (requires --features distributed): one --shard-endpoint per shard
# position, each "primary[,replica,...]" — the coordinator ships its frozen dict +
# tag space to every endpoint at connect (ADR-034/055).
cargo run --release --bin server --features distributed -- --cluster \
  --shard-endpoint http://10.0.0.1:50051,http://10.0.0.2:50051 \
  --shard-endpoint http://10.0.0.3:50051,http://10.0.0.4:50051 \
  --load-file queries.csv
```

Cluster-mode flags: `--cluster`, `--shards` (in-process K, default 8), `--replication-factor`
(in-process copies per position), `--shard-endpoint` (repeatable; remote mode). Remote links take
the **mesh security** flags (ADR-071): `--grpc-tls-ca` (PEM CA to verify shard servers — endpoints
then use `https://`), `--grpc-tls-domain` (SNI/verification override for raw-IP endpoints), and
`--cluster-token`/`RR_CLUSTER_TOKEN` (the shared mesh secret attached to every gRPC RPC — distinct
from the HTTP `--auth-token`). The server side of the mesh is configured on
`shardserver`/`controlserver` (`--tls-cert`/`--tls-key`/`--cluster-token`; both also take the
client half `--tls-ca`/`--tls-domain` — the controlserver for its peer Raft links, the
shardserver for the `RecoverFrom` outbound pull from a peer source). `shardserver` also takes
`--ranking-profiles-file`/`RR_RANKING_PROFILES_FILE`; it must follow the shared-registry and
attestation contract in the [ranking reference](../../ranking.md#5-topologies-and-distribution).
It also takes
`--max-grpc-result-bytes` (default/hard ceiling 4 MiB; any positive lower byte bound is valid),
enforced against exact protobuf size for compatibility replies, top-K replies, and every fetched
source stream item (ADR-110). ADR-114 adds node-local exhaustive-stream limits:
`--max-concurrent-exhaustive-streams` (default 2, non-queuing) and
`--max-exhaustive-stream-secs` (default 300, a hard ceiling on the coordinator/direct caller's
remaining budget). In remote mode, configure that duration at least as high as the coordinator's
`--exhaustive-job-timeout-secs`; an over-ask fails loud before shard admission.

**Shard-local engine settings (ADR-192).** A remote coordinator ships a dictionary to its shard
nodes, never engine configuration, so the settings that belong to the process holding a shard's
disk are `shardserver` flags:

| `shardserver` flag | Default | Effect |
|---|---|---|
| `--wal-sync-on-write <true\|false>` | false | Fsync the shard translog on every write, so an acknowledged write survives a power loss and not only a process crash |
| `--retain-source <true\|false>` | true | Keep query source text in memory, or on disk and read on demand (ADR-020) |
| `--max-segments N` | 8 | Base segments before compaction triggers |
| `--memtable-flush-threshold N` | 100000 | Memtable entries before an automatic flush |
| `--hot-anchor-threshold N` | 0 | Class-H threshold (ADR-105); run the coordinator's value |
| `--tag-segment-skipping <true\|false>` | true | ADR-174 kill switch; run the coordinator's value |
| `--broad-columnar <true\|false>` | true | Columnar broad evaluator; `false` is the kill switch (identical results) |
| `--broad-materialize <true\|false>` | true | Pure-anchor materialization fast path; `false` verifies instead (identical results) |

They apply to every slot on the node and are not stored with the data, so a restart with a
different flag changes them. The node prints its sync policy at startup and exports
`reverse_rusty_shard_translog_sync_on_write{shard}` on `/_metrics`. The coordinator's flags of
the same names never reach a remote shard: it **refuses** `--wal-sync-on-write` (it stores no
shard data, so the flag would promise durability it cannot provide) and warns about the rest.
On `server` the Boolean flags take a value too (`--retain-source false`, `--broad-columnar false`,
`--broad-materialize false`).
`--data-dir` makes
an **in-process** cluster durable (build once, reopen on restart — `--load-file` is skipped with a
warning when the reopened cluster is already populated). A **remote** coordinator is stateless and
refuses `--data-dir`: durability lives on the shard nodes (`shardserver --data-dir`, the per-shard
translog — ADR-039); restarting the coordinator reconnects and re-mints the identical frozen dict
from the same `--load-file`, so the fingerprint handshake holds. Its new boot ID may need to retry
until the 30-second renewable owner lease expires, then wait for any response bodies/streams
admitted under the prior owner to drain before taking over a node.

A resolve-only coordinator takes its shard count and placement generation from the committed
control document, so it follows a remote resize (ADR-180) without editing `--shards`; every other
mode must match its CLI topology. Once it holds exclusive shard claims, startup also aborts an
uncommitted recorded resize or finishes a committed one.

Topology-changing data movement requires a resolve-only coordinator after the initial seeded boot:
`--route-by-assignments`, `--control-endpoint`, the committed `--shards` count, and no
`--shard-endpoint`. `--reconcile-interval-secs` is accepted only in this mode; its optional
`--reconcile-max-parallel` width and `--reconcile-gc-orphans` epilogue share one admission slot with
manual `POST /_cluster/reconcile` and `POST /_cluster/gc`. One-shot GC also works during the initial
CLI-seeded assignment-routed boot because it does not change the map. See the
[reconcile](../cluster/reconcile.md) and [GC](../cluster/gc.md) API contracts for safety and shutdown
semantics.

An **in-process** coordinator may opt into governed automatic growth (ADR-179):
`--autoscale-resize-interval-secs N` with `--autoscale-split-threshold Q` (selective queries per
shard) observes load every `N` seconds and executes one bounded resize when the recommendation
persists. `--autoscale-resize-observations` (default 3), `--autoscale-resize-cooldown-secs`
(default 900), `--autoscale-resize-max-step` (default 8), `--autoscale-resize-max-shards`
(default 64, at most 1024), and `--autoscale-resize-min-relief-percent` (default 10; 0 disables the
futility hold) tune it. Automatic operations share the resize admission slot and appear in
[`GET /_cluster/resize`](../cluster/resize-operations.md). Remote topologies refuse the flag at
startup.

Behavior deltas from single-node mode (all deliberate, none silent):

- **`PUT /_doc/{id}` is a cluster-atomic upsert** — one coordinator log frame replaces every prior
  live copy (ES `index` semantics, the ADR-067 contract at the cluster). A partial multi-shard apply
  (remote clusters only) answers 200 with `"result": "partial"`: the write **is** durably logged and
  queued for repair — do **not** re-PUT (it would double-log); `POST /_cluster/resync` converges it.
  `op_type=create` uses the coordinator's atomic logical-id reservation and returns 409 without a
  log frame when the id exists. Remote startup reconstructs this membership through bounded shard
  enumeration; an unsupported, failed, or over-limit enumeration keeps create-only writes disabled
  ([details](../../../design/clustering-and-scaling.md#94-remote-admission-reconstruction)). `refresh=false|true|wait_for`
  are accepted under the stronger publish-before-response model; unsupported write parameters fail
  with 400 instead of being ignored.
- **`DELETE /_doc/{id}` is a log-first all-position remove** — successful and missing responses
  match the single-node contract and report one logical deletion rather than physical placement
  copies. `refresh=false|true|wait_for` are accepted under immediate visibility; every other control
  fails before mutation. A remote partial apply returns retryable 503 `"result": "partial"` with
  the applied and pending positions. Repeat the idempotent DELETE, or use `POST /_cluster/resync`
  while the same coordinator still owns its in-memory repair queue (ADR-125).
- **Per-request `include_broad`** is honored on compatibility and v2 search/batch surfaces. It adds
  class C and accepted D; class H remains default-visible.
- **`rank` works (ADR-075)** — the same block as single-node, scored at the shards against the shared
  tag space and merged `(score desc, _id asc)` with `from`/`size` + `_score`. One cluster-specific
  boundary: a **post-freeze (live-added) `priority` tag scores 0** — priority reads the tag's value
  string, which only a build-time interned tag has; boosts fire for both (id-equality). `explain` is
  rejected with 400 — never silently ignored. `profile` works (merged cross-shard `MatchStats`).
  This paragraph describes compatibility `/_search`/`/_mpercolate`.
- **`/v2/_search` uses ADR-110 bounded delivery** — at most K owned hits per routed position,
  exact coordinator merge, honest thresholded totals, current-source fetch for final winners, and
  coordinator-compiled explanations. It defaults `include_source=true`, supports remote shards, and
  fails the whole response on timeout, stale placement, missing source, fetch/protocol failure, or
  enrichment overflow; partial results are unsupported. Source/explanation requests fence direct
  mutations through match and fetch, while source-free reads stay concurrent. ES/OS aliases
  `_source`, numeric `track_total_hits`, and time-value `timeout` are strict body/query controls
  (ADR-127). Strict typed `rank_fields.priority` remains signed and available after tag-dict freeze.
- **Compatibility `include_source` defaults to `false`** (`_source` costs a per-hit source probe);
  explicitly requesting it on a remote cluster answers 501. ADR-110 source streaming applies only to
  `/v2/_search`.
- **Compatibility `/_mpercolate` keeps per-title cluster fan-out** rather than claiming the
  standalone ADR-026 columnar-batch optimization. Its ordered match slots remain exact, but
  `profile: true` returns `501 profile_unsupported`; the top-level broad summary is standalone-only
  (ADR-135).
- **`GET`/`HEAD /_settings` works in cluster mode** — it returns the live cluster + per-shard
  configuration (`mode`, `shards`, `replication_factor`, `include_broad`, `durable`, and the
  assembled `per_shard` `EngineConfig`), plus per-shard built-in `defaults` when requested. The read
  is strict, no-store, and bounded off the async runtime (ADR-159). **`PUT /_settings`** validates
  the same strict transport and patch contract before returning 501 in cluster mode (ADR-160; see
  the [`PUT /_settings` contract](../settings/update-settings.md)).
- **Single-node-only surfaces answer 501 naming the alternative:** `/_compact` / `/_forcemerge`
  (per-shard policy; use `POST /_checkpoint` for the durability commit), `PUT /_settings` (cluster
  settings are fixed at assembly — restart the coordinator and consistently configured shard nodes
  with the new flags), `/_cat/stats`, `/_cat/segments`.
- **Vocabulary admin** (`PUT /_vocab`, `/_vocab/learn_and_apply`, `/_vocab/aliases/*`) maps onto the
  cluster blue/green rebuild (ADR-046); its one refusal — non-local (gRPC) shards — surfaces as a 400
  with the engine's message. The current remote transport ships dictionaries but not normalizers, so
  remote shard servers support only the stock vocabulary (ADR-076). A
  **tagged** cluster is not refused (tags carry through by stored `TagId`, ADR-074), and a
  **multi-word alias activates** (P(T)-aware routing, ADR-076). At startup, `--vocab-file` on a fresh
  in-process cluster fully activates (`build_with_vocab`). A durable reopen keeps the manifest's
  feature model authoritative (ADR-184): a persisted vocabulary is restored and a differing file
  only warns; a manifest without a vocabulary reopens under the stock normalizer its queries were
  compiled with, activating the file through the rebuild funnel only when the cluster is **empty**
  (a **populated** reopen warns that the file was not applied — apply it via `PUT /_vocab`). A
  normalizer whose fingerprint differs from the manifest's refuses to open, as does a pre-ADR-184
  manifest without a vocabulary given a `--vocab-file` that changes normalization (its model
  cannot be verified; restart without the file, then `PUT /_vocab`). A REMOTE assembly
  refuses any `--vocab-file` at startup (shard servers run the stock normalizer, so even
  normalizer-level rules would silently diverge the feature space).

Cluster-only routes are cataloged by responsibility:

- durability operations: [`/_checkpoint` and `/_backup`](../ingest.md);
- control state and shard visibility:
  [`/_cat/shards` and `/_cluster/state`](../observability.md);
- membership and topology changes: [`/_cluster/*`](../cluster.md).

`GET /_stats` in cluster mode reports timing and `_shards` plus
`{shards, replication_factor, total_queries, shard_queries[], class_counts, epoch,
pending_repairs, has_tagged_queries, durable}`. Counts are the primary physical-row view (including
tombstones and content-driven multi-position copies), not distinct live logical IDs; a missing
position fails the whole response (ADR-140). `GET`/`HEAD /_health` validates every serving
position plus the committed control topology: green is ready, yellow has queued repairs, and red
means a required dependency or topology check failed (ADR-144).
