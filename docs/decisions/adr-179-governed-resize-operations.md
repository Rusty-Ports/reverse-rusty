# ADR-179 — Governed resize operations

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted (2026-09-30)

## Context

ADR-078 made an in-process shard-count change a correct blue/green rebuild, and ADR-167 gave it a
strict synchronous REST contract. Two gaps remained before the roadmap's first automatic-resize
step could be claimed:

- **No recommendation-versus-operation boundary.** `recommended_shard_count` is a pure function of
  one load snapshot. Nothing decided when a recommendation was stable enough to spend an
  `O(corpus)` rebuild on, and nothing drove it outside tests. ADR-045's "idempotence is the
  hysteresis" argument holds for rebalance but not for resize, whose cost is paid on every call.
- **No operation identity.** A resize request had no name. A client that lost its connection could
  only retry an absolute target, and a delayed retry could undo a later resize by another
  operator. There was no way to observe an operation's progress without taking the cluster lock.

Content-routed placement adds a third concern: every query for one hot anchor lives on one ring
position, so adding shards may not relieve the hottest position at all. A naive loop would grow
the ring to its ceiling without effect.

## Decision

### Pure, clock-injected governor

`ResizeGovernor` (lean core, `cluster::autoscale`) consumes one `ResizeObservation` per interval:
serving shard count, serving placement generation, the advisory recommendation, and the largest
per-shard selective corpus. It returns one verdict:

- **Hysteresis.** Growth is accepted only after `required_observations` consecutive observations of
  the same layout recommend it. A missing recommendation or any layout change resets the streak.
- **Cooldown.** No operation is accepted within `cooldown` of the last observed layout change. A
  fresh governor does not invent a cooldown; restart delay is the hysteresis alone.
- **Bounded, monotone steps.** A target is `min(recommended, current + max_step, max_shards)` with
  `max_shards <= 1024`. The recommendation never shrinks, so automatic operation cannot oscillate;
  shrinking remains an explicit operator decision.
- **Futility hold.** The first observation after a governed operation measures its immediate
  effect. If the hottest selective corpus fell by less than `min_relief_percent`, further automatic
  growth is held until the recommendation clears or an operator or vocabulary change replaces the
  layout. Later organic growth of the same shard is not treated as futility.

The governor has no wall-clock or thread state. The engine exposes only `resize_observation`, a
fail-closed read over the existing load snapshot.

### Operation records

The coordinator server keeps a bounded registry of 64 resize operations, evicting only terminal
records. It never takes the cluster lock. Each record carries an operation ID, origin
(`api`/`autoscaler`), target, optional precondition, lifecycle state
(`queued`/`running`/`succeeded`/`failed`/`not_started`), timestamps, and the attested outcome or
sanitized failure. The dedicated resize worker writes `running` and the terminal state itself, so
a record completes after an HTTP disconnect or loop abort. Drop guards mark a record
`not_started` when its caller disappears before dispatch, and `failed` if the worker unwinds. A
failed record that still holds the newest uncommitted swap is pinned against eviction like an
active one, because only its ID can finish that commit; the pin is released once any success
commits that generation or a newer swap supersedes it. Generated IDs are checked against retained
records so a caller-chosen ID can never alias one.

`POST /_cluster/resize` adds two optional body fields and two response fields:

- `operation_id` makes a request idempotent. A retained success replays its recorded response with
  `replayed: true` and does not rebuild. An active record returns `409 resize_in_progress`, and
  different parameters under a retained ID return `409 operation_id_conflict`. A failed or
  not-started record re-executes under the same ID, which preserves ADR-167's retry-to-heal path.
  An omitted ID is generated and returned.
- `if_placement_generation` is checked under the exclusive guards, before the rebuild starts. A
  mismatch returns `409 placement_generation_mismatch` and changes nothing. One exception keeps
  retry-to-heal available (codex review): when an operation's attempt swapped the serving layout
  but failed to commit it, the record keeps that `uncommitted_generation`, and a retry of the same
  operation passes its precondition at exactly that generation and target so it can finish its
  own commit. Any other layout change still fails.
- Responses now include `operation_id` and the attested `placement_generation`. Every error after
  admission also carries `operation_id`, so a caller that let the server generate the ID can still
  retry the one operation allowed to heal its own swap (codex review).

`GET /_cluster/resize` lists retained operations newest first plus the latest autoscaler
observation and verdict. `GET /_cluster/resize/{operation_id}` returns one record. Both are
strict, lock-free, and `no-store`.

Records are process-local. A restart forgets them, which is safe: targets are absolute, and a
client that needs protection across restarts supplies `if_placement_generation`, which is checked
against durable serving state.

### Opt-in server loop

`--autoscale-resize-interval-secs` starts a loop that observes off the async runtime, records the
verdict, and executes an accepted operation through the same admission slot, dedicated worker,
exclusive guards, and terminal attestation as the REST path. Each automatic operation carries its
observed placement generation as a precondition, so a concurrent operator resize makes it fail
closed instead of stacking a second change. If an automatic operation swaps the serving layout but
fails to commit it, which pauses durable writes under ADR-178, the loop retries that same
operation every interval, before and independently of any new growth decision, until it commits
(codex review). The loop requires an in-process cluster and a positive
`--autoscale-split-threshold`. Startup refuses remote topologies because remote shard-count
changes are not implemented. Shutdown aborts the loop first, and an in-flight rebuild keeps its
permit until the shutdown checkpoint acquires it.

## Alternatives

- **Drive `resize_to_recommended` from the ADR-045 tick.** Rejected: `tick` takes `&self`, runs
  without a clock, and has no memory to distinguish a stable recommendation from noise.
- **Durable operation records.** Rejected for this step. The precondition gives restart-safe
  compare-and-set without a new format, and the in-process rebuild is already crash-atomic at its
  manifest commit (ADR-078/178).
- **Automatic shrink with a low-water mark.** Deferred. Monotone growth, bounded by
  `max_shards`, cannot thrash; shrink decisions depend on workload knowledge the snapshot lacks.
- **Replay failures under an operation ID, as some payment APIs do.** Rejected: a failed post-swap
  commit must be retryable to heal (ADR-167/178), and the absolute target makes re-execution safe.

## Consequences

Operators and automation can name, observe, and safely retry resizes; automatic growth is opt-in,
bounded, and self-limiting. The remote blue/green resize remains roadmap work, and the governor
and records are its intended controller.

## Proof

- Governor unit tests cover the streak, noisy alternation (never accepted), bounded steps and
  ceiling, cooldown after operator and governed changes, the one-shot futility measurement and its
  release, failed-operation re-earning, and determinism.
- Registry tests cover ID validation, generation, replay, in-progress, conflict, failed
  re-execution, active-record retention, and ordering.
- Handler tests cover replay without rebuild, a delayed retry that cannot undo a later resize, the
  precondition refusing start, re-execution after an injected post-swap control failure, an active
  duplicate, not-started recording, strict status reads, and field validation.
- A loop test proves one bounded governed resize, then no further growth during cooldown, with
  identical match results before and after.
- A cluster oracle runs grow/shrink resizes concurrently with writer threads and requires every
  acknowledged query to match the brute-force oracle exactly.

**See also:** ADR-045, ADR-078, ADR-167, ADR-178.
