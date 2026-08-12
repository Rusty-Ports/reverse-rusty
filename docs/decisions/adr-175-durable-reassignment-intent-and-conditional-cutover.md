# ADR-175: Durable reassignment intent and conditional cutover

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Done (2026-08-12)

## Context

ADR-090 deliberately moved data before committing a new assignment. That kept the durable map from
naming an empty target, but it left a second failure boundary: if the live-routing swap succeeded and
the assignment proposal failed, the running coordinator served the target while the durable map still
named a complete move-time source. New writes then made that source stale. ADR-171 made RF=1 retries
safe, but an operator still had to restore the control plane and retry before coordinator restart.

The in-memory move ledger serialized one coordinator's physical work; it was not a replicated
compare-and-set. ADR-094's RF>1 group move had the same restart window, and raw handoff could leave a
different live authority that a later reassignment had to preserve without copying stale data over it.
Startup therefore needed a durable statement of authority and evidence, not a reachability guess.

## Decision

### Replicated move document

The control state now carries a versioned per-position assignment generation and a sorted set of
active move intents. An intent records one immutable transition predicate:

- operation and intent version, position, expected assignment generation, and placement generation;
- complete expected and desired assignments;
- every participating logical node plus its normalized endpoint identity;
- future live generation, the exact expected-source fence generation, and whether the expected or
  desired side was authoritative when the intent began; and
- `Preparing`, `Ready(evidence)`, or `Committed(evidence)` phase.

Recovery evidence is the live generation plus the exact content fingerprint and live-row count for
every desired member. Logical node IDs may alias an endpoint across the expected and desired
assignments, but one assignment may not contain the same physical endpoint twice.

`Begin`, `MarkReady`, `Commit`, `Abort`, and `Finish` are idempotent by operation ID. Their full
preconditions run inside the replicated state machine. `Begin` reserves every recorded endpoint
across coordinators and compares the assignment generation, complete assignment, placement
generation, and member identities. `Commit` is legal only from `Ready` with unchanged predicates;
it atomically installs the complete desired assignment, increments that position's generation, and
marks the intent `Committed`. Ordinary assignment writes also increment the generation, so they
cannot pass through a move race unnoticed.

### Cutover order

Normal RF=1 and RF>1 movement uses this order:

1. persist `Preparing` before any irreversible physical side effect;
2. recover or attest every desired member;
3. fence the exact source generation and drain its finite mutation tail;
4. persist the desired members' exact fingerprints as `Ready`;
5. conditionally commit the complete desired assignment in consensus;
6. swap the coordinator's live routing to the already-complete desired backing; and
7. clear retained-source fences and `Finish` the intent.

The target cannot accept live writes before consensus names it, so live routing never gets ahead of
the durable assignment. A clean pre-ready failure with expected authority proves an unfence, then
aborts the intent. An ambiguous control or fence outcome returns a typed error and preserves the
intent for startup; it is not converted to a successful degraded result.

An RF=1 target that is already live—because of raw handoff or earlier map/live divergence—begins an
intent with desired authority before fencing or adopting anything. The intent records and re-probes
the exact old-source fence, attests the live target, then commits without stale recopy. If a third
physical source is live while both requested sides differ, the coordinator first records and
completes a durable transition from the committed source to that authority, then plans the requested
move from the newly committed source. A logical-ID-only alias uses a zero source fence because both
IDs name one physical slot.

### Startup recovery and compatibility

Assignment-routed startup resolves all intents before assembling routes or serving reads and writes:

- `Preparing` with expected authority proves the source unfenced and aborts;
- `Preparing` with desired authority re-establishes the recorded source fence, attests the desired
  side, and commits;
- `Ready` requires the exact recorded fence and unchanged recovery evidence before commit; and
- `Committed` treats consensus as the authority decision, attests every recorded endpoint,
  placement, and source fence, clears any retained-source fence, then finishes. Post-cutover writes
  may legitimately advance the desired fingerprints.

Endpoint replacement, assignment/placement drift, missing quorum, unexpected fence generation,
changed ready evidence, overlapping intents, or any other ambiguity fails coordinator startup. No
branch selects an authority merely because an endpoint is reachable.

The first move command atomically rewrites a legacy Raft log to the one-way `RRL4` header before
appending. Snapshots carrying move state require move-control format 4. Pre-release `RRL2`/`RRL3`
and move-control formats 2/3 are rejected because their weaker predicates cannot be replayed under
the final state machine without divergence. An older binary rejects `RRL4` or the current snapshot
format instead of silently ignoring transition state.

## Correctness argument

Before `Commit`, the expected source remains the only write authority and either serves live or is
fenced with its authority recorded. Every desired member is derived from that source and `Ready`
binds the complete member set to exact evidence. `Commit` compares the same assignment generation,
placement generation, assignments, and endpoint identities that `Begin` observed, so a racing
coordinator or membership change cannot redirect the transition. After `Commit`, consensus names
only the complete desired group; the local swap exposes that already-proven group without a write
window on two primaries.

A crash lands in one durable phase. Startup either proves that a pre-ready expected-authority move
can be discarded, completes a recorded desired-authority or ready move, or follows the already
committed decision. If the required proof is unavailable, startup fails loud. Thus every
acknowledged query remains on the selected authority and no stale bare assignment is served.

## Alternatives

- **Keep move-then-commit plus operator retry.** Rejected because quorum loss plus coordinator crash
  still required a time-sensitive manual action and left RF>1 unresolved.
- **Commit before target recovery.** Rejected because a reader could route to an empty or incomplete
  target.
- **Infer authority from reachability or the largest fingerprint.** Rejected because availability
  and corpus size do not prove which side accepted the latest acknowledged write.
- **Use only the coordinator-local ledger.** Retained for efficient scheduling, but insufficient for
  multiple coordinators and restart because it is neither replicated nor durable.
- **Atomically include the live in-memory swap in Raft.** Impossible across the control state machine
  and shard processes. Recording evidence and committing before the local swap gives one durable
  decision that startup can reproduce.

## Consequences and evidence

Movement adds several small consensus entries and may leave an intent requiring quorum and endpoint
availability before a coordinator can restart. This is an intentional availability trade: ambiguous
authority blocks service rather than risking false negatives. The raw `/_cluster/handoff` endpoint
remains explicitly uncommitted for low-level testing; native reassignment, rebalance, reconcile, and
autoscaler movement use the durable protocol. Legacy `MovedButNotCommitted` response fields remain
source/API-compatible but the built-in durable mover no longer produces that outcome.

Control-state tests cover command idempotence, complete conditional predicates, endpoint overlap,
logical aliases, snapshots, log replay, unsupported format rejection, and concurrent proposals.
Localhost gRPC tests cover RF=1 and RF>1 restart recovery at preparing, ready, committed, live-swap,
and cleanup boundaries; injected commit-quorum loss; placement/member/fence/evidence ambiguity;
exact source authority; chained third-source reconciliation; and preservation of acknowledged
matches.
