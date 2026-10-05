# ADR-189 — A shard a node gave up serves nothing until it is recovered

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Orphan GC (`DropShard`, ADR-096) removes a slot whose data a handoff moved to another node. It
left no trace on the node that the shard had been there. The node kept its adopted feature space,
and both RPCs that create a slot (`AdoptDict`, `AddShard`) build an empty one that serves at once.

A coordinator with a stale view of the topology (an old process, a restart from an outdated
assignment, a misconfigured endpoint list) still names the old owner for that shard. It connects,
re-adopts the shard, gets a brand-new empty slot, and reads from it. Those reads succeed and
return no matches. Distributed exact reads are required to fail loudly instead.

ADR-180 closed the same hole for nodes retired by a remote resize, by refusing every RPC on a
retired node. GC is a different path: a node that loses one shard keeps serving its others.

## Decision

1. **The node remembers what it dropped.** `DropShard` records the shard id before it removes
   the slot. On a durable node the record is `dropped_shards.bin` at the node root, written with
   an atomic synced rename before the slot directory is quarantined, so no crash leaves a drop
   forgotten.
2. **A slot created for a remembered shard is born awaiting recovery.** `insert_slot`, which both
   slot-creating RPCs go through, sets the flag. Creation is not refused: a handoff *back* to this
   node begins with exactly the same adoption and then fills the slot with `RecoverFrom`.
3. **A slot awaiting recovery serves nothing.** `loaded_slot`, where every data RPC resolves its
   slot, refuses it as an ownership mismatch (`failed_precondition`, with the typed code a
   coordinator already treats as a superseded placement). Reads, writes, counts, leases, and
   serving as a recovery source are all refused. Only the RPCs that manage the slot reach it:
   `RecoverFrom`, and `Fence`/`Unfence` so that orphan GC can still arm and drop a slot an
   abandoned handoff left behind.
4. **A successful `RecoverFrom` clears it.** The slot then holds the current owner's data. The
   record is updated durably before the flag is cleared; if that fails the recovery fails and the
   slot keeps refusing.
5. **The record belongs to one layout.** It stores the dict and tag-dict fingerprints, the
   placement generation and the shard count. Adopting a different layout (possible only while no
   slot holds data) starts over, and a record left by a previous layout is ignored on restart.
   A damaged record fails the restart rather than being read as "nothing dropped".

## Alternatives considered

- **Refuse to re-create the slot until the node is wiped.** The simplest rule, and wrong for an
  elastic cluster: a rebalance that moves a shard back to a node that once held it would fail
  until an operator intervened.
- **Detect an unexpectedly empty slot in the coordinator.** It relies on every coordinator path,
  and every coordinator version, getting it right. The node is the one place all of them meet.
- **Retire the whole node when its last slot is dropped.** It does nothing for a node that still
  hosts other shards, which is the common case with co-location.

## Consequences

- A stale topology fails loudly where it used to read an empty slot. Its connect may succeed;
  its reads and writes on that shard do not.
- Moving a shard back to a node that gave it up works as before, through recovery.
- Re-seeding a node from scratch under the **same** layout (same dictionary, placement
  generation and shard count) is refused for the shards it had dropped: wipe the node's data
  directory first. A new layout needs nothing.
- An in-memory node keeps the record in memory. A restart of such a node loses its data and the
  record together, as it loses everything else.
- One small file per durable node; it is rewritten only when a shard is dropped or recovered.

## Proven

- `cluster/server/tests/dropped.rs`: a dropped shard re-created by `AdoptDict` or `AddShard`
  refuses reads, writes and counts as an ownership mismatch while a never-dropped slot serves;
  such a slot can still be fenced and dropped; adoption under a new placement generation serves;
  a durable restart remembers, including for a slot re-created before the restart.
- `cluster/server/dropped.rs` unit tests: the record round-trips, belongs to one layout, and a
  truncated, padded, mis-tagged or future-version file fails loud.
- `tests/cluster_grpc_oracle/gc_readopt.rs`: over real gRPC, a shard moves away, is dropped,
  moves back by recovery and serves with zero false negatives; a coordinator connected through
  the outdated topology never reads an incomplete result.

**See also:** ADR-096 (orphan GC), ADR-180 (node retirement), ADR-093 (per-shard slots),
ADR-109 (ownership validation).
