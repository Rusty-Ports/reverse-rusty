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
   forgotten. The drop runs in three steps: tombstone the slot's fence, so no fence traffic can
   bring it back; write the record, with the slot map unlocked, because every other shard on the
   node resolves its slot through that lock and a synced write must not stall them; then, under
   the map's write lock, quarantine the directory and remove the slot. If the record or the
   quarantine fails, the fence is restored, the record is taken back, and the slot stays hosted
   for a retry.
2. **A slot created for a remembered shard is born awaiting recovery.** `insert_slot`, which both
   slot-creating RPCs go through, sets the flag. Creation is not refused: a handoff *back* to this
   node begins with exactly the same adoption and then fills the slot with `RecoverFrom`.
3. **A slot awaiting recovery serves nothing.** `loaded_slot`, where every data RPC resolves its
   slot, refuses it as an ownership mismatch (`failed_precondition`, with the typed code a
   coordinator already treats as a superseded placement). Reads, writes, counts, leases, and
   serving as a recovery source are all refused. Only the RPCs that manage the slot reach it:
   `RecoverFrom`, and `Fence`/`Unfence` so that orphan GC can still arm and drop a slot an
   abandoned handoff left behind.
4. **A successful `RecoverFrom` clears it, in a fixed order.** Recovery publishes the recovered
   state and only then releases the slot; a data RPC checks that the slot is released *before*
   it loads the state. So a released slot always means the state loaded next is the recovered
   one. In the other order a request could load the empty state, find the slot released a
   moment later, and answer from the empty state. The record is updated durably before the slot
   is released; if that fails the recovery fails and the slot keeps refusing.
5. **The record belongs to one layout.** It stores the dict and tag-dict fingerprints, the
   placement generation and the shard count. When the node adopts a layout it takes up the
   record that belongs to it: empty for a different layout (possible only while no slot holds
   data), in which case slots still awaiting recovery from the old one are released too, since
   they are empty and an idempotent re-adoption would never replace them; and the stored record
   for a durable node that restarted pending and adopts the layout it had before. A damaged
   record fails loudly rather than being read as "nothing dropped". Adoption reads the record
   before it builds, persists or publishes anything, so an adoption that cannot read it leaves
   the node unchanged and a retry fails the same way; read after the layout was published, the
   retry would have found the layout already adopted and created a slot that serves.
6. **Every durable constructor loads the record.** `open_durable` and the pre-built
   `new_durable` restore it with their slots; `pending_durable` has no layout yet and takes it
   up at adoption.

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
  such a slot can still be fenced and dropped; adoption under a new placement generation serves,
  and releases a slot that was still awaiting recovery; a durable restart remembers through all
  three durable constructors, including for a slot re-created before the restart; a request
  interleaved with a recovery at the one point where the order matters is refused or sees the
  recovered state, never the empty one; an adoption over an unreadable record fails on the first
  attempt and on the retry, leaving the node pending; a drop is recorded before the slot is
  removed and with the slot-map lock free; and a drop that cannot be recorded, or cannot
  quarantine its directory, leaves the slot hosted at its fence with nothing remembered, also
  across a restart.
- `cluster/server/dropped.rs` unit tests: the record round-trips, belongs to one layout, and a
  truncated, padded, mis-tagged or future-version file fails loud.
- `tests/cluster_grpc_oracle/gc_readopt.rs`: over real gRPC, a shard moves away, is dropped,
  moves back by recovery and serves with zero false negatives; a coordinator connected through
  the outdated topology never reads an incomplete result.

**See also:** ADR-096 (orphan GC), ADR-180 (node retirement), ADR-093 (per-shard slots),
ADR-109 (ownership validation).
