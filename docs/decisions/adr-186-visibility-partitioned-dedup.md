# ADR-186 — Dedup groups are partitioned by visibility

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Canonical-body dedup (ADR-106) lets a query whose semantic body equals an existing leader's join
that leader's group. The member inserts no postings and takes the leader's class byte, so it is
reached, and gated for visibility, through the leader's lane. ADR-106 argued this was lossless
because identical bodies could differ only among the always-visible classes (A, B, H): class C was
"keyed to the frozen top-64 mask" and so could not differ between two copies of one body.

That holds for a body with a required feature, where the required mask is part of body equality.
It is false for a body with **no required feature**:

- **Frequency drift.** Such a body anchors on the any-of group whose most frequent member is least
  frequent *by live frequency*, and is class C only if that group has a top-64 member. Live
  inserts keep moving frequencies after the mask freezes, and body equality ignores frequency, so
  two copies of `(h,x) (g,y)` can anchor on different groups and plan C and B.
- **Compile before the first mask finalize.** Nothing is top-64 before the first finalize, so
  `(h,x)` compiled then is class B. Any-of groups are stored as raw feature ids, so the same body
  compiled after `h` gets a mask bit compares equal and is class C.

All three join sites (the memtable write, the grouped merge, the re-anchoring merge) copied the
leader's class without comparing visibility. A default-visible copy that joined a class-C leader
was hidden from every `include_broad=false` read, with default settings, and flush wrote the
adopted class to disk. With dedup off the same row was found, so the "dedup on ≡ dedup off"
guarantee was broken too. The reverse join exposed an opt-in row on default reads. The
re-anchoring merge's demote guard (ADR-056) did not help: it ran only for the leader that
re-derives a cover, and members bypassed it.

## Decision

1. **One definition of the boundary.** `CostClass::is_opt_in` (C and D) is used by the read-side
   visibility gate and by every dedup join.
2. **A join never crosses it.** Each join site adds "same side of the opt-in boundary" to its
   leader search, next to exact body equality:
   - the memtable write compares the leader's class with the new entry's own planned class;
   - the grouped merge compares it with the entry's stored class;
   - the re-anchoring merge compares it with where the entry's stored class would land under the
     same guard a leader goes through (`opt_in_after_reanchor`), given the class the leader's
     body re-derived to.
   An entry that finds no leader on its side leads a second group for the body, so a body has at
   most two groups per segment. In the re-anchoring merge it then goes through the guard like any
   leader and keeps its own cover.
3. **The guard is one function.** `refuses_visibility_move` decides for leaders and predicts for
   members, so the two cannot drift apart. It now refuses any always-visible → opt-in move, not
   only → C.
4. **Adoption inside one side is unchanged.** A member still adopts its leader's class among A, B
   and H (the θ-crossing case ADR-106 was designed for), and among opt-in classes.

The re-anchoring rule matters for sharing, not only for safety. Comparing stored classes alone
would be correct but would dissolve a group whose body legitimately re-plans from C to B: the
leader becomes visible, and every other copy, still stored as C, would refuse to join it and
would lead a group of its own.

## Alternatives considered

- **Make class C mask-keyed for any-of-only bodies** (prefer a group with no top-64 member). It
  removes the drift cause but not the compile-before-finalize cause, and it changes the class of
  existing rows on re-derivation. It may still be worth doing for cost reasons; the join
  condition is needed either way.
- **Refuse only the hiding direction.** Recall-safe, but a class-C row joining a visible leader
  is the silent visibility change ADR-105 refuses for C→H, and it leaves dedup on and off
  returning different default results.
- **Compare exact classes.** ADR-106 already rejected this: it splits groups on a θ-frequency race
  between two always-visible lanes for no benefit.

## Consequences

- A dedup join can no longer change which reads return a query. Dedup on and off return the same
  results in both `include_broad` modes, in the memtable, after flush and reopen, and after either
  merge.
- A body whose copies sit on both sides of the boundary costs two posting entries per segment
  instead of one.
- **Rows already hidden are not repaired.** A copy that adopted class C before this change and was
  flushed is an ordinary class-C row on disk. With `compaction_reanchor = true` it moves back to
  the main lane at the next merge when its body currently plans visible; otherwise it needs a
  recompile from retained source (a vocabulary reinstall, or a reindex). `include_broad=true`
  reads were never affected, and cluster shards were never affected: they compile against a
  frozen, finalized dictionary, so copies of one body always plan the same class.
- The statements in ADR-056, ADR-105 and ADR-106 that a class-C anchor cannot change under the
  frozen mask are corrected by outcome notes on those records.

## Proven

`tests/oracle/dedup_visibility.rs`: a drifted pair and a compile-before-finalize pair through the
memtable join (both directions), the grouped merge and the re-anchoring merge (both segment
orders), flush and a durable reopen; a drifting, duplicated any-of corpus that must match dedup-off
in both read modes after every lifecycle step; the same corpus through the re-anchoring merge,
which must hide no default-visible row; a 50-copy class-C group that re-plans visible and must
still scan as one posting entry; and the same group with the hot tier on, which re-plans class H
and must stay class C. Removing any one of the three join conditions fails at least one of these,
as does relaxing any of them to one direction, reducing the re-anchoring condition to a
stored-class comparison, or dropping either half of the guard.

**See also:** ADR-106 (dedup Stage A), ADR-056 (re-anchoring and the demote guard), ADR-105 (the
hot tier and the two-axis rule that cost movement must never imply visibility movement).
