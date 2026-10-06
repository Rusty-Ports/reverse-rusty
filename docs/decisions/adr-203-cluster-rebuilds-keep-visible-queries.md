# ADR-203 — A cluster rebuild never hides a query

> [Matching & verification decisions](areas/matching-and-verification.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Class C is opt-in: a read with the broad lane off does not return it. ADR-187 made a single-node
rebuild keep every query that default reads return, and recorded the cluster as not covered.

A cluster rebuilds its shards in three places: a vocabulary change (`set_vocab`, which is also how
an alias is activated), an in-process resize, and the compiler-semantics migration a durable
cluster runs on open. Each re-plans every stored query, and each placed it by the new plan alone.
A query that default reads returned could come out as class C and be replicated into the broad
lane.

- **An alias leaves only common anchors.** `widget pkg` becomes `widget (pkg, package)` when
  `pkg ≡ package` is activated. When `widget` and a member of the group both hold a top-64 bit,
  the query has no anchor outside the mask.
- **A vocabulary change ranks the mask again.** `set_vocab` mints a new dictionary over the live
  corpus and assigns its top-64 mask from the frequencies of that moment. A term that was outside
  the mask when a query was written can be inside it afterwards, for queries the change had
  nothing to do with.

Reproduced with the broad lane off. Three stored queries on a three-shard cluster: `widget pkg`
was returned for the title `widget pkg`, and after `pkg ≡ package` it was not. On the generated
test corpus, one rebuild for a set of equivalences removed 26 unrelated queries from the default
read of a single ordinary title.

ADR-187 expected the fix to need the mask preserved across the rebuild. It does not, and
preserving the mask would not have covered the alias case, which changes the plan under an
unchanged mask.

## Decision

1. **The rebuild carries each row's visibility.** Every live row is read back with its stored
   placement, and the placement mode is the record of its lane: a selective or
   replicated-always-visible row is in a lane every probe reads, and a row replicated into the
   broad lane is opt-in. `was_default_visible` reads that once, in `rebuild_from_corpus`, the one
   function behind all three rebuilds.
2. **A visible row whose new plan is class C is replicated always-visible.**
   `rebuild_placement_of` returns `ReplicatedAlwaysVisible` for it where `placement_of` returns
   `ReplicatedBroad`. Every other row is placed by `placement_of`, as before.
3. **A shard stores a class-C plan that arrives always-visible in its main lane.** The segment's
   compile step, the one place a stored row's plan is built, calls `SigPlan::pin_visible` when the
   row's placement mode is replicated-always-visible, as it does for ADR-187's `keep_visible`. The
   plan's top-64 signatures go to the main lane and the row is class B.

4. **Replicated rows are counted by placement.** A kept row is on every position, so adding
   shards does not shrink it. `collect_load` discounted only classes C and D from split pressure,
   and a kept row is class B. `Shard::replicated_rows` reports the rows stored at every position:
   an in-process shard counts them by placement mode, and `collect_load` subtracts that count.

It is lossless because the row is on every position, a title always probes at least one position,
a shard probes its main lane arity-1 with every title feature, and a title that satisfies the
query carries the feature each of those signatures was built from. The row is owned by the first
position the title routes to, like every replicated always-visible row (ADR-109), so it is
returned once.

The mode and the lane are both stored with the row in the sealed segment. A restart reads them
back. A replica rebuild and peer recovery ingest the row again with its placement, through the
same compile step. Compaction already refuses to move a main-lane row into the broad lane
(ADR-056). The next rebuild reads the mode back and keeps the row again.

## Alternatives considered

- **Preserve the top-64 mask across the rebuild.** It stops the re-ranking case and leaves the
  alias case. It also keeps a mask ranked for a corpus the cluster no longer holds.
- **Keep the row selective on the positions it had.** A row whose only anchors hold top-64 bits
  has nothing to route by: titles route on the features outside the mask. That is why class C is
  replicated in the first place.
- **Refuse a vocabulary change that would hide a stored query.** Rejected in ADR-187 for the same
  reason: it turns one affected query into a failed install, and it cannot apply to a migration
  on open.

## Consequences

- Activating an alias, changing the vocabulary, resizing in process, and opening across a
  compiler-semantics version never remove a stored query from a cluster's default reads.
- A kept row is stored on every position and rides a top-64 feature's main posting there, so a
  probed shard considers it for every title that carries the feature. Only rows that were visible
  and would otherwise have been hidden take this path. `class_counts` reports them as class B.
- Kept rows do not raise the resize recommendation. Counting by placement also discounts the
  replicated class-B rows that ADR-080 left as a residual (top-64 pairs and phrase proxies) on an
  in-process cluster. A remote shard reports classes C and D, as before.
- A row that was opt-in takes the lane its plan gives it, and no selective row is replicated that
  was not before. A resize, which reuses the dictionary, changes no read.
- Upserting a kept row compiles it fresh, so it takes the lane of its current plan, as on the
  single-node engine (ADR-187).
- **Remote clusters hold no kept row.** All three rebuilds run only where every shard is in
  process: a remote cluster refuses `set_vocab` and the in-process resize, and a remote shard
  that holds an older compiler version is refused. The remote resize (ADR-180) places the
  exported rows under the same dictionary, so each row gets the mode it had. Its export carries
  no placement, so a cross-process vocabulary change will have to add the row's visibility to it;
  the roadmap item for that change says so.

## Proven

- `tests/cluster_oracle/rebuild_visibility.rs`: the three-query case through an alias rebuild at
  one, three and eight shards and replicated; a generated corpus through an equivalence rebuild
  and then a resize, with every default read compared before and after; a resize that changes no
  default or broad read and replicates no selective row; broad reads exact against an independent
  oracle with kept rows present, and a kept row deleted on every position; a kept row through the
  compiler migration on reopen; sixty kept rows that are all counted as replicated, with one and
  two copies per position, and recommend no resize.
- `tests/cluster_durability_oracle/kept_visible.rs`: a kept row through a reopen, a resize after
  it, and a second reopen, with the same replicated share reported from the sealed segments.
- Ten mutations of the rule (never keep, keep every class-C row, forget either visible mode, no
  pin on the shard, replicate every visible row, and each rebuild path forgetting what it read)
  each fail at least one of those tests. So do ten of the count (by class at the coordinator or
  in any shard wrapper, either replicated mode left out, and each store left out).

**See also:** ADR-187 (the single-node rule this extends), ADR-056 (the compaction guard),
ADR-109 (ownership of replicated rows), ADR-054 and ADR-060 (alias expansion), ADR-180 (the remote
resize).
