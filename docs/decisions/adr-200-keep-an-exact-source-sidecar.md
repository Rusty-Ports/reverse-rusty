# ADR-200 — A commit keeps a source sidecar that is already exact

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A standalone engine keeps its stored documents (`_source`) in one immutable sidecar file, which
every manifest commit selects together with the segment registry (ADR-121). The sidecar is
always a complete copy of the live corpus, and every commit wrote a new one without asking
whether anything in it had changed:

- A **merge** (compaction, or resealing tombstoned segments) changes no document. It wrote the
  whole corpus to a new file all the same.
- A **flush** or a **bulk load of new ids** writes the corpus once, as it must. When it then
  merges, which by default it does once there are more than eight segments, the merge commit
  wrote the corpus a second time.

Each write builds the file in memory, checksums it and syncs it while the engine's write lock
is held, so every one stalls writers for time proportional to the corpus. A node loading in
bulk through REST paid two such writes per request for most of the load.

No metric showed any of it. `flush_time_seconds_total` covers sealing the memtable and stops
before the commit.

## Decision

1. **The source store reports a content version.** The store's mutable contents sit behind one
   lock type (`storage::sources::Tracked`). Taking it for writing changes the version, and
   nothing else reaches the contents. Versions are unique across every store in the process, so
   a store that is opened, created or re-mapped never has a version another one had. Two reads
   of the same version therefore mean the same store with nothing written in between.
2. **The engine remembers the version at which its selected sidecar was exact.** It records it
   when a sidecar it wrote becomes the selected one, when a bulk load has applied its documents
   to the store after its commit, and when a store has just been read from the selected
   sidecar at open, before the log is replayed into it.
3. **A commit that finds the store at that version selects the same sidecar again.** Only the
   manifest is written. Every standalone commit without staged documents goes through this one
   check (`commit_sources_and_manifest`): flush, merge, reseal, vocabulary recompile.
4. **Any doubt writes the corpus.** No recorded version, a version that differs, or a recovery
   that could not load its sidecar or skipped a segment: the commit behaves as before.
5. **A complete write of the corpus is an event.** `EngineEvent::SourceCommit` carries the
   bytes written and the time taken; the server exports `source_commits_total`,
   `source_commit_bytes_total` and `source_commit_time_seconds_total`.

## Alternatives considered

- **A dirty flag on the engine, set wherever it changes a document.** The engine changes
  documents in many places (inserts, upserts, deletes, bulk loads, log replay, rebuilds). One
  site that forgets the flag would keep a stale sidecar, and a stale sidecar is a wrong
  `_source` after restart and a wrong corpus for the next vocabulary rebuild. Counting writes
  at the lock makes forgetting impossible.
- **A counter of writes on the store alone.** A lazy store is replaced by a newly opened one
  after each commit; two stores would both be at count zero. Process-unique versions remove
  that case.
- **Never restage on a merge, with no version.** True today, since a merge changes no document,
  but a store reopened with a log tail has replayed documents that the selected sidecar lacks,
  and a merge is one of the commits that persists them.
- **Compare content** (a checksum of the store against the file). That costs a pass over the
  corpus, which is the cost being removed.
- **Write only what changed** (a base sidecar plus small deltas, or one sidecar per segment).
  This is the change that makes a flush and a bulk load proportional to their size. It needs a
  manifest format change, crash-window coverage and backup support, and is on the roadmap.

## Consequences

- A merge, and the merge that follows a flush or a bulk load, no longer writes the source
  corpus. A flush still writes it once, and so does every bulk load of new ids, whatever its
  size: loading through many small requests is still quadratic in total.
- The sidecar generation in the file name no longer advances at every commit. Nothing reads
  meaning into it; the manifest selects a sidecar by name.
- A cluster shard is unchanged. It writes its sidecar at a flush or checkpoint, not at a merge,
  and still rewrites it at a checkpoint when nothing changed.
- Taking the store's write lock counts as a change, also when the caller then writes nothing
  (an insert refused for an older generation, a removal of an absent id). The cost is one
  rewrite that was not needed; the other direction is never wrong.

## Proven

- `tests/persistence/sources/unchanged.rs`, in both source modes: a merge writes no source
  corpus and the manifest still selects the same sidecar (it wrote one before); a flush that
  merges, and a bulk load that merges, write it once (twice before); a new document, a
  replacement and a removal after a kept sidecar are all in the sidecar the next commit
  writes; a store reopened with a clean log keeps its sidecar through a merge, and one with
  replayed writes does not; and over seeded random mixes of upserts, removals, bulk loads,
  flushes, merges and restarts, every restart serves exactly the documents written.
- `storage/sources/tests.rs`: the version changes at every insert, replacement and removal in
  both modes, does not change for reads or for writing the file out, and differs for a store
  read back from its own file.

**See also:** ADR-014 (the source store), ADR-017 (bulk commit), ADR-121 (joint sidecar and
manifest commit), ADR-020 (the lazy source mode).
