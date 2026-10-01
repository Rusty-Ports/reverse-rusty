# ADR-184 — Record the feature model in the manifest

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Every compiled segment is a function of the normalizer it was compiled under, yet that normalizer
was the one durable input with no recorded identity. Nothing on reopen checked that the normalizer
the caller supplied was the one the committed corpus was built with, so the caller's choice won
silently:

- **Single node.** The manifest stored no vocabulary. A runtime vocabulary or alias change
  recompiled the corpus, committed it, and deleted the old segments, but the vocabulary itself
  lived only in memory. A restart with a stale, edited or missing `--vocab-file` (including the
  documented backup restore, which passes none) served the committed queries under a different
  normalizer. Example: after importing `ny => new york` at runtime, the stored query
  `new york inventory` compiled to the alias entity; after a restart without the file, the title
  `new york inventory` no longer emitted that entity and missed the query outright.
- **In-process cluster.** The coordinator manifest persisted the vocabulary, but nothing recorded
  the normalizer. The server reopened a populated cluster built *without* a vocabulary using the
  `--vocab-file` normalizer while logging that the file was "NOT applied", so the file's synonyms,
  collapse phrases and punctuation rules silently diverged titles from the stock-compiled queries.
- **Library callers.** `Engine::open(norm)` and `ClusterEngine::open(dir, norm, ..)` with a bare
  normalizer carried the same unchecked precondition; a vocabulary blob alone cannot cover them.
- Nested vocabulary JSON entries silently dropped unknown fields, so an older binary would ignore a
  newer phrase flag such as `additive` — a recall loss — rather than refuse the document.

## Decision

The manifest commit point — which every commit writes and every open reads — records the feature
model, and recovery serves exactly that model or refuses.

1. **Feature-model fingerprint.** `Normalizer::fingerprint()` is an FNV-1a hash, computed once at
   build, over a canonical form of everything that decides which features a text emits: phrases
   `(pattern, feature, kind, mode)` sorted, effective synonyms sorted, the full punctuation table,
   and the deduplicated number-context words. Derived automata are excluded. Equivalence groups
   are excluded: they only widen query-side compiles, they derive from the recorded vocabulary,
   and committed rows carry their expansion baked in. Normalization *code* is not hashed; a code
   change is a compiler-semantics bump (ADR-118), which must also accompany any change to what the
   fingerprint hashes.
2. **Manifest v8 (single node).** Every commit appends the fingerprint and the `Vocab` JSON (empty
   for a bare-normalizer engine). v8 is always written, so it is also the rollback fence. v1–v7
   read back with no recorded model. **Cluster manifest v8** appends the fingerprint; the
   vocabulary blob already existed (v3).
3. **Recovery.** A recorded vocabulary is authoritative: the normalizer is rebuilt from it, its
   equivalences installed before the WAL or log tail replays, and the caller's normalizer and
   vocabulary are ignored. Without a recorded vocabulary the caller's normalizer is used. Either
   way, a fingerprint that differs from the recorded one fails the open with
   `FeatureModelMismatch` (an `InvalidData` I/O error for `Engine`, a `ShardError` variant for the
   cluster), unless a compiler-semantics migration is pending — that migration rebuilds every live
   row from retained source under the current normalizer before serving. A pre-v8 single-node
   manifest trusts the caller as before; its first commit records the model.
4. **Every vocabulary change is committed before acknowledgement.** A recompile already commits.
   An engine with no compiled rows has nothing to recompile, so `set_vocab` commits directly, and
   the metadata-only alias seam (candidates, feedback evidence) commits too, restoring the previous
   vocabulary if the commit is refused. These vocabulary-only commits rewrite the manifest with
   the same registry but keep the **previously committed WAL watermark**: they capture no
   memtable state, and advancing the watermark would let recovery skip a logged delete whose
   insert still replays. They are refused while persistence is degraded, because the in-memory
   registry may then be a strict subset of the committed one.
5. **One model per commit.** While a `set_vocab` awaits its recompile, the corpus spans two models,
   so a standalone commit in that window fails closed (the old manifest and WAL stay
   authoritative) without marking persistence unhealthy. A freshly mapped segment now carries its
   source segment's vocabulary epoch, so a flush or compaction after a vocabulary change is not
   mistaken for stale.
6. **Startup seed policy.** `Engine::open_seeded` and `ClusterEngine::open_seeded` implement the
   server's `--vocab-file` rule and return a `VocabSeedOutcome` the server logs. The file seeds a
   fresh store. A store that recorded a vocabulary keeps it and warns when the file differs. A store
   recorded without a vocabulary opens under the stock normalizer it was built with and activates
   the file only while it holds no queries; a populated store warns that the file was not applied.
   A legacy single-node manifest trusts the file and warns.
7. **Strict vocabulary JSON.** Every nested entry type rejects unknown fields, and an optional
   top-level `format_version` accepts only `1` (absent means `1`; this binary never writes it).

## Alternatives considered

- **Write the vocabulary back to `--vocab-file`.** Rejected: the file may be a read-only
  ConfigMap, and a second write is not atomic with the manifest commit.
- **A manifest-selected vocabulary sidecar** (like the source sidecar) avoids rewriting a large
  alias registry on every flush. Rejected for now: the manifest already rewrites the dict blob,
  which is at least as large, on every commit, alias imports are capped at 10k rules, and an
  embedded blob matches the cluster manifest and needs no extra garbage collection or backup rule.
- **Refuse to start when a populated store's file differs.** Rejected: the common flow — deploy
  with file X, change to Y through `PUT /_vocab`, restart with unchanged arguments — would stop
  restarting. Serving the recorded model is self-consistent, so it cannot cause a false negative.
- **Include equivalence groups in the fingerprint.** Rejected for the reasons in decision 1.

## Consequences

- A restart, backup restore, or library reopen can no longer serve a committed corpus under a
  different normalizer without failing loud, and runtime vocabulary changes survive restarts
  without operator file management. The alias discover-and-record and feedback-apply responses
  report `persisted: true` on durable engines.
- The first commit by this binary writes manifest v8, which an older binary refuses to open.
- A library caller that relied on `open_with_vocab` to *change* a store's vocabulary at reopen now
  gets the recorded vocabulary instead (or a mismatch error for a bare-normalizer store); changes
  go through `set_vocab`. `adopt_vocab` refuses a vocabulary whose normalizer differs from the
  compiled corpus's.
- Remote shard servers are unchanged: they run the stock normalizer and already refuse custom
  vocabularies (ADR-076).

**See also:** ADR-015 (runtime vocabulary), ADR-046 (cluster vocabulary persistence), ADR-076
(cluster vocabulary shipping), ADR-079 (backup/restore), ADR-102/103 (alias metadata seams),
ADR-118 (compiler-semantics migration), ADR-147 (`PUT /_vocab`).
