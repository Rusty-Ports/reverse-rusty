# ADR-016: Snapshot-based read path (ArcSwap) over global RwLock

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted


- **Context:** The HTTP server held the engine behind a single `RwLock<Engine>`. Every write
  operation (put_doc, bulk_ingest, flush, compact) took a write lock, blocking all concurrent
  reads. Flush and compaction involve disk I/O (seal + write + fsync) that can take hundreds of
  milliseconds, during which search, stats, health, and metrics were all blocked. ES uses a
  refresh model (readers see last-refreshed snapshots). RocksDB uses version sets (readers hold
  immutable snapshot refs with generation-number fast path). ClickHouse merges against immutable
  parts with atomic pointer swaps.
- **Decision:** Replace `RwLock<Engine>` with `Mutex<Engine>` (write serialization only) plus
  `ArcSwap<EngineSnapshot>` (lock-free reads). `EngineSnapshot` is a frozen, read-only view of
  the engine state built entirely from `Arc` handles: `Arc<Normalizer>`, `Arc<Dict>`,
  `Vec<Arc<BaseSegment>>`, `Arc<Segment>` (memtable), and `Arc<RwLock<QueryStore>>`. Publishing a
  snapshot is a handful of `Arc::clone`s — no deep copy of any engine structure (see the
  structural-sharing refinement below). Read endpoints (`/_search`,
  `/_stats`, `/_health`, `GET /_doc/{id}`, `/_metrics`) load the snapshot via `ArcSwap::load()`
  — zero contention, zero blocking. Write endpoints acquire the `Mutex`, mutate the engine,
  then call `publish_snapshot()` which atomically stores a new `Arc<EngineSnapshot>`.
  `EngineSnapshot` implements all read operations (`match_title`, `match_titles_par`, `metrics`,
  `explain_hit`, etc.) directly. Added `arc-swap = "1"` as a dependency (~200 lines, no
  transitive deps). Changed `Engine.norm` to `Arc<Normalizer>` and `MmapSegment.mmap` to
  `Arc<Mmap>` so snapshots share large immutable data without cloning.
- **Consequence:** Reads are fully non-blocking — a compaction that takes seconds no longer
  stalls search traffic. Write-to-read visibility is immediate (publish after every mutation).
  Snapshot creation is O(1) in the corpus size — a fixed number of `Arc::clone`s plus a
  `Vec<Arc>` clone whose length is the segment count (tens, not millions). `EngineSnapshot` is
  `Send + Sync` (verified by compile-time assertion). Benchmark: 894k titles/sec/core selective
  — no regression from snapshot indirection. The `Mutex` still serializes writes, which is
  correct (concurrent writes to the LSM engine would violate internal invariants).

- **Refinement (structural sharing):** The original implementation deep-cloned the entire engine
  on every publish (`Arc::new(self.dict.clone())`, deep-copied segments/memtable/query store),
  making writes O(total engine size) — a single PUT on a 1M-query engine cost ~82 ms and the cost
  grew linearly with the corpus. The engine now holds `dict: Arc<Dict>`,
  `segments: Vec<Arc<BaseSegment>>`, `memtable: Arc<Segment>`, and
  `query_store: Arc<RwLock<QueryStore>>`; mutations use copy-on-write (`Arc::make_mut` for the
  dict and memtable, which are bounded by vocab/memtable size) and shared interior mutability
  (the `RwLock` query store is mutated in place, so it is never copied on publish). Sealed base
  segments are immutable and shared by `Arc::clone`. Result: PUT + publish dropped from 82 ms to
  ~2 µs at 1M queries (~40,000×), and snapshot/PUT/DELETE publish cost is now flat across corpus
  size (verified by `src/bin/snapbench.rs`). `std::sync::RwLock` is used (not `parking_lot`) to
  keep the core std-only; the poison case is recovered with `.unwrap_or_else(|e| e.into_inner())`
  (release builds use `panic = "abort"`, so poisoning cannot occur there).
- **Dependency:** `arc-swap v1` — used by TiKV, crossbeam, and other high-concurrency Rust
  infrastructure. Lock-free atomic `Arc` swaps with epoch-based reclamation.
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md)

## Later outcome — 2026-10-06

The refinement's result, "PUT + publish dropped from 82 ms to ~2 µs at 1M queries ... flat across
corpus size", does not describe a running server. `snapbench` dropped each snapshot as soon as it
was made, so the next write found the dict and the memtable unshared and copied nothing. A
server holds its published snapshot until the next one replaces it, so every write finds both
shared and `Arc::make_mut` copies them: the whole feature dictionary and the whole memtable, per
write, under the engine mutex.

Measured with the snapshot held (`snapbench`, Apple M4 Max, 2026-10-06): a PUT and publish costs
0.3 ms on a 200k-query corpus with an empty memtable and 2.5 ms with 100,000 rows in it; at 1M
queries, 1.5 ms and 3.7 ms. That is a few hundred to a few thousand single writes per second,
and it grows with the vocabulary and the memtable. An upsert or delete that tombstones a row of
an in-memory base segment copies that whole segment (27 ms and 23 ms at 1M); on mmap segments it
copies the liveness overlay only. The full capture is in
[`benchmark-results.txt`](../performance/benchmark-results.txt).

The structural sharing itself stands: a snapshot is still a handful of pointer copies, readers
are still lock-free, and the 82 ms whole-engine copy is gone. What does not hold is the write
cost and its flatness. Making a write cost proportional to the change is on the
[roadmap](../roadmap.md) ("Writes proportional to the change").
