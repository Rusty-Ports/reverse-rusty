# ADR-004: LSM write path over full rebuild

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted


- **Context:** The naive approach is to rebuild the entire index when queries change. At
  100M queries this is unacceptable (minutes of unavailability or double-buffering cost).
- **Decision:** Log-structured (LSM) write path with immutable segments + a mutable memtable
  (hot delta) + tombstones. Writes append to the memtable and become visible immediately via
  an atomic epoch swap. Segments are never mutated once sealed.
- **Consequence:** ~750k updates/sec/core with immediate visibility. Full rebuild is reserved
  for the initial seed and major feature-model changes (blue/green from the log, not
  stop-the-world). Read amplification grows with segment count — compaction caps it (ADR-009).
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md)

## Later outcome — 2026-10-06

"~750k updates/sec/core" is the rate of the bare memtable append in an early capture, without a
published snapshot. A server publishes after every write and holds the snapshot, which makes
each write copy the dictionary and the memtable; see the dated outcome on
[ADR-016](adr-016-snapshot-read-path-arcswap.md) for the measured server path.
