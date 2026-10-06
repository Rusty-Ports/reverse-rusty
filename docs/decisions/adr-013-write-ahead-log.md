

## Later outcome — 2026-10-06

[ADR-198](adr-198-wal-is-replaced-not-truncated.md): the log is reset and created by renaming a
complete empty log into place. The earlier truncate-then-write-header left a window, once per
flush, in which a crash produced a log shorter than its header and a node that refused to start.
Such a file now opens as an empty log.
