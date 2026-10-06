//! Engine flags that belong to whichever process holds a shard's disk (ADR-192).
//!
//! In-process, that is this server, and its flags configure every shard. Against remote
//! shard nodes this server holds no shard: the same flags reach nothing, and each node
//! takes its own from `shardserver`. Accepting them silently made a coordinator started
//! with `--wal-sync-on-write` look, and report itself, as durable against power loss
//! when no shard was.

use reverse_rusty::config::EngineConfig;

/// The `per_shard` keys a remote shard node decides for itself. `/_settings` lists them so
/// a reader knows which of the coordinator's values say nothing about the shards.
pub(crate) const SHARD_LOCAL_SETTINGS: [&str; 6] = [
    "wal_sync_on_write",
    "retain_source",
    "max_segments",
    "memtable_flush_threshold",
    "hot_anchor_threshold",
    "tag_segment_skipping",
];

/// What a remote coordinator makes of its shard-local engine flags.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardLocalFlags {
    /// A durability promise this process cannot keep. Startup fails.
    pub(crate) refused: Vec<String>,
    /// A cost or memory setting that has no effect here. Startup continues with a warning.
    pub(crate) inert: Vec<String>,
}

/// Classify the engine flags a remote coordinator was started with. `per_shard` is the
/// configuration assembled from its command line; a value that differs from the default was
/// set by the operator.
pub(crate) fn in_remote_mode(per_shard: &EngineConfig) -> ShardLocalFlags {
    let defaults = EngineConfig::default();
    let mut flags = ShardLocalFlags::default();
    if per_shard.wal_sync_on_write {
        flags.refused.push(
            "--wal-sync-on-write cannot be honoured by a remote coordinator: it stores no \
             shard data, so the flag would sync nothing. Start every shard node with \
             `shardserver --wal-sync-on-write true` instead"
                .to_string(),
        );
    }
    let mut inert = |set: bool, flag: &str| {
        if set {
            flags.inert.push(format!(
                "{flag} has no effect on remote shard nodes; set `shardserver {flag}` on \
                 each of them"
            ));
        }
    };
    inert(
        per_shard.max_segments != defaults.max_segments,
        "--max-segments",
    );
    inert(
        per_shard.memtable_flush_threshold != defaults.memtable_flush_threshold,
        "--memtable-flush-threshold",
    );
    inert(
        per_shard.retain_source != defaults.retain_source,
        "--retain-source",
    );
    flags
}

#[cfg(test)]
mod tests {
    use super::{in_remote_mode, ShardLocalFlags, SHARD_LOCAL_SETTINGS};
    use reverse_rusty::config::EngineConfig;

    #[test]
    fn default_flags_need_nothing() {
        assert_eq!(
            in_remote_mode(&EngineConfig::default()),
            ShardLocalFlags::default()
        );
    }

    #[test]
    fn fsync_per_write_is_refused_and_names_the_shard_flag() {
        let flags = in_remote_mode(&EngineConfig {
            wal_sync_on_write: true,
            ..EngineConfig::default()
        });
        assert_eq!(flags.refused.len(), 1, "{flags:?}");
        assert!(
            flags.refused[0].contains("shardserver --wal-sync-on-write true"),
            "{flags:?}"
        );
        assert!(flags.inert.is_empty(), "{flags:?}");
    }

    #[test]
    fn storage_flags_warn_and_name_the_shard_flag() {
        let flags = in_remote_mode(&EngineConfig {
            max_segments: 4,
            memtable_flush_threshold: 500,
            retain_source: false,
            ..EngineConfig::default()
        });
        assert!(flags.refused.is_empty(), "{flags:?}");
        assert_eq!(flags.inert.len(), 3, "{flags:?}");
        for flag in [
            "--max-segments",
            "--memtable-flush-threshold",
            "--retain-source",
        ] {
            assert!(
                flags
                    .inert
                    .iter()
                    .any(|warning| warning.contains(&format!("shardserver {flag}"))),
                "{flag}: {flags:?}"
            );
        }
    }

    /// Every listed key is a real `EngineConfig` field, so `/_settings` never names a key
    /// that `per_shard` does not have.
    #[test]
    fn every_shard_local_setting_is_a_per_shard_key() {
        let per_shard = serde_json::to_value(EngineConfig::default()).expect("serializable");
        for key in SHARD_LOCAL_SETTINGS {
            assert!(per_shard.get(key).is_some(), "{key} is not in per_shard");
        }
    }
}
