//! `impl ClusterEngine` — the construction seam + crash recovery: `from_parts`
//! (the shared assembly point that materializes a `ClusterEngine` from pre-built
//! parts, used by both `build` and the distributed/gRPC builders) and `open`
//! (reattach a durable cluster's committed segments + replay the log tail).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cluster::clog::{ClusterMutation, FileClusterLog, IfMissing, LogPos};
use crate::cluster::control::{ClusterStateChange, InMemoryControlPlane};
use crate::cluster::coordinator::layout::Layout;
use crate::cluster::coordinator::{
    into_shard, replica_dir, shard_dir, ClusterConfig, ClusterDurable, ClusterEngine,
    CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE,
};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::tagdict::{TagDict, TagId};

/// Resolve an already-acknowledged coordinator-log row against the persisted,
/// frozen tag space. Recovery must carry the resolved ids into the rebuild so a
/// runtime `max_tags` tightening cannot reclassify the row as fresh ingestion.
fn resolve_replayed_tag_ids(
    tag_dict: &TagDict,
    tags: &[(String, String)],
) -> Result<Vec<TagId>, ShardError> {
    let mut ids: Vec<TagId> = tags
        .iter()
        .map(|(key, value)| tag_dict.get_or_synthetic(key, value))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() > usize::from(u16::MAX) {
        return Err(ShardError::Log(format!(
            "replayed tag column exceeds the durable u16 ceiling ({} resolved tags)",
            ids.len()
        )));
    }
    Ok(ids)
}

impl ClusterEngine {
    /// Current logical placement generation (ADR-109). Physical checkpoints,
    /// compaction, replication, handoff, and node reassignment never change it.
    pub fn placement_generation(&self) -> crate::ownership::PlacementGeneration {
        let layout = &*self.layout();
        layout.generation
    }

    /// Assemble a cluster from pre-built parts — the construction seam shared by
    /// [`Self::build`] (which supplies `LocalShard`s) and the distributed builder /
    /// gRPC integration test (which supply boxed `RemoteShard`s). `shards.len()` must
    /// equal `ring.num_shards()`.
    // The internal assembly seam genuinely takes many independent parts (feature
    // space, ring, shards, the broad toggle, the rebuild config, and the durability
    // bundle); grouping them further would only obscure the construction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        ring: HashRing,
        shards: Vec<Box<dyn Shard>>,
        include_broad: bool,
        replication_factor: usize,
        per_shard: crate::config::EngineConfig,
        durable: ClusterDurable,
    ) -> Result<Self, ShardError> {
        if shards.len() != ring.num_shards() {
            return Err(ShardError::Config(format!(
                "shard count {} must match the ring's shard count {}",
                shards.len(),
                ring.num_shards()
            )));
        }
        let generation = durable.placement_generation;
        let num_shards = ring.num_shards() as u32;
        for (position, shard) in shards.iter().enumerate() {
            shard.validate_ownership(position as u32, generation, num_shards)?;
        }
        // Multi-word aliases are cluster-supported since ADR-076: `route` is P(T)-aware
        // (targets derived from the maximal positive view when multi-word aliases are
        // active), so a nested alias entity that lives only in `P(T)` still probes the
        // shard holding a query anchored on it — the ADR-061 single-node-only refusal
        // that guarded this assembly seam is retired (the shard-local two-view verifier
        // was already correct once the probe arrives). Cross-process callers
        // (`connect_remote`/`connect_replicated`) still arrange ONE normalizer
        // out-of-band — the same consistency every vocabulary feature already relies
        // on; ADR-076 records that trust model and keeps LIVE vocab changes on a
        // remote cluster refused (`set_vocab` non-local guard).
        let engine = ClusterEngine {
            layout: arc_swap::ArcSwap::from_pointee(Layout {
                norm,
                dict,
                vocab: None,
                ring,
                shards: Arc::new(shards),
                source_files: durable.source_files,
                #[cfg(feature = "distributed")]
                handoffs: Vec::new(),
                generation: durable.placement_generation,
            }),
            tag_dict,
            // Untagged by default; the tagged write paths + `open` latch it (ADR-055).
            tags_present: AtomicBool::new(false),
            logical_ids: std::sync::RwLock::new(
                super::super::logical_ids::LogicalIdDirectory::default(),
            ),
            logical_write_locks: super::super::write_locks::LogicalWriteLocks::default(),
            include_broad,
            replication_factor: replication_factor.max(1),
            per_shard,
            log: durable.log,
            epoch: AtomicU64::new(durable.epoch),
            vnodes: durable.vnodes,
            data_dir: durable.data_dir,
            pending_alias_import_predecessor: Mutex::new(None),
            layout_lock: std::sync::RwLock::new(()),
            layout_admission: Mutex::new(()),
            retired_layouts: Mutex::new(Vec::new()),
            #[cfg(test)]
            admission_hook: Mutex::new(None),
            rebuild_hook: Mutex::new(None),
            pending_alias_import_manifest: Mutex::new(None),
            committed_placement_generation: AtomicU64::new(
                durable
                    .manifest
                    .as_ref()
                    .map_or(durable.placement_generation.0, |m| m.placement_generation.0),
            ),
            committed_manifest: Mutex::new(durable.manifest),
            resize_write_fence: AtomicBool::new(false),
            control: durable.control,
            // A fresh transport-metrics collector (ADR-085); the gRPC builders REPLACE it with
            // the shared one they also hand to each `RemoteShard` (via `with_transport_metrics`),
            // so remote per-RPC stats aggregate here. The in-process path keeps this empty one.
            transport_metrics: arc_swap::ArcSwap::from_pointee(
                crate::cluster::transport_metrics::TransportMetrics::new(),
            ),
            observer: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
            pending_repair: Mutex::new(std::collections::BTreeMap::new()),
            // ADR-113: coordinator PIT registry (in-memory by design — a reopened
            // cluster serves no prior generation, so old cursors fail closed).
            pits: Mutex::new(crate::pit::PitRegistry::new()),
            pit_open_barrier: std::sync::RwLock::new(()),
            move_fence: crate::cluster::coordinator::move_fence::MoveFence::default(),
            // No position is handoff-wrapped by default; the gRPC builders install handles via
            // `with_handoffs`. Empty here ⇒ the in-process/default path is byte-identical (ADR-043).
            // Handoff drain caps default here (the in-process path never hands off); the gRPC
            // builders override them from `ClusterConfig` via `with_handoff_caps` (ADR-044/048).
            #[cfg(feature = "distributed")]
            handoff_drain_passes: ClusterConfig::DEFAULT_HANDOFF_DRAIN_PASSES,
            #[cfg(feature = "distributed")]
            handoff_final_drain_cap: ClusterConfig::DEFAULT_HANDOFF_FINAL_DRAIN_CAP,
            // No runtime handle on the in-process path (it never hands off); the gRPC builders set
            // it via `with_handle` so the autoscaler can drive `execute_handoff` (ADR-048).
            #[cfg(feature = "distributed")]
            handle: None,
            // No mesh security on the in-process path; the secure gRPC builders set it via
            // `with_client_security` (ADR-071).
            #[cfg(feature = "distributed")]
            client_security: crate::cluster::security::ClientSecurity::default(),
            // An exclusive remote builder replaces this before returning and
            // stamps the same id on every RemoteShard it assembled.
            #[cfg(feature = "distributed")]
            coordinator_id: None,
            // Empty; reserved only by the data-moving reassign path (ADR-090/095). Default-path
            // moves never happen (no `execute_handoff` in-process), so it is never contended.
            #[cfg(feature = "distributed")]
            move_ledger: crate::cluster::coordinator::reassign::MoveLedger::new(),
        };
        // A fresh assembly holding ZERO stored queries has an authoritative EMPTY
        // id directory — nothing to enumerate. A populated assembly stays
        // unauthoritative until the remote builders or durable open explicitly
        // install a complete enumeration. Until then `add_query` fails closed.
        // A count FAILURE also stays unauthoritative rather than failing the
        // assembly: construction semantics predate this check, and unauthoritative
        // is already the fail-closed disposition.
        if matches!(engine.num_queries(), Ok(0)) {
            engine.replace_logical_ids(Vec::new())?;
        }
        Ok(engine)
    }
    /// True if `data_dir` holds a committed cluster manifest — i.e. [`Self::open`]
    /// will reopen an existing durable cluster there; otherwise [`Self::build`] is the
    /// constructor. The boot-time predicate the coordinator-mode server branches on
    /// (ADR-070), exposed so callers need not string-match `open`'s error.
    pub fn cluster_exists(data_dir: &Path) -> bool {
        data_dir.join(CLUSTER_MANIFEST_FILE).exists()
    }

    /// Reopen a durable cluster from `data_dir` (built earlier with a `data_dir` set).
    /// Each shard **attaches-and-mmaps** its committed compiled segments (the
    /// `cluster_manifest.bin` registry) — NOT re-ingest — then the log tail strictly
    /// after the manifest's `snapshot_pos` is replayed through the same apply funnel as
    /// live writes (ADR-032). The frozen dict is restored from the manifest
    /// (fingerprint-checked — a mismatch is a loud [`ShardError::DictMismatch`], ADR-030
    /// parity) and the ring re-derived deterministically, so placement is byte-identical
    /// to the original → zero false negatives across the restart. `config` supplies the
    /// per-shard engine config + fsync policy (defaults if `None`).
    ///
    /// The manifest is authoritative for the feature model (ADR-184): a persisted vocabulary
    /// rebuilds the normalizer and `norm` is ignored; otherwise `norm` must match the recorded
    /// fingerprint, or `open` returns [`ShardError::FeatureModelMismatch`]. See
    /// [`open_seeded`](Self::open_seeded) for the server's startup-vocabulary policy.
    pub fn open(
        data_dir: impl Into<PathBuf>,
        norm: Normalizer,
        config: Option<&ClusterConfig>,
    ) -> Result<Self, ShardError> {
        let data_dir = data_dir.into();
        let manifest_path = data_dir.join(CLUSTER_MANIFEST_FILE);
        if !manifest_path.exists() {
            return Err(ShardError::Config(format!(
                "no cluster manifest at {}; use build() to create a durable cluster",
                manifest_path.display()
            )));
        }
        let manifest = crate::storage::read_cluster_manifest(&manifest_path)
            .map_err(|e| ShardError::Config(format!("reading cluster manifest: {e}")))?;
        // ADR-080 forward fence: a pre-ADR-080 durable cluster placed the broad lane (class C +
        // B-arity-2) on shard 0 ONLY. This binary evaluates broad on a rotating per-title
        // broad-eval shard, which would silently miss those queries whenever the chosen shard is
        // not 0 — a false negative. Refuse loudly rather than mis-route; the operator rebuilds the
        // cluster with this binary (which writes the v5 replicate-to-all layout).
        if !manifest.broad_replicate_all {
            return Err(ShardError::Config(format!(
                "cluster at {} predates ADR-080's replicate-to-all broad layout (its broad lane \
                 lives on shard 0 only); reopening it here would mis-route broad queries — rebuild \
                 the cluster with this binary",
                data_dir.display()
            )));
        }
        let dict = crate::storage::deserialize_dict(&manifest.dict_data)
            .map_err(|e| ShardError::Config(format!("deserializing cluster dict: {e}")))?;
        let mut dict = Arc::new(dict);
        // Fail loud if the restored dict's fingerprint disagrees with the manifest's —
        // the one false-negative path the fallible seam can't otherwise catch.
        let actual_fp = dict.fingerprint();
        if actual_fp != manifest.dict_fingerprint {
            return Err(ShardError::DictMismatch {
                expected: manifest.dict_fingerprint,
                actual: actual_fp,
            });
        }
        let num_shards = manifest.num_shards as usize;
        // Defensive: the registry + next-seg-id columns must agree with num_shards. A
        // malformed manifest must fail loud, never silently attach the wrong segments.
        if manifest.segment_registry.len() != num_shards
            || manifest.next_seg_ids.len() != num_shards
        {
            return Err(ShardError::Config(format!(
                "cluster manifest is inconsistent: num_shards={num_shards} but registry has \
                 {} shard list(s) and {} next-seg-id(s)",
                manifest.segment_registry.len(),
                manifest.next_seg_ids.len()
            )));
        }
        // ADR-046: if a runtime vocabulary change was committed, the manifest carries
        // the serialized vocab — rebuild the normalizer from IT (authoritative over the
        // caller-supplied one) so a declared alias survives the restart, and retain the
        // vocab so a later checkpoint re-persists it (else the next reopen would lose it).
        let mut restored_vocab = if manifest.vocab_data.is_empty() {
            None
        } else {
            let json = std::str::from_utf8(&manifest.vocab_data)
                .map_err(|e| ShardError::Config(format!("cluster vocab not utf-8: {e}")))?;
            let v = crate::vocab::Vocab::from_json(json)
                .map_err(|e| ShardError::Config(format!("deserializing cluster vocab: {e}")))?;
            Some(v)
        };
        let norm = match &restored_vocab {
            Some(v) => v.to_normalizer().map_err(|e| {
                ShardError::Config(format!("building normalizer from cluster vocab: {e}"))
            })?,
            None => norm,
        };
        // Self-heal stale-active aliases against the restored normalizer (codex R13, the same
        // demotion every other equivalence-install seam runs): a persisted vocab can carry an
        // Active entry the current classification can no longer express. Demotion can only
        // shrink the registered phrase set, so rebuild the normalizer when it fires (the
        // demoted state re-persists at the next checkpoint).
        let mut norm = norm;
        if let Some(v) = &mut restored_vocab {
            if v.aliases_mut().demote_unexpressible(&norm, &dict) > 0 {
                norm = v.to_normalizer().map_err(|e| {
                    ShardError::Config(format!("building normalizer from cluster vocab: {e}"))
                })?;
            }
        }
        let restored_vocab = restored_vocab.map(Arc::new);
        let norm = Arc::new(norm);
        // Re-install equivalence groups (ADR-054) on the recovered dict so a log-tail replay
        // and post-reopen incremental adds expand through them. The already-attached segments
        // carry their expansion baked in, so matching recovered queries needs no re-resolution;
        // this only re-equips the live compile path. No-op when the restored vocab declared none.
        if let Some(v) = &restored_vocab {
            let equiv = v.resolve_equivalences(&norm, &dict);
            if !equiv.is_empty() {
                Arc::make_mut(&mut dict).set_equivalences(equiv);
            }
        }
        // Restore the frozen tag space (ADR-049/055) like the dict, so a reopened cluster resolves a
        // request filter to the SAME `TagId`s its attached segments carry. An empty blob (a pre-v4
        // manifest / untagged cluster) deserializes to an empty tag dict — the back-compat path.
        let tag_dict = Arc::new(
            crate::storage::deserialize_tagdict(&manifest.tag_dict_data)
                .map_err(|e| ShardError::Config(format!("deserializing cluster tag dict: {e}")))?,
        );
        let ring = HashRing::new(num_shards, manifest.vnodes)?;

        let per_shard = config.map(|c| c.per_shard.clone()).unwrap_or_default();
        let fsync = config.is_some_and(|c| c.wal_sync_on_write);

        // The log is opened, or refused, BEFORE any shard is attached. Attaching a shard resets
        // its translog and reseeds its replicas. When the cluster log is gone, those shard
        // translogs are the only place the writes since the last checkpoint still exist, so an
        // open that is going to refuse must not have touched them (ADR-213).
        let log_path = data_dir.join(CLUSTER_LOG_FILE);
        // Does this manifest say the log exists? (ADR-212, ADR-213.)
        //
        // A manifest at epoch 1 or later was written with the log in place: by `build`, which
        // creates the log first, or by a checkpoint, which needs the log open and replaces it
        // only through a rename. Under it a log that is missing, or shorter than its header,
        // has been lost with the writes acknowledged since that manifest, and the cluster is
        // refused.
        //
        // Epoch 0 is the manifest that releases before ADR-213 wrote at build, before they
        // created the log (and, before ADR-212, before the log had its header). Under it a
        // missing or short log may be a build that was interrupted, so it is created or
        // finished. A cluster from such a release that has taken writes and never
        // checkpointed looks the same; its first checkpoint (a graceful stop makes one) ends
        // that.
        let written_with_its_log = manifest.written_with_its_log();
        let accept_lost_log = config.is_some_and(|c| c.accept_lost_log);
        let log_was_lost = written_with_its_log && !log_path.exists();
        if log_was_lost && !accept_lost_log {
            return Err(ShardError::Log(
                crate::storage::framed_log::lost_log(
                    &log_path,
                    &format!(
                        "the cluster manifest (epoch {}, log position {}) was written after it \
                         existed",
                        manifest.epoch, manifest.snapshot_pos
                    ),
                    "Restore the data directory from a backup, or start once with \
                     `accept_lost_log` (`--accept-lost-log`) to continue from the last \
                     checkpoint without those writes.",
                )
                .to_string(),
            ));
        }
        if !written_with_its_log {
            FileClusterLog::finish_interrupted_creation(&log_path)
                .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;
        }
        let if_missing = if written_with_its_log && !log_was_lost {
            IfMissing::Refuse
        } else {
            IfMissing::Create
        };
        let log = FileClusterLog::open(&log_path, fsync, LogPos(manifest.snapshot_pos), if_missing)
            .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;

        // Attach each shard's committed compiled segments (mmap) against the shared dict —
        // NOT re-ingest. Fails loud on a missing / CRC-corrupt segment (a skipped segment
        // is a silent shard-sized false negative).
        let rf = config.map_or(1, |c| c.replication_factor.max(1));
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        // The manifest marker covers the coordinator-log tail as well as the
        // segment base. A v6 manifest reads as semantics zero, so even an empty
        // base must be rebuilt before its tail is interpreted by current code.
        let mut needs_compiler_semantics_migration = manifest.compiler_semantics_version
            < crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION;
        for s in 0..num_shards {
            let primary_dir = shard_dir(&data_dir, s);
            let mut sc = per_shard.clone();
            sc.data_dir = Some(primary_dir.clone());
            // Coordinator recovery is the one attach path allowed to load a
            // older compiler-semantics segment: after every shard and the log
            // tail are present, `open` atomically blue/green rebuilds the whole
            // cluster before returning it to a caller.
            let primary = LocalShard::open_segments_for_compiler_migration_with_source_file(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                sc,
                &manifest.segment_registry[s],
                manifest.next_seg_ids[s],
                &manifest.source_files[s],
            )?;
            let shard_needs_compiler_migration =
                needs_compiler_semantics_migration || primary.needs_compiler_semantics_migration();
            needs_compiler_semantics_migration |= shard_needs_compiler_migration;
            // Re-seed replicas (rf-1) by peer recovery from the just-attached primary — replicas
            // are not in the manifest, so they are rebuilt from the durable primary on every open.
            // The log-tail replay below then feeds primary AND replicas through the composite.
            let mut copies: Vec<LocalShard> = Vec::with_capacity(rf);
            let mut recovered: Vec<LocalShard> = Vec::with_capacity(rf.saturating_sub(1));
            for r in 1..rf {
                // The high-water is irrelevant here: at open there are no concurrent writes,
                // so the primary's translog tail is empty and this peer_recover is a pure
                // segment copy; the coordinator-log replay below repopulates all copies.
                let recover = if shard_needs_compiler_migration {
                    crate::cluster::replica::peer_recover_for_compiler_migration
                } else {
                    crate::cluster::replica::peer_recover
                };
                let (replica, _hwm) = recover(
                    &norm,
                    &dict,
                    &tag_dict,
                    per_shard.clone(),
                    &primary,
                    &primary_dir,
                    &replica_dir(&data_dir, s, r),
                )?;
                recovered.push(replica);
            }
            copies.push(primary);
            copies.extend(recovered);
            shards.push(into_shard(copies)?);
        }
        // ADR-184: the committed base and the log tail replayed below were compiled under the
        // recorded feature model. A restored vocabulary rebuilt `norm` above; a cluster built
        // from a bare normalizer must be reopened with that normalizer, or it fails loud here.
        // A pending compiler-semantics migration rebuilds every row from source under `norm`
        // before serving, so it is exempt.
        if let Some(recorded) = manifest.feature_model_fingerprint {
            if recorded != norm.fingerprint() && !needs_compiler_semantics_migration {
                return Err(ShardError::FeatureModelMismatch(
                    crate::error::FeatureModelMismatch {
                        recorded,
                        supplied: norm.fingerprint(),
                    },
                ));
            }
        }

        let durable = ClusterDurable {
            log: Box::new(log),
            data_dir: Some(data_dir.clone()),
            epoch: manifest.epoch,
            placement_generation: manifest.placement_generation,
            source_files: manifest.source_files.clone(),
            manifest: Some(manifest.clone()),
            vnodes: manifest.vnodes,
            control: Box::new(InMemoryControlPlane::single_node_with_generation(
                manifest.num_shards,
                manifest.vnodes,
                manifest.dict_fingerprint,
                manifest.placement_generation,
            )),
        };
        let mut engine = Self::from_parts(
            norm,
            dict,
            tag_dict,
            ring,
            shards,
            manifest.include_broad,
            rf,
            per_shard,
            durable,
        )?;
        // Retain the vocab restored from the manifest so a later checkpoint re-persists it.
        engine.edit_layout(|layout| layout.vocab = restored_vocab);
        // Latch tags_present (ADR-055) from the restored tag space; the log-tail replay below
        // (`apply_add` → `note_tags`) additionally latches it for any un-checkpointed tagged add.
        if !engine.tag_dict.is_empty() {
            engine.tags_present.store(true, Ordering::Relaxed);
        }

        // Rebuild the compact logical-id directory from the committed base before
        // replaying the tail. Placement/replication legitimately expose the same row
        // on several shard positions, so collapse those physical copies here. Clusters
        // written by this version admitted at most one semantic row per id.
        //
        // A shard whose enumeration is INCOMPLETE (a source-less / partial store —
        // a supported degraded reopen shape) must not fail the open OR seed a
        // directory that under-holds live ids (insert-only admission would re-admit
        // a live id — codex review): leave the directory UNAUTHORITATIVE instead,
        // so serving works while `add_query` fails closed toward `upsert_query`,
        // and surface the degradation as a durability event.
        let mut committed_ids = Some(Vec::new());
        for shard in engine.layout().shards.iter() {
            match (shard.live_logical_ids(), &mut committed_ids) {
                (Ok(ids), Some(collected)) => collected.extend(ids),
                (Ok(_), None) => {}
                (Err(e), collected) => {
                    *collected = None;
                    engine.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::SourceStoreWrite,
                        detail: "logical-id directory not seeded (partial/source-less store); \
                                 insert-only add_query is disabled until a checkpointed reopen — \
                                 use upsert_query"
                            .to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }
        if let Some(mut committed_ids) = committed_ids {
            committed_ids.sort_unstable();
            committed_ids.dedup();
            engine.replace_logical_ids(committed_ids)?;
        }

        // The attached segments ARE the base (all entries ≤ snapshot_pos). Replay only the
        // log tail strictly after snapshot_pos, through the SAME apply funnel as live
        // writes — those entries are not in the attached segments, so no double-apply.
        if log_was_lost {
            engine.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::LogLost,
                detail: "the cluster log was missing and `accept_lost_log` is set: started \
                         with an empty one"
                    .to_string(),
                error: format!(
                    "every write acknowledged after log position {} (manifest epoch {}) is lost",
                    manifest.snapshot_pos, manifest.epoch
                ),
            });
        }
        let replay = engine.log.replay(LogPos(manifest.snapshot_pos))?;
        if replay.skipped_bytes > 0 {
            engine.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalTornTail,
                detail: format!(
                    "cluster log torn tail: {} trailing byte(s) skipped during recovery",
                    replay.skipped_bytes
                ),
                error: format!("{} bytes", replay.skipped_bytes),
            });
        }
        // ADR-118/119/120/#123/162: older segments may have lost clause boundaries,
        // multi-token any-of member boundaries, quoted adjacency, or complete
        // multi-feature forbidden terms, or pre-dedup ranking counts. Rebuild
        // from the complete, replayed
        // live corpus, append any newly exposed features without
        // re-ranking the frozen mask, re-place at one fresh generation, update
        // the control document, and commit the green registry before exposing
        // the cluster. Any incomplete source sidecar or failed checkpoint
        // returns an error; the old manifest stays authoritative and a later
        // restart can retry.
        if needs_compiler_semantics_migration {
            // Do not feed a legacy tail through `replay_apply`: that funnel
            // intentionally validates the stored placement decision against
            // the current compiler, and the whole reason for this migration is
            // that the decision can have changed. Fold the raw logical
            // mutations over the committed source corpus first, then compile
            // and place the resulting set exactly once under current semantics.
            let mut live: BTreeMap<u64, crate::cluster::shard::LiveTaggedQuery> =
                ClusterEngine::live_corpus_tagged(&engine.layout())?
                    .into_iter()
                    .map(|row| (row.0, row))
                    .collect();
            for (_pos, mutation) in replay.entries {
                match mutation {
                    ClusterMutation::Add {
                        logical,
                        version,
                        dsl,
                        tags,
                        placement,
                    } => {
                        if live.contains_key(&logical) {
                            return Err(ShardError::DuplicateLogicalId(logical));
                        }
                        let tag_ids = resolve_replayed_tag_ids(&engine.tag_dict, &tags)?;
                        if !tags.is_empty() {
                            engine.tags_present.store(true, Ordering::Relaxed);
                        }
                        live.insert(
                            logical,
                            (
                                logical,
                                dsl,
                                version,
                                0,
                                tags,
                                tag_ids,
                                crate::rank::RankValues::default(),
                                placement,
                            ),
                        );
                    }
                    ClusterMutation::Remove { logical } => {
                        live.remove(&logical);
                    }
                    ClusterMutation::Upsert {
                        logical,
                        version,
                        dsl,
                        tags,
                        placement,
                    } => {
                        let tag_ids = resolve_replayed_tag_ids(&engine.tag_dict, &tags)?;
                        if !tags.is_empty() {
                            engine.tags_present.store(true, Ordering::Relaxed);
                        }
                        live.insert(
                            logical,
                            (
                                logical,
                                dsl,
                                version,
                                0,
                                tags,
                                tag_ids,
                                crate::rank::RankValues::default(),
                                placement,
                            ),
                        );
                    }
                }
            }
            let change = engine.begin_layout_change()?;
            let current = change.current();
            let next_generation = current
                .generation
                .next()
                .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
            let (norm, ring) = (Arc::clone(&current.norm), current.ring.clone());
            drop(current);
            let (_rebuilt, migrated) = engine.rebuild_from_corpus(
                &change,
                live.into_values().collect(),
                norm,
                ring,
                None,
                next_generation,
                true,
            )?;
            engine
                .control
                .propose(ClusterStateChange::BumpModelVersion {
                    dict_fingerprint: migrated.dict.fingerprint(),
                })?;
            engine.checkpoint_quiesced(&migrated)?;
        } else {
            let layout = engine.layout();
            for (_pos, mutation) in replay.entries {
                engine.replay_apply(&layout, mutation)?;
            }
        }
        Ok(engine)
    }
}
