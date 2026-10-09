//! `impl ClusterEngine` — runtime vocabulary change (ADR-046 mechanism 2).
//!
//! A vocabulary change (e.g. a declared alias `ns ≡ northstar`) swaps the ONE
//! shared normalizer and rebuilds the cluster from its live source set: every
//! query is re-extracted under the new normalizer, **re-placed** (an alias can
//! change a query's anchor → hence its shard), and re-ingested. This is a
//! "blue/green rebuild from the log" (ADR-004): the dict is re-minted over the
//! live corpus so feature frequencies/hotness reflect the post-change
//! distribution, exactly as [`ClusterEngine::build`] does.
//!
//! The new layout is published in one step (no reader observes a half-state: a read
//! runs on the layout it loaded, ADR-208), so both surface forms of an alias resolve
//! to one feature with **zero false negatives**. Writes are refused while it is built
//! (ADR-209).
//!
//! **In-process only.** An alias is a normalizer operation and is NOT shipped to a
//! `RemoteShard` in v1, so [`ClusterEngine::set_vocab`] refuses a non-local cluster
//! (a remote shard would keep normalizing under the stale normalizer — a silent
//! cross-process false negative the dict-fingerprint handshake cannot catch, since
//! the alias does not change the interned-name set).
//!
//! **Per-query tags survive the rebuild (ADR-074).** The tag space is orthogonal to
//! vocabulary and preserved unchanged, so each query's stored `TagId`s — interned
//! dense or post-freeze *synthetic* (which have no recoverable string) — are gathered
//! alongside its DSL and carried verbatim to wherever re-placement puts it: the
//! cluster analogue of the single-node ADR-049 carry-through in
//! `Engine::recompile_stale_segments`.

use std::sync::{atomic::Ordering, Arc};

use crate::vocab::{CorpusLearnConfig, Vocab};

use super::{ClusterEngine, CLUSTER_MANIFEST_FILE};
use crate::cluster::control::ClusterStateChange;
use crate::cluster::coordinator::layout::{Layout, LayoutChange};
use crate::cluster::shard::ShardError;

enum AliasImportManifestState {
    Committed,
    PublishedCurrent(Box<crate::storage::ClusterManifest>),
    ImmediatePredecessor,
}

mod gather;

impl ClusterEngine {
    /// Change the cluster's vocabulary (ADR-046 mechanism 2) — e.g. declare an
    /// alias so two surface forms match. Rebuilds the cluster from its live source
    /// set under the new normalizer: re-mints the shared dict, re-places every
    /// query (an alias can move a query's anchor, hence its shard), and re-ingests —
    /// carrying each query's stored tags with it (ADR-074; the tag space is
    /// preserved unchanged). Published in one step; a durable cluster commits the
    /// rebuild via [`checkpoint`](Self::checkpoint). Returns the number of live
    /// queries rebuilt.
    ///
    /// Refuses (errors) if any shard is non-local or handoff-wrapped. A vocabulary
    /// that activates a multi-word alias is supported (ADR-076: P(T)-aware routing).
    pub fn set_vocab(&self, vocab: Vocab) -> Result<usize, ShardError> {
        let change = self.begin_layout_change()?;
        self.set_vocab_in(&change, vocab)
    }

    /// [`Self::set_vocab`] inside a layout change the caller began, so that reading the
    /// current vocabulary and replacing it are one step.
    pub(in crate::cluster::coordinator) fn set_vocab_in(
        &self,
        change: &LayoutChange<'_>,
        vocab: Vocab,
    ) -> Result<usize, ShardError> {
        let before = change.current();
        // 1. Correctness boundary: in-process only (see module doc). On a
        //    non-distributed build every shard is local, so this never fires — but
        //    it is always compiled, so a future non-local shard can't slip past it.
        if before.shards.iter().any(|s| !s.is_local()) {
            return Err(ShardError::Config(
                "set_vocab is in-process only: a cross-process (remote) shard is not shipped \
                 the new normalizer in v1 (it would be a silent false negative)"
                    .into(),
            ));
        }
        #[cfg(feature = "distributed")]
        if !before.handoffs.is_empty() {
            return Err(ShardError::Config(
                "set_vocab is in-process only: a handoff-wrapped (movable) shard position is not \
                 supported by a vocabulary change in v1"
                    .into(),
            ));
        }
        // 2. Build the new normalizer up front (a parse/build error aborts before any swap).
        let new_norm = Arc::new(
            vocab
                .to_normalizer()
                .map_err(|e| ShardError::Config(format!("building normalizer from vocab: {e}")))?,
        );
        // 2b. Self-heal stale-active aliases FIRST (codex R13/R14): a punctuation change in
        //     this vocab can make an Active alias form unexpressible;
        //     demote those to review candidates rather than install an alias that reports
        //     active and silently never matches. Demotion can only shrink the registered phrase
        //     set, so rebuild the normalizer when it fires, so every later consumer (the
        //     rebuild + the installed normalizer) judges the HEALED vocabulary (codex R13/R14;
        //     the multi-word refusal this once guarded is retired by ADR-076, the heal stays).
        let mut vocab = vocab;
        let new_norm =
            if vocab
                .aliases_mut()
                .demote_unexpressible(&new_norm, &before.dict)
                > 0
            {
                Arc::new(vocab.to_normalizer().map_err(|e| {
                    ShardError::Config(format!("building normalizer from vocab: {e}"))
                })?)
            } else {
                new_norm
            };

        // A vocab that activates a multi-word alias is cluster-supported since ADR-076:
        // `route` is P(T)-aware when multi-word aliases are active, so a nested alias
        // entity that lives only in the positive superset still probes the shard holding
        // a query anchored on it. The ADR-061 refusal that used to guard this swap is
        // retired; the rebuild below re-places every query under the new normalizer, so
        // routing and placement stay derived from the same vocabulary.

        // 3. Rebuild the cluster from its live source set under the new normalizer, KEEPING the
        //    ring (same shard count). The shared blue/green core (ADR-046/078) re-mints the dict,
        //    re-places every query, builds fresh shards, and publishes them in one step.
        //    `Some(vocab)` installs the new vocabulary and uses ITS equivalence groups; per-query
        //    tags carry through as stored `TagId`s (ADR-074). The resize path (ADR-078) calls the
        //    SAME core with a fresh ring instead of a new vocab.
        let next_generation = before
            .generation
            .next()
            .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
        let ring = before.ring.clone();
        // The old layout is released before the rebuild, which holds a second corpus.
        drop(before);
        let (rebuilt, after) =
            self.rebuild_from_live(change, new_norm, ring, Some(vocab), next_generation)?;
        let mut lap = super::resize::Lap::start();

        self.propose_layout_change(
            &after,
            ClusterStateChange::BumpModelVersion {
                dict_fingerprint: after.dict.fingerprint(),
            },
        )?;

        // 4. Commit a durable cluster's rebuild via `checkpoint`: seal the green shards, write the
        //    new manifest (re-minted dict + serialized vocab + green segment registry — the atomic
        //    commit point), truncate the log, and GC the superseded old segment files.
        if self.data_dir.is_some() {
            // Operations that loaded the old layout finish on it. Its files go once they
            // have, here or at a later checkpoint.
            self.await_retired_layouts();
            self.checkpoint_quiesced(&after)?;
        }
        self.note_rebuild_commit(lap.lap());
        Ok(rebuilt)
    }

    /// Learn relationships from the cluster's OWN live corpus and apply them (ADR-046
    /// mechanism 2). A pair of forms seen together in at least `min_count` any-of groups
    /// (e.g. `(new,pkg)`) becomes an equivalence applied by expansion (ADR-054, ADR-202): no
    /// stored query loses a match. It is merged UNDER the current vocabulary — a previously
    /// *declared* rule wins over a learned one — and the cluster is rebuilt via
    /// [`Self::set_vocab`]. Returns the number of queries
    /// rebuilt. Refuses a non-local cluster (the gather can't enumerate a remote shard).
    ///
    /// On-demand: a future step can drive this from compaction's "improve" phase (the
    /// LSM-shaped background re-materialize); this is the explicit trigger.
    ///
    /// A thin wrapper over [`learn_and_apply_with`](Self::learn_and_apply_with) with the
    /// default configuration: expansion, no NPMI phrase induction.
    pub fn learn_and_apply(&self, min_count: usize) -> Result<usize, ShardError> {
        self.learn_and_apply_with(&CorpusLearnConfig {
            anyof_min_count: min_count,
            ..Default::default()
        })
    }

    /// Learn vocabulary rules from the cluster's own live corpus WITHOUT applying them —
    /// the dry-run behind the coordinator-mode server's `POST /_vocab/learn` (ADR-070):
    /// the caller reviews the learned [`Vocab`] and decides whether to `PUT /_vocab` it.
    /// Compute-only (`&self`); refuses a non-local cluster (the gather boundary).
    pub fn learn_vocab(&self, cfg: &CorpusLearnConfig) -> Result<Vocab, ShardError> {
        let layout = &*self.layout();
        let corpus = Self::live_corpus(layout)?;
        Ok(crate::vocab::learn_vocab_from_corpus(&corpus, cfg))
    }

    /// Import a Solr/Lucene synonym file into the governed alias registry and apply it
    /// (ADR-060 at the cluster, ADR-070): classifies against the cluster's CURRENT
    /// normalizer + frozen dict, then rebuilds via [`Self::set_vocab`] — whose non-local
    /// refusal holds unchanged (tags carry through per ADR-074; multi-word activation is
    /// supported per ADR-076). Returns the engine-shaped apply report (`recompiled` =
    /// queries rebuilt).
    pub fn import_alias_synonyms(
        &self,
        solr_text: &str,
    ) -> Result<crate::segment::AliasApplyReport, ShardError> {
        // Reading the vocabulary and replacing it are one step: two imports must not both
        // start from the same vocabulary.
        let change = self.begin_layout_change()?;
        let current = change.current();
        let mut vocab = current.vocab.as_deref().cloned().unwrap_or_default();
        let before = vocab.aliases().clone();
        let activated = vocab
            .import_solr_aliases(solr_text, &current.norm, &current.dict)
            .map_err(|error| ShardError::Config(error.to_string()))?;
        let changed = vocab.aliases() != &before;
        if !changed {
            self.finish_pending_alias_import_commit(&current)?;
            return Ok(crate::segment::AliasApplyReport {
                applied: false,
                activated,
                recompiled: 0,
                summary: current
                    .vocab
                    .as_deref()
                    .map(Vocab::alias_summary)
                    .unwrap_or_default(),
            });
        }
        let predecessor = self.capture_alias_import_predecessor(&current)?;
        *self.alias_import_predecessor() = predecessor;
        *self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        // The old layout is released before the rebuild, which holds a second corpus.
        drop(current);
        let rebuilt = self.set_vocab_in(&change, vocab)?;
        self.clear_pending_alias_import_identity();
        Ok(crate::segment::AliasApplyReport {
            applied: true,
            activated,
            recompiled: rebuilt,
            summary: change
                .current()
                .vocab
                .as_deref()
                .map(Vocab::alias_summary)
                .unwrap_or_default(),
        })
    }

    /// Complete the post-swap commits a prior `set_vocab` attempt may have
    /// failed before publishing. A fully committed import remains a read-only
    /// no-op; incompatibility and attestation failures stay fail-loud.
    fn finish_pending_alias_import_commit(&self, layout: &Layout) -> Result<(), ShardError> {
        let generation = layout.generation;
        let dict_fingerprint = layout.dict.fingerprint();
        let manifest_state = self.alias_import_manifest_state(layout, generation)?;
        let state = self.control.cluster_state()?;
        let live_shards = u32::try_from(layout.ring.num_shards()).map_err(|_| {
            ShardError::ControlPlane(
                "live shard count exceeds the control-plane representation".into(),
            )
        })?;
        let assignments_match = state.assignments.len() == layout.ring.num_shards()
            && state
                .assignments
                .iter()
                .enumerate()
                .all(|(position, assignment)| assignment.position as usize == position);
        if state.num_shards != live_shards || state.vnodes != self.vnodes || !assignments_match {
            return Err(ShardError::ControlPlane(format!(
                "alias-import retry found control topology with {} shards, {} vnodes, and {} \
                 assignment(s); live topology has {} shards and {} vnodes",
                state.num_shards,
                state.vnodes,
                state.assignments.len(),
                layout.ring.num_shards(),
                self.vnodes
            )));
        }
        if state.placement_generation != generation.0 || state.dict_fingerprint != dict_fingerprint
        {
            let prior_generation = generation.0.checked_sub(1).ok_or_else(|| {
                ShardError::ControlPlane(
                    "cannot repair alias-import model state at placement generation zero".into(),
                )
            })?;
            if state.placement_generation != prior_generation {
                return Err(ShardError::ControlPlane(format!(
                    "alias-import retry found control placement generation {}, expected {} or {}",
                    state.placement_generation, prior_generation, generation.0
                )));
            }
            self.propose_layout_change(
                layout,
                ClusterStateChange::BumpModelVersion { dict_fingerprint },
            )?;
            let repaired = self.control.cluster_state()?;
            if repaired.placement_generation != generation.0
                || repaired.dict_fingerprint != dict_fingerprint
            {
                return Err(ShardError::ControlPlane(
                    "alias-import model-state repair was not committed".into(),
                ));
            }
        }

        match manifest_state {
            AliasImportManifestState::Committed => {}
            AliasImportManifestState::PublishedCurrent(manifest) => {
                let dir = self.data_dir.as_ref().ok_or_else(|| {
                    ShardError::Log(
                        "alias-import retry lost its durable directory before sync".into(),
                    )
                })?;
                crate::fault::sync_dir_of(&dir.join(CLUSTER_MANIFEST_FILE)).map_err(|error| {
                    ShardError::Log(format!(
                        "syncing the published alias-import manifest directory: {error}"
                    ))
                })?;
                self.epoch.store(manifest.epoch, Ordering::Relaxed);
                self.record_committed_manifest(*manifest);
            }
            AliasImportManifestState::ImmediatePredecessor => {
                self.checkpoint_quiesced(layout)?;
            }
        }
        self.clear_pending_alias_import_identity();
        Ok(())
    }

    /// The durable predecessor an alias import captured before it swapped the model, while
    /// that import's commit is incomplete.
    pub(in crate::cluster::coordinator) fn alias_import_predecessor(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<crate::storage::ClusterManifest>> {
        self.pending_alias_import_predecessor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn clear_pending_alias_import_identity(&self) {
        *self.alias_import_predecessor() = None;
        *self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Capture and attest the exact durable state an alias import is allowed to
    /// supersede. Retaining the parsed document makes a later retry compare every
    /// commit-identity field, including vocabulary and segment registry.
    fn capture_alias_import_predecessor(
        &self,
        layout: &Layout,
    ) -> Result<Option<crate::storage::ClusterManifest>, ShardError> {
        let Some(dir) = &self.data_dir else {
            return Ok(None);
        };
        let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|error| {
                ShardError::Log(format!(
                    "reading cluster manifest before alias import: {error}"
                ))
            })?;
        self.attest_alias_import_manifest_common(layout, &manifest)?;
        let committed_matches = self
            .committed_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            == Some(&manifest);
        let vocab_data = Self::alias_import_vocab_data(layout)?;
        if !committed_matches
            || manifest.epoch != self.epoch()
            || manifest.placement_generation != layout.generation
            || manifest.dict_fingerprint != layout.dict.fingerprint()
            || manifest.dict_data != crate::storage::serialize_dict(&layout.dict)
            || manifest.vocab_data != vocab_data
        {
            return Err(ShardError::Log(
                "cluster manifest diverged before alias import".into(),
            ));
        }
        Ok(Some(manifest))
    }

    /// Classify the durable commit point before an identical alias-import retry
    /// mutates the control plane or checkpoints. Only the live commit itself or
    /// its exact epoch/generation predecessor is admissible; every other readable
    /// manifest is divergent and must remain untouched.
    fn alias_import_manifest_state(
        &self,
        layout: &Layout,
        generation: crate::ownership::PlacementGeneration,
    ) -> Result<AliasImportManifestState, ShardError> {
        let Some(dir) = &self.data_dir else {
            return Ok(AliasImportManifestState::Committed);
        };
        let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|error| {
                ShardError::Log(format!(
                    "reading cluster manifest before alias-import retry: {error}"
                ))
            })?;
        self.attest_alias_import_manifest_common(layout, &manifest)?;
        let committed_manifest = self
            .committed_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let pending_manifest = self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        if manifest.placement_generation == generation {
            if self.alias_import_predecessor().is_some()
                && self
                    .epoch()
                    .checked_add(1)
                    .is_some_and(|next| manifest.epoch == next)
                && (pending_manifest.as_ref() == Some(&manifest)
                    || committed_manifest.as_ref() == Some(&manifest))
            {
                return Ok(AliasImportManifestState::PublishedCurrent(Box::new(
                    manifest,
                )));
            }
            if manifest.epoch == self.epoch() && committed_manifest.as_ref() == Some(&manifest) {
                return Ok(AliasImportManifestState::Committed);
            }
            return Err(ShardError::Log(format!(
                "current alias-import manifest epoch {} or recovery identity diverges from live \
                 epoch {}",
                manifest.epoch,
                self.epoch()
            )));
        }

        let prior_generation = generation.0.checked_sub(1).ok_or_else(|| {
            ShardError::Log(
                "cannot attest an alias-import predecessor at placement generation zero".into(),
            )
        })?;
        if manifest.placement_generation.0 != prior_generation
            || manifest.epoch != self.epoch()
            || self.alias_import_predecessor().as_ref() != Some(&manifest)
            || committed_manifest.as_ref() != Some(&manifest)
        {
            return Err(ShardError::Log(format!(
                "cluster manifest placement generation {} is not the alias-import predecessor {}",
                manifest.placement_generation.0, prior_generation
            )));
        }
        Ok(AliasImportManifestState::ImmediatePredecessor)
    }

    fn alias_import_vocab_data(layout: &Layout) -> Result<Vec<u8>, ShardError> {
        match &layout.vocab {
            Some(vocab) => vocab.to_json().map(String::into_bytes).map_err(|error| {
                ShardError::Log(format!("serializing cluster vocab for retry: {error}"))
            }),
            None => Ok(Vec::new()),
        }
    }

    fn attest_alias_import_manifest_common(
        &self,
        layout: &Layout,
        manifest: &crate::storage::ClusterManifest,
    ) -> Result<(), ShardError> {
        let topology_matches = manifest.num_shards as usize == layout.ring.num_shards()
            && manifest.vnodes == self.vnodes
            && manifest.include_broad == self.include_broad
            && manifest.broad_replicate_all
            && manifest.segment_registry.len() == layout.ring.num_shards()
            && manifest.next_seg_ids.len() == layout.ring.num_shards()
            && manifest.source_files.len() == layout.ring.num_shards();
        if !topology_matches
            || manifest.compiler_semantics_version
                != crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION
            || manifest.tag_dict_data != crate::storage::serialize_tagdict(&self.tag_dict)
        {
            return Err(ShardError::Log(format!(
                "cluster manifest diverged before alias-import retry: epoch {}, placement \
                 generation {}, {} shards, {} vnodes",
                manifest.epoch,
                manifest.placement_generation.0,
                manifest.num_shards,
                manifest.vnodes
            )));
        }

        let persisted_dict =
            crate::storage::deserialize_dict(&manifest.dict_data).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster dict before alias-import retry: {error}"
                ))
            })?;
        if persisted_dict.fingerprint() != manifest.dict_fingerprint {
            return Err(ShardError::Log(format!(
                "persisted cluster dict fingerprint diverged before alias-import retry: manifest \
                 {:#018x}, actual {:#018x}",
                manifest.dict_fingerprint,
                persisted_dict.fingerprint()
            )));
        }

        if !manifest.vocab_data.is_empty() {
            let persisted = std::str::from_utf8(&manifest.vocab_data).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
            let persisted_vocab = Vocab::from_json(persisted).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
            persisted_vocab.to_normalizer().map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Learn alias candidates from the cluster's OWN stored queries (any-of
    /// co-occurrence, ADR-060 item 2) into the registry and apply. Conservative: only
    /// clear single-token variants auto-activate; everything else stays a review
    /// candidate. Rebuilds via [`Self::set_vocab`] (all refusals hold).
    pub fn learn_aliases_and_apply(
        &self,
        min_count: usize,
    ) -> Result<crate::segment::AliasApplyReport, ShardError> {
        // Learning from the corpus and replacing the vocabulary are one step.
        let change = self.begin_layout_change()?;
        let current = change.current();
        let corpus = Self::live_corpus(&current)?;
        let mut vocab = current.vocab.as_deref().cloned().unwrap_or_default();
        let activated =
            vocab.learn_aliases_from_queries(&corpus, min_count, &current.norm, &current.dict);
        drop(current);
        let rebuilt = self.set_vocab_in(&change, vocab)?;
        Ok(crate::segment::AliasApplyReport {
            applied: true,
            activated,
            recompiled: rebuilt,
            summary: change
                .current()
                .vocab
                .as_deref()
                .map(Vocab::alias_summary)
                .unwrap_or_default(),
        })
    }

    /// Like [`learn_and_apply`](Self::learn_and_apply) but also runs opt-in **NPMI corpus
    /// phrase induction** when `cfg.corpus_phrases` is set (ADR-053): multi-token entities
    /// induced from the cluster's live query text are merged UNDER the current vocabulary
    /// (a declared alias/phrase wins on a token collision) and the cluster is rebuilt via
    /// [`Self::set_vocab`] (which re-places every query — a phrase can move a query's anchor,
    /// hence its shard). With `corpus_phrases = false` this is identical to
    /// `learn_and_apply(cfg.anyof_min_count)`. Phrases only — never aliases — so the
    /// same-normalizer gluing is lossless-cover safe. Refuses a non-local cluster.
    pub fn learn_and_apply_with(&self, cfg: &CorpusLearnConfig) -> Result<usize, ShardError> {
        // Learning from the corpus and replacing the vocabulary are one step.
        let change = self.begin_layout_change()?;
        let current = change.current();
        let corpus = Self::live_corpus(&current)?;
        let learned = crate::vocab::learn_vocab_from_corpus(&corpus, cfg);
        // Merge learned rules UNDER the current vocab (declared aliases win), then rebuild.
        let mut merged = Vocab::new();
        if let Some(v) = &current.vocab {
            merged.merge(v);
        }
        merged.merge(&learned);
        drop(current);
        self.set_vocab_in(&change, merged)
    }

    /// The vocabulary behind the current normalizer, if one was installed via
    /// [`Self::set_vocab`]/[`Self::learn_and_apply`] (`None` when built directly from
    /// a `Normalizer`).
    pub fn vocab(&self) -> Option<Arc<Vocab>> {
        self.layout().vocab.clone()
    }
}
