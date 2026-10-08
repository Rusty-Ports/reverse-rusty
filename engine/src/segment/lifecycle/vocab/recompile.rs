use super::{Arc, Engine, Segment};

impl Engine {
    /// Recompile every live query under the CURRENT normalizer, replacing all
    /// base segments (and the memtable) with one freshly-compiled segment at the
    /// current vocab epoch. This is the recompile pass that makes a normalizer
    /// change ([`set_vocab`](Self::set_vocab)) actually take effect on
    /// already-ingested queries: without it, segments compiled under the old
    /// normalizer carry stale feature ids, and a title normalized with the new
    /// normalizer can miss them — a **false negative**.
    ///
    /// Queries are recompiled READ-ONLY against the dict via
    /// [`extract_readonly`](crate::compile::extract_readonly): a declared alias
    /// collapses both surface forms to one feature (so both now match). Every name
    /// the current normalizer produces for a stored query must already be interned:
    /// [`set_vocab`](Self::set_vocab) and the compiler migration do that first
    /// (ADR-204). A name left to its synthetic id here would get a second, dense id
    /// from the next insert that uses it.
    ///
    /// A no-op (returns 0) when nothing is stale; after it, `has_stale_segments()`
    /// is false. Returns the number of queries recompiled.
    ///
    /// Atomicity: a caller that publishes snapshots (e.g. the server) must call
    /// this **before** publishing the next snapshot, so readers never observe the
    /// new normalizer against not-yet-recompiled segments.
    pub fn recompile_stale_segments(&mut self) -> usize {
        if !self.has_stale_segments() {
            return 0;
        }
        // Never replace a degraded/partial recovery with a freshly committed
        // strict subset. The old manifest must remain authoritative so the
        // unreadable segment can be repaired.
        if self.config.data_dir.is_some() && !self.persistence_healthy {
            return 0;
        }
        // Recompile the live source set read-only against the frozen dict under
        // the current normalizer into one fresh segment.
        let Ok(live) = self.live_source_documents_tagged() else {
            // A source/exact mismatch means rebuilding from the sidecar could
            // replace acknowledged match data with a stale document. Keep the
            // old segments intact and fail closed.
            return 0;
        };
        // A rebuild re-plans every query against today's dict, and a plan can come out
        // opt-in for a query that default reads return now: its anchor became top-64 after
        // it was compiled, or the new vocabulary moved its anchor into an any-of group
        // with a top-64 member. Such a query keeps an always-probed cover (ADR-187).
        let opt_in_only = self.opt_in_only_logical_ids();
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut lc = String::new();
        let mut recompiled = 0usize;
        for (logical, text, version, source_generation, _, tags, rank, placement) in &live {
            let Ok(ast) = crate::dsl::parse_for_recovery(text) else {
                return 0;
            };
            let ex = crate::compile::extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            if ex.column_overflow().is_some() {
                return 0;
            }
            // Carry tags, caller version, internal source generation, rank, and
            // placement unchanged: they are all orthogonal to normalization.
            // `accept_class_d = true` unconditionally (ADR-068): a STORED query
            // must survive a vocabulary change. A query whose positives vanish
            // under the new vocab is retained in the always-candidate lane.
            let knobs = crate::segment::CompileKnobs {
                accept_class_d: true,
                keep_visible: !opt_in_only.contains(logical),
                ..self.config.compile_knobs()
            };
            let Some(added) = seg.add_compiled_ranked_placed_with_source_generation(
                &ex,
                tags,
                &self.dict,
                *logical,
                *version,
                *rank,
                placement,
                *source_generation,
                knobs,
            ) else {
                return 0;
            };
            self.record_compiled(&added);
            recompiled += 1;
        }
        seg.build_filter();

        // Prepare the source snapshot while the old manifest + WAL remain
        // authoritative, but DO NOT select it yet. Selecting it here would also
        // advance the manifest WAL watermark before the replacement segment
        // captures memtable inserts/deletes; a crash in that window could replay
        // an insert while skipping its later acknowledged delete.
        let (staged_sources, sources_persisted) = if self.owns_manifest {
            match self.stage_query_sources(&[]) {
                Ok(staged) => (staged, true),
                Err(_) => (None, false),
            }
        } else {
            self.save_query_sources();
            (
                None,
                self.config.data_dir.is_none() || self.persistence_healthy,
            )
        };

        // Atomic swap: drop every (stale) base segment + the memtable and install
        // the one freshly-compiled segment, so no live query is left at an old
        // epoch. Old segment files are GC'd after the manifest commit.
        let old_files = self.collect_mmap_paths();
        self.segments.clear();
        self.segment_generations.clear();
        let mut fresh_mem = Segment::new();
        fresh_mem.vocab_epoch = self.vocab_epoch;
        self.memtable = Arc::new(fresh_mem);
        let persisted = self.seal_and_push(seg);
        // `vocab_epoch` is process-local (not part of the durable segment
        // format), so an mmap opened immediately after the write starts at zero.
        // Stamp the just-installed view with the live epoch before any caller
        // evaluates `has_stale_segments`.
        if let Some(segment) = self.segments.last_mut() {
            Arc::make_mut(segment).set_vocab_epoch(self.vocab_epoch);
        }

        // Persist like a flush, but FAIL CLOSED (ADR-051): only retire the old
        // segment files and advance the WAL (checkpoint marks the live queries
        // materialized, reset truncates them) once the freshly-compiled segment is
        // durably on disk AND the manifest — the commit point referencing it — has
        // been written. We just cleared the old segments from the vec, so if the
        // recompiled segment did NOT persist, deleting the old files or resetting
        // the WAL would erase the only durable copy of the whole corpus. Leaving
        // both intact lets a restart recover the pre-recompile state and re-apply
        // the vocab change. The recompiled segment is still served from memory
        // meanwhile; `persistence_healthy` is false to signal the degraded state.
        // If the source-sidecar write failed, still install the fully-built
        // replacement in RAM: the caller may already have installed the new
        // normalizer, so retaining old exact plans would create false
        // negatives. The old manifest + WAL remain the restart authority
        // because the manifest commit is deliberately skipped; the unhealthy
        // flag tells operators that this coherent live state is not durable.
        let committed = if sources_persisted && persisted {
            if self.owns_manifest {
                self.commit_staged_sources_and_manifest(staged_sources)
            } else {
                self.save_manifest_if_persistent()
            }
        } else {
            self.discard_staged_sources(staged_sources);
            false
        };
        if committed {
            self.checkpoint_wal();
            self.reset_wal_if_safe();
            // A standalone engine owns the manifest commit above, so the old files are
            // removed now. A cluster shard does NOT own its registry: its coordinator
            // manifest or `shard.ckpt` sidecar must point at the replacement first, so
            // there they are kept for that owner to release after its commit (ADR-118,
            // ADR-214). `cleanup_segment_files` makes that distinction for every caller.
            self.cleanup_segment_files(&old_files);
        }
        recompiled
    }

    /// Learn relationships from this engine's live corpus and apply them (ADR-046
    /// mechanism 2): a pair of forms seen together in at least `min_count` any-of groups
    /// (e.g. `(new,pkg)`) becomes an equivalence applied by expansion (ADR-054, ADR-202), so
    /// a query that names one form also accepts the other and no stored query loses a
    /// match. It is merged UNDER the current vocabulary (a previously set rule wins) and the
    /// index is recompiled so the change takes effect immediately. Returns the number of
    /// queries recompiled.
    ///
    /// A thin wrapper over [`learn_and_apply_with`](Self::learn_and_apply_with) with the
    /// default configuration: expansion, no NPMI phrase induction. Collapse synonyms
    /// (ADR-015) are available there with `AnyOfLearnMode::Collapse`.
    pub fn learn_and_apply(
        &mut self,
        min_count: usize,
    ) -> Result<usize, crate::error::NormalizerError> {
        self.learn_and_apply_with(&crate::vocab::CorpusLearnConfig {
            anyof_min_count: min_count,
            ..Default::default()
        })
    }

    /// Like [`learn_and_apply`](Self::learn_and_apply) but also runs opt-in **NPMI corpus
    /// phrase induction** when `cfg.corpus_phrases` is set (ADR-053): multi-token entities
    /// induced from the live query text (e.g. `north star`) are merged UNDER the current
    /// vocabulary (a declared alias/phrase wins on a token collision) and the index is
    /// recompiled. With the default mode and `corpus_phrases = false` this is identical to
    /// `learn_and_apply(cfg.anyof_min_count)`. Induced phrases are phrases only — never
    /// aliases — so the same-normalizer gluing is lossless-cover safe (zero false
    /// negatives). `cfg.anyof_mode` says how any-of relationships are applied: by expansion
    /// (the default), or as collapse synonyms, which can remove matches stored queries had
    /// (ADR-202). Returns the number of queries recompiled.
    pub fn learn_and_apply_with(
        &mut self,
        cfg: &crate::vocab::CorpusLearnConfig,
    ) -> Result<usize, crate::error::NormalizerError> {
        let corpus = self.live_sources();
        let learned = crate::vocab::learn_vocab_from_corpus(&corpus, cfg);
        let mut merged = crate::vocab::Vocab::new();
        if let Some(v) = &self.vocab {
            merged.merge(v);
        }
        merged.merge(&learned);
        self.set_vocab(merged)?; // bumps the epoch / marks segments stale
        Ok(self.recompile_stale_segments())
    }
}
