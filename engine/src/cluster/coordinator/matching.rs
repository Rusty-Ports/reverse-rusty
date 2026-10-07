//! `impl ClusterEngine` — the read path: routing, `percolate` (+ stats / explicit-broad
//! variants) and the cross-shard merge. Counts, fan-out introspection and document reads are
//! in `inspect`.

use crate::cluster::shard::ShardError;
use crate::compile::is_hot;
use crate::dict::FeatureId;
use crate::exact::TagPredicate;
use crate::segment::MatchStats;

use super::ClusterEngine;
use crate::cluster::coordinator::layout::Layout;

/// A mutation-frozen cluster view for a read that must combine Boolean matches
/// with later source enrichment.
///
/// Ordinary percolation remains lock-free at this level. This stronger view is
/// intentionally explicit because it takes the exclusive side of the
/// coordinator mutation barrier until drop, so every direct library mutation
/// waits until all match IDs and requested sources have been read.
pub struct ClusterReadView<'a> {
    cluster: &'a ClusterEngine,
    _mutation_guard: std::sync::RwLockWriteGuard<'a, ()>,
}

impl ClusterReadView<'_> {
    /// Match one title with merged statistics under this view.
    pub fn percolate_filtered_with_stats(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.cluster
            .percolate_filtered_with_stats(title, filter, include_broad)
    }

    /// Match and score one title under this view.
    pub fn percolate_filtered_ranked(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
        rank: &crate::rank::RankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.cluster
            .percolate_filtered_ranked(title, filter, include_broad, rank)
    }

    /// Exact bounded top-K matching under this mutation-frozen view.
    pub fn try_percolate_filtered_top_k(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        deadline: Option<std::time::Instant>,
    ) -> Result<crate::cluster::ClusterRankedMatch, crate::cluster::ClusterRankedError> {
        self.cluster
            .try_percolate_filtered_top_k(title, filter, options, program, deadline)
    }

    /// PIT-scoped bounded top-K matching under this mutation-frozen view.
    #[allow(clippy::too_many_arguments)]
    pub fn try_percolate_filtered_top_k_pit(
        &self,
        pit: crate::pit::PitId,
        title: &str,
        filter: &[(String, Vec<String>)],
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        deadline: Option<std::time::Instant>,
        now: std::time::Instant,
    ) -> Result<crate::cluster::ClusterRankedMatch, crate::cluster::ClusterRankedError> {
        self.cluster
            .try_percolate_filtered_top_k_pit(pit, title, filter, options, program, deadline, now)
    }

    /// Fetch finalized winner sources while direct mutations remain frozen.
    pub fn fetch_ranked_sources_bounded(
        &self,
        ranked: &crate::cluster::ClusterRankedMatch,
        max_source_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<String>, crate::cluster::ClusterRankedError> {
        self.cluster
            .fetch_ranked_sources_bounded(ranked, max_source_bytes, deadline)
    }

    /// Exact bounded top-K matching for a title batch under this view.
    pub fn try_percolate_filtered_top_k_batch(
        &self,
        titles: &[impl AsRef<str> + Sync],
        filter: &[(String, Vec<String>)],
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        deadline: Option<std::time::Instant>,
    ) -> Result<crate::cluster::ClusterBatchRankedMatch, crate::cluster::ClusterRankedError> {
        self.cluster
            .try_percolate_filtered_top_k_batch(titles, filter, options, program, deadline)
    }

    /// Fetch all batch winner sources while direct mutations remain frozen.
    pub fn fetch_ranked_sources_batch_bounded(
        &self,
        ranked: &crate::cluster::ClusterBatchRankedMatch,
        max_source_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<Vec<String>>, crate::cluster::ClusterRankedError> {
        self.cluster
            .fetch_ranked_sources_batch_bounded(ranked, max_source_bytes, deadline)
    }

    /// Compile a winner explanation under the same coordinator vocabulary.
    pub fn explain_ranked_source(
        &self,
        logical_id: u64,
        source: &str,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        self.cluster
            .explain_ranked_source(logical_id, source, title)
    }

    /// Fetch one live source under the same mutation-frozen view as matching.
    pub fn get_source(&self, logical: u64) -> Result<Option<String>, ShardError> {
        self.cluster.get_source(logical)
    }
}

impl ClusterEngine {
    /// Freeze direct cluster mutations and return a coherent read view.
    ///
    /// This is not a long-lived PIT. It is a short request-scoped fence for
    /// operations such as compatibility search that must match first and then
    /// materialize sources without pairing an old match with a replacement
    /// query. Callers should drop the view immediately after enrichment.
    pub fn consistent_read_view(&self) -> ClusterReadView<'_> {
        let mutation_guard = self
            .pit_open_barrier
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ClusterReadView {
            cluster: self,
            _mutation_guard: mutation_guard,
        }
    }

    /// The shards a title is routed to: shard 0 (the replicated-lane evaluator)
    /// plus the shard owning each anchor-eligible (non-hot) title feature. Reuses
    /// the same `match_features` primitives the match path uses, so routing and
    /// matching cannot drift.
    ///
    /// **P(T)-aware under multi-word aliases (ADR-076):** with an active multi-word
    /// alias, routing derives targets from the **maximal positive view** `P(T)` —
    /// the same superset the shard-local verifier reads required/any-of against
    /// (ADR-061) — instead of the canonical leftmost-longest `N(T)`. The cover
    /// argument: a query's anchor is one of its extracted positive features, and
    /// `P(T)` contains every feature ANY parse of the title emits (the parse-union
    /// property the ADR-061 oracle pins), so a title that could satisfy a query
    /// always routes to the query's anchor shard — zero false negatives. `P(T) ⊇
    /// N(T)` means fan-out only ever widens, and only on alias-bearing titles; with
    /// no active multi-word alias `P(T) == N(T)` and this takes the single-view
    /// path, byte-identical to the pre-ADR-076 routing.
    pub(in crate::cluster::coordinator) fn route(
        layout: &Layout,
        title: &str,
    ) -> (Vec<usize>, usize) {
        let mut lc = String::new();
        let mut sc = crate::normalize::NormScratch::new();
        let mut feats: Vec<FeatureId> = Vec::new();
        if layout.norm.has_multiword_aliases() {
            let mut neg: Vec<FeatureId> = Vec::new();
            layout.norm.match_features_dual(
                title,
                &layout.dict,
                &mut lc,
                &mut sc,
                &mut neg,
                &mut feats,
            );
        } else {
            layout
                .norm
                .match_features(title, &layout.dict, &mut lc, &mut sc, &mut feats);
        }
        // Selective targets: the shard owning each anchor-eligible (non-hot) feature.
        let mut targets: Vec<usize> = Vec::with_capacity(feats.len() + 1);
        for &f in &feats {
            if !is_hot(&layout.dict, f) {
                targets.push(layout.ring.lookup(f));
            }
        }
        targets.sort_unstable();
        targets.dedup();
        // Broad-eval shard: the ONE shard that evaluates the replicated-to-all broad lane for
        // this title (ADR-080), picked by a stable title hash so no shard is a broad hotspot.
        // Free-ride an already-probed selective target when there is one (zero extra fan-out);
        // otherwise probe a single hashed shard (a title with no selective anchor — all-hot or
        // empty). Either way `broad_eval_shard ∈ targets`, so every title evaluates broad on a
        // shard it probes, and that shard holds the complete (replicated) broad lane.
        let h = crate::util::fnv1a64(title.as_bytes());
        let broad_eval_shard = if targets.is_empty() {
            let s = (h % layout.ring.num_shards() as u64) as usize;
            targets.push(s);
            s
        } else {
            targets[(h % targets.len() as u64) as usize]
        };
        (targets, broad_eval_shard)
    }

    /// Match one title against the cluster, using the cluster's default broad-lane
    /// setting. Returns matched logical ids (sorted, deduped).
    pub fn percolate(&self, title: &str) -> Result<Vec<u64>, ShardError> {
        let layout = &*self.layout();
        Ok(self
            .percolate_inner(layout, title, self.include_broad, &TagPredicate::empty())?
            .0)
    }

    /// [`Self::percolate`] plus merged [`MatchStats`] across the probed shards.
    pub fn percolate_with_stats(&self, title: &str) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let layout = &*self.layout();
        self.percolate_inner(layout, title, self.include_broad, &TagPredicate::empty())
    }

    /// Match one title with an explicit broad-lane toggle (overriding the cluster
    /// default) — used by the oracle to sweep broad on/off on one cluster.
    pub fn percolate_with_broad(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        let layout = &*self.layout();
        Ok(self
            .percolate_inner(layout, title, include_broad, &TagPredicate::empty())?
            .0)
    }

    /// Match one title narrowed by a tag filter (ADR-049/055): a conjunction of `(key, [values])`
    /// groups, compiled ONCE against the shared frozen tag space and fanned to every probed shard.
    /// Returns the matched logical ids that also satisfy the filter (sorted, deduped). An empty
    /// filter is byte-identical to [`Self::percolate`]. Mirrors the single-node
    /// `compile_tag_predicate` + `match_title_filtered` so cluster ≡ single-node under a filter.
    pub fn percolate_filtered(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
    ) -> Result<Vec<u64>, ShardError> {
        let layout = &*self.layout();
        let pred = self.compile_tag_predicate(filter);
        Ok(self
            .percolate_inner(layout, title, self.include_broad, &pred)?
            .0)
    }

    /// [`Self::percolate_filtered`] with an explicit broad-lane toggle — used by the oracle to sweep
    /// broad on/off under a filter on one cluster.
    pub fn percolate_filtered_with_broad(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        let layout = &*self.layout();
        let pred = self.compile_tag_predicate(filter);
        Ok(self.percolate_inner(layout, title, include_broad, &pred)?.0)
    }

    /// Compile a request filter — a conjunction of `(key, [values])` groups — into a
    /// [`TagPredicate`] against the coordinator's frozen tag space (ADR-049/055). Each value resolves
    /// via [`get_or_synthetic`](crate::tagdict::TagDict::get_or_synthetic), so a value never seen at
    /// ingest yields a `TagId` no stored query carries (matches nothing — the safe `terms`
    /// semantics), never an over-match. The same frozen tag space the shards resolved their stored
    /// tags against, so the integer groups are directly comparable across the cluster.
    pub fn compile_tag_predicate(&self, filter: &[(String, Vec<String>)]) -> TagPredicate {
        let groups = filter
            .iter()
            .map(|(key, values)| {
                values
                    .iter()
                    .map(|v| self.tag_dict.get_or_synthetic(key, v))
                    .collect()
            })
            .collect();
        TagPredicate::new(groups)
    }

    /// The unfenced percolate: one consistent view per call (ADR-185). A pass that
    /// overlapped an upsert moving a query between shards is discarded and repeated.
    fn percolate_inner(
        &self,
        layout: &Layout,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.move_fence
            .read(|pass| Self::percolate_pass(layout, title, include_broad, pred, pass))
    }

    fn percolate_pass(
        layout: &Layout,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        pass: &super::move_fence::ReadPass<'_>,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let (targets, broad_eval_shard) = Self::route(layout, title);
        let ownership = crate::ownership::OwnershipContext::new(
            layout.generation,
            layout.shards.len() as u32,
            targets.iter().map(|&position| position as u32).collect(),
            include_broad.then_some(broad_eval_shard as u32),
        )?;
        // The broad lane is replicated to every shard (ADR-080) but evaluated on exactly ONE
        // shard per title — its broad-eval shard — so a broad query is counted once; the other
        // probed shards run with broad off (they would re-scan the same replicated lane). A
        // failed shard probe propagates rather than being dropped: a silently missing shard
        // would shrink the union into a FALSE NEGATIVE.
        let parts: Vec<(Vec<u64>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| {
                    layout.shards[s].percolate_filtered_owned(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                        &ownership,
                        s as u32,
                    )
                })
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| {
                    layout.shards[s].percolate_filtered_owned(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                        &ownership,
                        s as u32,
                    )
                })
                .collect::<Result<_, _>>()?
        };

        let mut out = Vec::new();
        let mut stats = MatchStats::default();
        for (ids, st) in parts {
            out.extend_from_slice(&ids);
            stats.merge(st);
        }
        let shard_rows = out.len();
        out.sort_unstable();
        out.dedup();
        debug_assert!(
            shard_rows == out.len() || pass.overlapped_a_move(),
            "ADR-109 ownership-aware shard replies must not overlap"
        );
        stats.record_cross_source_duplicates(shard_rows, out.len());
        stats.matches = out.len() as u32;
        Ok((out, stats))
    }

    /// Compile a request `rank` block against the coordinator's frozen tag space
    /// (ADR-059/075) — the ranking analogue of [`Self::compile_tag_predicate`], with
    /// the same `get_or_synthetic` resolution the single-node
    /// `EngineSnapshot::compile_rank_spec` uses: a boost value never seen at ingest
    /// yields a `TagId` no stored query carries and simply never fires. The shards
    /// resolved their stored tags against this SAME shared dict, so the integer
    /// boost ids are directly comparable cluster-wide.
    pub fn compile_rank_spec(&self, spec: &crate::rank::RankSpec) -> crate::rank::CompiledRankSpec {
        let boosts = spec
            .boosts
            .iter()
            .map(|(key, value, weight)| (self.tag_dict.get_or_synthetic(key, value), *weight))
            .collect();
        crate::rank::CompiledRankSpec::new(spec.priority_key.clone(), boosts)
    }

    /// [`Self::percolate_filtered_with_stats`] plus a per-id ranking score (the
    /// cluster `rank` path, ADR-059/075). The spec is compiled ONCE here and fanned
    /// to every probed shard, which scores its own matched ids against its stored
    /// tag columns; the merge dedups by id — copies of one logical are
    /// version-identical across shards (identical op streams), so every shard
    /// reports the same score and dedup cannot lose information. Returns the scored
    /// set sorted by id (the same order the unranked merge returns); the caller owns
    /// the `(score desc, _id asc)` presentation order + `from`/`size`, exactly as
    /// with the single-node `EngineSnapshot::rank`. Ranking only reorders — the id
    /// set is identical to the unranked percolate (zero-FN trivially preserved).
    pub fn percolate_filtered_ranked(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
        rank: &crate::rank::RankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let layout = &*self.layout();
        let pred = self.compile_tag_predicate(filter);
        let spec = self.compile_rank_spec(rank);
        self.move_fence
            .read(|pass| Self::ranked_pass(layout, title, include_broad, &pred, &spec, pass))
    }

    fn ranked_pass(
        layout: &Layout,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        pass: &super::move_fence::ReadPass<'_>,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let (targets, broad_eval_shard) = Self::route(layout, title);
        let ownership = crate::ownership::OwnershipContext::new(
            layout.generation,
            layout.shards.len() as u32,
            targets.iter().map(|&position| position as u32).collect(),
            include_broad.then_some(broad_eval_shard as u32),
        )?;
        // Same fan-out + fail-loud shape as `percolate_inner` (a dropped shard probe
        // would shrink the union into a false negative); broad on the one broad-eval shard.
        let parts: Vec<(Vec<(u64, i64)>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| {
                    layout.shards[s].percolate_filtered_ranked_owned(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                        spec,
                        &ownership,
                        s as u32,
                    )
                })
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| {
                    layout.shards[s].percolate_filtered_ranked_owned(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                        spec,
                        &ownership,
                        s as u32,
                    )
                })
                .collect::<Result<_, _>>()?
        };

        let mut out: Vec<(u64, i64)> = Vec::new();
        let mut stats = MatchStats::default();
        for (scored, st) in parts {
            out.extend_from_slice(&scored);
            stats.merge(st);
        }
        let shard_rows = out.len();
        out.sort_unstable_by_key(|&(id, _)| id);
        out.dedup_by_key(|&mut (id, _)| id);
        debug_assert!(
            shard_rows == out.len() || pass.overlapped_a_move(),
            "ADR-109 ranked ownership-aware shard replies must not overlap"
        );
        stats.record_cross_source_duplicates(shard_rows, out.len());
        stats.matches = out.len() as u32;
        Ok((out, stats))
    }
}

mod inspect;
