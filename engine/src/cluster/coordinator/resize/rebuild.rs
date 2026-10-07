//! The shared rebuild core behind a resize and a vocabulary change (ADR-046, ADR-078): gather
//! the live corpus, re-extract and re-place every query, build the new shards beside the old
//! ones, and publish them as one layout (ADR-208, ADR-209).

use std::path::Path;
use std::sync::Arc;

use crate::cluster::coordinator::layout::{Layout, LayoutChange};
use crate::cluster::coordinator::{into_shard, replica_dir, shard_dir, ClusterEngine, Target};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::compile::{extract, extract_readonly, Extracted};
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::ownership::PlacementGeneration;
use crate::segment::PlacedQuery;
use crate::tagdict::TagId;
use crate::vocab::Vocab;

use super::visibility::{rebuild_placement_of, was_default_visible};

type RebuildExtractedQuery = (
    u64,
    Extracted,
    String,
    u32,
    u64,
    Vec<(String, String)>,
    Vec<TagId>,
    crate::rank::RankValues,
    // Whether a default read could return the row before this rebuild (ADR-203).
    bool,
);

impl ClusterEngine {
    /// The shared blue/green rebuild core (ADR-046/078): gather the deduped live corpus, obtain a
    /// dict (REUSE the frozen one when `new_norm` is the current normalizer — a resize — else
    /// re-mint it — a `set_vocab`), re-place every query under `new_ring`, build fresh shards, and
    /// atomically swap `norm`/`dict`/`ring`/`shards`. Does NOT checkpoint — the caller owns the
    /// durable commit (so it can interleave control-plane + orphan-cleanup steps first). Returns
    /// the number of live queries rebuilt.
    ///
    /// `new_vocab`: `Some(v)` (a [`set_vocab`](Self::set_vocab) call) installs `v` and uses its
    /// equivalence groups; `None` (a [`resize`](Self::resize) call) PRESERVES the existing
    /// the layout's vocabulary (its equivalences are already installed on the reused dict).
    pub(in crate::cluster::coordinator) fn rebuild_from_live(
        &self,
        change: &LayoutChange<'_>,
        new_norm: Arc<Normalizer>,
        new_ring: HashRing,
        new_vocab: Option<Vocab>,
        new_generation: PlacementGeneration,
    ) -> Result<(usize, Arc<Layout>), ShardError> {
        // Gather the deduped live `(logical, dsl, tag_ids)` set across shards. A selective /
        // any-of query lives on several shards but has ONE dsl (and one tag set — every
        // fanned-out copy carries the same tags) — dedup by logical id. Tags ride as stored
        // `TagId`s, NOT raw strings: the tag space is orthogonal to vocabulary AND to the ring,
        // preserved unchanged through the rebuild, so a stored id — interned dense or post-freeze
        // synthetic (which has no recoverable string) — stays valid and is carried verbatim to
        // the query's new shard (ADR-074). Untagged ⇒ every tag vec is empty ⇒ byte-identical to
        // the pre-tag rebuild.
        let live = Self::live_corpus_tagged(&change.current())?;
        self.rebuild_from_corpus(
            change,
            live,
            new_norm,
            new_ring,
            new_vocab,
            new_generation,
            false,
        )
    }

    /// Rebuild from an already-folded logical corpus. Recovery uses this seam to
    /// fold a legacy coordinator-log tail without first validating its stale
    /// placement decisions. `append_missing_features` is true for compiler-semantics
    /// migrations because splitting the legacy clause stream can expose feature
    /// names that the old frozen dictionary never interned. That path appends only:
    /// existing frequencies and top-64 mask bits stay frozen so a recovery-only
    /// compiler rewrite cannot change an unrelated query's visibility.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::cluster::coordinator) fn rebuild_from_corpus(
        &self,
        change: &LayoutChange<'_>,
        live: Vec<crate::cluster::shard::LiveTaggedQuery>,
        new_norm: Arc<Normalizer>,
        new_ring: HashRing,
        new_vocab: Option<Vocab>,
        new_generation: PlacementGeneration,
        append_missing_features: bool,
    ) -> Result<(usize, Arc<Layout>), ShardError> {
        let current = change.current();
        // Pass A — produce the (dict, extracted) the rebuild re-places. Two paths, keyed off
        // whether the NORMALIZER changed (an `Arc::ptr_eq` against the current one):
        //
        //  - **Normalizer unchanged (a resize):** the feature space cannot have changed, so REUSE
        //    the frozen dict verbatim — same dense ids, same hot-mask, same fingerprint. A resize
        //    is a ring change, NOT a model change: reusing the dict keeps the manifest's dict
        //    fingerprint invariant and the control-plane's `dict_fingerprint` valid (re-minting
        //    would renumber ids if the live corpus order differed from the original build, or if
        //    post-freeze terms were added — a spurious fingerprint change desyncing cluster
        //    state). `extract_readonly` resolves each query against it, auto-expanding installed
        //    equivalences (ADR-054) and resolving post-freeze terms to their stable synthetic ids
        //    (ADR-046) — so placement is exactly the live cluster's, just re-distributed.
        //  - **Compiler semantics changed, normalizer unchanged:** append newly exposed
        //    features to the frozen dict, preserving every existing frequency and mask bit.
        //    Re-ranking the top-64 mask could move an unrelated A query behind class C's
        //    `include_broad` boundary merely because deletes preceded the recovery.
        //  - **Normalizer changed (a `set_vocab`):** re-mint the dict over the live corpus under
        //    `new_norm` (interning + frequencies + hot-mask), exactly as `build`, then resolve +
        //    expand the new vocab's equivalence groups onto it.
        let mut lc = String::new();
        let mut extracted: Vec<RebuildExtractedQuery> = Vec::with_capacity(live.len());
        let same_normalizer = Arc::ptr_eq(&new_norm, &current.norm);
        let new_dict = if same_normalizer && !append_missing_features {
            let dict = Arc::clone(&current.dict);
            for (logical, text, version, source_generation, raw_tags, tag_ids, rank, placement) in
                live
            {
                let ast = crate::dsl::parse_for_recovery(&text).map_err(|error| {
                    ShardError::Config(format!("stored query {logical} cannot be rebuilt: {error}"))
                })?;
                let ex = extract_readonly(&ast, &new_norm, &dict, &mut lc);
                if let Some(width) = ex.column_overflow() {
                    return Err(ShardError::Config(format!(
                        "stored query {logical} exceeds the exact-store column limit \
                         ({width} features) during rebuild"
                    )));
                }
                extracted.push((
                    logical,
                    ex,
                    text,
                    version,
                    source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    was_default_visible(&placement),
                ));
            }
            dict
        } else if same_normalizer {
            // ADR-118 migration is a compiler rewrite, not a vocabulary change.
            // Discover component features exposed by splitting the legacy joint
            // stream in an append-only clone. Existing IDs, frequencies, and mask
            // bits are durability semantics: changing the top-64 membership can
            // change class C visibility. Newly appended features keep the counts
            // observed in this complete live corpus and receive no mask bit.
            let mut dict = current.dict.as_ref().clone();
            let old_len = dict.len();
            let old_freqs: Vec<u32> = (0..old_len)
                .map(|id| dict.freq(id as crate::dict::FeatureId))
                .collect();
            let old_masks: Vec<u8> = (0..old_len)
                .map(|id| dict.mask_bit(id as crate::dict::FeatureId))
                .collect();

            for row in &live {
                let logical = row.0;
                let text = &row.1;
                let ast = crate::dsl::parse_for_recovery(text).map_err(|error| {
                    ShardError::Config(format!("stored query {logical} cannot be rebuilt: {error}"))
                })?;
                let ex = extract(&ast, &new_norm, &mut dict, &mut lc);
                if let Some(width) = ex.column_overflow() {
                    return Err(ShardError::Config(format!(
                        "stored query {logical} exceeds the exact-store column limit \
                         ({width} features) during rebuild"
                    )));
                }
            }
            for id in 0..old_len {
                dict.set_freq_and_mask(id as crate::dict::FeatureId, old_freqs[id], old_masks[id]);
            }

            // Newly interned equivalence members must be resolved against their
            // dense IDs before the final read-only materialization pass.
            if let Some(vocab) = new_vocab.as_ref().or(current.vocab.as_deref()) {
                let equiv = vocab.resolve_equivalences(&new_norm, &dict);
                dict.set_equivalences(equiv);
            }

            lc.clear();
            for (logical, text, version, source_generation, raw_tags, tag_ids, rank, placement) in
                live
            {
                let ast = crate::dsl::parse_for_recovery(&text).map_err(|error| {
                    ShardError::Config(format!("stored query {logical} cannot be rebuilt: {error}"))
                })?;
                let ex = extract_readonly(&ast, &new_norm, &dict, &mut lc);
                if let Some(width) = ex.column_overflow() {
                    return Err(ShardError::Config(format!(
                        "stored query {logical} exceeds the exact-store column limit \
                         ({width} features) during rebuild"
                    )));
                }
                extracted.push((
                    logical,
                    ex,
                    text,
                    version,
                    source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    was_default_visible(&placement),
                ));
            }
            Arc::new(dict)
        } else {
            let mut dict = Dict::new();
            for (logical, text, version, source_generation, raw_tags, tag_ids, rank, placement) in
                live
            {
                let ast = crate::dsl::parse_for_recovery(&text).map_err(|error| {
                    ShardError::Config(format!("stored query {logical} cannot be rebuilt: {error}"))
                })?;
                let ex = extract(&ast, &new_norm, &mut dict, &mut lc);
                if let Some(width) = ex.column_overflow() {
                    return Err(ShardError::Config(format!(
                        "stored query {logical} exceeds the exact-store column limit \
                         ({width} features) during rebuild"
                    )));
                }
                extracted.push((
                    logical,
                    ex,
                    text,
                    version,
                    source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    was_default_visible(&placement),
                ));
            }
            dict.finalize_mask();
            // Resolve declared/learned equivalence groups (ADR-054) against the freshly-minted
            // dict and apply them via expansion: widen the already-extracted queries (so THIS
            // rebuild's re-placement + ingest use the FN-safe widened form — a query whose anchor
            // is now an any-of fans to every member's shard), then install the map on the dict so
            // future incremental adds expand through `extract`. The groups come from `new_vocab`
            // (set_vocab) or, when preserving, the EXISTING `current.vocab`. No groups ⇒ no-op.
            let equiv = new_vocab
                .as_ref()
                .or(current.vocab.as_deref())
                .map(|v| v.resolve_equivalences(&new_norm, &dict));
            if let Some(equiv) = equiv {
                for (_, ex, _, _, _, _, _, _, _) in &mut extracted {
                    ex.expand_equivalences(&equiv);
                }
                dict.set_equivalences(equiv);
            }
            Arc::new(dict)
        };
        let rebuilt = extracted.len();

        // Pass B — re-place each query under the NEW dict + NEW ring and bucket per shard. Tags
        // travel with the query (`tag_ids`, the ADR-074 carry-through): a different shard count
        // moves a query's anchor — hence its shard — and the filtered-read contract requires its
        // tags on whichever shard now holds it.
        let num_shards = new_ring.num_shards();
        let mut buckets: Vec<Vec<PlacedQuery>> = (0..num_shards).map(|_| Vec::new()).collect();
        let mut accepted_ids = Vec::new();
        for (logical, ex, text, version, source_generation, raw_tags, tag_ids, rank, was_visible) in
            extracted
        {
            // Re-placing ALREADY-STORED queries: a stored class-D was accepted when it was
            // added, so a rebuild (resize / set_vocab) must never drop it via the current knob
            // (mirrors the single-node ADR-068 vocab recompile, which passes accept=true
            // unconditionally). The empty-forbidden guard in `placement_of` still rejects the
            // never-stored empty query, so passing `true` cannot resurrect one.
            //
            // And a row that default reads could return must stay where they can (ADR-203):
            // `rebuild_placement_of` keeps it always-visible when today's plan would put it
            // in the opt-in broad lane.
            let target = rebuild_placement_of(
                &new_dict,
                &new_ring,
                &ex,
                self.per_shard.hot_anchor_threshold,
                was_visible,
            );
            let placement = target.placement(new_generation, num_shards as u32)?;
            if !matches!(&target, Target::Reject) {
                accepted_ids.push(logical);
            }
            match target {
                Target::Reject => {}
                Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                    // The broad lane is replicated to every shard (ADR-080). Carry the stored
                    // version through the rebuild so a re-placed query keeps version N rather
                    // than being reset to 1 (the version-preserving rebuild, ADR-074).
                    for bucket in &mut buckets {
                        bucket.push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version,
                            source_generation: Some(source_generation),
                            tags: raw_tags.clone(),
                            tag_ids: tag_ids.clone(),
                            rank,
                            placement: placement.clone(),
                        });
                    }
                }
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version,
                            source_generation: Some(source_generation),
                            tags: raw_tags.clone(),
                            tag_ids: tag_ids.clone(),
                            rank,
                            placement: placement.clone(),
                        });
                    }
                }
            }
        }

        // Construct fresh shards sharing the new norm + rebuilt dict + unchanged tag space,
        // `replication_factor` copies per position, ingesting each bucket into EVERY copy
        // (identical op stream ⇒ copies set-equal, as in `build`). Two cases by position:
        //
        //  - EXISTING position (`s < old_num_shards`): rebuild in the SAME shard dir, numbering
        //    green segments ABOVE the old ones (the set_vocab coexist path), so the new `.seg`
        //    coexist with the still-committed old ones until the manifest commit — a crash before
        //    the commit leaves the old manifest + old segments authoritative.
        //  - NEW position (`s ≥ old_num_shards`, grow only): no old shard to coexist with.
        //    FORCE-CLEAN the dir first so a stale orphan from a PRIOR shrink can't resurrect data
        //    (its checkpoint sidecar would self-restart `new_durable` into an old corpus, or its
        //    `sources.dat` would shadow the green ingest), then build a fresh durable shard —
        //    exactly `build`'s path. The post-commit `remove_orphan_shard_dirs` keeps the
        //    invariant "a resize commit leaves exactly shard_000..shard_{K′-1} on disk".
        let old_num_shards = current.shards.len();
        let rf = self.replication_factor.max(1);
        let data_dir = self.data_dir.clone();
        let green_source_file = format!("sources_g{:020}.dat", new_generation.0);
        // The rebuild re-places ALREADY-STORED queries, so stored class-D must survive regardless
        // of the current front-door knob: `placement_of(.., true)` above buckets it, and the shards
        // are coordinator-gated storage that always accept (forced in `LocalShard`), so the fresh
        // shards re-ingest it. NEW class-D adds stay gated at the coordinator by the unchanged
        // `self.per_shard.accept_class_d`.
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for (s, bucket) in buckets.into_iter().enumerate() {
            let mut copies = Vec::with_capacity(rf);
            for r in 0..rf {
                let copy = match &data_dir {
                    Some(dir) => {
                        let mut sc = self.per_shard.clone();
                        let cdir = if r == 0 {
                            shard_dir(dir, s)
                        } else {
                            replica_dir(dir, s, r)
                        };
                        sc.data_dir = Some(cdir.clone());
                        // A failed prior attempt at this generation may have
                        // left an uncommitted green sidecar. The old manifest
                        // never selected it, so remove it before rebuilding the
                        // complete corpus rather than merging stale overlay
                        // records into this attempt.
                        let green_source_path = cdir.join(&green_source_file);
                        match std::fs::remove_file(&green_source_path) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => {
                                return Err(ShardError::Log(format!(
                                    "clearing uncommitted source sidecar {}: {e}",
                                    green_source_path.display()
                                )));
                            }
                        }
                        if s < old_num_shards {
                            // Existing position: coexist green segments above the old ones.
                            let next_seg = current.shards[s].next_seg_id()?;
                            LocalShard::open_segments_with_source_file(
                                Arc::clone(&new_norm),
                                Arc::clone(&new_dict),
                                Arc::clone(&self.tag_dict),
                                sc,
                                &[],
                                next_seg,
                                &green_source_file,
                            )?
                        } else {
                            // New position (grow): clean any stale dir, then attach
                            // an empty green base with the generation-selected
                            // source sidecar.
                            clean_shard_dir(&cdir)?;
                            LocalShard::open_segments_with_source_file(
                                Arc::clone(&new_norm),
                                Arc::clone(&new_dict),
                                Arc::clone(&self.tag_dict),
                                sc,
                                &[],
                                1,
                                &green_source_file,
                            )?
                        }
                    }
                    None => LocalShard::new(
                        Arc::clone(&new_norm),
                        Arc::clone(&new_dict),
                        Arc::clone(&self.tag_dict),
                        self.per_shard.clone(),
                    ),
                };
                if !bucket.is_empty() {
                    copy.ingest_local(&bucket);
                }
                copies.push(copy);
            }
            let shard = into_shard(copies)?;
            shard.validate_ownership(s as u32, new_generation, num_shards as u32)?;
            shards.push(shard);
        }

        // The directory mirrors the rebuilt corpus exactly, like reopen's
        // live-enumeration seeding: a query whose re-extraction under the new
        // vocab flips to `Target::Reject` is dropped from every new shard, so
        // keeping its reservation would 409 a re-add on the LIVE coordinator
        // while a REOPENED one accepts it (review finding). The write fence the
        // change holds makes this race-free with every per-ID writer.
        // One swap: no read observes a half-state. The normalizer is `new_norm` (the same
        // instance on a resize). The vocabulary is replaced only when a new one was supplied
        // (`set_vocab`); a resize passes `None` and keeps it. The logical-id directory changes
        // with the layout.
        let next = change.publish(
            Layout {
                norm: new_norm,
                dict: new_dict,
                vocab: new_vocab.map(Arc::new).or_else(|| current.vocab.clone()),
                ring: new_ring,
                shards: Arc::new(shards),
                source_files: vec![green_source_file; num_shards],
                #[cfg(feature = "distributed")]
                handoffs: current.handoffs.clone(),
                generation: new_generation,
            },
            || self.replace_logical_ids(accepted_ids),
        )?;
        Ok((rebuilt, next))
    }
}

/// Remove a shard directory and all its contents, treating "not found" as success. Used to
/// guarantee a NEW position (grow) builds over a verified-clean dir — never self-restarting
/// `LocalShard::new_durable` from a leftover checkpoint sidecar / `sources.dat`.
fn clean_shard_dir(dir: &Path) -> Result<(), ShardError> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(ShardError::Log(format!(
            "cleaning new shard dir {} before a grow: {e}",
            dir.display()
        ))),
    }
}
