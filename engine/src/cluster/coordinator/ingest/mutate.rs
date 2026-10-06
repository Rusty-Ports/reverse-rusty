use super::repair::Redrive;
use super::{
    extract_readonly, planned, AddOutcome, ClusterEngine, ClusterMutation, DurabilityOp,
    EngineEvent, Extracted, ShardError, Target,
};

impl ClusterEngine {
    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict: vocabulary not seen at
    /// [`Self::build`] time is **absorbed** into the reserved synthetic-ID range (a
    /// deterministic hash, ADR-046), not dropped — so a required term new to the dict
    /// still anchors its query (a hash collision is a bounded over-match the exact
    /// matcher rejects, never a dropped required term).
    ///
    /// WAL-first: an ACCEPTED mutation is durably logged BEFORE it is applied to any shard, so a
    /// crash can never leave an acknowledged add that [`Self::open`] would lose. A log append
    /// failure rejects the add (shards untouched) and surfaces a
    /// [`DurabilityFailure`](EngineEvent::DurabilityFailure) — the cluster analogue of the
    /// engine's WAL-first write path (ADR-013). A REJECTED write (class D with the lane off, an
    /// empty query, or a parse error) is classified out BEFORE the log, so the log holds only
    /// accepted mutations and replay is configuration-independent (codex review).
    pub fn add_query(&self, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
        self.add_query_with_tags(id, dsl, &[])
    }

    /// [`add_query`](Self::add_query) carrying per-query metadata tags (ADR-049/055). The raw tags
    /// ride the cluster log alongside the DSL (logged BEFORE apply, like the DSL), and are resolved
    /// read-only against the shared frozen tag space on each target shard, so a tagged add and a
    /// later filtered percolate agree on the tag's `TagId`. Empty tags ⇒ byte-identical to
    /// [`add_query`](Self::add_query).
    pub fn add_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        tags: &[(String, String)],
    ) -> Result<AddOutcome, ShardError> {
        self.create_query_with_tags(id, dsl, 1, tags)
    }

    /// Atomically create one query only when `id` is absent. This is the
    /// cluster-core operation behind REST `op_type=create`: the logical-ID
    /// lock makes the absence check + reservation indivisible from every
    /// add/upsert/remove of the same id, and a conflict writes no log frame.
    ///
    /// Unlike [`add_query_with_tags`](Self::add_query_with_tags), the caller's
    /// display `version` is preserved in the coordinator log and every shard,
    /// matching the versioned REST upsert path.
    pub fn create_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<AddOutcome, ShardError> {
        // Check conflicts before compilation, matching the single-node REST
        // boundary: an already-live id is the decisive create-only error even
        // when the replacement body would fail DSL compilation. The ID lock is
        // load-bearing here. The directory also contains provisional reservations
        // while their coordinator-log append is in flight; an unlocked read could
        // report a false conflict if that append subsequently failed and rolled the
        // reservation back. Waiting on the ID lock observes the committed/rolled-back
        // result. This is still only an early conflict return, never an absence
        // proof — the second check below closes a create arriving during compilation.
        if self.logical_ids_authoritative() {
            // Preserve the global mutation lock order: PIT barrier, then logical
            // ID lock. Resync/exhaustive mutation code relies on this order.
            let _pit_barrier = self
                .pit_open_barrier
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let _logical_guard = self.logical_write_guard(id);
            if let Some(conflict) = self.create_conflict(id) {
                return Err(conflict);
            }
        }
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay).
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        // Reject an over-large tag set BEFORE the log too: it would truncate the u16 tag
        // column on apply and silently drop a real tag. Like a parse error, it carries no
        // replayable mutation (cluster analogue of the single-node front-door gate).
        if let Err(e) = self.check_tag_limit(tags) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        // Classify BEFORE logging (against the CURRENT knob): a REJECTED write — class D with the
        // lane off, or an effectively-empty query — carries no replayable mutation and must NEVER
        // reach the log. Else, replaying it under a since-flipped knob would resurrect a query the
        // caller was told was rejected (codex review). This is the cluster analogue of the
        // single-node "the WAL records only accepted mutations" (ADR-068); the apply/replay funnel
        // then forces accept=true, so replay reproduces the writer's decision regardless of config.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Reject a column-overflowing compiled query before the log too: it would
        // truncate the shards' u16 exact-store counts on apply (a false negative).
        if let Err(e) = Self::check_column_limit(&ex) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        let target = self.placement(&ex);
        if matches!(target, Target::Reject) {
            return Ok(AddOutcome::RejectedClassD);
        }
        let placement = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        self.ensure_serving_layout_committed()?;
        // Global lock order is PIT/mutation barrier -> logical-ID lock. Resync
        // uses the same order; taking the ID lock first can deadlock behind a
        // queued exhaustive writer on writer-preferring RwLock implementations.
        // Hold the barrier through the durable append and complete shard fan-out.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_resize_write_fence_open()?;
        // ADR-110's bounded merge requires one live distributed row per logical id.
        // Content-derived placement cannot guarantee a common owner for two different
        // rows sharing an id, so cluster adds are insert-only; replacements use upsert.
        // The ID lock closes the same-id check/reservation race without serializing
        // unrelated writes.
        let _logical_guard = self.logical_write_guard(id);
        // A coordinator attached to an already-populated cluster it could not
        // enumerate successfully has an unauthoritative directory, so
        // the duplicate check below would be vacuous — fail closed instead of
        // silently admitting a second physical row for a live id (review finding).
        // `upsert_query` stays available: it re-drives replace-by-id on every
        // shard and does not depend on the directory.
        if !self.logical_ids_authoritative() {
            return Err(ShardError::Config(
                "insert-only add_query requires the logical-id directory, which this \
                 coordinator could not seed from its (already-populated) remote shards; \
                 use upsert_query"
                    .to_string(),
            ));
        }
        if let Some(conflict) = self.create_conflict(id) {
            return Err(conflict);
        }
        let inserted = self.insert_logical_id(id);
        debug_assert!(inserted);
        let m = ClusterMutation::Add {
            logical: id,
            version,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
            placement: placement.clone(),
        };
        if let Err(e) = self.log.append(&m) {
            self.remove_logical_id(id);
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster add_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_add(id, version, dsl, tags, &placement)
    }

    /// Why a create-only write of `id` is refused, or `None` when the id is free. The caller
    /// holds the mutation barrier and `id`'s lock.
    ///
    /// A reserved id whose last write is still queued for repair is not simply "already there":
    /// some shard does not hold that write, and whoever sent it was told it failed (ADR-194).
    /// The repair is re-driven first, so a retried create converges its own earlier attempt, and
    /// a create after a half-applied delete finishes the delete and then finds the id free.
    /// While a shard still refuses, the answer says that the EARLIER write is unconverged and
    /// that this create was neither applied nor queued, so nobody takes a later resync of the
    /// earlier write for this one.
    fn create_conflict(&self, id: u64) -> Option<ShardError> {
        if !self.contains_logical_id(id) {
            return None;
        }
        // A re-drive is a shard write like any other, and a resize copy refuses those.
        if let Err(fenced) = self.ensure_resize_write_fence_open() {
            return Some(fenced);
        }
        if let Redrive::StillPending { pending, detail } = self.redrive_pending(id) {
            return Some(ShardError::EarlierWriteUnconverged {
                logical: id,
                pending,
                detail,
            });
        }
        self.contains_logical_id(id)
            .then_some(ShardError::DuplicateLogicalId(id))
    }

    /// Atomically replace a query by logical id — ES `index` semantics at the cluster
    /// (ADR-070, the coordinator analogue of the engine's ADR-067 upsert): every prior
    /// live copy is tombstoned and the new version inserted under ONE log frame
    /// ([`ClusterMutation::Upsert`]), so a crash replays the whole replacement or none
    /// of it — never a remove that lost its re-add. Returns the number of prior entries
    /// removed (0 ⇒ created, >0 ⇒ updated) plus where the new version landed. A
    /// rejected new version (parse / class D) **never deletes** — the prior version
    /// stays live and matchable. `version` is the caller-supplied per-logical version
    /// (default 1 from the REST layer); it rides the log frame so replay reproduces the
    /// stored version — passing 1 keeps the in-process / RF=1 path byte-identical.
    pub fn upsert_query(
        &self,
        id: u64,
        dsl: &str,
        version: u32,
    ) -> Result<(usize, AddOutcome), ShardError> {
        self.upsert_query_with_tags(id, dsl, version, &[])
    }

    /// [`upsert_query`](Self::upsert_query) carrying per-query metadata tags for the NEW
    /// version (ADR-055 semantics: raw tags ride the log frame and resolve read-only
    /// against the shared frozen tag space on each target shard). `version` is threaded
    /// into [`ClusterMutation::Upsert`] so a `PUT /_doc/{id} {"version":N}` stores version
    /// N and reopens to N (matching single-node `try_upsert_live_with_tags`).
    pub fn upsert_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<(usize, AddOutcome), ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay) — and a failed
        // replace never deletes.
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok((0, AddOutcome::RejectedParse(e))),
        };
        // Reject an over-large tag set BEFORE the log (and before any tombstone): it would
        // truncate the u16 tag column on apply. A failed replace never deletes, so this
        // returns 0 replaced — the prior version stays live.
        if let Err(e) = self.check_tag_limit(tags) {
            return Ok((0, AddOutcome::RejectedParse(e)));
        }
        // Classify BEFORE logging (current knob): a rejected new version carries no replayable
        // mutation AND must not delete the prior version, so it never reaches the log or the
        // tombstone pass. Same config-independent-replay discipline as add (codex review): the
        // log holds only accepted mutations, and apply/replay forces accept=true.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Reject a column-overflowing compiled query before the log (and before any
        // tombstone): it would truncate the shards' u16 exact-store counts on apply.
        // A failed replace never deletes, so the prior version stays live (0 replaced).
        if let Err(e) = Self::check_column_limit(&ex) {
            return Ok((0, AddOutcome::RejectedParse(e)));
        }
        let target = self.placement(&ex);
        if matches!(target, Target::Reject) {
            return Ok((0, AddOutcome::RejectedClassD));
        }
        let placement = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        self.ensure_serving_layout_committed()?;
        // Keep the same barrier -> logical-ID order as add/remove/resync.
        // The barrier spans the log append and the whole shard fan-out.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_resize_write_fence_open()?;
        // Serialize against an insert-only add/remove for the same id. An upsert
        // keeps the id present; a fresh upsert reserves it before the log append so
        // a concurrent add cannot create a second physical row.
        let _logical_guard = self.logical_write_guard(id);
        let fresh_id = self.insert_logical_id(id);
        let m = ClusterMutation::Upsert {
            logical: id,
            version,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
            placement: placement.clone(),
        };
        if let Err(e) = self.log.append(&m) {
            if fresh_id {
                self.remove_logical_id(id);
            }
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster upsert_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_upsert(id, version, dsl, tags, &placement, fresh_id)
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    /// WAL-first, like [`Self::add_query`].
    pub fn remove_query(&self, id: u64) -> Result<usize, ShardError> {
        // Canonical barrier -> logical-ID order; see add/upsert. Keeping
        // this guard through append + fan-out excludes torn exhaustive/PIT views.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_resize_write_fence_open()?;
        let _logical_guard = self.logical_write_guard(id);
        let m = ClusterMutation::Remove { logical: id };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster remove_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        let removed = self.apply_remove(id);
        // A partially-applied remove keeps the id reserved: allowing a fresh Add
        // before repair could coexist with an old row on the failed shard. Upsert
        // remains available because it re-drives delete+insert on every shard.
        if removed.is_ok() {
            self.remove_logical_id(id);
        }
        removed
    }

    /// Insert a compiled query on a set of target shards, collecting partial-apply failures
    /// (ADR-047): try EVERY shard rather than bailing on the first error, so a mid-fan-out
    /// remote failure is queued for repair (keyed by logical id; the re-drive replaces the id on
    /// the failed shards, since one of them may have applied the insert before it errored)
    /// instead of leaving a silent partial mutation. In-process inserts are
    /// infallible ⇒ `failed` stays empty ⇒ byte-identical to a plain loop. On any failure it
    /// queues the repair, emits, and returns the honest error; otherwise it returns `success`.
    /// Shared by the `Selective` (its placement shards) and `Replicated` (every shard, ADR-080)
    /// arms of [`Self::apply_add`].
    #[allow(clippy::too_many_arguments)]
    fn insert_on_shards(
        &self,
        shards: &[usize],
        ex: &Extracted,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
        success: AddOutcome,
    ) -> Result<AddOutcome, ShardError> {
        let mut applied = Vec::with_capacity(shards.len());
        let mut failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for &s in shards {
            match self.shards[s]
                .insert_extracted_with_placement(ex, id, version, dsl, tags, placement)
            {
                Ok(_) => applied.push(s),
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !failed.is_empty() {
            return Err(self.note_partial(
                ClusterMutation::Add {
                    logical: id,
                    version,
                    dsl: dsl.to_string(),
                    tags: tags.to_vec(),
                    placement: placement.clone(),
                },
                id,
                applied,
                failed,
                first_err,
            ));
        }
        Ok(success)
    }

    /// Apply an ADD to the shards — the state-machine `apply` for adds, shared by the live
    /// write path ([`Self::add_query`], after logging) and log replay ([`Self::open`]).
    /// Re-deriving placement here from the frozen dict makes live and replayed application
    /// byte-identical.
    pub(super) fn apply_add(
        &self,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<AddOutcome, ShardError> {
        // Latch tags_present (ADR-055, `/_stats` introspection) — covers both the live add
        // (`add_query_with_tags`) and a tagged log-tail entry replayed on `open`.
        self.note_tags(tags);
        // The front door validated this row before appending it. Apply/replay
        // uses only durable structural ceilings so a later policy/default
        // tightening cannot discard an acknowledged mutation.
        let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
            ShardError::Log(format!(
                "parsing acknowledged cluster add during apply: {error}"
            ))
        })?;
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Force accept=true (same only-accepted-writes invariant as apply_upsert): apply/replay
        // reproduces the writer's decision regardless of the current knob, so a knob flip on
        // reopen cannot drop or resurrect a class-D write (codex review). Rejected writes never
        // reach the log (classified out in add_query), so the Reject arm is defensive.
        let (target, class) = planned(
            &self.dict,
            &self.ring,
            &ex,
            true,
            self.per_shard.hot_anchor_threshold,
        );
        let expected = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        if &expected != placement {
            return Err(crate::ownership::OwnershipError::PlacementDecisionMismatch.into());
        }
        let outcome = match target {
            // Defensive: an effectively-empty query is rejected before logging, so a logged
            // mutation never lands here; a replayed no-op (stored nowhere) is still safe.
            Target::Reject => return Ok(AddOutcome::RejectedClassD),
            // The broad lane (class C / B arity-2 / accepted D): replicated to EVERY shard
            // (ADR-080). Same fail-collect fan-out as Selective, so a mid-fan-out remote failure
            // is queued for repair rather than a silent partial. In-process inserts are infallible
            // ⇒ the outcome is byte-identical save that the entry now lands on every shard.
            Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                let all: Vec<usize> = (0..self.shards.len()).collect();
                self.insert_on_shards(
                    &all,
                    &ex,
                    id,
                    version,
                    dsl,
                    tags,
                    placement,
                    AddOutcome::Replicated { class },
                )?
            }
            Target::Selective(shards) => self.insert_on_shards(
                &shards,
                &ex,
                id,
                version,
                dsl,
                tags,
                placement,
                AddOutcome::Placed {
                    shards: shards.clone(),
                    class,
                },
            )?,
        };
        // A successful full apply supersedes any stale partial-apply queued for this id, so
        // `resync` never re-drives an outdated mutation. Cheap no-op on the default path.
        self.clear_pending(id);
        Ok(outcome)
    }

    /// Apply a REMOVE to the shards — the state-machine `apply` for removes. The shard
    /// memtable/segment liveness is the authority; there is no separate coordinator live
    /// set to keep in sync (the durable base is the per-shard segments — ADR-032).
    pub(super) fn apply_remove(&self, id: u64) -> Result<usize, ShardError> {
        // Remove fans the idempotent delete out to EVERY shard. Try them all (don't bail on the
        // first error) and collect failures, so a partial remove is repairable rather than a
        // silent half-delete (ADR-047). In-process deletes are infallible ⇒ `failed` stays empty
        // ⇒ byte-identical to the old `.sum()`.
        let mut removed = 0usize;
        let mut failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for (s, shard) in self.shards.iter().enumerate() {
            match shard.delete_by_logical_id(id) {
                Ok(n) => removed += n,
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !failed.is_empty() {
            let applied: Vec<usize> = (0..self.shards.len())
                .filter(|s| !failed.contains(s))
                .collect();
            return Err(self.note_partial(
                ClusterMutation::Remove { logical: id },
                id,
                applied,
                failed,
                first_err,
            ));
        }
        // A successful full delete supersedes any queued partial Add/Remove for this id.
        self.clear_pending(id);
        Ok(removed)
    }
}
