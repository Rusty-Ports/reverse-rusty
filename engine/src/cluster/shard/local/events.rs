use super::{
    translog, DurabilityOp, Engine, EngineEvent, Instant, LocalShard, LogPos, PoisonError,
    ShardError,
};

impl LocalShard {
    /// Commit the already-durable base received by peer recovery before publishing the shard.
    /// The source's watermark belongs to a different log: attachment starts this local log at
    /// zero. This writes only the selector, without rewriting the source corpus just received.
    #[cfg(feature = "distributed")]
    pub(crate) fn commit_recovered_checkpoint(&self) -> Result<(), ShardError> {
        let eng = self.lock();
        let p = self.translog.last_pos()?;
        if p != LogPos(0) {
            return Err(ShardError::Log(
                "recovery checkpoint requires an unpublished shard with a fresh translog".into(),
            ));
        }
        self.write_checkpoint(&eng, p)
    }

    fn write_checkpoint(&self, eng: &Engine, p: LogPos) -> Result<(), ShardError> {
        if let Some(dir) = &self.data_dir {
            let segment_files = eng.segment_filenames().map_err(|e| {
                ShardError::Log(format!("collecting segment filenames for checkpoint: {e}"))
            })?;
            translog::write_sidecar(
                dir,
                &translog::ShardCheckpoint {
                    next_seg_id: eng.next_seg_id(),
                    local_checkpoint: p.0,
                    dict_fingerprint: self.dict.fingerprint(),
                    segment_files,
                    compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                    source_file_name: eng.source_file_name().to_string(),
                },
            )?;
        }
        Ok(())
    }

    /// Make this shard's checkpoint file its commit record (ADR-214): a shard on a shard
    /// node, which restarts from that file. Called by the node when it takes the shard into
    /// a slot. From then on, when the shard replaces a segment file, it reads the checkpoint
    /// file from disk: a file it does not name is removed at once, and one it names is kept
    /// for [`Self::remove_replaced_files`].
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn own_the_commit_record(&self) {
        let dir = self.data_dir.clone();
        self.lock()
            .set_replaced_files(crate::segment::ReplacedFiles::KeptWhileItsRecordNamesThem(
                Box::new(move || {
                    let checkpoint = translog::read_sidecar(dir.as_deref()?).ok()??;
                    Some(checkpoint.segment_files)
                }),
            ));
    }

    /// Remove the segment files this shard replaced and kept, now that its checkpoint file
    /// no longer names them. The checkpoint file is read from disk here; a file it still
    /// names stays.
    ///
    /// Only for a caller that holds the node's installation barrier on the slot's installed
    /// shard (ADR-214). A kept file is removed by name, and a recovery into the slot writes
    /// received files into the same directory, under names that can be the same. The barrier
    /// is what keeps the two apart, and a shard that has been replaced is never reached
    /// through it.
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn remove_replaced_files(&self) {
        self.lock().remove_kept_files_the_record_no_longer_names();
    }

    #[cfg(all(test, feature = "distributed"))]
    pub(crate) fn owns_its_commit_record(&self) -> bool {
        self.lock().keeps_what_its_record_names()
    }

    /// Deliver a degraded-path event to the installed sink, if any (best-effort: dropped when no
    /// observer is attached — the default, byte-identical path). Library code never writes stderr
    /// (ADR-021); the observer turns this into logs + metrics.
    pub(super) fn emit(&self, ev: &EngineEvent) {
        let sink = self
            .event_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(sink) = sink {
            sink(ev);
        }
    }

    /// The clock-injectable core of [`Shard::seal_for_checkpoint`]: flush + reseal + publish, write
    /// the durable sidecar, reap any stuck retention lease as of `now` (ADR-048), then trim the
    /// translog to the retention floor (ADR-040). `now` is the wall clock the lease-TTL reap
    /// measures against; the trait method passes `Instant::now()`, while a test passes a synthetic
    /// instant to drive expiry deterministically (no sleeps). Visible within the cluster module for
    /// that test; production code always reaches it through the trait method.
    pub(in crate::cluster) fn seal_for_checkpoint_at(
        &self,
        now: Instant,
    ) -> Result<LogPos, ShardError> {
        let mut eng = self.lock();
        // Seal the memtable into a base segment; ALSO persists `sources.dat` when the
        // memtable is empty (a plain `flush` would early-return past its sources save),
        // so the on-disk source store mirrors the live set as of `p` — otherwise a
        // reopen's `live_sources` omits bulk-loaded ids / resurrects tombstone-deleted
        // ones into the vocabulary rebuild (ADR-074).
        eng.flush_and_persist_sources_for_checkpoint();
        eng.reseal_tombstoned_segments(); // bake base-segment tombstones onto disk
                                          // Fail closed (ADR-051): if the flush / reseal / sources write could not durably
                                          // persist, the on-disk state does NOT yet reflect every flushed write / applied
                                          // delete (a failed reseal keeps the original, un-baked segment). Bail BEFORE
                                          // reading `p` and trimming the translog, so its tail still carries those ops for
                                          // the next recovery — advancing the checkpoint now would let a delete resurrect
                                          // (false positive) or a write vanish on reopen. The caller treats this as a
                                          // transient failed checkpoint; the data is safe in the translog.
        if !eng.persistence_healthy {
            return Err(ShardError::Log(
                "checkpoint aborted: flush/reseal could not durably persist; translog left intact \
                 so the un-sealed tail replays on recovery"
                    .into(),
            ));
        }
        Self::publish(&eng, &self.snapshot);
        // Everything ≤ `p` is now durably in the sealed/resealed segments; trim the translog
        // to it so its remaining tail is exactly the un-sealed ops > `p` (ADR-039). Held under
        // the engine lock, so no concurrent write advances `last_pos` between flush and read.
        let p = self.translog.last_pos()?;
        // A durable shard records a checkpoint sidecar so the data node can self-recover after a
        // crash (ADR-039 §6): write it AFTER the segments are durable and BEFORE trimming the
        // translog, so a crash in between just replays an already-captured (position-filtered)
        // prefix — never a loss, never a double-apply.
        self.write_checkpoint(&eng, p)?;
        // Reap any stuck retention lease (ADR-048) before reading the floor: a lease that has not
        // heartbeated within the TTL belongs to a crashed/stalled recovery and must no longer pin
        // the tail (`renew` is the heartbeat, so a live recovery is never reaped). Disabled
        // (`None`) ⇒ no reap ⇒ byte-identical to ADR-040.
        //
        // Then trim the translog only to the retention floor (ADR-040): a live, heartbeating lease
        // keeps the tail a recovery still needs even though we seal here. With no lease the floor
        // is absent and this is `p` — byte-identical to ADR-039. The segments still capture every
        // op ≤ `p` (the sidecar's `local_checkpoint` is `p`); any retained ops in `(trim_to, p]`
        // are redundant with the segments and position-filtered out on replay (replay is from `p`).
        let (trim_to, reaped) = {
            let mut r = self
                .retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let reaped = match self.retention_lease_ttl {
                Some(ttl) => r.reap_expired(now, ttl),
                None => 0,
            };
            (r.floor().map_or(p.0, |f| p.0.min(f)), reaped)
        };
        self.translog.checkpoint(LogPos(trim_to))?;
        // Release the engine lock before emitting so a slow sink can't block other writers (the
        // emit also takes the separate event-sink lock; ordering it after the drop avoids any
        // lock-order question with the engine→retention path above).
        drop(eng);
        if reaped > 0 {
            // A reap means a recovery was abandoned — surface it (ADR-021/048) rather than
            // silently reclaiming its tail. `ReplicaDesync` (benign housekeeping ⇒ warn) is the
            // same op the handoff lease-release failure uses.
            self.emit(&EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: "expired stuck peer-recovery retention lease(s) past the TTL; a crashed \
                         or stalled recovery's translog tail is now reclaimable"
                    .into(),
                error: format!("{reaped} lease(s) reaped"),
            });
        }
        Ok(p)
    }
}
