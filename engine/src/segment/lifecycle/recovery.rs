//! `impl Engine` — crash recovery / reopen: [`open`](Engine::open) (manifest +
//! mmap'd segments + WAL replay, skip-corrupt-and-degrade) and
//! [`open_shared_segments`](Engine::open_shared_segments) (the cluster-shard
//! attach-an-explicit-file-list path, fail-loud). The construction builders live
//! in [`construct`](super::construct).

use crate::segment::{fresh_segment_generations, BaseSegment, Engine, Segment, SourceCommitState};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::{MmapSegment, SourceStore};
use crate::tagdict::TagDict;
use crate::wal::{Wal, WalEntry};

/// Map a [`NormalizerError`](crate::error::NormalizerError) into the `io::Result` space of
/// the open path.
fn invalid_input(e: &crate::error::NormalizerError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
}

fn seed_next_source_generation(
    segments: &[Arc<BaseSegment>],
    query_store: &SourceStore,
) -> std::io::Result<u64> {
    let exact_max = segments
        .iter()
        .map(|segment| segment.max_source_generation())
        .max()
        .unwrap_or(0);
    exact_max
        .max(query_store.max_source_generation())
        .checked_add(1)
        .filter(|&generation| generation != 0)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source generation space exhausted",
            )
        })
}

/// Replay the WAL tail (entries after the last flush checkpoint) into a constructed
/// engine — the ONE recovery loop, shared by the manifest path and the fresh
/// (no-manifest-yet) path so ADR-013's contract ("every acknowledged mutation
/// recovers") holds on both. `watermark` is the manifest's `wal_seq_watermark`
/// (ADR-066) — 0 on the fresh path, where nothing is baked anywhere.
///
/// Two positions divide the log (ADR-223). Records at or below the one the manifest sealed
/// through are in its segments, effects and all, and are skipped whole. The records above
/// it are what the memtable held, or acted on it, and all of them are replayed in order,
/// so the memtable comes back row for row, positions included. Among those, a record at or
/// below `watermark` has its effect on the SEGMENTS in the commit already (the tombstone
/// bitmaps), so only its effect on the memtable is replayed.
///
/// A manifest written before ADR-223 does not say how far it sealed. For that one, an
/// insert or an upsert at or below the watermark is skipped when a segment or the memtable
/// already holds its row, which is the rule this replaced.
fn replay_wal_tail(
    engine: &mut Engine,
    wal_path: &std::path::Path,
    watermark: u64,
) -> std::io::Result<()> {
    let recovery = Wal::recover(wal_path)?;
    let repaired_bytes = engine.wal.as_mut().map_or(0, Wal::take_repaired_tail_bytes);
    let skipped_bytes = recovery.skipped_bytes + repaired_bytes;
    if skipped_bytes > 0 {
        engine
            .pending_events
            .push(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::WalTornTail,
                detail: "WAL recovery repaired an incomplete final write".to_string(),
                error: format!("{skipped_bytes} bytes"),
            });
    }
    let sealed_through = engine.sealed_through;
    for entry in recovery.entries {
        if sealed_through.is_some_and(|sealed| entry.seq() <= sealed) {
            continue;
        }
        match entry {
            WalEntry::Insert {
                seq,
                logical,
                version,
                text,
                tags,
                priority,
                source_generation,
                class_d_accepted,
            } => {
                // Replay without re-writing to WAL — tags included so a recovered
                // insert keeps its metadata (ADR-049). The class-D accept decision
                // is the FRAME's marker (WAL v5, ADR-068), never the live knob: an
                // op-5 frame was accepted at its write and must survive a knob
                // flip; a legacy op-0 frame may have been acknowledged as rejected
                // (pre-v5 binaries logged before classifying) and must not
                // resurrect.
                //
                // A manifest that says how far it sealed has already had its records
                // skipped above, and what is left is not in any segment. One that does not
                // say (written before ADR-223) can hold this mutation in a segment and in
                // the log both, after a crash between the manifest rename and the log
                // reset. Then the row is not materialized twice, and the test is by row,
                // not by `seq <= watermark` alone, because a merge moves the watermark
                // past an insert that only the memtable holds.
                let captured = sealed_through.is_none()
                    && seq <= watermark
                    && engine.has_materialized_source_generation(logical, source_generation);
                if !captured {
                    engine.replay_insert(
                        &text,
                        logical,
                        version,
                        &tags,
                        priority.map(|priority| crate::rank::RankValues { priority }),
                        source_generation,
                        class_d_accepted,
                    );
                }
            }
            WalEntry::Tombstone {
                seq,
                seg_idx,
                local_id,
            } => {
                // ADR-066: a positional frame targeting a BASE segment is valid
                // only against the segment list it was written under. Frames at or
                // below the manifest's watermark are already baked into the commit
                // (tombstone bitmap, or the entry was dropped by a merge) — and the
                // positions they address may have been renumbered since, so
                // replaying one could tombstone an unrelated query. Frames above
                // the watermark were appended against exactly the committed list
                // (every segments-vec mutation commits a manifest), so they replay
                // correctly. Memtable frames (the u32::MAX sentinel) always replay:
                // the memtable is rebuilt purely from this WAL tail. With the records
                // the manifest sealed through skipped, the position such a frame names
                // is the one it named when it was written (ADR-223).
                if seg_idx == u32::MAX || seq > watermark {
                    engine.replay_tombstone(seg_idx, local_id);
                }
            }
            WalEntry::DeleteByLogical { seq, logical } => {
                // Address-free (ADR-066): re-derive the affected copies from the
                // recovered state. For a frame at/below the watermark the SEGMENT
                // half is skipped, not just for economy: bulk ingest bypasses the
                // WAL (its segment + manifest commit IS its durability, ADR-017),
                // so a same-id query bulk-ingested AFTER this delete is already in
                // the attached segments — replaying the older delete over it would
                // erase the newer query (codex P1). The commit that covered this
                // frame baked its segment tombstones, so that half loses nothing.
                //
                // The MEMTABLE half always replays. A compaction or bulk commit
                // advances the watermark without sealing the memtable, so the
                // insert this frame deleted may exist only as an earlier frame
                // that was just replayed; skipping the delete would bring an
                // acknowledged delete back.
                engine.apply_delete_by_logical(logical, seq > watermark);
            }
            WalEntry::Upsert {
                seq,
                logical,
                version,
                text,
                tags,
                priority,
                source_generation,
                class_d_accepted,
            } => {
                // ADR-067: the segment-tombstone half follows the watermark rule
                // (baked bitmaps below it; and a same-id bulk ingest after the
                // frame must not be erased), while prior MEMTABLE copies are always
                // re-tombstoned — they are WAL-truth, recreated by earlier replayed
                // frames. See `apply_upsert`. `class_d_accepted` is the frame's
                // marker (op 6, ADR-068): a legacy op-4 frame replays under the old
                // reject gate, so a logged-but-rejected class-D upsert can never
                // tombstone the acknowledged-live prior version.
                //
                // Under a manifest written before ADR-223, the insert half replays
                // unless a commit already holds this frame's row, as for an insert. Its
                // delete half replays even then (ADR-221): a merge drops a row that
                // was replaced, so the frame that wrote the row is replayed, and only
                // this frame takes it away again.
                let captured = sealed_through.is_none()
                    && seq <= watermark
                    && engine.has_materialized_source_generation(logical, source_generation);
                if captured {
                    engine.retire_replayed_copies(logical, source_generation);
                } else {
                    engine.replay_upsert(
                        &text,
                        logical,
                        version,
                        &tags,
                        priority.map(|priority| crate::rank::RankValues { priority }),
                        source_generation,
                        seq > watermark,
                        class_d_accepted,
                    );
                }
            }
            WalEntry::FlushCheckpoint { .. } => {
                // Skip — already handled by manifest
            }
        }
    }
    Ok(())
}

mod feature_model;
mod migration;
mod open;
mod shared;

#[cfg(test)]
mod compiler_migration_tests;
#[cfg(test)]
mod replayed_upsert_tests;
#[cfg(test)]
mod sealed_log_tests;
