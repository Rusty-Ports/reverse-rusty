//! Shards this node gave up (ADR-189).
//!
//! Orphan GC (`DropShard`, ADR-096) removes a slot whose data was handed off to another node.
//! The node keeps its adopted feature space, and both slot-creating RPCs (`AdoptDict`,
//! `AddShard`) build an empty slot that serves at once. A coordinator with a stale view (it
//! still believes this node owns the shard) would therefore get a brand-new empty slot back
//! and read no matches from it, where the contract is a loud failure.
//!
//! Creating the slot cannot simply be refused: a handoff BACK to this node starts with the
//! same adoption and then fills the slot with `RecoverFrom`. So the node remembers which shard
//! ids it dropped, and a slot created for one of them is born **awaiting recovery**: every data
//! RPC on it is refused as an ownership mismatch until a peer recovery installs the current
//! owner's data. Adopting a different layout (another dict, tag space, placement generation or
//! shard count) starts over, because the old layout's shard ids mean nothing in it.
//!
//! The record is persisted before a drop takes effect, so a restarted node still remembers.

use std::collections::BTreeSet;
use std::path::Path;

use tonic::Status;

use crate::cluster::shard::ShardError;

use super::{AdoptedSpace, ShardServer, ShardSlot};

/// The record's file under a durable node's data directory.
const DROPPED_FILE: &str = "dropped_shards.bin";
const DROPPED_MAGIC: &[u8; 4] = b"RRDS";
const DROPPED_VERSION: u32 = 1;
/// magic + version + dict fp + tag fp + generation + num_shards + count.
const HEADER_LEN: usize = 4 + 4 + 8 + 8 + 8 + 4 + 4;

/// The layout a dropped-shard record belongs to. A record written under another layout is
/// stale: the node has since been re-adopted from empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SpaceId {
    dict_fingerprint: u64,
    tag_dict_fingerprint: u64,
    placement_generation: u64,
    num_shards: u32,
}

impl SpaceId {
    pub(super) fn new(
        dict_fingerprint: u64,
        tag_dict_fingerprint: u64,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Self {
        SpaceId {
            dict_fingerprint,
            tag_dict_fingerprint,
            placement_generation: placement_generation.get(),
            num_shards,
        }
    }

    pub(super) fn of(space: &AdoptedSpace) -> Self {
        SpaceId {
            dict_fingerprint: space.dict.fingerprint(),
            tag_dict_fingerprint: space.tag_dict.fingerprint(),
            placement_generation: space.placement_generation.get(),
            num_shards: space.num_shards,
        }
    }
}

/// Persist `dropped` for `space` under `dir` with an atomic, synced rename.
fn persist(dir: &Path, space: SpaceId, dropped: &BTreeSet<u32>) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut blob = Vec::with_capacity(HEADER_LEN + dropped.len() * 4);
    blob.extend_from_slice(DROPPED_MAGIC);
    blob.extend_from_slice(&DROPPED_VERSION.to_le_bytes());
    blob.extend_from_slice(&space.dict_fingerprint.to_le_bytes());
    blob.extend_from_slice(&space.tag_dict_fingerprint.to_le_bytes());
    blob.extend_from_slice(&space.placement_generation.to_le_bytes());
    blob.extend_from_slice(&space.num_shards.to_le_bytes());
    blob.extend_from_slice(&(dropped.len() as u32).to_le_bytes());
    for shard_id in dropped {
        blob.extend_from_slice(&shard_id.to_le_bytes());
    }
    let tmp = dir.join(format!("{DROPPED_FILE}.tmp"));
    std::fs::write(&tmp, &blob)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, dir.join(DROPPED_FILE))?;
    std::fs::File::open(dir)?.sync_all()
}

/// The shard ids recorded as dropped under `dir` for `space`. An absent record, or one written
/// under another layout, is empty. A malformed record fails loud: guessing could let a shard
/// this node gave up serve again as an empty slot.
pub(super) fn restore(dir: &Path, space: SpaceId) -> Result<BTreeSet<u32>, ShardError> {
    let path = dir.join(DROPPED_FILE);
    let blob = match std::fs::read(&path) {
        Ok(blob) => blob,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => {
            return Err(ShardError::Log(format!(
                "reading {}: {error}",
                path.display()
            )))
        }
    };
    let corrupt = || ShardError::Log(format!("corrupt dropped-shard record {}", path.display()));
    let u32_at = |at: usize| -> Result<u32, ShardError> {
        blob.get(at..at + 4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or_else(corrupt)
    };
    let u64_at = |at: usize| -> Result<u64, ShardError> {
        blob.get(at..at + 8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(corrupt)
    };
    if blob.get(0..4) != Some(DROPPED_MAGIC.as_slice()) {
        return Err(corrupt());
    }
    let version = u32_at(4)?;
    if version != DROPPED_VERSION {
        return Err(ShardError::Log(format!(
            "unsupported dropped-shard record version {version} in {}",
            path.display()
        )));
    }
    let recorded = SpaceId {
        dict_fingerprint: u64_at(8)?,
        tag_dict_fingerprint: u64_at(16)?,
        placement_generation: u64_at(24)?,
        num_shards: u32_at(32)?,
    };
    let count = u32_at(36)? as usize;
    if blob.len() != HEADER_LEN + count * 4 {
        return Err(corrupt());
    }
    if recorded != space {
        return Ok(BTreeSet::new());
    }
    (0..count).map(|i| u32_at(HEADER_LEN + i * 4)).collect()
}

impl ShardSlot {
    /// Refuse to serve from a slot that was re-created for a shard this node gave up and has
    /// not been recovered since. Attached as an ownership mismatch, so a coordinator reports a
    /// superseded placement rather than an unexpected transport failure.
    pub(super) fn ensure_recovered(&self, shard_id: u32) -> Result<(), Status> {
        if !self
            .awaiting_recovery
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        Err(crate::cluster::ranked_wire::attach(
            Status::failed_precondition(format!(
                "placement configuration mismatch: shard {shard_id} was dropped from this node \
                 after a handoff and re-created empty; it serves nothing until a peer recovery \
                 repopulates it or the node is wiped"
            )),
            crate::cluster::ranked_wire::RankedWireCode::OwnershipMismatch,
            None,
        ))
    }
}

impl ShardServer {
    fn dropped_set(&self) -> Result<std::sync::MutexGuard<'_, BTreeSet<u32>>, Status> {
        self.dropped
            .lock()
            .map_err(|_| Status::internal("dropped-shard record lock poisoned"))
    }

    /// Whether this node dropped `shard_id` under its current layout and has not recovered it.
    pub(super) fn was_dropped(&self, shard_id: u32) -> Result<bool, Status> {
        Ok(self.dropped_set()?.contains(&shard_id))
    }

    /// Replace the record with `next`, durably first.
    fn store_dropped(
        &self,
        current: &mut BTreeSet<u32>,
        next: BTreeSet<u32>,
    ) -> Result<(), Status> {
        if *current == next {
            return Ok(());
        }
        if let (Some(dir), Some(space)) = (&self.data_dir, self.node_dict.load_full()) {
            persist(dir, SpaceId::of(&space), &next).map_err(|error| {
                Status::internal(format!("persisting the dropped-shard record: {error}"))
            })?;
        }
        *current = next;
        Ok(())
    }

    /// Remember that `shard_id` is being dropped. Called before the slot is removed, so a
    /// crash in between leaves the record, never a forgotten drop; and called with NO
    /// slot-map lock held, because on a durable node this writes and syncs a file, and
    /// that must not stall the node's other shards. Returns whether the id was newly added.
    pub(super) fn record_dropped(&self, shard_id: u32) -> Result<bool, Status> {
        #[cfg(test)]
        while_recording_a_drop();
        let mut current = self.dropped_set()?;
        if current.contains(&shard_id) {
            return Ok(false);
        }
        let mut next = current.clone();
        next.insert(shard_id);
        self.store_dropped(&mut current, next)?;
        Ok(true)
    }

    /// Take back a record made for a drop that did not happen. The slot is still hosted, so
    /// nothing was given up.
    pub(super) fn unrecord_dropped(&self, shard_id: u32) -> Result<(), Status> {
        let mut current = self.dropped_set()?;
        let mut next = current.clone();
        next.remove(&shard_id);
        self.store_dropped(&mut current, next)
    }

    /// Forget `shard_id`: a peer recovery has installed the current owner's data in `slot`.
    pub(super) fn mark_recovered(&self, shard_id: u32, slot: &ShardSlot) -> Result<(), Status> {
        let mut current = self.dropped_set()?;
        let mut next = current.clone();
        next.remove(&shard_id);
        self.store_dropped(&mut current, next)?;
        slot.awaiting_recovery
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// The record that belongs to the layout `space`, read BEFORE the node commits to that
    /// layout. A different layout is a fresh start, so its record is empty. A durable node
    /// that restarted pending and adopts the layout it had before finds that layout's record
    /// on disk and keeps honouring it.
    ///
    /// Fallible on purpose and first: if the record cannot be read, the adoption fails with
    /// the node unchanged, so a retry fails the same way instead of finding the layout
    /// already adopted and skipping this step.
    pub(super) fn layout_record(&self, space: SpaceId) -> Result<BTreeSet<u32>, Status> {
        match &self.data_dir {
            Some(dir) => restore(dir, space).map_err(|error| {
                Status::failed_precondition(format!(
                    "cannot read this node's dropped-shard record: {error}"
                ))
            }),
            None => Ok(BTreeSet::new()),
        }
    }

    /// Take up `record` for the layout the node has just adopted and bring every hosted
    /// slot in line. A slot still flagged from another layout is released: it is empty (a
    /// layout can change only while no slot holds data) and an idempotent re-adoption
    /// would never replace it. Cannot fail, so nothing after the adoption's commit point can.
    pub(super) fn install_layout_record(&self, record: BTreeSet<u32>) {
        use std::sync::PoisonError;
        // Never hold the record lock across the slot map.
        {
            let slots = self.shards.read().unwrap_or_else(PoisonError::into_inner);
            for (shard_id, slot) in slots.iter() {
                slot.awaiting_recovery.store(
                    record.contains(shard_id),
                    std::sync::atomic::Ordering::Release,
                );
            }
        }
        *self.dropped.lock().unwrap_or_else(PoisonError::into_inner) = record;
    }
}

/// The record a durable constructor starts with: the shards dropped under `space`, with every
/// restored slot among them flagged. Every durable constructor that has a space calls this,
/// so no restart path forgets a drop.
pub(super) fn restore_for_slots<'a>(
    dir: &Path,
    space: &AdoptedSpace,
    slots: impl IntoIterator<Item = (&'a u32, &'a std::sync::Arc<ShardSlot>)>,
) -> Result<std::sync::Mutex<BTreeSet<u32>>, ShardError> {
    let dropped = restore(dir, SpaceId::of(space))?;
    for (shard_id, slot) in slots {
        if dropped.contains(shard_id) {
            slot.awaiting_recovery
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
    Ok(std::sync::Mutex::new(dropped))
}

#[cfg(test)]
thread_local! {
    static BETWEEN_READINESS_AND_STATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static WHILE_RECORDING_A_DROP: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Test seam: run `action` once, on this thread, when a drop is being recorded.
#[cfg(test)]
pub(super) fn arm_while_recording_a_drop(action: impl FnOnce() + 'static) {
    WHILE_RECORDING_A_DROP.with(|cell| *cell.borrow_mut() = Some(Box::new(action)));
}

#[cfg(test)]
fn while_recording_a_drop() {
    let action = WHILE_RECORDING_A_DROP.with(|cell| cell.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

/// Test seam: run `action` once, on this thread, at the point where `loaded_slot` has
/// finished its first step and not yet started its second.
#[cfg(test)]
pub(super) fn arm_between_readiness_and_state(action: impl FnOnce() + 'static) {
    BETWEEN_READINESS_AND_STATE.with(|cell| *cell.borrow_mut() = Some(Box::new(action)));
}

#[cfg(test)]
pub(super) fn between_readiness_and_state() {
    let action = BETWEEN_READINESS_AND_STATE.with(|cell| cell.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

#[cfg(test)]
mod tests {
    use super::{persist, restore, SpaceId, DROPPED_FILE};
    use std::collections::BTreeSet;

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rr_dropped_record_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn space(generation: u64) -> SpaceId {
        SpaceId {
            dict_fingerprint: 0xD1C7,
            tag_dict_fingerprint: 0x7A65,
            placement_generation: generation,
            num_shards: 16,
        }
    }

    #[test]
    fn the_record_round_trips_and_belongs_to_one_layout() {
        let dir = scratch("round_trip");
        assert!(restore(&dir, space(1)).expect("absent").is_empty());

        let dropped: BTreeSet<u32> = [3, 9, 0].into_iter().collect();
        persist(&dir, space(1), &dropped).expect("persist");
        assert_eq!(restore(&dir, space(1)).expect("same layout"), dropped);
        // A record left behind by a previous layout says nothing about this one.
        assert!(restore(&dir, space(2)).expect("other layout").is_empty());

        persist(&dir, space(1), &BTreeSet::new()).expect("persist empty");
        assert!(restore(&dir, space(1)).expect("emptied").is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_damaged_record_fails_loud() {
        let dir = scratch("damaged");
        let dropped: BTreeSet<u32> = [3].into_iter().collect();
        persist(&dir, space(1), &dropped).expect("persist");
        let path = dir.join(DROPPED_FILE);
        let good = std::fs::read(&path).expect("read");

        for damaged in [
            good[..good.len() - 1].to_vec(),                 // truncated
            [good.clone(), vec![0, 0, 0, 0]].concat(),       // trailing bytes
            [b"XXXX".to_vec(), good[4..].to_vec()].concat(), // wrong magic
        ] {
            std::fs::write(&path, &damaged).expect("write damaged");
            assert!(
                restore(&dir, space(1)).is_err(),
                "a damaged record must not be read as 'nothing dropped'"
            );
        }
        let mut future = good;
        future[4..8].copy_from_slice(&2u32.to_le_bytes());
        std::fs::write(&path, &future).expect("write future version");
        assert!(restore(&dir, space(1)).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
