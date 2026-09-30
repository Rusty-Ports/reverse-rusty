//! Durable node retirement for remote resize (ADR-180).
//!
//! A resize builds its new layout on disjoint nodes, so retiring a layout means retiring whole
//! nodes. Before the control plane may commit the successor layout, the resizing coordinator
//! retires every node of the current one. A retired node refuses every slot RPC (reads
//! included), adoption, and new slots, so no coordinator (stale, restarted, or this one after a
//! crash, cancellation, or lost reply) can answer from a layout that may no longer be the layout
//! of record. Only the same operation lifts it, once the control plane proves the resize did not
//! commit; a finished resize leaves the node retired until it is wiped. The record is persisted
//! before it takes effect, so a restarted retired node stays retired.

use std::path::Path;

use tonic::Status;

use crate::cluster::shard::ShardError;

use super::ShardServer;

/// The retirement record's file under a durable node's data directory.
const RETIREMENT_FILE: &str = "retired.bin";
const RETIREMENT_MAGIC: &[u8; 4] = b"RRTR";
const RETIREMENT_VERSION: u32 = 1;
const RETIREMENT_LEN: usize = 24;

/// A retirement recorded by one remote resize.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Retirement {
    pub(super) operation_id: u64,
    /// The generation of the layout that supersedes this node's.
    pub(super) successor_generation: u64,
}

/// Persist `retirement` under `dir` with an atomic, synced rename.
pub(super) fn persist_retirement(dir: &Path, retirement: Retirement) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut blob = Vec::with_capacity(RETIREMENT_LEN);
    blob.extend_from_slice(RETIREMENT_MAGIC);
    blob.extend_from_slice(&RETIREMENT_VERSION.to_le_bytes());
    blob.extend_from_slice(&retirement.operation_id.to_le_bytes());
    blob.extend_from_slice(&retirement.successor_generation.to_le_bytes());
    let tmp = dir.join(format!("{RETIREMENT_FILE}.tmp"));
    std::fs::write(&tmp, &blob)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, dir.join(RETIREMENT_FILE))?;
    std::fs::File::open(dir)?.sync_all()
}

/// Remove the retirement record under `dir`, syncing the directory. An absent record is fine.
pub(super) fn clear_retirement(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(dir.join(RETIREMENT_FILE)) {
        Ok(()) => std::fs::File::open(dir)?.sync_all(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// The retirement record under `dir`, if any. A malformed record fails loud: guessing could put
/// a retired node back into service.
pub(super) fn read_retirement(dir: &Path) -> Result<Option<Retirement>, ShardError> {
    let path = dir.join(RETIREMENT_FILE);
    let blob = match std::fs::read(&path) {
        Ok(blob) => blob,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ShardError::Log(format!(
                "reading {}: {error}",
                path.display()
            )))
        }
    };
    let corrupt = || ShardError::Log(format!("corrupt retirement record {}", path.display()));
    if blob.len() != RETIREMENT_LEN || blob.get(0..4) != Some(RETIREMENT_MAGIC.as_slice()) {
        return Err(corrupt());
    }
    let word = |range: std::ops::Range<usize>| -> Result<u64, ShardError> {
        blob.get(range)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(corrupt)
    };
    let version = blob
        .get(4..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(corrupt)?;
    if version != RETIREMENT_VERSION {
        return Err(ShardError::Log(format!(
            "unsupported retirement record version {version} in {}",
            path.display()
        )));
    }
    let operation_id = word(8..16)?;
    let successor_generation = word(16..24)?;
    if operation_id == 0 {
        return Err(corrupt());
    }
    Ok(Some(Retirement {
        operation_id,
        successor_generation,
    }))
}

impl ShardServer {
    /// The operation that retired this node, or 0.
    pub(super) fn retired_operation(&self) -> u64 {
        self.retired
            .load_full()
            .map_or(0, |retirement| retirement.operation_id)
    }

    /// Refuse any use of a retired node. Attached as an ownership mismatch, so coordinators
    /// report it as a superseded placement rather than an unexpected transport failure.
    pub(super) fn ensure_not_retired(&self) -> Result<(), Status> {
        let Some(retirement) = self.retired.load_full() else {
            return Ok(());
        };
        Err(crate::cluster::ranked_wire::attach(
            Status::failed_precondition(format!(
                "placement configuration mismatch: node retired by remote resize {} (superseded \
                 by placement generation {}); it serves nothing until that resize is resolved or \
                 the node is wiped",
                retirement.operation_id, retirement.successor_generation
            )),
            crate::cluster::ranked_wire::RankedWireCode::OwnershipMismatch,
            None,
        ))
    }

    /// Record `retirement` durably, then make it effective. Idempotent for the same operation;
    /// a different operation's retirement is refused.
    pub(super) fn record_retirement(&self, retirement: Retirement) -> Result<(), Status> {
        if let Some(existing) = self.retired.load_full() {
            if existing.operation_id == retirement.operation_id {
                return Ok(());
            }
            return Err(Status::failed_precondition(format!(
                "node is already retired by remote resize {}",
                existing.operation_id
            )));
        }
        if let Some(dir) = &self.data_dir {
            persist_retirement(dir, retirement).map_err(|error| {
                Status::internal(format!("persisting node retirement: {error}"))
            })?;
        }
        self.retired.store(Some(std::sync::Arc::new(retirement)));
        Ok(())
    }

    /// Lift a retirement recorded by `operation_id`. Returns whether the node was retired;
    /// another operation's retirement is refused.
    pub(super) fn lift_retirement(&self, operation_id: u64) -> Result<bool, Status> {
        let Some(existing) = self.retired.load_full() else {
            return Ok(false);
        };
        if existing.operation_id != operation_id {
            return Err(Status::failed_precondition(format!(
                "node is retired by remote resize {}, not {operation_id}",
                existing.operation_id
            )));
        }
        if let Some(dir) = &self.data_dir {
            clear_retirement(dir)
                .map_err(|error| Status::internal(format!("clearing node retirement: {error}")))?;
        }
        self.retired.store(None);
        Ok(true)
    }
}
