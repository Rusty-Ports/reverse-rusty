//! Strict decoding of complete, CRC-valid logical mutation records.

use std::io;

use crate::ownership::{PlacementGeneration, QueryPlacement};
use crate::storage::framed_log::RecordReader;

use super::{ClusterMutation, LogPos, OP_ADD, OP_REMOVE, OP_UPSERT};

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(super) fn decode(body: &[u8]) -> io::Result<(LogPos, ClusterMutation)> {
    let mut reader = RecordReader::new(body);
    let seq = reader.u64()?;
    let op = reader.u8()?;
    let mutation = match op {
        OP_ADD | OP_UPSERT => {
            let logical = reader.u64()?;
            let version = reader.u32()?;
            let len = reader.u32()? as usize;
            let dsl = reader.string(len)?;
            let count = reader.u32()? as usize;
            if count > reader.remaining().len() / 8 {
                return Err(invalid("cluster log tag count exceeds payload"));
            }
            let mut tags = Vec::new();
            for _ in 0..count {
                let len = reader.u32()? as usize;
                let key = reader.string(len)?;
                let len = reader.u32()? as usize;
                let value = reader.string(len)?;
                tags.push((key, value));
            }
            let generation = PlacementGeneration(reader.u64()?);
            let num_shards = reader.u32()?;
            let mode = reader.u8()?;
            let count = reader.u32()? as usize;
            if count > reader.remaining().len() / 4 {
                return Err(invalid("cluster log position count exceeds payload"));
            }
            let mut positions = Vec::with_capacity(count);
            for _ in 0..count {
                positions.push(reader.u32()?);
            }
            let placement = QueryPlacement::from_raw(generation, num_shards, mode, positions)
                .map_err(|e| invalid(format!("invalid cluster log placement: {e}")))?;
            if op == OP_ADD {
                ClusterMutation::Add {
                    logical,
                    version,
                    dsl,
                    tags,
                    placement,
                }
            } else {
                ClusterMutation::Upsert {
                    logical,
                    version,
                    dsl,
                    tags,
                    placement,
                }
            }
        }
        OP_REMOVE => ClusterMutation::Remove {
            logical: reader.u64()?,
        },
        _ => return Err(invalid(format!("unknown cluster log opcode {op}"))),
    };
    reader.finish()?;
    Ok((LogPos(seq), mutation))
}
