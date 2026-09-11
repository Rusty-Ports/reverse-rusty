//! Bounded, complete logical-ID snapshot protocol (ADR-176).

use super::proto;
use super::ranked_wire::{attach, RankedWireCode};

pub(crate) const MAX_LIVE_LOGICAL_IDS: usize = 100_000_000;
pub(crate) const LOGICAL_IDS_PER_FRAME: usize = 4096;
pub(crate) const MAX_ENUMERATION_DURATION: std::time::Duration = std::time::Duration::from_mins(1);

pub(crate) fn invalid(detail: &str) -> tonic::Status {
    attach(
        tonic::Status::failed_precondition(detail),
        RankedWireCode::Protocol,
        None,
    )
}

pub(crate) struct IdCollector {
    shard_id: u32,
    generation: u64,
    num_shards: u32,
    max_ids: usize,
    total: Option<usize>,
    complete: bool,
    ids: Vec<u64>,
}

impl IdCollector {
    pub(crate) fn new(request: &proto::LiveLogicalIdsRequest) -> Self {
        Self {
            shard_id: request.shard_id,
            generation: request.placement_generation,
            num_shards: request.num_shards,
            max_ids: request.max_ids.min(MAX_LIVE_LOGICAL_IDS as u64) as usize,
            total: None,
            complete: false,
            ids: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, frame: proto::LiveLogicalIdsFrame) -> Result<(), tonic::Status> {
        if self.complete {
            return Err(invalid("logical-ID frame after completion"));
        }
        if frame.shard_id != self.shard_id
            || frame.placement_generation != self.generation
            || frame.num_shards != self.num_shards
        {
            return Err(invalid("logical-ID snapshot identity mismatch"));
        }
        let total = usize::try_from(frame.total_ids)
            .map_err(|_| invalid("logical-ID count is out of range"))?;
        if total > self.max_ids || frame.logical_ids.len() > LOGICAL_IDS_PER_FRAME {
            return Err(invalid("logical-ID snapshot exceeds enumeration limits"));
        }
        if self.total.is_some_and(|previous| previous != total) {
            return Err(invalid("logical-ID snapshot count changed"));
        }
        self.total = Some(total);
        if frame.complete {
            if !frame.logical_ids.is_empty() || self.ids.len() != total {
                return Err(invalid("logical-ID completion count mismatch"));
            }
            self.complete = true;
            return Ok(());
        }
        if frame.logical_ids.is_empty()
            || frame.logical_ids.len() > total.saturating_sub(self.ids.len())
            || !frame.logical_ids.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .ids
                .last()
                .is_some_and(|last| *last >= frame.logical_ids[0])
        {
            return Err(invalid(
                "logical-ID data is empty, unordered, duplicated, or over count",
            ));
        }
        self.ids
            .try_reserve(frame.logical_ids.len())
            .map_err(|_| tonic::Status::resource_exhausted("allocating logical-ID directory"))?;
        self.ids.extend(frame.logical_ids);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<u64>, tonic::Status> {
        if !self.complete {
            return Err(invalid("logical-ID stream ended without completion"));
        }
        Ok(self.ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> IdCollector {
        IdCollector::new(&proto::LiveLogicalIdsRequest {
            shard_id: 2,
            placement_generation: 3,
            num_shards: 4,
            max_ids: 10,
            ..Default::default()
        })
    }

    fn frame(ids: &[u64], total: u64, complete: bool) -> proto::LiveLogicalIdsFrame {
        proto::LiveLogicalIdsFrame {
            logical_ids: ids.to_vec(),
            total_ids: total,
            complete,
            shard_id: 2,
            placement_generation: 3,
            num_shards: 4,
        }
    }

    #[test]
    fn logical_ids_require_completion_even_for_empty_and_exact_count_streams() {
        assert!(collector().finish().is_err());
        let mut empty = collector();
        empty.push(frame(&[], 0, true)).expect("empty summary");
        assert!(empty.finish().expect("complete empty set").is_empty());
        let mut truncated = collector();
        truncated
            .push(frame(&[0, u64::MAX], 2, false))
            .expect("data");
        assert!(truncated.finish().is_err());
        let mut complete = collector();
        complete.push(frame(&[0], 2, false)).expect("first");
        complete.push(frame(&[u64::MAX], 2, false)).expect("second");
        complete.push(frame(&[], 2, true)).expect("summary");
        assert_eq!(complete.finish().expect("complete"), vec![0, u64::MAX]);
    }

    #[test]
    fn logical_ids_reject_malformed_frames_and_partial_completions() {
        let cases = [
            frame(&[], 2, false),
            frame(&[2, 1], 2, false),
            frame(&[1, 1], 2, false),
            frame(&[1], 0, false),
            frame(&[1], 11, false),
            frame(&[1], 1, true),
            frame(&[], 1, true),
            proto::LiveLogicalIdsFrame {
                shard_id: 9,
                ..frame(&[1], 1, false)
            },
            proto::LiveLogicalIdsFrame {
                placement_generation: 9,
                ..frame(&[1], 1, false)
            },
            proto::LiveLogicalIdsFrame {
                num_shards: 9,
                ..frame(&[1], 1, false)
            },
            frame(&vec![1; LOGICAL_IDS_PER_FRAME + 1], 2, false),
        ];
        for bad in cases {
            assert!(collector().push(bad).is_err());
        }
        for bad in [
            frame(&[1], 2, false),
            frame(&[2], 3, false),
            frame(&[], 2, true),
        ] {
            let mut partial = collector();
            partial.push(frame(&[1], 2, false)).expect("first");
            assert!(partial.push(bad).is_err());
        }
        for extra in [frame(&[], 0, true), frame(&[1], 1, false)] {
            let mut done = collector();
            done.push(frame(&[], 0, true)).expect("summary");
            assert!(done.push(extra).is_err());
        }
    }
}
