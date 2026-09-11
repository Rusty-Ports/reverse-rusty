//! Bounded integer-only snapshot for remote coordinator admission (ADR-176).

use std::sync::TryLockError;
use std::time::{Duration, Instant};

use super::{LocalShard, ShardError};

impl LocalShard {
    pub(crate) fn bounded_live_logical_ids(
        &self,
        max_ids: usize,
        deadline: Instant,
    ) -> Result<Vec<u64>, ShardError> {
        let mut check = || {
            if Instant::now() >= deadline {
                Err(ShardError::DeadlineExceeded)
            } else {
                Ok(())
            }
        };
        let engine = loop {
            check()?;
            match self.engine.try_lock() {
                Ok(engine) => break engine,
                Err(TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(1)),
            }
        };
        let live = engine.num_live_queries();
        if live > max_ids {
            return Err(ShardError::Config(format!(
                "live logical-ID count {live} exceeds enumeration limit {max_ids}"
            )));
        }
        let mut ids = Vec::new();
        ids.try_reserve_exact(live).map_err(|error| {
            ShardError::Config(format!("allocating logical-ID snapshot: {error}"))
        })?;
        engine.visit_live_logical_ids(
            |id| {
                if ids.len() >= live {
                    return Err(ShardError::Protocol("live logical-ID count changed".into()));
                }
                ids.push(id);
                Ok(())
            },
            check,
        )?;
        drop(engine);
        sort_ids(&mut ids, 56, &mut check)?;
        if ids.len() != live {
            return Err(ShardError::Protocol(
                "live logical-ID snapshot has missing index rows".into(),
            ));
        }
        validate_unique(&ids, check)?;
        Ok(ids)
    }
}

// In-place MSD radix partitioning bounds work between deadline polls without
// allocating another corpus-sized buffer. Recursion uses at most eight levels;
// only small leaves use the standard, non-cancellable sort.
fn sort_ids(
    ids: &mut [u64],
    shift: u32,
    check: &mut impl FnMut() -> Result<(), ShardError>,
) -> Result<(), ShardError> {
    check()?;
    if ids.len() <= 4096 {
        ids.sort_unstable();
        return check();
    }
    let bucket = |id: u64| usize::from((id >> shift) as u8);
    let mut counts = [0_usize; 256];
    for chunk in ids.chunks(256) {
        check()?;
        for &id in chunk {
            counts[bucket(id)] += 1;
        }
    }
    let mut next = [0_usize; 256];
    let mut end = 0;
    for (start, &count) in next.iter_mut().zip(&counts) {
        *start = end;
        end += count;
    }
    let mut moves = 0_usize;
    end = 0;
    for (group, &count) in counts.iter().enumerate() {
        end += count;
        while next[group] < end {
            if moves.is_multiple_of(256) {
                check()?;
            }
            let current = next[group];
            let target = bucket(ids[current]);
            // Each swap fills the next free slot in the value's own bucket.
            // Counts guarantee that slot exists until its bucket is complete.
            ids.swap(current, next[target]);
            next[target] += 1;
            moves += 1;
        }
    }
    if shift > 0 {
        let mut start = 0;
        for count in counts {
            if count > 1 {
                sort_ids(&mut ids[start..start + count], shift - 8, check)?;
            }
            start += count;
        }
    }
    check()
}

fn validate_unique(
    ids: &[u64],
    mut check: impl FnMut() -> Result<(), ShardError>,
) -> Result<(), ShardError> {
    for (index, pair) in ids.windows(2).enumerate() {
        if index.is_multiple_of(256) {
            check()?;
        }
        if pair[0] == pair[1] {
            return Err(ShardError::Protocol(
                "live logical-ID snapshot has duplicate index rows".into(),
            ));
        }
    }
    check()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::shard::Shard;
    use std::sync::Arc;

    #[test]
    fn logical_ids_cancellable_sort_matches_standard_sort() {
        let mut seed = 41_u64;
        let shuffled: Vec<_> = (0..20_000)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                seed
            })
            .collect();
        for mut ids in [
            Vec::new(),
            vec![u64::MAX, 0, 1, u64::MAX],
            (0..20_000).rev().collect(),
            vec![u64::MAX; 20_000],
            (0..20_000).map(|id| id % 512).collect(),
            shuffled,
        ] {
            let mut expected = ids.clone();
            expected.sort_unstable();
            sort_ids(&mut ids, 56, &mut || Ok(())).expect("sort");
            assert_eq!(ids, expected);
        }
    }

    #[test]
    fn logical_ids_sort_and_validation_observe_mid_operation_cancellation() {
        // Expire during counting, partitioning, and recursion, not only at entry.
        for stop_at in [2, 50, 100, 200] {
            let mut ids: Vec<_> = (0..20_000).rev().collect();
            let mut polls = 0;
            assert!(matches!(
                sort_ids(&mut ids, 56, &mut || {
                    polls += 1;
                    if polls == stop_at {
                        Err(ShardError::DeadlineExceeded)
                    } else {
                        Ok(())
                    }
                }),
                Err(ShardError::DeadlineExceeded)
            ));
            assert_eq!(polls, stop_at);
        }
        let ids: Vec<_> = (0..20_000).collect();
        let mut polls = 0;
        assert!(matches!(
            validate_unique(&ids, || {
                polls += 1;
                if polls == 3 {
                    Err(ShardError::DeadlineExceeded)
                } else {
                    Ok(())
                }
            }),
            Err(ShardError::DeadlineExceeded)
        ));
        assert_eq!(polls, 3);
        assert!(validate_unique(&[0, 1, 1], || Ok(())).is_err());
        assert!(validate_unique(&[0, 1, u64::MAX], || Ok(())).is_ok());
    }

    #[test]
    fn logical_ids_scan_index_and_refuse_duplicate_rows_and_expired_lock_waits() {
        let norm = Arc::new(crate::normalize::Normalizer::default_vocab().expect("vocab"));
        let mut dict = crate::dict::Dict::new();
        let ast = crate::dsl::parse("indexneedle").expect("DSL");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut String::new());
        dict.finalize_mask();
        let shard = LocalShard::new(
            norm,
            Arc::new(dict),
            Arc::new(crate::tagdict::TagDict::new()),
            crate::config::EngineConfig::default(),
        );
        shard
            .insert_extracted_with_tags(&ex, 7, 1, "indexneedle", &[])
            .expect("insert");
        let deadline = || Instant::now() + Duration::from_secs(2);
        assert_eq!(
            shard
                .bounded_live_logical_ids(1, deadline())
                .expect("index ids"),
            vec![7]
        );
        assert!(shard.bounded_live_logical_ids(0, deadline()).is_err());
        let locked = shard.lock();
        assert!(matches!(
            shard.bounded_live_logical_ids(1, Instant::now() + Duration::from_millis(5)),
            Err(ShardError::DeadlineExceeded)
        ));
        drop(locked);
        shard
            .insert_extracted_with_tags(&ex, 7, 2, "indexneedle", &[])
            .expect("duplicate row");
        assert!(shard.bounded_live_logical_ids(2, deadline()).is_err());
        shard.delete_by_logical_id(7).expect("delete both copies");
        assert!(shard
            .bounded_live_logical_ids(1, deadline())
            .expect("empty")
            .is_empty());
    }
}
