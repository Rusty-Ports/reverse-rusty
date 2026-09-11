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
        let check = || {
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
        ids.sort_unstable();
        check()?;
        if ids.len() != live || ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ShardError::Protocol(
                "live logical-ID snapshot has duplicate or missing index rows".into(),
            ));
        }
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::shard::Shard;
    use std::sync::Arc;

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
