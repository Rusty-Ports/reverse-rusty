//! Cluster-wide live-corpus export for remote resize (ADR-180).

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::{ClusterEngine, ShardError};
use crate::cluster::coordinator::layout::Layout;

/// One exported logical query: the canonical source plus the metadata a rebuild re-applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportedQuery {
    pub logical_id: u64,
    pub dsl: String,
    pub version: u32,
    pub tags: Vec<(String, String)>,
}

/// Order-sensitive digest of one stored copy (source, version, raw tags). Every copy of a logical
/// query is written by the same operation, so copies must agree exactly.
fn copy_digest(dsl: &str, version: u32, tags: &[(String, String)]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    };
    mix(dsl.as_bytes());
    mix(&version.to_le_bytes());
    for (key, value) in tags {
        mix(key.as_bytes());
        mix(value.as_bytes());
    }
    hash
}

impl ClusterEngine {
    /// Stream the cluster's deduplicated live corpus to `visit`, one position at a time. A
    /// query stored on several positions (replicated broad rows, multi-anchor placements) is
    /// visited once, and every other copy must match it exactly: copies that disagree (for
    /// example after an unrepaired partial upsert) fail the export rather than silently keeping
    /// one version. Returns the number of distinct queries visited.
    ///
    /// The caller must hold writes paused for the duration: every position exports a fixed
    /// snapshot, and a corpus that changes mid-export fails loud rather than being skipped.
    pub fn export_live_corpus(
        &self,
        visit: &mut (dyn FnMut(ExportedQuery) -> Result<(), ShardError> + Send),
    ) -> Result<u64, ShardError> {
        Self::export_live_corpus_in(&self.layout(), visit)
    }

    pub(in crate::cluster::coordinator) fn export_live_corpus_in(
        layout: &Layout,
        visit: &mut (dyn FnMut(ExportedQuery) -> Result<(), ShardError> + Send),
    ) -> Result<u64, ShardError> {
        let mut seen: HashMap<u64, u64> = HashMap::new();
        for shard in layout.shards.iter() {
            shard.visit_live_sources(&mut |(logical_id, dsl, version, tags)| {
                let digest = copy_digest(&dsl, version, &tags);
                match seen.entry(logical_id) {
                    Entry::Occupied(existing) => {
                        if *existing.get() != digest {
                            return Err(ShardError::Protocol(format!(
                                "stored copies of logical id {logical_id} disagree; repair or \
                                 upsert it before exporting the corpus"
                            )));
                        }
                    }
                    Entry::Vacant(slot) => {
                        slot.insert(digest);
                        visit(ExportedQuery {
                            logical_id,
                            dsl,
                            version,
                            tags,
                        })?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(seen.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
    use crate::compile::extract_readonly;
    use crate::normalize::Normalizer;

    fn cluster() -> ClusterEngine {
        let config = ClusterConfig {
            num_shards: 2,
            include_broad: true,
            ..ClusterConfig::default()
        };
        ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &[(1, "1994 acme".to_string()), (2, "1995 vertex".to_string())],
        )
        .expect("cluster")
    }

    fn put(cluster: &ClusterEngine, shard: usize, logical: u64, version: u32, dsl: &str) {
        let ast = crate::dsl::parse(dsl).expect("dsl");
        let mut lc = String::new();
        let ex = extract_readonly(
            &ast,
            &cluster.layout().norm,
            &cluster.layout().dict,
            &mut lc,
        );
        cluster.layout().shards[shard]
            .insert_extracted_with_tags(&ex, logical, version, dsl, &[])
            .expect("insert");
    }

    #[test]
    fn identical_copies_export_once_and_disagreeing_copies_fail() {
        let agreeing = cluster();
        for shard in 0..2 {
            put(&agreeing, shard, 77, 3, "zzcopied widget");
        }
        let mut seen = Vec::new();
        agreeing
            .export_live_corpus(&mut |query| {
                seen.push(query.logical_id);
                Ok(())
            })
            .expect("agreeing copies export");
        assert_eq!(seen.iter().filter(|&&id| id == 77).count(), 1);

        for (version, dsl) in [(4, "zzcopied widget"), (3, "zzdiverged widget")] {
            let disagreeing = cluster();
            put(&disagreeing, 0, 77, 3, "zzcopied widget");
            put(&disagreeing, 1, 77, version, dsl);
            let error = disagreeing
                .export_live_corpus(&mut |_| Ok(()))
                .expect_err("disagreeing copies must fail the export");
            assert!(error.to_string().contains("disagree"), "{error}");
        }
    }
}
