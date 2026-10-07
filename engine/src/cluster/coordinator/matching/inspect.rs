//! `impl ClusterEngine` — what the read path reports about itself: fan-out and count
//! introspection, the filtered match with merged statistics, and reads of one stored document.

use crate::cluster::shard::ShardError;
use crate::segment::MatchStats;

use super::ClusterEngine;

impl ClusterEngine {
    /// Introspection: the shards a title would be routed to (its fan-out) — the selective
    /// targets plus the one broad-eval shard (ADR-080).
    pub fn shard_fanout(&self, title: &str) -> Vec<usize> {
        let layout = &*self.layout();
        Self::route(layout, title).0
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        let layout = &*self.layout();
        layout.ring.num_shards()
    }

    /// How many replicas, across all positions, reads may not fail over to: a replicated write
    /// to them failed, or they were not proven equal to their primary at connect (ADR-195).
    /// Redundancy is reduced by that many copies until they are recovered. 0 without replicas.
    #[must_use]
    pub fn out_of_sync_replicas(&self) -> usize {
        self.layout().out_of_sync_replicas()
    }

    /// A point-in-time snapshot of the cluster gRPC transport metrics (ADR-085): per-RPC
    /// call counts, errors, timeouts, retries, and summed latency. All-zero for an
    /// in-process cluster (no remote RPCs). Off the hot path — introspection / scraping.
    pub fn transport_metrics(&self) -> crate::cluster::TransportMetricsSnapshot {
        self.transport_metrics.load().snapshot()
    }

    /// Total physical query count across shards (a replicated/any-of query is
    /// counted once per shard holding it — physical, not distinct-logical).
    pub fn num_queries(&self) -> Result<usize, ShardError> {
        let layout = &*self.layout();
        layout.shards.iter().map(|s| s.num_queries()).sum()
    }

    /// Per-shard physical query counts (introspection / tests).
    pub fn shard_query_counts(&self) -> Result<Vec<usize>, ShardError> {
        let layout = &*self.layout();
        layout.shards.iter().map(|s| s.num_queries()).collect()
    }

    /// Cluster-wide per-class entry tally `[A, B, C, D, H]`, summed across shards
    /// (replicated/any-of queries counted per holding shard; H appended at index
    /// 4 — ADR-105). Used by the oracle to assert each placement branch is
    /// actually exercised.
    pub fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.layout().class_counts()
    }

    /// [`Self::percolate_filtered_with_broad`] also returning the merged [`MatchStats`]
    /// across the probed shards — the coordinator-mode server's `/_search` profile path
    /// (ADR-070). An empty filter + the cluster default broad toggle is byte-identical
    /// to [`Self::percolate_with_stats`].
    pub fn percolate_filtered_with_stats(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let layout = &*self.layout();
        let pred = self.compile_tag_predicate(filter);
        self.percolate_inner(layout, title, include_broad, &pred)
    }

    /// The live source DSL stored for `logical`, probing each shard's source store
    /// (first live copy wins — every copy of one logical id is identical). `Ok(None)`
    /// only when EVERY shard answered "not held"; a shard that cannot answer (a
    /// `RemoteShard` in v1) fails the lookup loud rather than letting the coordinator
    /// report a false "not found" (ADR-070).
    pub fn get_source(&self, logical: u64) -> Result<Option<String>, ShardError> {
        let layout = &*self.layout();
        let mut first_err: Option<ShardError> = None;
        for s in layout.shards.iter() {
            match s.source_of(logical) {
                Ok(Some(dsl)) => return Ok(Some(dsl)),
                Ok(None) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// The canonical stored document for `logical`, including write version and
    /// read-back tags. Like [`Self::get_source`], every shard must be capable of
    /// answering before absence is reported; remote v1 shards fail loud.
    pub fn get_document(
        &self,
        logical: u64,
    ) -> Result<Option<crate::storage::StoredSource>, ShardError> {
        let layout = &*self.layout();
        let mut first_err: Option<ShardError> = None;
        for shard in layout.shards.iter() {
            match shard.document_of(logical) {
                Ok(Some(document)) => return Ok(Some(document)),
                Ok(None) => {}
                Err(error) => {
                    first_err.get_or_insert(error);
                }
            }
        }
        match first_err {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// Whether any shard holds a live exact row for `logical`, without reading
    /// source metadata. Used by document HEAD so a missing/damaged sidecar does
    /// not change existence semantics. As with source lookup, an incapable shard
    /// prevents a definitive negative and therefore fails loud.
    pub fn document_exists(&self, logical: u64) -> Result<bool, ShardError> {
        let layout = &*self.layout();
        let mut first_err: Option<ShardError> = None;
        for shard in layout.shards.iter() {
            match shard.has_live_query(logical) {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    first_err.get_or_insert(error);
                }
            }
        }
        match first_err {
            Some(error) => Err(error),
            None => Ok(false),
        }
    }

    /// The cluster's default broad-lane toggle (what [`Self::percolate`] uses).
    pub fn include_broad(&self) -> bool {
        self.include_broad
    }

    /// Replication factor (copies per shard position).
    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// Whether this cluster persists durable artifacts (built/opened with a `data_dir`).
    pub fn is_durable(&self) -> bool {
        self.data_dir.is_some()
    }

    /// Whether shard execution is hosted on remote nodes, each with its own durability.
    pub fn is_remote(&self) -> bool {
        #[cfg(feature = "distributed")]
        {
            self.handle.is_some()
        }
        #[cfg(not(feature = "distributed"))]
        {
            false
        }
    }

    /// The per-shard engine configuration the cluster was assembled with.
    pub fn per_shard_config(&self) -> &crate::config::EngineConfig {
        &self.per_shard
    }

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055).
    /// Introspection for operators (cluster-mode `/_stats`, ADR-070); best-effort
    /// across reopen (a checkpointed synthetic-only cluster restores it `false`).
    /// No longer gates anything: a vocabulary change carries tags through the
    /// rebuild by stored `TagId` (ADR-074).
    pub fn has_tagged_queries(&self) -> bool {
        self.has_tags()
    }
}
