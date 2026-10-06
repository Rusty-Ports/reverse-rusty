//! The old or stale peer the ownership-handshake tests connect to.

use std::pin::Pin;
use std::time::Duration;

use raw::shard_service_server::ShardService;
use reverse_rusty_shard_proto as raw;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

fn current_compiler_semantics_version() -> u32 {
    reverse_rusty::segment::Segment::new().compiler_semantics_version()
}

/// A minimal mock `ShardService` with matching feature-space fingerprints and a configurable
/// ownership attestation. Every other RPC is unimplemented: the connect guard rejects first.
pub(crate) struct LegacyOwnershipServer {
    pub(crate) dict_fp: u64,
    pub(crate) tag_fp: u64,
    pub(crate) placement_generation: u64,
    pub(crate) num_shards: u32,
    pub(crate) top_k_delay: Option<Duration>,
    /// ADR-185 attestation; `false` models a pre-ADR-185 shard server.
    pub(crate) atomic_replace: bool,
    /// ADR-205 attestation; `false` models a shard server whose title view predates it.
    pub(crate) alias_form_words: bool,
    /// `Some(n)`: answer `NumQueries` with `n`, as every released node does. `None`: that RPC
    /// is unimplemented too.
    pub(crate) stored_queries: Option<u64>,
}

impl LegacyOwnershipServer {
    /// One slot of the given feature space, at generation 1 of a one-shard layout.
    pub(crate) fn one_shard(dict_fp: u64, tag_fp: u64) -> Self {
        LegacyOwnershipServer {
            dict_fp,
            tag_fp,
            placement_generation: 1,
            num_shards: 1,
            top_k_delay: None,
            atomic_replace: true,
            alias_form_words: true,
            stored_queries: None,
        }
    }
}

#[tonic::async_trait]
impl ShardService for LegacyOwnershipServer {
    type LiveLogicalIdsStream =
        Pin<Box<dyn Stream<Item = Result<raw::LiveLogicalIdsFrame, Status>> + Send>>;

    async fn live_logical_ids(
        &self,
        _: Request<raw::LiveLogicalIdsRequest>,
    ) -> Result<Response<Self::LiveLogicalIdsStream>, Status> {
        Err(Status::unimplemented(
            "legacy peer has no logical-ID enumeration",
        ))
    }

    type LiveSourcesStream =
        Pin<Box<dyn Stream<Item = Result<raw::LiveSourcesFrame, Status>> + Send>>;

    async fn live_sources(
        &self,
        _: Request<raw::LiveSourcesRequest>,
    ) -> Result<Response<Self::LiveSourcesStream>, Status> {
        Err(Status::unimplemented(
            "legacy peer has no live-source export",
        ))
    }

    async fn dict_fingerprint(
        &self,
        _req: Request<raw::Empty>,
    ) -> Result<Response<raw::DictFingerprintReply>, Status> {
        Ok(Response::new(raw::DictFingerprintReply {
            fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_fp,
            broad_replicate_all: true,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
            coordinator_id: 0,
            compiler_semantics_version: current_compiler_semantics_version(),
            retired_operation: 0,
            atomic_replace: self.atomic_replace,
            alias_form_words: self.alias_form_words,
        }))
    }

    async fn adopt_dict(
        &self,
        req: Request<raw::AdoptDictRequest>,
    ) -> Result<Response<raw::AdoptDictReply>, Status> {
        // Echo the shipped fingerprints so only the ownership attestation decides the result.
        let coordinator_id = req
            .metadata()
            .get("x-reverse-rusty-coordinator-id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or_default();
        let r = req.into_inner();
        Ok(Response::new(raw::AdoptDictReply {
            fingerprint: r.fingerprint,
            tag_dict_fingerprint: r.tag_dict_fingerprint,
            broad_replicate_all: true,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
            coordinator_id,
            compiler_semantics_version: current_compiler_semantics_version(),
            atomic_replace: self.atomic_replace,
            alias_form_words: self.alias_form_words,
        }))
    }

    async fn add_shard(
        &self,
        req: Request<raw::AddShardRequest>,
    ) -> Result<Response<raw::AddShardReply>, Status> {
        // Echo the attested fingerprints, as `adopt_dict` does.
        let coordinator_id = req
            .metadata()
            .get("x-reverse-rusty-coordinator-id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or_default();
        let r = req.into_inner();
        Ok(Response::new(raw::AddShardReply {
            dict_fingerprint: r.dict_fingerprint,
            tag_dict_fingerprint: r.tag_dict_fingerprint,
            broad_replicate_all: true,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
            coordinator_id,
            compiler_semantics_version: current_compiler_semantics_version(),
            atomic_replace: self.atomic_replace,
            alias_form_words: self.alias_form_words,
        }))
    }
    async fn percolate(
        &self,
        _req: Request<raw::PercolateRequest>,
    ) -> Result<Response<raw::PercolateReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn percolate_top_k(
        &self,
        req: Request<raw::PercolateTopKRequest>,
    ) -> Result<Response<raw::PercolateTopKReply>, Status> {
        let Some(delay) = self.top_k_delay else {
            return Err(Status::unimplemented("legacy mock"));
        };
        tokio::time::sleep(delay).await;
        let request = req.into_inner();
        Ok(Response::new(raw::PercolateTopKReply {
            hits: Vec::new(),
            total_hits: Some(raw::BoundedTotalHits {
                value: 0,
                exact: true,
            }),
            stats: Some(raw::MatchStats::default()),
            rank_stats: Some(raw::BoundedRankStats::default()),
            bounded: true,
            ownership_applied: true,
            requested_size: request.size,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
            rank_profile: None,
        }))
    }
    type PercolateAllStream =
        Pin<Box<dyn Stream<Item = Result<raw::PercolateAllFrame, Status>> + Send>>;
    async fn percolate_all(
        &self,
        _req: Request<raw::PercolateAllRequest>,
    ) -> Result<Response<Self::PercolateAllStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    type FetchMatchesStream = Pin<Box<dyn Stream<Item = Result<raw::FetchMatch, Status>> + Send>>;
    async fn fetch_matches(
        &self,
        _req: Request<raw::FetchMatchesRequest>,
    ) -> Result<Response<Self::FetchMatchesStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    type PercolateTopKBatchStream =
        Pin<Box<dyn Stream<Item = Result<raw::PercolateTopKBatchFrame, Status>> + Send>>;
    async fn percolate_top_k_batch(
        &self,
        _req: Request<raw::PercolateTopKBatchRequest>,
    ) -> Result<Response<Self::PercolateTopKBatchStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn num_queries(
        &self,
        _req: Request<raw::ShardRef>,
    ) -> Result<Response<raw::CountReply>, Status> {
        match self.stored_queries {
            Some(count) => Ok(Response::new(raw::CountReply { count })),
            None => Err(Status::unimplemented("legacy mock")),
        }
    }
    async fn class_counts(
        &self,
        _req: Request<raw::ShardRef>,
    ) -> Result<Response<raw::ClassCountsReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn ingest_extracted(
        &self,
        _req: Request<raw::IngestRequest>,
    ) -> Result<Response<raw::IngestReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn stage_ingest(
        &self,
        _req: Request<tonic::Streaming<raw::IngestRequest>>,
    ) -> Result<Response<raw::IngestReply>, Status> {
        Err(Status::unimplemented("legacy peer has no staged load"))
    }
    async fn retire(
        &self,
        _req: Request<raw::RetireRequest>,
    ) -> Result<Response<raw::RetireReply>, Status> {
        Err(Status::unimplemented("legacy peer cannot be retired"))
    }
    async fn unretire(
        &self,
        _req: Request<raw::UnretireRequest>,
    ) -> Result<Response<raw::UnretireReply>, Status> {
        Err(Status::unimplemented("legacy peer cannot be retired"))
    }
    async fn set_bulk_load_state(
        &self,
        _req: Request<raw::SetBulkLoadStateRequest>,
    ) -> Result<Response<raw::BulkLoadStateReply>, Status> {
        Err(Status::unimplemented(
            "legacy peer records no bulk-load state",
        ))
    }
    async fn bulk_load_state(
        &self,
        _req: Request<raw::ShardRef>,
    ) -> Result<Response<raw::BulkLoadStateReply>, Status> {
        Err(Status::unimplemented(
            "legacy peer records no bulk-load state",
        ))
    }
    async fn insert_extracted(
        &self,
        _req: Request<raw::InsertRequest>,
    ) -> Result<Response<raw::InsertReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn replace_extracted(
        &self,
        _req: Request<raw::ReplaceRequest>,
    ) -> Result<Response<raw::ReplaceReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn delete(
        &self,
        _req: Request<raw::DeleteRequest>,
    ) -> Result<Response<raw::DeleteReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn flush(
        &self,
        _req: Request<raw::FlushRequest>,
    ) -> Result<Response<raw::FlushReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }

    async fn seal(
        &self,
        _req: Request<raw::SealRequest>,
    ) -> Result<Response<raw::SealReply>, Status> {
        Err(Status::unimplemented("legacy peer has no durable Seal RPC"))
    }

    type FetchSegmentsStream =
        Pin<Box<dyn Stream<Item = Result<raw::FetchSegmentsChunk, Status>> + Send>>;
    async fn fetch_segments(
        &self,
        _req: Request<raw::FetchSegmentsRequest>,
    ) -> Result<Response<Self::FetchSegmentsStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn recover_from(
        &self,
        _req: Request<raw::RecoverFromRequest>,
    ) -> Result<Response<raw::RecoverFromReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }

    type FetchTranslogStream =
        Pin<Box<dyn Stream<Item = Result<raw::TranslogEntry, Status>> + Send>>;
    async fn fetch_translog(
        &self,
        _req: Request<raw::FetchTranslogRequest>,
    ) -> Result<Response<Self::FetchTranslogStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn retention_lease(
        &self,
        _req: Request<raw::RetentionLeaseRequest>,
    ) -> Result<Response<raw::RetentionLeaseReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn fence(
        &self,
        _req: Request<raw::FenceRequest>,
    ) -> Result<Response<raw::FenceReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn unfence(
        &self,
        _req: Request<raw::UnfenceRequest>,
    ) -> Result<Response<raw::UnfenceReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn list_shards(
        &self,
        _req: Request<raw::Empty>,
    ) -> Result<Response<raw::ListShardsReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn drop_shard(
        &self,
        _req: Request<raw::DropShardRequest>,
    ) -> Result<Response<raw::DropShardReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn content_fingerprint(
        &self,
        _req: Request<raw::ContentFingerprintRequest>,
    ) -> Result<Response<raw::ContentFingerprintReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
}
