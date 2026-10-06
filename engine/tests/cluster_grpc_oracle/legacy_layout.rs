//! ADR-109 ownership handshake: a coordinator must refuse a pre-ADR-109 server that cannot attest
//! its placement generation and shard count, and a server carrying a stale generation must fail
//! closed on adoption. Otherwise the coordinator could trust a reply whose rows use a different
//! emission-owner function. A real `ShardServer` attests both fields, so the happy path is exercised
//! by every other oracle in this suite; this file supplies the old/stale peer controls.

use std::sync::Arc;
use std::time::{Duration, Instant};

use raw::shard_service_server::ShardServiceServer;
use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterRankedError, RemoteShard, ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty_shard_proto as raw;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

mod mock;
pub(crate) use mock::LegacyOwnershipServer;

/// Serve a peer that answers `UNIMPLEMENTED` to every RPC newer than the ownership handshake,
/// as one slot of a `num_shards` layout over the given feature space. `stored_queries` is what
/// it answers `NumQueries` with. Returns its endpoint.
pub(crate) fn spawn_legacy_peer(
    rt: &tokio::runtime::Runtime,
    dict_fp: u64,
    num_shards: u32,
    stored_queries: Option<u64>,
) -> String {
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
    let address = incoming.local_addr().expect("address");
    let peer = LegacyOwnershipServer {
        num_shards,
        stored_queries,
        ..LegacyOwnershipServer::one_shard(dict_fp, empty_tag_dict().fingerprint())
    };
    rt.spawn(
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(peer))
            .serve_with_incoming(incoming),
    );
    format!("http://{address}")
}

/// A one-shard cluster whose only node is such a peer.
pub(crate) fn cluster_on_a_legacy_peer(
    rt: &tokio::runtime::Runtime,
    stored_queries: Option<u64>,
) -> ClusterEngine {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let endpoint = spawn_legacy_peer(rt, dict.fingerprint(), 1, stored_queries);
    ClusterEngine::connect_remote(
        norm,
        dict,
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            ..Default::default()
        },
        &[endpoint],
        rt.handle(),
    )
    .expect("old peer can still attach")
}

#[test]
fn grpc_logical_ids_unsupported_peer_keeps_create_only_admission_closed() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let cluster = cluster_on_a_legacy_peer(&rt, None);
    assert!(matches!(
        cluster.add_query(1, "freshneedle"),
        Err(ShardError::Config(_))
    ));
    let metrics = cluster.transport_metrics();
    let enumeration = metrics
        .methods
        .iter()
        .find(|row| row.method == "live_logical_ids")
        .expect("metric");
    assert_eq!(enumeration.calls, 1);
    assert_eq!(enumeration.errors, 1);
}

/// Both connect paths refuse an old peer with no ownership attestation; adoption also refuses a
/// nonzero but stale generation. A real ADR-109 server is the positive control.
#[test]
fn grpc_connect_refuses_missing_or_stale_ownership_attestation() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint(); // matches ShardServer::new's finalized empty space

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let start_mock = |placement_generation, num_shards| {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let svc = ShardServiceServer::new(LegacyOwnershipServer {
            placement_generation,
            num_shards,
            ..LegacyOwnershipServer::one_shard(dict_fp, tag_fp)
        });
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming),
        );
        wait_until_listening(addr);
        format!("http://{addr}")
    };
    let legacy_ep = start_mock(0, 0);

    // 1. The bare handshake refuses a proto3-defaulted pre-ADR-109 peer.
    match RemoteShard::connect(&legacy_ep, rt.handle().clone(), dict_fp, tag_fp, 0) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected typed ownership refusal, got {e}"),
        Ok(_) => panic!("connect SUCCEEDED against a pre-ADR-109 peer"),
    }

    // 2. The adopt path refuses the same old peer.
    let dict_bytes = reverse_rusty::storage::serialize_dict(&dict);
    let tag_bytes = reverse_rusty::storage::serialize_tagdict(&empty_tag_dict());
    let coordinator_id = RemoteShard::new_coordinator_id();
    match RemoteShard::connect_and_adopt(
        &legacy_ep,
        rt.handle().clone(),
        dict_bytes.clone(),
        dict_fp,
        tag_bytes.clone(),
        tag_fp,
        0,
        coordinator_id,
    ) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected typed ownership refusal, got {e}"),
        Ok(_) => panic!("connect_and_adopt SUCCEEDED against a pre-ADR-109 peer"),
    }

    // 3. A nonzero but stale peer is also refused; zero-only checking would miss this.
    let stale_ep = start_mock(2, 1);
    let stale_coordinator_id = RemoteShard::new_coordinator_id();
    match RemoteShard::connect_and_adopt(
        &stale_ep,
        rt.handle().clone(),
        dict_bytes,
        dict_fp,
        tag_bytes,
        tag_fp,
        0,
        stale_coordinator_id,
    ) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected stale-generation ownership refusal, got {e}"),
        Ok(_) => panic!("connect_and_adopt SUCCEEDED against a stale ownership generation"),
    }

    // Control: a real ADR-109 server attests generation + shard count, so connect succeeds.
    let real_addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    wait_until_listening(real_addr);
    let real_ep = format!("http://{real_addr}");
    RemoteShard::connect(&real_ep, rt.handle().clone(), dict_fp, tag_fp, 0)
        .expect("a real ADR-109 server attests ownership configuration");
}

/// A peer can satisfy ADR-109 ownership yet predate ADR-110. The additive RPC
/// must fail loud with UNIMPLEMENTED; the coordinator never falls back to the
/// unbounded compatibility percolate and presents it as an exact bounded result.
#[test]
fn distributed_top_k_refuses_pre_adr_110_peer() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let tag_dict = empty_tag_dict();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoint = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let address = incoming.local_addr().expect("address");
        let service = ShardServiceServer::new(LegacyOwnershipServer::one_shard(
            dict.fingerprint(),
            tag_dict.fingerprint(),
        ));
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );
        wait_until_listening(address);
        format!("http://{address}")
    };
    let cluster = ClusterEngine::connect_remote(
        norm,
        dict,
        tag_dict,
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        &[endpoint],
        rt.handle(),
    )
    .expect("ADR-109 handshake succeeds");
    let error = cluster
        .checkpoint()
        .expect_err("an unsupported Seal RPC must fail checkpoint");
    assert!(error.to_string().contains("durable Seal RPC"), "{error}");
    let program = cluster
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("rank program");
    let error = cluster
        .try_percolate_filtered_top_k(
            "acme chrome",
            &[],
            reverse_rusty::TopKOptions::default(),
            &program,
            None,
        )
        .expect_err("pre-ADR-110 peer must be refused");
    assert!(matches!(
        error,
        ClusterRankedError::Shard(ShardError::Remote(_))
    ));
}

/// A pre-ADR-163 shard can understand the bounded top-K RPC but cannot attest
/// which ranking profile produced its scores. Even the default static profile
/// must fail closed so a mixed-version mesh cannot present unverified ranking.
#[test]
fn distributed_top_k_refuses_pre_adr_163_profile_echo() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let tag_dict = empty_tag_dict();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoint = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let address = incoming.local_addr().expect("address");
        let service = ShardServiceServer::new(LegacyOwnershipServer {
            top_k_delay: Some(Duration::ZERO),
            ..LegacyOwnershipServer::one_shard(dict.fingerprint(), tag_dict.fingerprint())
        });
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );
        wait_until_listening(address);
        format!("http://{address}")
    };
    let cluster = ClusterEngine::connect_remote(
        norm,
        dict,
        tag_dict,
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        &[endpoint],
        rt.handle(),
    )
    .expect("legacy bounded peer still passes the ownership handshake");
    let program = cluster
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("rank program");
    let error = cluster
        .try_percolate_filtered_top_k(
            "acme chrome",
            &[],
            reverse_rusty::TopKOptions::default(),
            &program,
            None,
        )
        .expect_err("missing profile echo must be refused");
    assert!(matches!(
        error,
        ClusterRankedError::Shard(ShardError::Protocol(ref detail))
            if detail.contains("profile attestation")
    ));
}

#[test]
fn distributed_top_k_keeps_one_absolute_deadline_across_transport() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let tag_dict = empty_tag_dict();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoint = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let address = incoming.local_addr().expect("address");
        let service = ShardServiceServer::new(LegacyOwnershipServer {
            // Far longer than the bound asserted below: a transport that ignored the
            // request's deadline would wait this long, a shared CI runner never does.
            top_k_delay: Some(Duration::from_secs(5)),
            ..LegacyOwnershipServer::one_shard(dict.fingerprint(), tag_dict.fingerprint())
        });
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );
        wait_until_listening(address);
        format!("http://{address}")
    };
    let cluster = ClusterEngine::connect_remote(
        norm,
        dict,
        tag_dict,
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        &[endpoint],
        rt.handle(),
    )
    .expect("remote cluster");
    let program = cluster
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("rank program");
    let started = Instant::now();
    let error = cluster
        .try_percolate_filtered_top_k(
            "acme chrome",
            &[],
            reverse_rusty::TopKOptions::default(),
            &program,
            Some(Instant::now() + Duration::from_millis(15)),
        )
        .expect_err("delayed shard must exceed the request deadline");
    assert!(
        matches!(error, ClusterRankedError::DeadlineExceeded),
        "unexpected deadline error: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "transport did not honor the original absolute deadline: {:?}",
        started.elapsed()
    );
}
