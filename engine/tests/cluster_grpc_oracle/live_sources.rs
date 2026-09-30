//! ADR-180 live-corpus export over gRPC: every live query, exactly once, with its version and
//! tags, across co-located and separate slots; frames are byte-capped and complete.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{
    AddOutcome, ClusterConfig, ClusterEngine, ExportedQuery, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

fn spawn(rt: &tokio::runtime::Runtime, server: ShardServer) -> (String, JoinHandle<()>) {
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
    let address: SocketAddr = incoming.local_addr().expect("bound address");
    let task = rt.spawn(async move {
        server.serve_with_incoming(incoming).await.expect("serve");
    });
    (format!("http://{address}"), task)
}

/// Source, version, and raw tags expected for one exported query.
type ExpectedQuery = (String, u32, Vec<(String, String)>);

#[test]
fn grpc_live_corpus_export_is_complete_deduplicated_and_versioned() {
    let (mut queries, _titles) = build_corpus();
    queries.truncate(400);
    let tags = tags_parallel(&queries);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tag_dict = frozen_tag_dict_over(&tags);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    // Two nodes, three positions: one node co-locates two slots.
    let a = spawn(
        &rt,
        ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
    )
    .0;
    let b = spawn(
        &rt,
        ShardServer::pending(Arc::clone(&norm), EngineConfig::default()),
    )
    .0;
    let endpoints = vec![a.clone(), b, a];
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        tag_dict,
        &config,
        &endpoints,
        rt.handle(),
        801,
    )
    .expect("connect");
    cluster.ingest_with_tags(&queries, &tags).expect("ingest");
    let removed = queries[3].0;
    cluster.remove_query(removed).expect("remove");
    let upserted = queries[5].0;
    cluster
        .upsert_query_with_tags(upserted, "1994 vertex zzupserted", 7, &[])
        .expect("upsert");

    let mut exported: BTreeMap<u64, ExportedQuery> = BTreeMap::new();
    let count = cluster
        .export_live_corpus(&mut |query| {
            assert!(
                exported.insert(query.logical_id, query).is_none(),
                "a query was exported twice"
            );
            Ok(())
        })
        .expect("export");
    assert_eq!(count as usize, exported.len());

    let expected: BTreeMap<u64, ExpectedQuery> = queries
        .iter()
        .zip(&tags)
        .filter(|((id, _), _)| *id != removed)
        .map(|((id, dsl), tags)| {
            if *id == upserted {
                (*id, ("1994 vertex zzupserted".to_string(), 7, Vec::new()))
            } else {
                (*id, (dsl.clone(), 1, tags.clone()))
            }
        })
        .collect();
    let rejected: Vec<u64> = expected
        .keys()
        .filter(|id| !exported.contains_key(id))
        .copied()
        .collect();
    // Rows the front door rejected (class D with the lane off, parse errors) are never stored;
    // everything else must round-trip exactly.
    for (id, query) in &exported {
        let (dsl, version, tags) = expected.get(id).expect("exported an unknown query");
        assert_eq!(&query.dsl, dsl, "source of {id}");
        assert_eq!(query.version, *version, "version of {id}");
        assert_eq!(&query.tags, tags, "tags of {id}");
    }
    // Anything not exported must be a query the front door never stores.
    let probe = ClusterEngine::build(vocab(), &config, &[]).expect("probe cluster");
    for id in &rejected {
        let (dsl, _, _) = &expected[id];
        let outcome = probe.add_query(*id, dsl).expect("probe add");
        assert!(
            matches!(
                outcome,
                AddOutcome::RejectedClassD | AddOutcome::RejectedParse(_)
            ),
            "stored query {id} ({dsl:?}) was not exported: {outcome:?}"
        );
    }
    assert!(!exported.is_empty());
}

#[test]
fn grpc_live_corpus_export_streams_many_small_frames_and_refuses_an_oversized_document() {
    let queries: Vec<(u64, String)> = (1..=300)
        .map(|id| (id, format!("framedneedle{id} vintage")))
        .collect();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let small = |bytes| {
        ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
            .with_max_grpc_result_bytes(bytes)
            .expect("cap")
    };
    let endpoints = vec![spawn(&rt, small(1024)).0, spawn(&rt, small(1024)).0];
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..Default::default()
    };
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &config,
        &endpoints,
        rt.handle(),
        802,
    )
    .expect("connect");
    cluster.ingest(&queries).expect("ingest");
    let mut ids = Vec::new();
    let count = cluster
        .export_live_corpus(&mut |query| {
            ids.push(query.logical_id);
            Ok(())
        })
        .expect("a many-frame export completes");
    ids.sort_unstable();
    assert_eq!(count, 300);
    assert_eq!(ids, (1..=300).collect::<Vec<u64>>());

    // A single source larger than the cap cannot be split, so the export fails loud.
    let huge = format!("framedneedle1 zz{}", "p".repeat(1500));
    let (_, outcome) = cluster.upsert_query(1, &huge, 2).expect("upsert oversized");
    assert!(
        matches!(outcome, AddOutcome::Placed { .. } | AddOutcome::Replicated),
        "the oversized source must be stored: {outcome:?}"
    );
    let error = cluster
        .export_live_corpus(&mut |_| Ok(()))
        .expect_err("an oversized document must fail the export");
    assert!(
        error.to_string().contains("byte cap"),
        "unexpected error: {error}"
    );
}
