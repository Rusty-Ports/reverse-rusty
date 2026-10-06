//! ADR-185 capability handshake: a shard server that does not attest the atomic per-shard
//! replace is refused by every way a coordinator can connect to it.

use std::sync::Arc;

use raw::shard_service_server::ShardServiceServer;
use reverse_rusty::cluster::{RemoteShard, ShardError};
use reverse_rusty_shard_proto as raw;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;
use crate::legacy_layout::LegacyOwnershipServer;

type Handshake<'a> = (
    &'a str,
    Box<dyn Fn(&str) -> Result<RemoteShard, ShardError> + 'a>,
);

/// Against such a server a cluster upsert could only be the reader-visible
/// delete-then-insert, so the coordinator fails loud at startup instead of at the first
/// re-put. A normal startup adopts (and adds co-located slots) without ever probing, so the
/// check must not live on the probe alone.
#[test]
fn every_grpc_handshake_refuses_a_peer_without_atomic_replace() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint();
    let dict_bytes = reverse_rusty::storage::serialize_dict(&dict);
    let tag_bytes = reverse_rusty::storage::serialize_tagdict(&empty_tag_dict());
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let start_mock = |atomic_replace| {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let svc = ShardServiceServer::new(LegacyOwnershipServer {
            atomic_replace,
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
    let handshakes: Vec<Handshake<'_>> = vec![
        (
            "connect",
            Box::new(|endpoint| {
                RemoteShard::connect(endpoint, rt.handle().clone(), dict_fp, tag_fp, 0)
            }),
        ),
        (
            "connect_and_adopt",
            Box::new(|endpoint| {
                RemoteShard::connect_and_adopt(
                    endpoint,
                    rt.handle().clone(),
                    dict_bytes.clone(),
                    dict_fp,
                    tag_bytes.clone(),
                    tag_fp,
                    0,
                    RemoteShard::new_coordinator_id(),
                )
            }),
        ),
        (
            "connect_and_add_shard",
            Box::new(|endpoint| {
                RemoteShard::connect_and_add_shard(
                    endpoint,
                    rt.handle().clone(),
                    dict_fp,
                    tag_fp,
                    0,
                    RemoteShard::new_coordinator_id(),
                )
            }),
        ),
    ];
    for (name, handshake) in &handshakes {
        match handshake(&start_mock(false)) {
            Err(ShardError::Remote(message)) => {
                assert!(message.contains("ADR-185"), "{name}: {message}");
            }
            Err(e) => panic!("{name}: expected the ADR-185 refusal, got {e}"),
            Ok(_) => panic!("{name} SUCCEEDED against a pre-ADR-185 peer"),
        }
        // Control: the same mock attesting the capability is accepted.
        if let Err(e) = handshake(&start_mock(true)) {
            panic!("{name}: a peer that attests the atomic replace was refused: {e}");
        }
    }
}
