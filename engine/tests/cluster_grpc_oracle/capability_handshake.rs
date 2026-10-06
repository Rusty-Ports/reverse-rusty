//! Capability handshake: a shard server that does not attest the atomic per-shard replace
//! (ADR-185), or the title view that carries an alias form by its words (ADR-205), is refused
//! by every way a coordinator can connect to it.

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
    refuses_a_peer_without("ADR-185", |server, attested| {
        server.atomic_replace = attested;
    });
}

/// A coordinator routes a title by a view that holds a multi-word alias form whenever it
/// holds the form's words, and a shard verifies by its own view. Against a server whose
/// view lacks the rule, the answer would silently lack the matches the rule adds.
#[test]
fn every_grpc_handshake_refuses_a_peer_without_alias_form_words() {
    refuses_a_peer_without("ADR-205", |server, attested| {
        server.alias_form_words = attested;
    });
}

fn refuses_a_peer_without(decision: &str, attest: impl Fn(&mut LegacyOwnershipServer, bool)) {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint();
    let dict_bytes = reverse_rusty::storage::serialize_dict(&dict);
    let tag_bytes = reverse_rusty::storage::serialize_tagdict(&empty_tag_dict());
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let start_mock = |attested| {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let mut server = LegacyOwnershipServer::one_shard(dict_fp, tag_fp);
        attest(&mut server, attested);
        let svc = ShardServiceServer::new(server);
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
                assert!(message.contains(decision), "{name}: {message}");
            }
            Err(e) => panic!("{name}: expected the {decision} refusal, got {e}"),
            Ok(_) => panic!("{name} SUCCEEDED against a peer without the {decision} capability"),
        }
        // Control: the same mock attesting the capability is accepted.
        if let Err(e) = handshake(&start_mock(true)) {
            panic!("{name}: a peer that attests the {decision} capability was refused: {e}");
        }
    }
}
