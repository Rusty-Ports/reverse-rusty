use std::future::poll_fn;
use std::sync::mpsc;
use std::task::Poll;

use super::*;

fn seal_req() -> Request<proto::SealRequest> {
    Request::new(proto::SealRequest {
        shard_id: 0,
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
    })
}

/// Queue a real seal behind an occupied blocking pool, then cancel its RPC. Re-adoption
/// must wait for the worker rather than let its old checkpoint erase a later ingest.
#[test]
fn cancelled_seal_worker_cannot_overwrite_replacement_checkpoint() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["sealneedle"], &n);
    let dir = std::env::temp_dir().join(format!("rr_cancelled_seal_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let srv = ShardServer::pending_durable(Arc::clone(&n), EngineConfig::default(), dir.clone());
    rt.block_on(srv.adopt_dict(adopt_req(&d)))
        .expect("initial adopt");

    // Occupying the sole blocking worker makes the seal's enqueue deterministic.
    let (release, waiting) = mpsc::channel();
    let (ready, started) = mpsc::channel();
    let blocker = rt.spawn_blocking(move || {
        ready.send(()).expect("announce blocker");
        waiting.recv().expect("release blocker");
    });
    started.recv().expect("blocker running");

    rt.block_on(async {
        let mut seal = srv.seal(seal_req());
        poll_fn(|cx| {
            assert!(seal.as_mut().poll(cx).is_pending(), "worker is queued");
            Poll::Ready(())
        })
        .await;
        drop(seal); // The blocking worker retains its guard after RPC cancellation.

        let mut replacement = adopt_req(&d);
        replacement.get_mut().placement_generation = 2;
        let mut adopt = srv.adopt_dict(replacement);
        poll_fn(|cx| {
            assert!(
                adopt.as_mut().poll(cx).is_pending(),
                "replacement must wait for the cancelled seal worker"
            );
            Poll::Ready(())
        })
        .await;

        // Another seal waits behind adoption, and must revalidate its old stamp after
        // the lock becomes available rather than sealing a stale snapshot.
        let mut stale_seal = srv.seal(seal_req());
        poll_fn(|cx| {
            assert!(stale_seal.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).expect("release queued worker");
        blocker.await.expect("blocker completes");
        adopt
            .await
            .expect("replacement adopts after seal completes");
        assert_eq!(
            stale_seal.await.expect_err("stale placement").code(),
            Code::FailedPrecondition
        );
    });

    srv.ingest_dsl(&[(42, "sealneedle".to_string())]);
    let reopened = ShardServer::open_durable(n, EngineConfig::default(), dir.clone())
        .expect("reopen replacement checkpoint");
    let count = rt
        .block_on(reopened.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect("restored count")
        .into_inner()
        .count;
    assert_eq!(count, 1, "replacement ingest survives the cancelled seal");
    let _ = std::fs::remove_dir_all(dir);
}
