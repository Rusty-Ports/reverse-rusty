//! A checkpoint describes one set of writes (ADR-197).
//!
//! The manifest a checkpoint commits says "every write up to log position P is in these
//! segments", and the log is then truncated through P. That is only true if no write is
//! half-way between its log append and its shard when the checkpoint looks. The public
//! `checkpoint`, `flush` and `backup_to` therefore wait for writes in flight and hold new
//! ones back. Each test pauses one side at the point where the other used to slip past.

use super::*;

/// How long a call is given to finish while the thing it must wait for is paused. A call
/// that does not wait finishes in well under this; a call that waits cannot finish at all.
const WOULD_HAVE_FINISHED: Duration = Duration::from_millis(500);

fn durable(tag: &str) -> (ClusterEngine, ClusterConfig, PathBuf) {
    let dir = scratch_dir(tag);
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("durable cluster");
    (cluster, cfg, dir)
}

/// Pause the first shard call that `matches`, at the moment it is about to run.
fn pause_first(
    cluster: &mut ClusterEngine,
    matches: impl Fn(WriteCall) -> bool + Send + Sync + 'static,
) -> Arc<FirstAppendGate> {
    let gate = Arc::new(FirstAppendGate::default());
    let once = Arc::new(AtomicBool::new(true));
    instrument(cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |_position, call| {
            if matches(call) && once.swap(false, Ordering::SeqCst) {
                pause(&gate);
            }
            Ok(())
        })
    });
    gate
}

/// A write that is logged but has not reached its shard yet. A checkpoint that ran now
/// would seal the shard without the row, record a log position that covers the write, and
/// truncate the frame: the acknowledged write would exist only in a memtable.
#[test]
fn a_checkpoint_waits_for_a_write_that_has_not_reached_its_shard() {
    let (mut cluster, cfg, dir) = durable("ckpt_waits_for_write");
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Insert(7)));
    let early = std::thread::scope(|scope| {
        let writer = scope.spawn(|| cluster.add_query(7, "zzcheckpointed"));
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let checkpointer = scope.spawn(move || {
            tx.send(cluster.checkpoint()).expect("checkpoint result");
        });
        let early = rx.recv_timeout(WOULD_HAVE_FINISHED);
        gate.release_first();
        writer.join().expect("writer").expect("write accepted");
        checkpointer.join().expect("checkpointer");
        let checkpointed = match &early {
            Ok(_) => Ok(()),
            Err(_) => rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the checkpoint returns once the write is done"),
        };
        checkpointed.expect("checkpoint");
        early.map(|result| result.is_ok())
    });
    assert!(
        early.is_err(),
        "the checkpoint finished while a logged write had not reached its shard"
    );
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    assert_eq!(
        reopened.percolate("zzcheckpointed").expect("percolate"),
        vec![7],
        "an acknowledged write must survive a checkpoint and a restart"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A checkpoint that has read its log position and is still sealing. A create that ran now
/// would be sealed into the segments AND stay in the log tail, and the reopen would refuse
/// the cluster for a duplicate id.
#[test]
fn a_write_waits_for_a_checkpoint_that_has_read_its_log_position() {
    let (mut cluster, cfg, dir) = durable("write_waits_for_ckpt");
    cluster.add_query(1, "zzbefore").expect("a first write");
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Seal));
    let early = std::thread::scope(|scope| {
        let checkpointer = scope.spawn(|| cluster.checkpoint());
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let writer = scope.spawn(move || {
            tx.send(cluster.add_query(9, "zzslipped"))
                .expect("write result");
        });
        let early = rx.recv_timeout(WOULD_HAVE_FINISHED);
        gate.release_first();
        checkpointer
            .join()
            .expect("checkpointer")
            .expect("checkpoint");
        writer.join().expect("writer");
        if let Ok(result) = early {
            result.expect("write accepted");
            true
        } else {
            rx.recv_timeout(Duration::from_secs(10))
                .expect("the write returns once the checkpoint is done")
                .expect("write accepted");
            false
        }
    });
    assert!(!early, "a write completed inside a checkpoint");
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg))
        .expect("the cluster reopens: the write is in the segments or in the log, not both");
    assert_eq!(reopened.percolate("zzslipped").expect("percolate"), vec![9]);
    assert_eq!(reopened.percolate("zzbefore").expect("percolate"), vec![1]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two checkpoints each compute the next epoch from the current one. Run together they
/// would commit the same epoch, and one's orphan sweep could delete the other's segments.
#[test]
fn concurrent_checkpoints_run_one_after_the_other() {
    let (mut cluster, _cfg, dir) = durable("ckpt_serial");
    cluster.add_query(1, "zzepoch").expect("a write");
    let before = cluster.epoch();
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Seal));
    std::thread::scope(|scope| {
        let first = scope.spawn(|| cluster.checkpoint());
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let second = scope.spawn(move || {
            tx.send(cluster.checkpoint()).expect("checkpoint result");
        });
        let early = rx.recv_timeout(WOULD_HAVE_FINISHED);
        gate.release_first();
        first.join().expect("first").expect("first checkpoint");
        second.join().expect("second");
        if early.is_err() {
            rx.recv_timeout(Duration::from_secs(10))
                .expect("the second checkpoint returns")
                .expect("second checkpoint");
        }
    });
    assert_eq!(
        cluster.epoch(),
        before + 2,
        "each checkpoint commits its own epoch"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A flush writes a segment file. Between a checkpoint's list of segments and its sweep of
/// files that are not on the list, that file would be swept.
#[test]
fn a_flush_waits_for_a_checkpoint() {
    let (mut cluster, _cfg, dir) = durable("flush_waits_for_ckpt");
    cluster.add_query(1, "zzflushed").expect("a write");
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Seal));
    let early = std::thread::scope(|scope| {
        let checkpointer = scope.spawn(|| cluster.checkpoint());
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let flusher = scope.spawn(move || {
            tx.send(cluster.flush()).expect("flush result");
        });
        let early = rx.recv_timeout(WOULD_HAVE_FINISHED).is_ok();
        gate.release_first();
        checkpointer
            .join()
            .expect("checkpointer")
            .expect("checkpoint");
        flusher.join().expect("flusher");
        early
    });
    assert!(!early, "a flush ran inside a checkpoint");
    assert_eq!(cluster.percolate("zzflushed").expect("percolate"), vec![1]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A backup is a checkpoint and a copy. Taken without any lock of the caller's, it still
/// holds every write that was acknowledged before it returned.
#[test]
fn a_backup_waits_for_a_write_and_contains_it() {
    let (mut cluster, cfg, dir) = durable("backup_waits_for_write");
    let dest = scratch_dir("backup_waits_for_write_dest");
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Insert(7)));
    let early = std::thread::scope(|scope| {
        let writer = scope.spawn(|| cluster.add_query(7, "zzbackedup"));
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let dest = &dest;
        let backup = scope.spawn(move || {
            tx.send(cluster.backup_to(dest)).expect("backup result");
        });
        let early = rx.recv_timeout(WOULD_HAVE_FINISHED).is_ok();
        gate.release_first();
        writer.join().expect("writer").expect("write accepted");
        backup.join().expect("backup");
        if !early {
            rx.recv_timeout(Duration::from_secs(10))
                .expect("the backup returns once the write is done")
                .expect("backup");
        }
        early
    });
    assert!(
        !early,
        "the backup ran while a write had not reached its shard"
    );
    drop(cluster);
    let restored = ClusterEngine::open(dest.clone(), vocab(), Some(&cfg)).expect("open the backup");
    assert_eq!(
        restored.percolate("zzbackedup").expect("percolate"),
        vec![7]
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dest);
}

/// Two backups to the same destination. The second waits for the first, then finds the
/// destination taken and is refused before it checkpoints: a request that cannot succeed
/// must not bump the epoch and truncate the log on its way to saying so.
#[test]
fn a_second_backup_to_the_same_destination_is_refused_without_a_checkpoint() {
    let (mut cluster, _cfg, dir) = durable("backup_same_dest");
    let dest = scratch_dir("backup_same_dest_dest");
    cluster.add_query(1, "zzbackedup").expect("a write");
    let before = cluster.epoch();
    let gate = pause_first(&mut cluster, |call| matches!(call, WriteCall::Seal));
    let second = std::thread::scope(|scope| {
        let cluster = &cluster;
        let dest = &dest;
        let first = scope.spawn(move || cluster.backup_to(dest));
        gate.wait_until_entered();
        let second = scope.spawn(move || cluster.backup_to(dest));
        // Give the second time to get as far as it can while the first is paused.
        std::thread::sleep(WOULD_HAVE_FINISHED);
        gate.release_first();
        first.join().expect("first").expect("the first backup");
        second.join().expect("second")
    });
    assert!(
        matches!(second, Err(ShardError::Config(_))),
        "the destination exists by then: {second:?}"
    );
    assert_eq!(
        cluster.epoch(),
        before + 1,
        "only the backup that succeeded checkpointed"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dest);
}

/// A bulk load into a durable cluster checkpoints at its end while it still holds the
/// barrier shared. It must use the variant that does not take the barrier again, or it
/// would wait for itself.
#[test]
fn a_bulk_load_checkpoints_without_waiting_for_itself() {
    let (cluster, cfg, dir) = durable("bulk_ckpt");
    let before = cluster.epoch();
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let cluster = &cluster;
        scope.spawn(move || {
            tx.send(cluster.ingest(&[(1, "zzbulked".to_string())]))
                .expect("ingest result");
        });
        rx.recv_timeout(Duration::from_secs(20))
            .expect("the bulk load returns")
            .expect("bulk load");
    });
    assert_eq!(cluster.epoch(), before + 1, "the load was checkpointed");
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    assert_eq!(reopened.percolate("zzbulked").expect("percolate"), vec![1]);
    let _ = std::fs::remove_dir_all(&dir);
}
