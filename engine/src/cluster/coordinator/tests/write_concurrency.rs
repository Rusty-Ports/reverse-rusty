use super::*;

mod bench;
mod harness;
use harness::{instrument, pause, WriteCall};

#[test]
fn formerly_colliding_write_proceeds_during_failed_log_append() {
    let mut cluster =
        ClusterEngine::build(vocab(), &ClusterConfig::default(), &[]).expect("empty cluster");
    let gate = Arc::new(FirstAppendGate::default());
    cluster.log = Box::new(FailFirstAppendLog {
        gate: Arc::clone(&gate),
    });
    std::thread::scope(|scope| {
        let first = scope.spawn(|| cluster.add_query(7, "zzfirst"));
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let second = scope.spawn(move || {
            let result = cluster.add_query(263, "zzsecond");
            tx.send(result).expect("second write result");
        });
        let independent = rx.recv_timeout(Duration::from_secs(5));
        gate.release_first();
        assert!(matches!(
            first.join().expect("first writer"),
            Err(ShardError::Log(_))
        ));
        second.join().expect("second writer");
        independent
            .expect("unrelated write completes before first append returns")
            .expect("unrelated write accepted");
    });
    cluster
        .add_query(7, "zzfirst")
        .expect("failed reservation released");
    assert_eq!(cluster.percolate("zzfirst").expect("first"), vec![7]);
    assert_eq!(cluster.percolate("zzsecond").expect("second"), vec![263]);
}

#[test]
fn same_id_log_order_spans_complete_fanout_and_reopen() {
    let dir = scratch_dir("per_id_fanout");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("durable cluster");
    let gate = Arc::new(FirstAppendGate::default());
    let pause_once = Arc::new(AtomicBool::new(true));
    instrument(&mut cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |position, call| {
            if position == 2
                && call == WriteCall::Delete(7)
                && pause_once.swap(false, Ordering::SeqCst)
            {
                pause(&gate);
            }
            Ok(())
        })
    });
    std::thread::scope(|scope| {
        let first = scope.spawn(|| cluster.upsert_query(7, "zzoldbody", 1));
        gate.wait_until_entered();
        assert_eq!(cluster.log.last_pos().expect("first logged"), LogPos(1));
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let second = scope.spawn(move || {
            let result = cluster.upsert_query(7, "zznewbody", 2);
            tx.send(result).expect("second result");
        });
        let early = rx.recv_timeout(Duration::from_millis(50));
        let log_before_release = cluster.log.last_pos().expect("log position");
        let (other_tx, other_rx) = mpsc::channel();
        let other = scope.spawn(move || {
            other_tx
                .send(cluster.upsert_query(263, "zzindependent", 1))
                .expect("other result");
        });
        let independent = other_rx.recv_timeout(Duration::from_secs(5));
        gate.release_first();
        first.join().expect("first writer").expect("first accepted");
        second.join().expect("second writer");
        other.join().expect("other writer");
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(
            log_before_release,
            LogPos(1),
            "same-ID log append cannot overtake apply"
        );
        independent
            .expect("colliding ID proceeds through fanout")
            .expect("other accepted");
        rx.recv_timeout(Duration::from_secs(5))
            .expect("second returns")
            .expect("second accepted");
    });
    let titles = ["zzoldbody", "zznewbody", "zzindependent"];
    let expected = [vec![], vec![7], vec![263]];
    for (title, ids) in titles.iter().zip(&expected) {
        assert_eq!(&cluster.percolate(title).expect("live result"), ids);
    }
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    for (title, ids) in titles.iter().zip(&expected) {
        assert_eq!(&reopened.percolate(title).expect("replayed result"), ids);
    }
    drop(reopened);
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn bulk_load_excludes_create_until_directory_and_shards_agree() {
    let mut cluster =
        ClusterEngine::build(vocab(), &ClusterConfig::default(), &[]).expect("empty cluster");
    let gate = Arc::new(FirstAppendGate::default());
    instrument(&mut cluster, {
        let gate = Arc::clone(&gate);
        Arc::new(move |_, call| {
            if call == WriteCall::Bulk {
                pause(&gate);
            }
            Ok(())
        })
    });
    std::thread::scope(|scope| {
        let bulk = scope.spawn(|| cluster.ingest(&[(263, "zzbulkbody".to_string())]));
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let writer = scope.spawn(move || {
            tx.send(cluster.add_query(263, "zzlivebody"))
                .expect("create result");
        });
        let early = rx.recv_timeout(Duration::from_millis(50));
        gate.release_first();
        bulk.join()
            .expect("bulk thread")
            .expect("bulk load succeeds");
        writer.join().expect("writer thread");
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("create returns"),
            Err(ShardError::DuplicateLogicalId(263))
        ));
    });
    assert_eq!(
        cluster.log.last_pos().expect("no conflicting append"),
        LogPos(0)
    );
    assert_eq!(
        cluster.percolate("zzbulkbody").expect("bulk body"),
        vec![263]
    );
    assert!(cluster
        .percolate("zzlivebody")
        .expect("rejected body")
        .is_empty());
}

#[test]
fn resync_does_not_reapply_a_repair_superseded_by_a_successful_write() {
    let dir = scratch_dir("per_id_repair");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("durable cluster");
    let fail = Arc::new(AtomicBool::new(true));
    let gate = Arc::new(FirstAppendGate::default());
    instrument(&mut cluster, {
        let fail = Arc::clone(&fail);
        let gate = Arc::clone(&gate);
        Arc::new(move |position, call| {
            if fail.load(Ordering::SeqCst) {
                return Err(ShardError::Remote("injected failure".into()));
            }
            if position == 2 && call == WriteCall::Delete(7) {
                pause(&gate);
            }
            Ok(())
        })
    });
    for logical in [7, 263] {
        assert!(matches!(
            cluster.upsert_query(logical, "zzoldbody", 1),
            Err(ShardError::PartiallyApplied { .. })
        ));
    }
    assert_eq!(cluster.pending_repairs(), 2);
    fail.store(false, Ordering::SeqCst);
    std::thread::scope(|scope| {
        let repair = scope.spawn(|| cluster.resync());
        gate.wait_until_entered();
        let (tx, rx) = mpsc::channel();
        let cluster = &cluster;
        let writer = scope.spawn(move || {
            tx.send(cluster.upsert_query(263, "zznewbody", 2))
                .expect("new write");
        });
        let independent = rx.recv_timeout(Duration::from_secs(5));
        gate.release_first();
        writer.join().expect("writer thread");
        let report = repair.join().expect("repair thread");
        independent
            .expect("different ID proceeds while repair waits")
            .expect("replacement accepted");
        assert_eq!(report.still_pending, 0);
    });
    assert_eq!(cluster.pending_repairs(), 0);
    let live_old = cluster.percolate("zzoldbody").expect("old live");
    let live_new = cluster.percolate("zznewbody").expect("new live");
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
    assert_eq!(
        live_old,
        reopened.percolate("zzoldbody").expect("old replay")
    );
    assert_eq!(
        live_new,
        reopened.percolate("zznewbody").expect("new replay")
    );
    assert_eq!(live_old, vec![7]);
    assert_eq!(live_new, vec![263]);
    drop(reopened);
    std::fs::remove_dir_all(dir).expect("cleanup");
}
