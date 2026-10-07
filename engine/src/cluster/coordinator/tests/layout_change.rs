//! A layout change has the engine to itself except for searches, and leaves a replaced
//! layout's files alone until nothing is running on it (ADR-209).

use super::*;

fn corpus(count: u64) -> Vec<(u64, String)> {
    (1..=count)
        .map(|id| (id, format!("zzitem{id} zzgroup{}", id % 7)))
        .collect()
}

fn in_memory(shards: usize, count: u64) -> ClusterEngine {
    let cfg = ClusterConfig {
        num_shards: shards,
        include_broad: true,
        ..Default::default()
    };
    ClusterEngine::build(vocab(), &cfg, &corpus(count)).expect("cluster")
}

/// A mutation takes the layout lock, then the mutation barrier, and only then loads the
/// layout. It is stopped between the locks and the load while a resize starts. Taken in that
/// order, the resize waits for it and the write is in the layout the resize builds. Loaded
/// first, the mutation would hold nothing while it was stopped, the resize would finish, and
/// the mutation would then write to shards that have been replaced.
#[test]
fn a_layout_change_waits_for_a_mutation_that_has_been_admitted() {
    let cluster = in_memory(3, 200);
    let (paused, is_paused) = mpsc::channel();
    let (resume, resumed) = mpsc::channel::<()>();
    let resumed = Mutex::new(resumed);
    let first = AtomicBool::new(true);
    *cluster.admission_hook.lock().expect("hook") = Some(Arc::new(move || {
        if first.swap(false, Ordering::SeqCst) {
            let _ = paused.send(());
            // Bounded, so that a broken build fails the test instead of hanging it.
            let _ = resumed
                .lock()
                .expect("resumed")
                .recv_timeout(Duration::from_secs(10));
        }
    }));
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| cluster.add_query(9_001, "zzlate zzarrival"));
        is_paused
            .recv_timeout(Duration::from_secs(10))
            .expect("the write never reached its admission");
        let resizer = scope.spawn(|| cluster.resize(5));
        // Give the resize time to get as far as it can beside the stopped mutation.
        std::thread::sleep(Duration::from_millis(200));
        let resized_beside_it = resizer.is_finished();
        resume.send(()).expect("resume");
        let written = writer.join().expect("writer");
        resizer.join().expect("resizer").expect("resize");

        let matched = cluster
            .percolate_with_broad("zzlate zzarrival", true)
            .expect("read");
        assert!(
            !resized_beside_it,
            "the resize did not wait for a mutation that had been admitted (the write: {written:?})"
        );
        written.expect("an admitted write is applied");
        assert_eq!(
            matched,
            vec![9_001],
            "the write was acknowledged and no read finds it"
        );
    });
    assert_eq!(cluster.num_shards(), 5);
}

#[test]
fn a_layout_change_holds_writes_back_and_lets_searches_through() {
    let cluster = in_memory(3, 200);
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let add = scope.spawn(|| cluster.add_query(9_002, "zzheld zzback"));
        let remove = scope.spawn(|| cluster.remove_query(2));
        std::thread::sleep(Duration::from_millis(150));
        let wrote_beside_it = add.is_finished() || remove.is_finished();
        // Searches, of every kind, run on the layout that is published.
        let read = cluster.percolate_with_broad("zzitem5 zzgroup5", true);
        let view = cluster
            .consistent_read_view()
            .percolate_filtered_with_stats("zzitem6 zzgroup6", &[], true)
            .map(|(matched, _)| matched);
        let counted = cluster.num_queries();
        let untouched = cluster.percolate_with_broad("zzitem2 zzgroup2", true);
        // Released before anything can fail: both writes are waiting for it.
        drop(change);
        let added = add.join().expect("add thread");
        let removed = remove.join().expect("remove thread");

        assert!(!wrote_beside_it, "a write ran beside a layout change");
        assert_eq!(read.expect("read"), vec![5]);
        assert_eq!(view.expect("frozen view"), vec![6]);
        assert!(counted.expect("count") > 0);
        assert_eq!(
            untouched.expect("read"),
            vec![2],
            "the remove had not run yet"
        );
        assert!(matches!(
            added,
            Ok(AddOutcome::Placed { .. } | AddOutcome::Replicated { .. })
        ));
        removed.expect("remove");
    });
    assert_eq!(
        cluster
            .percolate_with_broad("zzheld zzback", true)
            .expect("read"),
        vec![9_002]
    );
    assert!(cluster
        .percolate_with_broad("zzitem2 zzgroup2", true)
        .expect("read")
        .is_empty());
}

/// A load snapshot is in this list because it reads the control state together with the
/// layout, and a layout change updates the one after the other.
#[test]
fn everything_but_a_search_waits_for_a_layout_change() {
    let cluster = in_memory(3, 100);
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let checkpoint = scope.spawn(|| cluster.checkpoint());
        let resize = scope.spawn(|| cluster.resize(4));
        let load = scope
            .spawn(|| cluster.collect_load(&crate::cluster::autoscale::AutoscaleConfig::default()));
        std::thread::sleep(Duration::from_millis(150));
        let ran_beside_it = [
            ("a checkpoint", checkpoint.is_finished()),
            ("a second layout change", resize.is_finished()),
            ("a load snapshot", load.is_finished()),
        ];
        // Released before anything can fail: all three are waiting for it.
        drop(change);
        checkpoint
            .join()
            .expect("checkpoint thread")
            .expect("checkpoint");
        resize.join().expect("resize thread").expect("resize");
        let snapshot = load.join().expect("load thread").expect("load snapshot");
        for (what, ran) in ran_beside_it {
            assert!(!ran, "{what} ran beside a layout change");
        }
        assert_eq!(snapshot.num_shards as usize, snapshot.shard_corpus.len());
    });
    assert_eq!(cluster.num_shards(), 4);
}

/// The control state is keyed by shard position, so a write to it waits for a layout change
/// like any other operation. A rebalance that read three positions, let a resize to one
/// finish, and then committed its three assignments left a map that no later resize accepted.
#[test]
fn a_write_to_the_control_state_waits_for_a_layout_change() {
    let cluster = in_memory(3, 100);
    let before = cluster.control_state().expect("control state");
    let node = before.nodes[0].clone();
    let first = cluster.assignment_for(0).expect("assignment");
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let rebalance = scope.spawn(|| cluster.rebalance(1));
        let assign = scope.spawn(|| cluster.reassign_shard(first));
        // Both leave the membership as it is: the node is already registered, and no node
        // has the other id.
        let join = scope.spawn(|| cluster.register_node(node));
        let leave = scope.spawn(|| cluster.deregister_node(crate::cluster::NodeId(u64::MAX)));
        std::thread::sleep(Duration::from_millis(150));
        let ran_beside_it = [
            ("a rebalance", rebalance.is_finished()),
            ("a shard assignment", assign.is_finished()),
            ("a node registration", join.is_finished()),
            ("a node deregistration", leave.is_finished()),
        ];
        let resized = cluster.resize_in(&change, 1);
        // Released before anything can fail: all four are waiting for it.
        drop(change);
        let rebalanced = rebalance.join().expect("rebalance thread");
        let assigned = assign.join().expect("assign thread");
        let joined = join.join().expect("join thread");
        let left = leave.join().expect("leave thread");
        resized.expect("resize");
        rebalanced.expect("rebalance");
        assigned.expect("assign");
        joined.expect("register");
        left.expect("deregister");
        for (what, ran) in ran_beside_it {
            assert!(!ran, "{what} ran beside a layout change");
        }
    });
    let after = cluster.control_state().expect("control state");
    assert_eq!(after.num_shards, 1);
    assert_eq!(
        after.assignments.len(),
        1,
        "the map has an assignment for a position the resize removed: {:?}",
        after.assignments
    );
    cluster.resize(2).expect("a later resize");
}

/// The other direction: an operation that is not a search holds the layout lock until it is
/// done, so a layout change that arrives meanwhile waits for it.
#[test]
fn a_layout_change_waits_for_an_operation_in_flight() {
    let cluster = in_memory(3, 100);
    std::thread::scope(|scope| {
        let in_flight = cluster.stable();
        let resize = scope.spawn(|| cluster.resize(4));
        std::thread::sleep(Duration::from_millis(150));
        let changed_beside_it = resize.is_finished() || !cluster.is_published(&in_flight.layout);
        // Released before anything can fail: the resize is waiting for it.
        drop(in_flight);
        resize.join().expect("resize thread").expect("resize");
        assert!(
            !changed_beside_it,
            "the layout was changed under an operation that was still running"
        );
    });
    assert_eq!(cluster.num_shards(), 4);
}

fn segment_files(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "seg") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// An operation that loaded a layout finishes on it, so its files have to outlive the swap:
/// they go only when the last such operation has returned.
#[test]
fn the_files_of_a_replaced_layout_stay_until_nothing_runs_on_it() {
    let dir = scratch_dir("retired_layout_files");
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus(300)).expect("durable cluster");
    cluster.checkpoint().expect("checkpoint");
    let before = segment_files(&dir);
    assert!(!before.is_empty(), "the checkpoint wrote no segment");

    // A read in flight: it holds the layout it loaded.
    let held = cluster.layout();
    cluster.resize(5).expect("resize");
    let kept: Vec<bool> = before.iter().map(|path| path.exists()).collect();
    let still_reads = held.shards[0].live_sources().map(|rows| rows.len());
    drop(held);

    assert!(
        kept.iter().all(|kept| *kept),
        "a segment of the replaced layout was removed while an operation still ran on it"
    );
    assert!(still_reads.expect("read on the replaced layout") > 0);
    // With the layout released, the next checkpoint removes what it left.
    cluster.checkpoint().expect("checkpoint after release");
    let left: Vec<&PathBuf> = before.iter().filter(|path| path.exists()).collect();
    assert!(
        left.is_empty(),
        "superseded segments were never removed: {left:?}"
    );
    for id in [1u64, 150, 300] {
        let title = format!("zzitem{id} zzgroup{}", id % 7);
        assert_eq!(
            cluster.percolate_with_broad(&title, true).expect("read"),
            vec![id]
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The new layout is built in the old shards' directories and shares their segment numbers,
/// log and checkpoint sidecar. So the shards a layout change replaces refuse to write there,
/// whoever asks, and go on answering reads.
#[test]
fn a_replaced_shard_no_longer_writes_storage() {
    let dir = scratch_dir("replaced_shard_frozen");
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus(300)).expect("durable cluster");
    cluster.checkpoint().expect("checkpoint");
    let held = cluster.layout();
    cluster.resize(3 + 2).expect("resize");
    cluster.checkpoint().expect("checkpoint the new layout");
    let before = segment_files(&dir);

    let old = &held.shards[0];
    let sealed = old.seal_for_checkpoint().map(|_| ());
    let flushed = old.flush();
    let deleted = old.delete_by_logical_id(1).map(|_| ());
    let still_reads = old.live_sources().map(|rows| rows.len());
    let after = segment_files(&dir);
    drop(held);

    for (what, outcome) in [("seal", sealed), ("flush", flushed), ("delete", deleted)] {
        let error = outcome
            .err()
            .unwrap_or_else(|| panic!("a replaced shard accepted a {what}"))
            .to_string();
        assert!(
            error.contains("replaced by a layout change"),
            "{what}: {error}"
        );
    }
    assert!(still_reads.expect("read on the replaced shard") > 0);
    assert_eq!(before, after, "a replaced shard wrote a segment");
    // What the new layout committed is what a restart opens.
    drop(cluster);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert_eq!(reopened.num_shards(), 5);
    for id in [1u64, 150, 300] {
        let title = format!("zzitem{id} zzgroup{}", id % 7);
        assert_eq!(
            reopened.percolate_with_broad(&title, true).expect("read"),
            vec![id]
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A layout change that fails leaves the old layout serving, writes included.
#[test]
fn a_layout_change_that_does_not_publish_thaws_the_shards_it_froze() {
    let cluster = in_memory(3, 50);
    let frozen = layout::FrozenShards::freeze(cluster.layout());
    let while_frozen = cluster.layout().shards[0].flush();
    drop(frozen);
    let thawed = cluster.layout().shards[0].flush();
    let kept = layout::FrozenShards::freeze(cluster.layout());
    kept.keep();
    let after_keep = cluster.layout().shards[0].flush();
    assert!(while_frozen.is_err(), "a frozen shard flushed");
    thawed.expect("a thawed shard flushes");
    assert!(after_keep.is_err(), "a replaced shard was thawed");
}

/// A replica recovery seals its primary, which writes into the primary's directory. It is
/// maintenance, and waits for a layout change the way a checkpoint does.
#[test]
fn a_replica_recovery_waits_for_a_layout_change() {
    let cluster = in_memory(3, 50);
    let target = scratch_dir("replica_waits");
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let recovery = scope.spawn(|| cluster.add_replica(0, &target, 4));
        std::thread::sleep(Duration::from_millis(150));
        let ran_beside_it = recovery.is_finished();
        // Released before anything can fail: the recovery is waiting for it.
        drop(change);
        // An in-memory cluster has nothing to copy; that it answers at all is the point.
        assert!(recovery.join().expect("recovery thread").is_err());
        assert!(
            !ran_beside_it,
            "a replica recovery ran beside a layout change"
        );
    });
}

/// A remote resize holds the layout lock shared for its whole copy. A layout change that
/// queued for the lock behind it would hold every other operation back until the copy was
/// done, so it is refused at once while that resize has writes fenced.
#[test]
fn a_layout_change_is_refused_at_once_while_a_remote_resize_is_copying() {
    let cluster = in_memory(3, 50);
    cluster.resize_write_fence.store(true, Ordering::Release);
    let refused = cluster
        .resize(4)
        .expect_err("a resize during a remote resize");
    let vocabulary = cluster
        .import_alias_synonyms("package, pkg")
        .expect_err("a vocabulary change during a remote resize");
    cluster.resize_write_fence.store(false, Ordering::Release);
    for error in [refused, vocabulary] {
        assert!(error.to_string().contains("writes are paused"), "{error}");
    }
    assert_eq!(cluster.num_shards(), 3);
    cluster.resize(4).expect("a resize once the fence is down");
}

/// The check for a remote copy and the request for the layout lock are one step. A copy
/// that is about to start holds the admission, so a layout change waits there, where it
/// holds nothing back, and is refused once the copy's fence is up. Without the admission it
/// would pass the check, queue for the lock behind the copy, and hold every other operation
/// back until the copy was done.
#[test]
fn a_layout_change_cannot_slip_in_before_a_remote_copy_starts() {
    let cluster = in_memory(3, 50);
    std::thread::scope(|scope| {
        // A remote resize: admission first, then the layout lock shared, then the fence.
        let admission = cluster.layout_admission();
        let copying = cluster.stable();
        let change = scope.spawn(|| cluster.resize(4));
        std::thread::sleep(Duration::from_millis(150));
        // Nothing is queued for the layout lock: another operation gets it at once.
        let held_back = cluster.layout_lock.try_read().is_err();
        cluster.resize_write_fence.store(true, Ordering::Release);
        drop(admission);
        // Released before the join: a change that did queue behind the copy needs it to end.
        drop(copying);
        let refused = change.join().expect("change thread");
        cluster.resize_write_fence.store(false, Ordering::Release);
        assert!(
            !held_back,
            "a layout change queued for the lock behind a copy that was about to start"
        );
        let error = refused.expect_err("the change is refused once the fence is up");
        assert!(error.to_string().contains("writes are paused"), "{error}");
    });
    assert_eq!(cluster.num_shards(), 3);
}

/// An exhaustive read has a deadline and can be cancelled. It gives up on either while a
/// layout change holds it back, where it would otherwise sit out the whole rebuild.
#[test]
fn an_exhaustive_read_keeps_its_deadline_while_a_layout_change_runs() {
    let cluster = in_memory(3, 50);
    let cluster = &cluster;
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let (answer, answered) = mpsc::channel();
        scope.spawn(move || {
            let mut sink = RecordingExhaustiveSink::default();
            let outcome = cluster.try_percolate_filtered_all(
                "zzitem5 zzgroup5",
                &[],
                crate::result::QueryScope::Standard,
                None,
                16,
                Some(Instant::now() + Duration::from_millis(30)),
                &mut sink,
            );
            let _ = answer.send(outcome.map(|_| ()));
        });
        let gave_up = answered.recv_timeout(Duration::from_secs(3));
        // Released before anything can fail: a read that ignored its deadline waits for it.
        drop(change);
        let outcome = gave_up.expect("the read waited past its deadline for the layout change");
        assert!(
            matches!(outcome, Err(ShardError::DeadlineExceeded)),
            "{outcome:?}"
        );
    });
}

/// A deadline that has already passed means "do not wait", not "do not start": an operation
/// takes the layout lock when it is free and gives up at once when a layout change holds it.
#[test]
fn an_expired_deadline_still_takes_a_free_layout_lock() {
    let cluster = in_memory(3, 50);
    let expired = || Err(ShardError::DeadlineExceeded);
    assert!(
        cluster.stable_while(expired).is_ok(),
        "an operation with no time left did not start though nothing held it back"
    );
    let change = cluster.begin_layout_change().expect("begin");
    let held_back = cluster.stable_while(expired).map(|_| ());
    // Released before anything can fail.
    drop(change);
    assert!(
        matches!(held_back, Err(ShardError::DeadlineExceeded)),
        "{held_back:?}"
    );
}

/// A reader that takes no lock and combines the layout with the control state is told when a
/// layout change overlapped it: `None` while one is running, and a second run when one came
/// and went.
#[test]
fn a_lock_free_read_is_told_when_a_layout_change_overlapped_it() {
    let cluster = in_memory(3, 50);
    let pair = || {
        (
            cluster.num_shards(),
            cluster.control_state().expect("control").num_shards as usize,
        )
    };
    assert_eq!(cluster.read_between_layout_changes(pair), Some((3, 3)));

    // One is running from before the read to after it.
    let change = cluster.begin_layout_change().expect("begin");
    let during = cluster.read_between_layout_changes(pair);
    drop(change);
    assert_eq!(during, None, "a read beside a running change was passed");

    // One is running when the read would start. It may already have replaced half of what
    // the read combines, so the read is refused before it begins. A helper that began it
    // anyway would pass it here: the change ends inside the read, and by the time the read
    // ends nothing is running and the layout is the one it started with.
    let mut change = Some(cluster.begin_layout_change().expect("begin"));
    let begun_under_one = cluster.read_between_layout_changes(|| {
        drop(change.take());
        pair()
    });
    drop(change);
    assert_eq!(
        begun_under_one, None,
        "a read that began under a running change was passed"
    );

    // One comes and goes inside the read: the read is run again, on the new layout.
    let mut runs = 0;
    let across = cluster.read_between_layout_changes(|| {
        runs += 1;
        if runs == 1 {
            let before = pair();
            cluster.resize(4).expect("resize");
            before
        } else {
            pair()
        }
    });
    assert_eq!((across, runs), (Some((4, 4)), 2));

    // One begins inside the read and is still running when the read ends.
    let cluster = &cluster;
    std::thread::scope(|scope| {
        let (begun, has_begun) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let mut released = Some(released);
        let overlapped = cluster.read_between_layout_changes(|| {
            let seen = (cluster.num_shards(), 0);
            let released = released.take().expect("the read runs once");
            let begun = begun.clone();
            scope.spawn(move || {
                let change = cluster.begin_layout_change().expect("begin");
                begun.send(()).expect("begun");
                let _ = released.recv();
                drop(change);
            });
            has_begun.recv().expect("the change began");
            seen
        });
        // Released before anything can fail.
        drop(release);
        assert_eq!(
            overlapped, None,
            "a read that a change began under was passed"
        );
    });
}

/// An in-memory engine never runs the cleanup that forgets the layouts it has replaced.
#[test]
fn replaced_layouts_are_forgotten_once_released() {
    let cluster = in_memory(3, 50);
    for shards in [4, 5, 6, 3, 4, 5, 6, 3] {
        cluster.resize(shards).expect("resize");
    }
    let remembered = cluster.retired_layouts.lock().expect("retired").len();
    assert!(
        remembered <= 1,
        "{remembered} replaced layouts are still remembered after they were released"
    );
}
