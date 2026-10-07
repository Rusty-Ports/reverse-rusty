//! A layout change holds writes out, lets reads through, and leaves a replaced layout's
//! files alone until nothing is running on it (ADR-209).

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

/// The order inside a mutation's admission is the whole of its safety: barrier, then layout.
/// A mutation is stopped between the two steps while a resize runs. Taken in that order, the
/// resize waits for it. Taken the other way round, the resize would finish first and the
/// mutation would then write to shards nothing reads any more.
#[test]
fn a_mutation_admitted_before_a_layout_change_is_not_lost_to_it() {
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
        match written {
            Ok(_) => assert_eq!(
                matched,
                vec![9_001],
                "the write was acknowledged and no read finds it (the resize ran beside the \
                 stopped mutation: {resized_beside_it})"
            ),
            // Refused at the fence the resize raised while it waited: the caller retries.
            Err(error) => {
                assert!(error.to_string().contains("writes are paused"), "{error}");
                assert!(matched.is_empty());
            }
        }
        assert!(
            !resized_beside_it,
            "the resize did not wait for a mutation that had been admitted"
        );
    });
    assert_eq!(cluster.num_shards(), 5);
}

#[test]
fn a_layout_change_refuses_writes_and_lets_reads_through() {
    let cluster = in_memory(3, 200);
    let change = cluster.begin_layout_change().expect("begin");
    let refused = cluster.add_query(9_002, "zzheld zzout");
    let upsert = cluster.upsert_query(1, "zzitem1 zzgroup1", 2);
    let remove = cluster.remove_query(2);
    let read = cluster.percolate_with_broad("zzitem5 zzgroup5", true);
    let view = cluster
        .consistent_read_view()
        .percolate_filtered_with_stats("zzitem6 zzgroup6", &[], true)
        .map(|(matched, _)| matched);
    drop(change);
    for (what, error) in [
        ("add", refused.err().map(|e| e.to_string())),
        ("upsert", upsert.err().map(|e| e.to_string())),
        ("remove", remove.err().map(|e| e.to_string())),
    ] {
        let error = error.unwrap_or_else(|| panic!("{what} was accepted during a layout change"));
        assert!(error.contains("writes are paused"), "{what}: {error}");
    }
    assert_eq!(read.expect("read"), vec![5]);
    assert_eq!(view.expect("frozen view"), vec![6]);
    // Nothing the refused writes asked for happened, and writes are back.
    assert_eq!(
        cluster
            .percolate_with_broad("zzitem2 zzgroup2", true)
            .expect("read"),
        vec![2]
    );
    assert!(matches!(
        cluster.add_query(9_002, "zzheld zzout"),
        Ok(AddOutcome::Placed { .. } | AddOutcome::Replicated { .. })
    ));
}

#[test]
fn a_checkpoint_and_a_second_layout_change_wait_for_the_one_in_progress() {
    let cluster = in_memory(3, 100);
    std::thread::scope(|scope| {
        let change = cluster.begin_layout_change().expect("begin");
        let checkpoint = scope.spawn(|| cluster.checkpoint());
        let resize = scope.spawn(|| cluster.resize(4));
        std::thread::sleep(Duration::from_millis(150));
        let (checkpointed_beside_it, resized_beside_it) =
            (checkpoint.is_finished(), resize.is_finished());
        // Released before anything can fail: both threads are waiting for it.
        drop(change);
        checkpoint
            .join()
            .expect("checkpoint thread")
            .expect("checkpoint");
        resize.join().expect("resize thread").expect("resize");
        assert!(
            !checkpointed_beside_it,
            "a checkpoint ran beside a layout change"
        );
        assert!(!resized_beside_it, "two layout changes ran at once");
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
