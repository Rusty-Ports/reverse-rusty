use super::*;

/// Full coordinator mutations, with optional downstream write latency. Copy
/// this test module and its harness to the comparison revision unchanged.
/// Timings are diagnostic; exact final match sets are asserted every round.
#[test]
#[ignore = "manual same-machine write-concurrency capture"]
fn write_concurrency_capture() {
    const WORKERS: usize = 8;
    for delay_us in [0, 1_000] {
        let iterations = if delay_us == 0 { 1_000 } else { 64 };
        for (shape, stride) in [("spread", 1), ("collision", 256), ("same_id", 0)] {
            let mut samples = Vec::new();
            let ids: Vec<u64> = (0..WORKERS)
                .map(|worker| 7 + worker as u64 * stride)
                .collect();
            let mut expected = ids.clone();
            expected.sort_unstable();
            expected.dedup();
            for _ in 0..5 {
                let cfg = ClusterConfig {
                    num_shards: 3,
                    ..Default::default()
                };
                let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
                instrument(
                    &mut cluster,
                    Arc::new(move |position, call| {
                        if delay_us != 0 && position == 2 && matches!(call, WriteCall::Delete(_)) {
                            std::thread::sleep(Duration::from_micros(delay_us));
                        }
                        Ok(())
                    }),
                );
                let start = std::sync::Barrier::new(WORKERS + 1);
                let elapsed = std::thread::scope(|scope| {
                    let cluster = &cluster;
                    let start = &start;
                    let mut writers = Vec::new();
                    for &id in &ids {
                        writers.push(scope.spawn(move || {
                            start.wait();
                            for iteration in 0..iterations {
                                let dsl = if iteration % 2 == 0 {
                                    "zzoldbody"
                                } else {
                                    "zznewbody"
                                };
                                let (_, outcome) =
                                    cluster.upsert_query(id, dsl, iteration + 1).expect("write");
                                assert!(matches!(outcome, AddOutcome::Placed { .. }));
                            }
                        }));
                    }
                    let before = Instant::now();
                    start.wait();
                    for writer in writers {
                        writer.join().expect("writer");
                    }
                    before.elapsed()
                });
                assert_eq!(cluster.percolate("zznewbody").expect("new"), expected);
                assert!(cluster.percolate("zzoldbody").expect("old").is_empty());
                assert_eq!(cluster.pending_repairs(), 0);
                samples.push(elapsed.as_secs_f64() * 1_000.0);
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "write_concurrency shape={shape} workers={WORKERS} iterations={iterations} delay_us={delay_us} rounds_ms={samples:?} median_ops_s={:.0} final_ids={expected:?}",
                WORKERS as f64 * f64::from(iterations) / (samples[2] / 1_000.0)
            );
        }
    }
}
