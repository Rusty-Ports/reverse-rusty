//! Snapshot-publish microbenchmark (P1-16).
//!
//! Measures the cost of `Engine::snapshot()` — the operation the server runs
//! after every write to publish a new lock-free read view (ADR-016). The audit
//! (P1-16) flagged that this deep-clones the entire engine on every write,
//! making writes O(total engine size) rather than O(delta).
//!
//! Usage: snapbench [num_queries] [iters] [data_dir]
//!
//! Reports:
//!   - time per bare `snapshot()` call (the publish cost)
//!   - time per PUT + publish and DELETE + publish with the snapshot dropped at once
//!     (a lower bound: nothing is shared, so nothing is copied)
//!   - the same with the published snapshot held until the next one replaces it, which
//!     is how the server runs: per memtable size, for an upsert and a delete of a base
//!     row, and with one more snapshot pinned
//!   - time per bulk(1k) + publish cycle
//!
//! Build a large sealed engine first (build_from_queries seals into a base
//! segment), so `snapshot()` must reckon with the full corpus — exactly the
//! server's steady state.

use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::Engine;
use reverse_rusty::Normalizer;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 1_000_000);
    let iters = arg_usize(&args, 2, 200);

    let cfg = GenConfig {
        num_queries,
        num_titles: 1_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x00C0_FFEE,
        num_entities: (num_queries / 40).max(2_000),
        num_collections: (num_queries / 100).max(1_000),
    };

    eprintln!("[gen] queries={num_queries}");
    let data = generate(&cfg);

    let norm = Normalizer::default_vocab().expect("default vocabulary");
    // With a data directory the base segments are mmap-backed, as on a server started with
    // `--data-dir`; without one they are in-memory segments, which a write that tombstones a
    // base row copies whole.
    let data_dir = args.get(3).map(std::path::PathBuf::from);
    let mut eng = match &data_dir {
        Some(dir) => {
            let _ = std::fs::remove_dir_all(dir);
            Engine::with_config(
                norm,
                reverse_rusty::config::EngineConfig {
                    data_dir: Some(dir.clone()),
                    ..Default::default()
                },
            )
        }
        None => Engine::new(norm),
    };
    let tb = Instant::now();
    eng.build_from_queries(&data.queries);
    eprintln!(
        "[build] {} queries sealed into base segment(s) in {:.2}s",
        eng.num_queries(),
        tb.elapsed().as_secs_f64()
    );

    println!("================ SNAPSHOT PUBLISH COST ================");
    println!("corpus              : {} queries", eng.num_queries());
    println!("base segments       : {}", eng.num_segments() - 1);
    println!(
        "storage             : {}",
        if data_dir.is_some() {
            "durable (mmap base segments)"
        } else {
            "in-memory"
        }
    );
    println!("dict features       : {}", eng.dict_len());

    // ---- bare snapshot() (the publish) ----
    // warmup
    for _ in 0..5 {
        std::hint::black_box(eng.snapshot());
    }
    let t = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(eng.snapshot());
    }
    let per_snap = t.elapsed().as_secs_f64() / iters as f64;
    println!(
        "snapshot()          : {:.3} ms/call   ({:.0} publishes/sec)",
        per_snap * 1e3,
        1.0 / per_snap
    );

    // ---- PUT + publish, with nobody holding the previous snapshot ----
    // A lower bound, and not how the server runs: the snapshot is dropped at the end of
    // each statement, so the next write finds the dict and the memtable unshared and
    // copies nothing.
    let t = Instant::now();
    for i in 0..iters {
        let logical = 10_000_000 + i as u64;
        eng.insert_live(
            "1994 north star wireless mouse limited pro -damaged",
            logical,
            1,
        );
        std::hint::black_box(eng.snapshot());
    }
    report("PUT + publish (snapshot dropped)", t, iters);
    let t = Instant::now();
    for i in 0..iters {
        let logical = 10_000_000 + i as u64; // delete the ones we just inserted
        let _ = eng.delete_by_logical_id(logical);
        std::hint::black_box(eng.snapshot());
    }
    report("DELETE + publish (snapshot dropped)", t, iters);

    // ---- The server's path: the published snapshot is held until the next replaces it ----
    // `AppState` keeps the last snapshot in an `ArcSwap`, so at every write the engine's
    // dict and memtable are shared with it, and `Arc::make_mut` copies them (RR-016).
    println!("--- published snapshot held, as the server holds it ---");
    let mut published = Arc::new(eng.snapshot());
    for memtable_rows in [0usize, 10_000, 50_000, 99_000] {
        if memtable_rows > 0 {
            // Fill the memtable to this many live rows without paying the copy per row.
            drop(std::mem::replace(
                &mut published,
                Arc::new(Engine::new(Normalizer::default_vocab().expect("vocabulary")).snapshot()),
            ));
            let have = eng.metrics().memtable_entries;
            for i in have..memtable_rows {
                eng.insert_live(
                    "1994 north star wireless mouse limited pro -damaged",
                    30_000_000 + i as u64,
                    1,
                );
            }
            published = Arc::new(eng.snapshot());
        }
        let t = Instant::now();
        for i in 0..iters {
            eng.insert_live(
                "1994 north star wireless mouse limited pro -damaged",
                40_000_000 + (memtable_rows * 1_000 + i) as u64,
                1,
            );
            published = Arc::new(eng.snapshot());
        }
        report(
            &format!("PUT + publish, memtable ~{memtable_rows} rows"),
            t,
            iters,
        );
    }

    // An upsert that replaces a row in a base segment, and a delete of one: each also
    // copies the liveness overlay of the segment it touches.
    let base_ids: Vec<u64> = data
        .queries
        .iter()
        .take(2 * iters)
        .map(|(id, _)| *id)
        .collect();
    let (replaced, deleted) = base_ids.split_at(iters.min(base_ids.len() / 2));
    let t = Instant::now();
    for id in replaced {
        let _ = eng.try_upsert_live("1994 north star wireless mouse limited pro", *id, 2);
        published = Arc::new(eng.snapshot());
    }
    report("UPSERT of a base row + publish", t, replaced.len());
    let t = Instant::now();
    for id in deleted {
        let _ = eng.delete_by_logical_id(*id);
        published = Arc::new(eng.snapshot());
    }
    report("DELETE of a base row + publish", t, deleted.len());

    // A point in time pins one more snapshot. The write path is the same; what changes is
    // that the copy it pins is not freed.
    let pinned = Arc::clone(&published);
    let t = Instant::now();
    for i in 0..iters {
        eng.insert_live(
            "1994 north star wireless mouse limited pro -damaged",
            50_000_000 + i as u64,
            1,
        );
        published = Arc::new(eng.snapshot());
    }
    report("PUT + publish, one PIT open", t, iters);
    drop(pinned);
    drop(published);

    // ---- bulk(1k) + publish ----
    let bulk_n = 1_000usize;
    let t = Instant::now();
    let bulk_iters = iters.clamp(1, 20);
    for b in 0..bulk_iters {
        let batch: Vec<(u64, String)> = (0..bulk_n)
            .map(|j| {
                let logical = 20_000_000 + (b * bulk_n + j) as u64;
                (
                    logical,
                    "1994 north star wireless mouse limited pro".to_string(),
                )
            })
            .collect();
        eng.bulk_ingest(&batch);
        std::hint::black_box(eng.snapshot());
    }
    let per_bulk = t.elapsed().as_secs_f64() / bulk_iters as f64;
    println!(
        "bulk(1k) + publish  : {:.3} ms/op    ({:.0} queries/sec)",
        per_bulk * 1e3,
        bulk_n as f64 / per_bulk
    );

    if let Some(dir) = &data_dir {
        drop(eng);
        let _ = std::fs::remove_dir_all(dir);
    }
    println!("======================================================");
    println!(
        "ideal: snapshot()/PUT/DELETE publish should be ~independent of corpus size\n\
         (O(delta), not O(total)). Re-run at multiple --num_queries to see scaling."
    );
}

fn report(label: &str, started: Instant, ops: usize) {
    let per_op = started.elapsed().as_secs_f64() / ops.max(1) as f64;
    println!(
        "{label:<40}: {:.3} ms/op    ({:.0} writes/sec)",
        per_op * 1e3,
        1.0 / per_op
    );
}

fn arg_usize(a: &[String], i: usize, d: usize) -> usize {
    a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d)
}
