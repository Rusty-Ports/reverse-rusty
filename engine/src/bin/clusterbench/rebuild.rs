//! `clusterbench rebuild`: what a resize and a vocabulary change cost a serving coordinator.
//!
//! Usage: clusterbench rebuild [num_queries] [num_titles] [shards_from] [shards_to] [seed]
//!
//! Builds a durable in-process cluster, measures search latency with nothing else running,
//! and then rebuilds it twice, once to another shard count and once under another
//! vocabulary, with one thread searching and one thread writing beside each rebuild. It
//! reports, for each: the engine's own time for each part (`ClusterEngine::last_rebuild`),
//! search latency while the rebuild ran, how long writes waited, and the process's peak
//! memory against what it held before.
//!
//! Every number here depends on the machine. What carries over is their shape: which part of
//! a rebuild the time goes to, that searches go on and writes wait, and the memory a rebuild
//! needs on top of the corpus.

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, RebuildTimings};
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::vocab::Vocab;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) fn run(args: &[String]) {
    let num_queries = super::arg_usize(args, 2, 200_000);
    let num_titles = super::arg_usize(args, 3, 5_000);
    let shards_from = super::arg_usize(args, 4, 4);
    let shards_to = super::arg_usize(args, 5, 8);
    let seed = super::arg_u64(args, 6, 0x00C0_FFEE);

    let data = generate(&GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_entities: (num_queries / 40).max(2_000),
        num_collections: (num_queries / 100).max(1_000),
    });
    let dir = std::env::temp_dir().join(format!("rr_clusterbench_rebuild_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = ClusterConfig {
        num_shards: shards_from,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    let started = Instant::now();
    let cluster =
        Arc::new(ClusterEngine::build(super::vocab(), &config, &data.queries).expect("build"));
    cluster.checkpoint().expect("checkpoint");
    println!("================ REBUILD (durable, in-process) ================");
    println!(
        "corpus              : {} queries on {shards_from} shards, built and checkpointed in {:.2}s",
        cluster.num_queries().expect("num_queries"),
        started.elapsed().as_secs_f64()
    );
    let resident = rss_mb();
    println!("resident before     : {resident:.0} MB");

    let titles = Arc::new(data.titles);
    let quiet = search_for(&cluster, &titles, Duration::from_secs(2));
    println!(
        "search, nothing else: p50 {:.0} us  p99 {:.0} us  ({} titles)",
        quiet.p50, quiet.p99, quiet.count
    );

    let next_id = AtomicU64::new(10_000_000_000);
    report(
        &format!("RESIZE {shards_from} -> {shards_to}"),
        &beside(&cluster, &titles, &next_id, || {
            cluster.resize(shards_to).expect("resize");
        }),
        resident,
    );
    let mut vocabulary = Vocab::default();
    vocabulary.add_synonym("zzbenchalias", "zzbenchcanonical", FeatureKind::Generic);
    report(
        "VOCABULARY CHANGE",
        &beside(&cluster, &titles, &next_id, || {
            cluster.set_vocab(vocabulary).expect("set_vocab");
        }),
        resident,
    );
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

struct Latency {
    count: usize,
    p50: f64,
    p99: f64,
}

fn latency(mut micros: Vec<f64>) -> Latency {
    micros.sort_by(f64::total_cmp);
    let at = |q: f64| {
        micros
            .get(((micros.len().max(1) - 1) as f64 * q).round() as usize)
            .copied()
            .unwrap_or(0.0)
    };
    Latency {
        count: micros.len(),
        p50: at(0.50),
        p99: at(0.99),
    }
}

fn search_for(cluster: &ClusterEngine, titles: &[String], how_long: Duration) -> Latency {
    let stop = Instant::now() + how_long;
    let mut micros = Vec::new();
    'all: loop {
        for title in titles {
            let started = Instant::now();
            cluster.percolate(title).expect("percolate");
            micros.push(started.elapsed().as_secs_f64() * 1e6);
            if Instant::now() >= stop {
                break 'all;
            }
        }
    }
    latency(micros)
}

struct Beside {
    timings: RebuildTimings,
    wall: Duration,
    search: Latency,
    longest_search: Duration,
    /// When the longest search began, from the start of the rebuild.
    longest_search_at: Duration,
    writes: usize,
    longest_write: Duration,
    peak_mb: f64,
}

/// Run `rebuild` with one thread searching, one writing and one sampling memory beside it.
fn beside(
    cluster: &Arc<ClusterEngine>,
    titles: &Arc<Vec<String>>,
    next_id: &AtomicU64,
    rebuild: impl FnOnce(),
) -> Beside {
    let done = Arc::new(AtomicBool::new(false));
    let origin = Instant::now();
    std::thread::scope(|scope| {
        let searcher = scope.spawn({
            let (cluster, titles, done) =
                (Arc::clone(cluster), Arc::clone(titles), Arc::clone(&done));
            move || {
                let (mut micros, mut longest) = (Vec::new(), (Duration::ZERO, origin));
                'all: loop {
                    for title in titles.iter() {
                        let started = Instant::now();
                        cluster.percolate(title).expect("percolate");
                        let took = started.elapsed();
                        if took > longest.0 {
                            longest = (took, started);
                        }
                        micros.push(took.as_secs_f64() * 1e6);
                        if done.load(Ordering::Acquire) {
                            break 'all;
                        }
                    }
                }
                (latency(micros), longest)
            }
        });
        let writer = scope.spawn({
            let (cluster, done) = (Arc::clone(cluster), Arc::clone(&done));
            move || {
                let (mut writes, mut longest) = (0usize, Duration::ZERO);
                while !done.load(Ordering::Acquire) {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    cluster
                        .upsert_query(id, "zzbench rebuild writer", 1)
                        .expect("upsert");
                    longest = longest.max(started.elapsed());
                    writes += 1;
                }
                (writes, longest)
            }
        });
        let sampler = scope.spawn({
            let done = Arc::clone(&done);
            move || {
                let mut peak = 0f64;
                while !done.load(Ordering::Acquire) {
                    peak = peak.max(rss_mb());
                    std::thread::sleep(Duration::from_millis(50));
                }
                peak
            }
        });
        // Let the two settle before the rebuild begins.
        std::thread::sleep(Duration::from_millis(300));
        let started = Instant::now();
        rebuild();
        let wall = started.elapsed();
        done.store(true, Ordering::Release);
        let (search, (longest_search, longest_at)) = searcher.join().expect("searcher");
        let (writes, longest_write) = writer.join().expect("writer");
        Beside {
            timings: cluster.last_rebuild().expect("a rebuild ran"),
            wall,
            search,
            longest_search,
            longest_search_at: longest_at.saturating_duration_since(started),
            writes,
            longest_write,
            peak_mb: sampler.join().expect("sampler"),
        }
    })
}

fn report(what: &str, ran: &Beside, resident_before: f64) {
    let t = &ran.timings;
    let secs = |d: Duration| d.as_secs_f64();
    println!("---------------- {what} ----------------");
    println!(
        "rebuilt             : {} queries in {:.2}s  ({:.0} queries/sec)",
        t.queries,
        secs(ran.wall),
        t.queries as f64 / secs(ran.wall).max(1e-9)
    );
    println!(
        "  gather {:.2}s  extract {:.2}s  place {:.2}s  build {:.2}s  publish {:.3}s  commit {:.2}s",
        secs(t.gather),
        secs(t.extract),
        secs(t.place),
        secs(t.build),
        secs(t.publish),
        secs(t.commit)
    );
    println!(
        "search beside it    : p50 {:.0} us  p99 {:.0} us  ({} titles); the longest took {:.1} ms, {:.2}s into the rebuild",
        ran.search.p50,
        ran.search.p99,
        ran.search.count,
        secs(ran.longest_search) * 1e3,
        secs(ran.longest_search_at)
    );
    println!(
        "writes beside it    : {} acknowledged, the longest waited {:.2}s",
        ran.writes,
        secs(ran.longest_write)
    );
    println!(
        "peak resident       : {:.0} MB  ({:.2}x the {resident_before:.0} MB before)",
        ran.peak_mb,
        ran.peak_mb / resident_before.max(1.0)
    );
}

/// The process's resident memory, from `ps`, which answers on Linux and macOS alike.
fn rss_mb() -> f64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|kib| kib.trim().parse::<f64>().ok())
        .map_or(0.0, |kib| kib / 1024.0)
}
