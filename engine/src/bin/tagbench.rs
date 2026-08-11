//! Filter-driven segment-skipping benchmark (ADR-174).
//!
//! The corpus is split into category-local immutable segments, then the exact
//! same snapshots, titles, and compiled filter are measured with the skip kill
//! switch disabled and enabled. The benchmark asserts per-title result equality
//! before reporting avoided work or throughput.
//!
//! Usage: tagbench [num_queries] [num_titles] [segments] [broad_frac] [seed]
//! Defaults: 160k queries, 2k titles, 8 segments, broad_frac 0.05, seed 0x174.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::{Engine, EngineSnapshot, MatchScratch, MatchStats};
use reverse_rusty::Normalizer;
use std::hint::black_box;
use std::time::Instant;

const TAG_KEY: &str = "category";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 160_000).max(1);
    let num_titles = arg_usize(&args, 2, 2_000).max(1);
    let segments = arg_usize(&args, 3, 8).clamp(1, num_queries);
    let broad_frac = arg_f64(&args, 4, 0.05);
    let seed = arg_u64(&args, 5, 0x0174);

    let data = generate(&GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_entities: (num_queries / 40).max(2_000),
        num_collections: (num_queries / 100).max(1_000),
    });
    eprintln!(
        "[gen] queries={} titles={} segments={} broad_frac={broad_frac}",
        data.queries.len(),
        data.titles.len(),
        segments
    );

    let mut config = EngineConfig {
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        tag_segment_skipping: false,
        ..EngineConfig::default()
    };
    let mut engine = build_category_local(&data.queries, segments, config.clone());
    let disabled = engine.snapshot();
    config.tag_segment_skipping = true;
    engine.set_config(config);
    let enabled = engine.snapshot();

    let filter = vec![(TAG_KEY.to_string(), vec![category(0)])];
    let disabled_capture = capture(&disabled, &data.titles, &filter, broad_frac > 0.0);
    let enabled_capture = capture(&enabled, &data.titles, &filter, broad_frac > 0.0);
    assert_eq!(
        enabled_capture.rows, disabled_capture.rows,
        "tag segment skipping changed filtered results"
    );

    let (disabled_tps, enabled_tps) =
        paired_throughput(&disabled, &enabled, &data.titles, &filter, broad_frac > 0.0);
    let n = data.titles.len().max(1) as f64;
    let disabled_work = &disabled_capture.stats;
    let enabled_work = &enabled_capture.stats;

    println!("============================= TAG SEGMENT SKIPPING =============================");
    println!(
        "queries={} titles={} immutable_segments={} filter={TAG_KEY}={} summary_bytes={}",
        data.queries.len(),
        data.titles.len(),
        segments,
        category(0),
        enabled.metrics().tag_summary_bytes
    );
    println!(
        "{:>10} | {:>14} | {:>14} | {:>14} | {:>14}",
        "mode", "skips/title", "cands/title", "posts/title", "titles/sec"
    );
    println!("{}", "-".repeat(81));
    print_row("disabled", disabled_work, disabled_tps, n);
    print_row("enabled", enabled_work, enabled_tps, n);
    println!("{}", "-".repeat(81));
    println!(
        "equivalent=true posting_reduction={:.1}% candidate_reduction={:.1}% throughput_speedup={:.2}x",
        reduction(disabled_work.postings_scanned, enabled_work.postings_scanned),
        reduction(
            disabled_work.unique_candidates,
            enabled_work.unique_candidates
        ),
        enabled_tps / disabled_tps
    );
    println!(
        "totals disabled(candidates={},postings={}) enabled(candidates={},postings={},skips={}) result_rows={} result_checksum={:016x}",
        disabled_work.unique_candidates,
        disabled_work.postings_scanned,
        enabled_work.unique_candidates,
        enabled_work.postings_scanned,
        enabled_work.tag_segments_skipped,
        enabled_capture.rows.iter().map(Vec::len).sum::<usize>(),
        result_checksum(&enabled_capture.rows)
    );
}

fn build_category_local(
    queries: &[(u64, String)],
    segments: usize,
    config: EngineConfig,
) -> Engine {
    let mut groups = vec![Vec::new(); segments];
    for (logical, query) in queries {
        groups[*logical as usize % segments].push((*logical, query.clone()));
    }

    let mut engine = Engine::with_config(
        Normalizer::default_vocab().expect("default vocabulary"),
        config,
    );
    for (group, rows) in groups.into_iter().enumerate() {
        let tags = vec![vec![(TAG_KEY.to_string(), category(group))]; rows.len()];
        if group == 0 {
            engine
                .try_build_from_queries_with_tags(&rows, &tags)
                .expect("first tagged segment build");
        } else {
            engine
                .try_bulk_ingest_detailed_with_tags(&rows, &tags)
                .expect("tagged segment ingest");
        }
    }
    engine
}

struct Capture {
    rows: Vec<Vec<u64>>,
    stats: MatchStats,
}

fn capture(
    snapshot: &EngineSnapshot,
    titles: &[String],
    filter: &[(String, Vec<String>)],
    include_broad: bool,
) -> Capture {
    let pred = snapshot.compile_tag_predicate(filter);
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut rows = Vec::with_capacity(titles.len());
    let mut stats = MatchStats::default();
    for title in titles {
        let current =
            snapshot.match_title_filtered(title, &mut scratch, &mut out, include_broad, &pred);
        stats.merge(current);
        rows.push(out.clone());
    }
    Capture { rows, stats }
}

fn paired_throughput(
    disabled: &EngineSnapshot,
    enabled: &EngineSnapshot,
    titles: &[String],
    filter: &[(String, Vec<String>)],
    include_broad: bool,
) -> (f64, f64) {
    let disabled_pred = disabled.compile_tag_predicate(filter);
    let enabled_pred = enabled.compile_tag_predicate(filter);
    warm(disabled, titles, include_broad, &disabled_pred);
    warm(enabled, titles, include_broad, &enabled_pred);

    let reps = (100_000 / titles.len().max(1)).max(1);
    let mut disabled_samples = Vec::with_capacity(3);
    let mut enabled_samples = Vec::with_capacity(3);
    for round in 0..3 {
        if round % 2 == 0 {
            disabled_samples.push(time_pass(
                disabled,
                titles,
                include_broad,
                &disabled_pred,
                reps,
            ));
            enabled_samples.push(time_pass(
                enabled,
                titles,
                include_broad,
                &enabled_pred,
                reps,
            ));
        } else {
            enabled_samples.push(time_pass(
                enabled,
                titles,
                include_broad,
                &enabled_pred,
                reps,
            ));
            disabled_samples.push(time_pass(
                disabled,
                titles,
                include_broad,
                &disabled_pred,
                reps,
            ));
        }
    }
    (median(&mut disabled_samples), median(&mut enabled_samples))
}

fn warm(
    snapshot: &EngineSnapshot,
    titles: &[String],
    include_broad: bool,
    pred: &reverse_rusty::exact::TagPredicate,
) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    for title in titles.iter().take(500) {
        snapshot.match_title_filtered(title, &mut scratch, &mut out, include_broad, pred);
        black_box(out.len());
    }
}

fn time_pass(
    snapshot: &EngineSnapshot,
    titles: &[String],
    include_broad: bool,
    pred: &reverse_rusty::exact::TagPredicate,
    reps: usize,
) -> f64 {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let started = Instant::now();
    for _ in 0..reps {
        for title in titles {
            snapshot.match_title_filtered(title, &mut scratch, &mut out, include_broad, pred);
            black_box(out.len());
        }
    }
    (reps * titles.len()) as f64 / started.elapsed().as_secs_f64()
}

fn print_row(mode: &str, stats: &MatchStats, tps: f64, n: f64) {
    println!(
        "{:>10} | {:>14.2} | {:>14.2} | {:>14.2} | {:>14.0}",
        mode,
        f64::from(stats.tag_segments_skipped) / n,
        f64::from(stats.unique_candidates) / n,
        f64::from(stats.postings_scanned) / n,
        tps
    );
}

fn reduction(before: u32, after: u32) -> f64 {
    if before == 0 {
        0.0
    } else {
        (1.0 - f64::from(after) / f64::from(before)) * 100.0
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn result_checksum(rows: &[Vec<u64>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for row in rows {
        hash ^= row.len() as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        for logical in row {
            hash ^= *logical;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

fn category(segment: usize) -> String {
    format!("segment-{segment}")
}

fn arg_usize(args: &[String], index: usize, default: usize) -> usize {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn arg_f64(args: &[String], index: usize, default: f64) -> f64 {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn arg_u64(args: &[String], index: usize, default: u64) -> u64 {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
