//! Scenario A — kill mid WAL-append (the headline case). With no flush and no
//! compaction the ONLY durable window is a WAL frame's `write` + sync, so every
//! recovered ack comes back through `replay_wal_tail` against a possibly-absent
//! manifest. Run in BOTH durability modes: `fsync=false` (page-cache durable — the
//! realistic SIGKILL setting; the OS cache survives the process death) and
//! `fsync=true` (power-loss durable). The ACK-before-return ordering is identical,
//! so a read ack is must-survive in both.

use crate::harness::{
    crash_iters, full_reference, jitter_for, reopen_and_diff, spawn_and_kill, unique_dir, Corpus,
    Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn wal_append_acked_writes_survive_sigkill() {
    let corpus = Corpus::generate("wal_append", 0xC0DE_0001, 16_000, 500);
    let full = full_reference(&corpus);
    let iters = crash_iters();
    let mut exercised = 0usize;
    for i in 0..iters {
        for &fsync in &[false, true] {
            let dir = unique_dir("wal_append");
            let res = spawn_and_kill(
                "wal_append",
                &dir,
                &corpus.tsv,
                &[],
                fsync,
                Trigger::Acks(2_000),
                jitter_for(i),
            );
            assert!(
                res.killed,
                "[wal_append] writer finished before the kill — raise the corpus size"
            );
            exercised += reopen_and_diff(
                &dir,
                &corpus,
                &full,
                &res.acked,
                &res.tombed,
                fsync,
                &format!("wal_append/fsync={fsync}/iter={i}"),
            );
            // Reuse the actual SIGKILL artifact. The first open must repair it before
            // acknowledging a new write, and the next crash must recover that write.
            let cfg = reverse_rusty::config::EngineConfig {
                data_dir: Some(dir.clone()),
                wal_sync_on_write: fsync,
                memtable_flush_threshold: usize::MAX,
                ..reverse_rusty::config::EngineConfig::default()
            };
            let mut engine = reverse_rusty::segment::Engine::open(
                reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
                cfg.clone(),
            )
            .unwrap();
            engine
                .try_insert_live("restart canary", u64::MAX, 1)
                .unwrap();
            drop(engine); // abrupt engine lifetime end: no flush or checkpoint
            let engine = reverse_rusty::segment::Engine::open(
                reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
                cfg,
            )
            .unwrap();
            let mut scratch = reverse_rusty::segment::MatchScratch::new();
            let mut ids = Vec::new();
            engine.match_title("restart canary", &mut scratch, &mut ids, true);
            assert!(
                ids.contains(&u64::MAX),
                "new acknowledgement lost on second restart"
            );
        }
    }
    assert!(
        exercised > 0,
        "wal_append: no iteration produced a match (degenerate corpus)"
    );
}
