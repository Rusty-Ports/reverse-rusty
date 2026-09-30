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
fn wal_append_acked_writes_survive_two_sigkills_in_the_same_directory() {
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
            let second = spawn_and_kill(
                "wal_append",
                &dir,
                &corpus.tsv,
                &["--offset".into(), "8000".into()],
                fsync,
                Trigger::Acks(2_000),
                jitter_for(i + 1),
            );
            assert!(
                second.killed,
                "second writer must be killed after acknowledging progress"
            );
            let mut acked = res.acked;
            acked.extend(second.acked);
            acked.sort_unstable();
            acked.dedup();
            exercised += reopen_and_diff(
                &dir,
                &corpus,
                &full,
                &acked,
                &[],
                fsync,
                &format!("wal_append/double-kill/fsync={fsync}/iter={i}"),
            );
        }
    }
    assert!(
        exercised > 0,
        "wal_append: no iteration produced a match (degenerate corpus)"
    );
}
