use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Barrier};
use std::time::{Duration, Instant};

fn users(table: &LogicalWriteLocks, logical: u64) -> usize {
    table
        .entries
        .lock()
        .expect("registry")
        .get(&logical)
        .map_or(0, |entry| entry.users)
}

#[test]
fn formerly_colliding_ids_can_hold_locks_together() {
    let table = LogicalWriteLocks::default();
    let first = table.lock(7);
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        let table = &table;
        scope.spawn(move || {
            // The old (id ^ (id >> 32)) % 256 table mapped both to stripe 7.
            let _second = table.lock(263);
            tx.send(()).expect("signal second ID");
        });
        let independent = rx.recv_timeout(Duration::from_secs(5));
        // Release before asserting, so a regressed implementation cannot hang
        // the scoped thread's join instead of reporting a failed test.
        drop(first);
        independent.expect("unrelated ID must not wait for the first ID");
    });
    assert!(table.entries.lock().expect("registry").is_empty());
}

#[test]
fn waiters_pin_one_lock_until_the_last_registered_caller_leaves() {
    let table = LogicalWriteLocks::default();
    let owner = table.lock(u64::MAX);
    let in_section = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let table = &table;
            let in_section = &in_section;
            scope.spawn(move || {
                for _ in 0..200 {
                    let _guard = table.lock(u64::MAX);
                    assert_eq!(in_section.fetch_add(1, Ordering::SeqCst), 0);
                    std::thread::yield_now();
                    assert_eq!(in_section.fetch_sub(1, Ordering::SeqCst), 1);
                }
            });
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while users(&table, u64::MAX) != 9 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let registered = users(&table, u64::MAX);
        // A notification with no predicate change must not admit a waiter.
        owner.lock.ready.notify_all();
        assert_eq!(in_section.load(Ordering::SeqCst), 0);
        drop(owner);
        assert_eq!(registered, 9, "all waiters must register before waiting");
    });
    assert!(table.entries.lock().expect("registry").is_empty());
}

#[test]
fn high_cardinality_churn_retains_only_active_ids() {
    let table = LogicalWriteLocks::default();
    let start = Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let table = &table;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for offset in 0..10_000 {
                    let logical = (worker * 10_000 + offset) * 256;
                    let _guard = table.lock(logical);
                    assert!(table.entries.lock().expect("registry").len() <= 8);
                }
            });
        }
    });
    assert!(table.entries.lock().expect("registry").is_empty());
}

#[test]
fn bulk_excludes_registered_and_new_id_writers() {
    let table = LogicalWriteLocks::default();
    let writer = table.lock(1);
    assert!(matches!(
        table.bulk.try_write(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    drop(writer);

    let bulk = table.lock_bulk();
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        let table = &table;
        scope.spawn(move || {
            let _writer = table.lock(2);
            tx.send(()).expect("signal writer");
        });
        let early = rx.recv_timeout(Duration::from_millis(50));
        let entries_during_bulk = table.entries.lock().expect("registry").len();
        drop(bulk);
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(entries_during_bulk, 0);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("writer resumes after bulk");
    });
    assert!(table.entries.lock().expect("registry").is_empty());
}

#[test]
fn unwinding_a_holder_releases_the_id_and_bulk_barrier() {
    let table = LogicalWriteLocks::default();
    let result = std::panic::catch_unwind(|| {
        let _guard = table.lock(0);
        panic!("injected caller panic");
    });
    assert!(result.is_err());
    assert!(table.entries.lock().expect("registry").is_empty());
    drop(table.lock(0));
    drop(table.lock_bulk());
}
