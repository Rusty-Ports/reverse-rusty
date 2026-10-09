//! A layout that has been replaced is freed by a thread started for it, and not by a search
//! that was still reading it (ADR-225).

use super::layout_change::in_memory;
use super::*;

/// Whoever drops the last handle on a replaced layout frees every shard it held. A search
/// holds a layout for one title, so its handle must not be the last: after a layout is
/// replaced, something other than the reader keeps it until the readers are done, and frees
/// it then.
#[test]
fn a_reader_is_not_the_one_that_frees_a_replaced_layout() {
    let cluster = in_memory(2, 200);
    let read_by_a_search = cluster.layout();
    let replaced = Arc::downgrade(&read_by_a_search);

    cluster.resize(3).expect("resize");
    assert!(
        replaced.strong_count() >= 2,
        "the search's handle is the only one left on the layout it reads"
    );
    drop(read_by_a_search);
    // The other holder lets go once the reader has, and the layout is freed there.
    let deadline = Instant::now() + Duration::from_secs(10);
    while replaced.strong_count() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(replaced.strong_count(), 0, "the replaced layout was freed");
}

/// An in-memory engine never runs the cleanup that forgets the layouts it has replaced, so
/// a layout change forgets the ones that have been freed. They are freed by other threads,
/// in their own time: the test waits for those, and the next change is then left
/// remembering the one layout it replaced.
#[test]
fn replaced_layouts_are_forgotten_once_released() {
    let cluster = in_memory(3, 50);
    for shards in [4, 5, 6, 3, 4, 5, 6] {
        cluster.resize(shards).expect("resize");
    }
    let all_freed = || {
        let retired = cluster.retired_layouts.lock().expect("retired");
        retired.iter().all(|layout| layout.strong_count() == 0)
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !all_freed() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(all_freed(), "a replaced layout that nothing reads is held");

    cluster.resize(3).expect("resize");
    let remembered = cluster.retired_layouts.lock().expect("retired").len();
    assert_eq!(
        remembered, 1,
        "{remembered} replaced layouts are remembered after all but the last were freed"
    );
}

/// When the thread cannot be started the layout change still publishes, the replaced layout
/// is freed by its last holder as it was before there was a thread, and the failure is
/// reported as one that put no data at risk.
#[test]
fn a_release_thread_that_cannot_be_started_is_reported() {
    let cluster = in_memory(2, 200);
    let reported = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&reported);
    cluster.set_observer(Arc::new(move |event: &crate::events::EngineEvent| {
        if let crate::events::EngineEvent::DurabilityFailure { op, detail, .. } = event {
            seen.lock().expect("seen").push((*op, detail.clone()));
        }
    }));
    cluster.no_release_thread.store(true, Ordering::SeqCst);
    let read_by_a_search = cluster.layout();
    let replaced = Arc::downgrade(&read_by_a_search);

    cluster.resize(3).expect("resize");
    assert_eq!(cluster.num_shards(), 3);
    {
        let reported = reported.lock().expect("reported");
        let [(op, detail)] = reported.as_slice() else {
            panic!("one failure is reported, and these were: {reported:?}");
        };
        assert_eq!(*op, DurabilityOp::ThreadStart);
        assert_eq!(op.as_str(), "thread_start");
        assert!(!op.is_data_at_risk());
        assert!(detail.contains("rr-layout-release"), "{detail}");
    }
    assert_eq!(
        replaced.strong_count(),
        1,
        "something other than the reader holds the replaced layout"
    );
    drop(read_by_a_search);
    assert_eq!(replaced.strong_count(), 0, "the replaced layout was freed");

    // The next change starts its thread again.
    cluster.no_release_thread.store(false, Ordering::SeqCst);
    cluster.resize(2).expect("resize");
    assert_eq!(reported.lock().expect("reported").len(), 1);
}
