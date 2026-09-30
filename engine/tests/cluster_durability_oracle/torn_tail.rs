//! A torn log tail drops only the torn record; acknowledged writes survive — and the
//! fsync policy is invisible to recovery.

use crate::harness::*;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use std::sync::{Arc, Mutex};

#[test]
fn torn_log_tail_recovers_acknowledged_writes() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("torn");

    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), true), &queries)
            .expect("durable cluster builds");
        apply_churn(&cluster, &added, &removed);
    }
    // Corrupt the tail: append junk that cannot frame a valid record.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("cluster.log"))
            .expect("open log");
        f.write_all(&[0xFF, 0xFF, 0xFF, 0x7F, 0x01, 0x02, 0x03])
            .expect("corrupt");
    }

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    reopened.set_observer(Arc::new(move |event: &EngineEvent| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::WalTornTail,
            error,
            ..
        } = event
        {
            seen.lock().unwrap().push(error.clone());
        }
    }));
    assert_eq!(*events.lock().unwrap(), vec!["7 bytes"]);
    let brute = Brute::build(&final_live(&queries, &added, &removed));
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "torn-tail reopen {t:?}");
    }

    let later_id = u64::MAX - 1;
    reopened
        .upsert_query(later_id, &queries[0].1, 1)
        .expect("append after repair");
    drop(reopened);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("second reopen");
    reopened.set_observer(Arc::new(|event| {
        assert!(
            !matches!(
                event,
                EngineEvent::DurabilityFailure {
                    op: DurabilityOp::WalTornTail,
                    ..
                }
            ),
            "clean second restart must not repeat the diagnostic"
        );
    }));
    let mut live = final_live(&queries, &added, &removed);
    live.push((later_id, queries[0].1.clone()));
    let brute = Brute::build(&live);
    for title in &titles {
        let want = brute.matches(title, &mut lc, &mut feats);
        let got: HashSet<_> = reopened.percolate(title).unwrap().into_iter().collect();
        assert_eq!(got, want, "second restart after repair {title:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---- fsync policy is invisible to recovery ----

#[test]
fn fsync_policy_does_not_change_recovery() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    for &fsync in &[false, true] {
        let dir = unique_dir(&format!("fsync_{fsync}"));
        {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), fsync), &queries)
                    .expect("durable cluster builds");
            apply_churn(&cluster, &added, &removed);
        }
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        let brute = Brute::build(&final_live(&queries, &added, &removed));
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for t in &titles {
            let want = brute.matches(t, &mut lc, &mut feats);
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, want, "fsync={fsync} {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
