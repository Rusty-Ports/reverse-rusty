//! A lost log can be accepted, by one explicit step that is written down first (ADR-216).

use std::sync::{Arc, Mutex};

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use reverse_rusty::segment::Engine;
use reverse_rusty::storage::{accepted_log_losses, ACCEPTED_LOG_LOSSES_FILE};

const FLUSHED: &str = "1986 vertex wireless mouse new";
const ONLY_IN_THE_LOG: &str = "2003 acme mechanical keyboard new";

/// A store with query 1 flushed to a segment and query 2 acknowledged and only in the log,
/// and then the log gone.
fn a_store_that_lost_its_log(tag: &str) -> (std::path::PathBuf, EngineConfig) {
    let dir = test_dir(tag);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.insert_live("wireless mouse", 1, 1);
        engine.flush();
        assert!(engine.persistence_healthy(), "precondition: flushed");
        engine.insert_live("mechanical keyboard", 2, 1);
    }
    std::fs::remove_file(dir.join("wal.log")).expect("lose the log");
    (dir, config)
}

fn accepting(config: &EngineConfig, token: &str) -> EngineConfig {
    EngineConfig {
        accept_lost_log: Some(token.to_string()),
        ..config.clone()
    }
}

/// The refusal of `config`'s store, and the token it names.
fn refusal(config: &EngineConfig) -> (String, String) {
    let reason = match Engine::open(make_norm(), config.clone()) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("a store without its log opened"),
    };
    let named = reason
        .split("accept_lost_log set to \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("the refusal names no token: {reason}"))
        .to_string();
    (reason, named)
}

/// A store whose log is gone refuses, and the refusal names a token for that loss. Opened
/// with the token, the store records the loss, puts an empty log in place and opens with
/// what it had flushed. The token is for that loss only: a second loss is refused under it.
#[test]
fn a_lost_log_is_accepted_by_the_token_its_refusal_names() {
    let (dir, config) = a_store_that_lost_its_log("accepted_loss");
    let (_, token) = refusal(&config);
    assert!(
        token.starts_with("wal.log:") && token.ends_with(":1"),
        "{token}"
    );

    let events = Arc::new(Mutex::new(Vec::new()));
    {
        let mut engine =
            Engine::open(make_norm(), accepting(&config, &token)).expect("the loss is accepted");
        let seen = Arc::clone(&events);
        engine.set_observer(move |event: &EngineEvent| {
            if let EngineEvent::DurabilityFailure { op, .. } = event {
                seen.lock().unwrap().push(*op);
            }
        });
        assert!(match_ids(&engine, FLUSHED).contains(&1));
        assert!(
            !match_ids(&engine, ONLY_IN_THE_LOG).contains(&2),
            "the write that was only in the log is gone"
        );
        // The store takes writes again, and they survive a restart.
        engine.insert_live("usb hub", 3, 1);
    }
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [DurabilityOp::LogLost],
        "the start that accepts a loss reports it"
    );
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert!(recorded[0].applied && recorded[0].log == "wal.log" && recorded[0].accepted_at > 0);

    // A later start needs no token and accepts nothing more, with or without the old one.
    for config in [config.clone(), accepting(&config, &token)] {
        let mut engine = Engine::open(make_norm(), config).expect("a store with its log");
        assert!(match_ids(&engine, "a usb hub").contains(&3));
        engine.flush();
        engine.insert_live("laptop stand", 4, 1);
    }
    assert_eq!(accepted_log_losses(&dir).expect("the record").len(), 1);

    // The log is lost a second time. The old token does not accept it.
    std::fs::remove_file(dir.join("wal.log")).expect("lose the log again");
    let (reason, second) = refusal(&accepting(&config, &token));
    assert!(reason.contains("names a different loss"), "{reason}");
    assert!(second.ends_with(":2") && second != token, "{second}");
    assert!(!dir.join("wal.log").exists(), "a refusal created a log");
    assert_eq!(accepted_log_losses(&dir).expect("the record").len(), 1);

    drop(Engine::open(make_norm(), accepting(&config, &second)).expect("the second loss"));
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 2);
    assert!(recorded.iter().all(|loss| loss.applied));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A token that is not the one the refusal names accepts nothing and changes nothing.
#[test]
fn a_token_for_another_loss_accepts_nothing() {
    let (dir, config) = a_store_that_lost_its_log("wrong_token");
    for wrong in ["true", "wal.log", "wal.log:segment-0-seq-0:1"] {
        let (reason, _) = refusal(&accepting(&config, wrong));
        assert!(reason.contains("names a different loss"), "{reason}");
        assert!(!dir.join("wal.log").exists(), "a refusal created a log");
        assert!(
            !dir.join(ACCEPTED_LOG_LOSSES_FILE).exists(),
            "a refusal recorded a loss"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The loss is on record before the log is replaced. A start that accepts it and stops
/// before the empty log is in place has left a pending entry and no log: the store is still
/// refused without the token, and the same token finishes it without recording it twice.
#[test]
fn the_loss_is_recorded_before_the_log_is_replaced() {
    let (dir, config) = a_store_that_lost_its_log("recorded_first");
    let (_, token) = refusal(&config);
    // The empty log is written to this name and renamed: a directory there stops it.
    let blocker = dir.join("wal.log.tmp");
    std::fs::create_dir(&blocker).expect("block the new log");
    assert!(
        Engine::open(make_norm(), accepting(&config, &token)).is_err(),
        "precondition: the start stopped after accepting"
    );
    std::fs::remove_dir(&blocker).expect("unblock");

    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "the loss is on record: {recorded:?}");
    assert!(!recorded[0].applied, "and not carried out");
    assert!(!dir.join("wal.log").exists());

    // Without the token the store is refused, and is offered the same token.
    let (_, again) = refusal(&config);
    assert_eq!(again, token, "an unfinished acceptance changes the token");

    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("finished"));
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "recorded twice: {recorded:?}");
    assert!(recorded[0].applied);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A start that accepted the loss, replaced the log and then stopped has left a pending
/// entry beside a log. The next start is given no token, finds a log and opens, and the
/// record says the loss was carried out: it is not forgotten because the start that made it
/// did not finish. (This is the case the first form of this feature lost: its evidence was
/// in the memory of the start that failed.)
#[test]
fn a_start_that_stopped_after_replacing_the_log_has_still_left_the_record() {
    let (dir, config) = a_store_that_lost_its_log("stopped_after_replacing");
    let (_, token) = refusal(&config);
    let blocker = dir.join("wal.log.tmp");
    std::fs::create_dir(&blocker).expect("block the new log");
    assert!(Engine::open(make_norm(), accepting(&config, &token)).is_err());
    std::fs::remove_dir(&blocker).expect("unblock");
    // The empty log that start would have put in place, taken from a new store.
    let fresh = test_dir("stopped_after_replacing_fresh");
    drop(
        Engine::open(
            make_norm(),
            EngineConfig {
                data_dir: Some(fresh.clone()),
                ..EngineConfig::default()
            },
        )
        .expect("a new store"),
    );
    std::fs::copy(fresh.join("wal.log"), dir.join("wal.log")).expect("the empty log");

    let engine = Engine::open(make_norm(), config).expect("a store with a log, and no token");
    assert!(match_ids(&engine, FLUSHED).contains(&1));
    drop(engine);
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0].applied,
        "the loss was carried out: {recorded:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&fresh);
}

/// The record is evidence. A store whose record cannot be read does not open as if it had
/// none, with its log or without it.
#[test]
fn a_record_that_cannot_be_read_is_not_ignored() {
    let (dir, config) = a_store_that_lost_its_log("unreadable_record");
    let (_, token) = refusal(&config);
    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("accepted"));
    std::fs::write(dir.join(ACCEPTED_LOG_LOSSES_FILE), "not a record\n").expect("damage it");
    let reason = match Engine::open(make_norm(), config.clone()) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("opened over a record it could not read"),
    };
    assert!(reason.contains("cannot be read"), "{reason}");
    assert!(accepted_log_losses(&dir).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
