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
    assert!(DurabilityOp::LogLost.is_data_at_risk());
    assert_eq!(DurabilityOp::LogLost.as_str(), "log_lost");
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

/// The token counts the losses a directory has accepted, so a second loss has its own token
/// even when nothing was flushed in between and the manifest says exactly what it said the
/// first time. A token left in the start-up arguments accepts one loss, not each one.
#[test]
fn a_second_loss_under_the_same_manifest_needs_its_own_token() {
    let (dir, config) = a_store_that_lost_its_log("second_loss_same_manifest");
    let (_, first) = refusal(&config);
    {
        let mut engine =
            Engine::open(make_norm(), accepting(&config, &first)).expect("the first loss");
        // Acknowledged, and only in the new log. Nothing is flushed.
        engine.insert_live("usb hub", 3, 1);
    }
    std::fs::remove_file(dir.join("wal.log")).expect("lose the log again");

    let (reason, second) = refusal(&accepting(&config, &first));
    assert!(reason.contains("names a different loss"), "{reason}");
    assert_eq!(
        second.rsplit_once(':').map(|(what, _)| what),
        first.rsplit_once(':').map(|(what, _)| what),
        "precondition: the manifest says what it said: {first} then {second}"
    );
    assert!(first.ends_with(":1") && second.ends_with(":2"));
    assert!(
        !dir.join("wal.log").exists(),
        "the old token accepted a new loss"
    );
    assert_eq!(accepted_log_losses(&dir).expect("the record").len(), 1);
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

/// A write of the record can fail after it has renamed the file into place and before the
/// directory is synced. The record is then visible and not yet durable. A later start must
/// not act on it as it is: it makes the record durable first, and when it cannot, it fails.
/// (Here the directory cannot be opened for the sync at all. Acting on the visible entry,
/// successive starts replaced the log, marked the entry applied and then opened, with no
/// sync of the directory having succeeded.)
#[cfg(unix)]
#[test]
fn a_record_that_cannot_be_made_durable_is_not_acted_on() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, config) = a_store_that_lost_its_log("record_not_durable");
    let (_, token) = refusal(&config);
    let original = std::fs::metadata(&dir).expect("metadata").permissions();
    // Write and search, no read: files in the directory can be created, renamed and read,
    // and the directory itself cannot be opened to be synced.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o300)).expect("chmod");
    let opened: Vec<bool> = (0..5)
        .map(|_| Engine::open(make_norm(), accepting(&config, &token)).is_ok())
        .collect();
    std::fs::set_permissions(&dir, original).expect("restore permissions");

    assert_eq!(
        opened, [false; 5],
        "a start acted on a record it could not make durable"
    );
    assert!(
        !dir.join("wal.log").exists(),
        "the log was replaced on the strength of a record that was not durable"
    );
    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("with the directory back"));
    let recorded = accepted_log_losses(&dir).expect("the record");
    assert_eq!(recorded.len(), 1, "{recorded:?}");
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

/// A backup carries the record: a store restored from it holds no more than the one it was
/// taken from, and says so.
#[test]
fn a_backup_carries_the_record_of_accepted_losses() {
    let (dir, config) = a_store_that_lost_its_log("backup_carries_record");
    let (_, token) = refusal(&config);
    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("accepted"));
    let copy = test_dir("backup_carries_record_copy");
    let _ = std::fs::remove_dir_all(&copy);
    reverse_rusty::storage::copy_engine_dir(&dir, &copy).expect("backup");
    assert_eq!(
        accepted_log_losses(&copy).expect("the copy's record"),
        accepted_log_losses(&dir).expect("the record")
    );
    let restored = Engine::open(
        make_norm(),
        EngineConfig {
            data_dir: Some(copy.clone()),
            ..EngineConfig::default()
        },
    )
    .expect("the copy opens");
    assert!(match_ids(&restored, FLUSHED).contains(&1));
    drop(restored);

    // A record that cannot be read would restore to a store that does not open: such a
    // backup is not taken, and one that holds such a record does not verify.
    std::fs::write(dir.join(ACCEPTED_LOG_LOSSES_FILE), "not a record\n").expect("damage it");
    let refused = test_dir("backup_carries_record_refused");
    let _ = std::fs::remove_dir_all(&refused);
    assert!(
        reverse_rusty::storage::copy_engine_dir(&dir, &refused).is_err(),
        "a backup was taken with a record no open can read"
    );
    assert!(!refused.exists(), "a refused backup was left in place");
    std::fs::write(copy.join(ACCEPTED_LOG_LOSSES_FILE), "not a record\n").expect("damage it");
    assert!(reverse_rusty::storage::verify_backup(&copy).is_err());
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&copy);
}

/// The record is evidence, and it is checked for being whole: it ends with a line that
/// counts and checksums its entries. A record that was damaged, emptied or cut short does
/// not open as a shorter history, with the store's log present or without it. If it did, the
/// losses it had recorded would no longer be reported, and a token that had been spent
/// would accept a second loss.
#[test]
fn a_record_that_is_not_whole_is_not_read_as_a_shorter_history() {
    let (dir, config) = a_store_that_lost_its_log("record_not_whole");
    let (_, token) = refusal(&config);
    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("accepted"));
    let record = dir.join(ACCEPTED_LOG_LOSSES_FILE);
    let whole = std::fs::read_to_string(&record).expect("the record");
    let entry_only = whole
        .lines()
        .next()
        .map(|entry| format!("{entry}\n"))
        .expect("one entry");
    assert_ne!(entry_only, whole, "precondition: a closing line follows");

    for (what, damaged) in [
        ("damaged", "not a record\n".to_string()),
        ("emptied", String::new()),
        ("cut short after its entry", entry_only),
    ] {
        std::fs::write(&record, &damaged).expect("damage it");
        let reason = match Engine::open(make_norm(), config.clone()) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("{what}: opened over a record that is not whole"),
        };
        assert!(reason.contains("cannot be read"), "{what}: {reason}");
        assert!(
            reason.contains("a token that was spent would be good once more"),
            "{what}: the refusal does not say what moving the record aside does: {reason}"
        );
        assert!(accepted_log_losses(&dir).is_err(), "{what}");

        // The log is lost again, under the same manifest. The spent token accepts nothing.
        let wal = dir.join("wal.log");
        let held = std::fs::read(&wal).expect("the log");
        std::fs::remove_file(&wal).expect("lose the log");
        assert!(
            Engine::open(make_norm(), accepting(&config, &token)).is_err(),
            "{what}: a spent token accepted a second loss"
        );
        assert!(!wal.exists(), "{what}: a log was created");
        std::fs::write(&wal, held).expect("put the log back");
    }

    std::fs::write(&record, whole).expect("restore the record");
    drop(Engine::open(make_norm(), config).expect("with its record whole"));
    assert_eq!(accepted_log_losses(&dir).expect("the record").len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A record can be whole and still hold an entry this release does not understand (one
/// written by a later release, say). Such an entry is not skipped: skipping it would read
/// the record as a shorter history, and the count of losses is what a token is checked
/// against.
#[test]
fn an_entry_this_release_does_not_understand_is_not_skipped() {
    let (dir, config) = a_store_that_lost_its_log("entry_not_understood");
    let (_, token) = refusal(&config);
    drop(Engine::open(make_norm(), accepting(&config, &token)).expect("accepted"));
    let record = dir.join(ACCEPTED_LOG_LOSSES_FILE);
    let whole = std::fs::read_to_string(&record).expect("the record");
    let known = whole.lines().next().expect("one entry");
    // The entry it has, and one in a format of the future, under a closing line that
    // matches both.
    let entries = format!("{known}\nv2\t1700000000\tapplied\twal.log\tsomething\tnew\n");
    let closing = format!(
        "end\t2\t{:08x}\n",
        reverse_rusty::storage::crc32(entries.as_bytes())
    );
    std::fs::write(&record, format!("{entries}{closing}")).expect("a record from the future");

    let reason = match Engine::open(make_norm(), config) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("opened over an entry it does not understand"),
    };
    assert!(reason.contains("cannot be read"), "{reason}");
    assert!(accepted_log_losses(&dir).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
