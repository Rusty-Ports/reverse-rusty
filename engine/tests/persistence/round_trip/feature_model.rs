//! ADR-184 (RR-003): a durable single-node engine records the feature model it compiled its
//! corpus under, restores it on reopen, and refuses to serve the corpus under any other.

use super::*;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::error::FeatureModelMismatch;
use reverse_rusty::normalize::NormalizerBuilder;

fn durable(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        // Keep live inserts in the WAL tail unless a test flushes explicitly.
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    }
}

/// `ny => new york`: an active multi-word alias. Queries naming either form collapse to the
/// `term:new_york` entity, which only a normalizer that registers the alias phrase emits.
pub(super) fn ny_alias() -> Vocab {
    let mut vocab = Vocab::new();
    vocab
        .import_solr_aliases(
            "ny => new york",
            &Normalizer::default_vocab().expect("stock normalizer"),
            &reverse_rusty::dict::Dict::new(),
        )
        .expect("valid alias fixture");
    vocab
}

pub(super) fn tee_normalizer() -> Normalizer {
    NormalizerBuilder::new()
        .synonym("tee", "term:shirt", FeatureKind::Generic)
        .build()
        .expect("synonym normalizer")
}

fn mismatch(error: &std::io::Error) -> FeatureModelMismatch {
    *error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<FeatureModelMismatch>())
        .unwrap_or_else(|| panic!("expected a FeatureModelMismatch, got {error}"))
}

/// The RR-003 failure: a runtime alias recompiles and commits the corpus, and a restart
/// without the vocab file used to reopen it under the stock normalizer — so the title
/// `new york inventory` (no alias phrase registered) missed the query entirely.
#[test]
fn restart_without_the_vocab_file_keeps_a_runtime_alias() {
    let dir = test_dir("rr003_runtime_alias");
    {
        let mut engine = Engine::open(make_norm(), durable(&dir)).expect("fresh durable engine");
        engine.build_from_queries(&[(1, "new york inventory".into())]);
        engine.set_vocab(ny_alias()).expect("runtime alias import");
        assert_eq!(engine.recompile_stale_segments(), 1);
        assert!(engine.persistence_healthy());
        for title in ["new york inventory", "ny inventory"] {
            assert!(
                match_ids(&engine, title).contains(&1),
                "before restart: {title}"
            );
        }
    }

    // Reopen exactly as a server with no --vocab-file does.
    let engine = Engine::open(make_norm(), durable(&dir)).expect("reopen");
    assert!(
        engine.vocab().is_some(),
        "the manifest must restore the recorded vocabulary"
    );
    for title in ["new york inventory", "ny inventory"] {
        assert!(
            match_ids(&engine, title).contains(&1),
            "FN after restart without the vocab file: {title}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Queries written after the vocabulary change — flushed, and still only in the WAL tail —
/// recover under the recorded alias. A segment flushed after the change must not look stale
/// (its mmap epoch is carried over), or ADR-184 would refuse to commit it.
#[test]
fn restart_recovers_post_change_writes_under_the_recorded_alias() {
    let dir = test_dir("rr003_wal_tail_alias");
    {
        let mut engine = Engine::open(make_norm(), durable(&dir)).expect("fresh durable engine");
        engine.build_from_queries(&[(1, "new york inventory".into())]);
        engine.set_vocab(ny_alias()).expect("runtime alias import");
        engine.recompile_stale_segments();
        engine.try_insert_live("ny catalog", 2, 1).expect("insert");
        engine.flush();
        assert!(
            !engine.has_stale_segments(),
            "a segment flushed under the current vocabulary is not stale"
        );
        engine.try_insert_live("ny parts", 3, 1).expect("insert");
    }

    let engine = Engine::open(make_norm(), durable(&dir)).expect("reopen");
    for (title, id) in [
        ("new york inventory", 1),
        ("new york catalog", 2),
        ("ny catalog", 2),
        ("new york parts", 3),
    ] {
        assert!(
            match_ids(&engine, title).contains(&id),
            "FN after restart: {title} must match {id}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A vocabulary change on an empty engine has nothing to recompile, so the recompile that
/// normally commits it is a no-op. It must still be recorded: otherwise the restart builds a
/// fresh stock engine and replays the post-change write without the alias.
#[test]
fn a_vocab_change_on_an_empty_durable_engine_survives_restart() {
    let dir = test_dir("rr003_empty_engine_vocab");
    {
        let mut engine = Engine::open(make_norm(), durable(&dir)).expect("fresh durable engine");
        engine
            .set_vocab(ny_alias())
            .expect("vocabulary on an empty engine");
        assert_eq!(engine.recompile_stale_segments(), 0);
        assert!(engine.persistence_healthy());
        engine.try_insert_live("ny catalog", 1, 1).expect("insert");
    }

    let engine = Engine::open(make_norm(), durable(&dir)).expect("reopen");
    assert!(
        engine.vocab().is_some(),
        "the vocabulary change was recorded"
    );
    assert!(
        match_ids(&engine, "new york catalog").contains(&1),
        "FN: the WAL tail must replay under the recorded alias"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A corpus built from a bare normalizer has no vocabulary to restore, so the recorded
/// fingerprint is the only guard: reopening it under another normalizer fails loud, with
/// both fingerprints named, instead of silently missing every synonym-bearing title.
#[test]
fn reopen_under_a_different_bare_normalizer_fails_loud() {
    let dir = test_dir("rr003_bare_normalizer_mismatch");
    {
        let mut engine = Engine::open(tee_normalizer(), durable(&dir)).expect("fresh engine");
        engine.build_from_queries(&[(1, "black tee".into())]);
        assert!(match_ids(&engine, "black tee").contains(&1));
    }

    let error = Engine::open(make_norm(), durable(&dir)).expect_err("stock normalizer");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        mismatch(&error),
        FeatureModelMismatch {
            recorded: tee_normalizer().fingerprint(),
            supplied: make_norm().fingerprint(),
        }
    );
    // A vocabulary whose normalizer differs is refused the same way.
    let mut punctuation = Vocab::new();
    punctuation.fold_punctuation('\'');
    let error = Engine::open_with_vocab(punctuation, durable(&dir)).expect_err("other vocab");
    mismatch(&error);

    let engine = Engine::open(tee_normalizer(), durable(&dir)).expect("the recorded normalizer");
    assert!(match_ids(&engine, "black tee").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A vocabulary-only commit (here: recording review candidates) captures no memtable state, so
/// it must keep the previous WAL watermark. Advancing it would make recovery skip the logged
/// delete while the insert it deletes still replays — resurrecting an acknowledged delete.
#[test]
fn a_vocabulary_only_commit_does_not_resurrect_a_logged_delete() {
    let dir = test_dir("rr003_vocab_commit_watermark");
    {
        let mut engine = Engine::open(make_norm(), durable(&dir)).expect("fresh durable engine");
        engine.build_from_queries(&[(1, "usb hub silver".into())]);
        engine
            .try_insert_live("desk lamp chrome", 2, 1)
            .expect("insert");
        engine.delete_by_logical_id(2).expect("delete");
        engine
            .record_discovered_aliases(&[])
            .expect("vocabulary-only commit");
        assert!(engine.persistence_healthy());
        assert!(match_ids(&engine, "desk lamp chrome").is_empty());
    }

    let engine = Engine::open(make_norm(), durable(&dir)).expect("reopen");
    assert!(
        !match_ids(&engine, "desk lamp chrome").contains(&2),
        "an acknowledged delete resurrected after a vocabulary-only commit"
    );
    assert!(match_ids(&engine, "usb hub silver").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// While a `set_vocab` awaits its recompile, no single recorded model describes the corpus,
/// so a commit in that window is refused and the previous manifest stays authoritative.
#[test]
fn no_commit_records_a_corpus_compiled_under_two_models() {
    let dir = test_dir("rr003_stale_commit_refused");
    {
        let mut engine = Engine::open(make_norm(), durable(&dir)).expect("fresh durable engine");
        engine.build_from_queries(&[(1, "new york inventory".into())]);
        engine.set_vocab(ny_alias()).expect("alias");
        assert!(engine.has_stale_segments());
        engine
            .try_insert_live("usb hub silver", 2, 1)
            .expect("insert");
        engine.flush();
        assert!(
            engine.has_stale_segments(),
            "the flush must not hide the stale base"
        );
    }

    let manifest = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("read");
    assert!(
        manifest.vocab_data.is_empty(),
        "the refused commit left the pre-change (stock) manifest authoritative"
    );
    let engine = Engine::open(make_norm(), durable(&dir)).expect("reopen the stock model");
    assert!(match_ids(&engine, "new york inventory").contains(&1));
    assert!(
        match_ids(&engine, "usb hub silver").contains(&2),
        "the write survives in the WAL the refused flush kept"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
