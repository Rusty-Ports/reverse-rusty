//! The `--load-file` startup policy for the single-node engine (ADR-188): the file seeds an
//! EMPTY engine. A reopened data directory that already holds queries keeps them and the
//! file is skipped, exactly as in cluster mode — loading it again would store every query
//! in the file a second time on each restart.

use reverse_rusty::segment::{Engine, IngestReport};

/// What the startup query file did.
#[derive(Debug)]
pub(crate) enum Preload {
    /// The engine was empty and the file's queries are now its first base segment.
    Loaded(IngestReport),
    /// The engine already held `existing` queries; the file was not applied.
    SkippedPopulated { existing: usize },
}

/// Apply the startup query file to `engine`. All-or-nothing: an error means the initial
/// load could not be durably committed and nothing was ingested.
pub(crate) fn preload_queries(
    engine: &mut Engine,
    queries: &[(u64, String)],
) -> std::io::Result<Preload> {
    match engine.num_queries() {
        0 => engine.try_build_from_queries(queries).map(Preload::Loaded),
        existing => Ok(Preload::SkippedPopulated { existing }),
    }
}

#[cfg(test)]
mod tests {
    use super::{preload_queries, Preload};
    use reverse_rusty::config::EngineConfig;
    use reverse_rusty::normalize::Normalizer;
    use reverse_rusty::segment::{Engine, MatchScratch};

    fn read(engine: &Engine, title: &str) -> Vec<u64> {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        engine.match_title(title, &mut scratch, &mut out, true);
        out.sort_unstable();
        out
    }

    #[test]
    fn a_restart_with_the_same_load_file_changes_nothing() {
        let dir = std::env::temp_dir().join(format!(
            "reverse_rusty_preload_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        let norm = || Normalizer::default_vocab().expect("normalizer");
        let file: Vec<(u64, String)> = (0..40u64)
            .map(|i| (i, format!("widget model{i}")))
            .collect();
        {
            let mut engine = Engine::with_config(norm(), config.clone());
            let loaded = preload_queries(&mut engine, &file).expect("first load");
            assert!(matches!(loaded, Preload::Loaded(report) if report.ingested == 40));
            // A live write, so the store is not just the file.
            engine.insert_live("gadget special", 500, 1);
        }
        for restart in 0..2 {
            let mut engine = Engine::open(norm(), config.clone()).expect("reopen");
            let before = (engine.num_queries(), read(&engine, "widget model7 extra"));
            let outcome = preload_queries(&mut engine, &file).expect("restart load");
            assert!(
                matches!(outcome, Preload::SkippedPopulated { existing: 41 }),
                "restart {restart}: {outcome:?}"
            );
            assert_eq!(
                (engine.num_queries(), read(&engine, "widget model7 extra")),
                before,
                "restart {restart}: the load file must not be applied twice"
            );
            assert_eq!(before.1, vec![7]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
