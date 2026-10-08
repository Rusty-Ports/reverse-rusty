//! What the server says about the log losses its store has accepted (ADR-216).
//!
//! The record is a file in the data directory, so this reads the file and does not depend
//! on an event having been delivered: a start that accepted a loss and then failed has
//! still left it, and the start after reports it.

use std::path::Path;

use prometheus::{IntGauge, Opts};
use reverse_rusty::storage::{accepted_log_losses, AcceptedLogLoss, ACCEPTED_LOG_LOSSES_FILE};
use tracing::{error, warn};

use crate::metrics::PrometheusMetrics;

/// The losses recorded in `data_dir`, or an exit: a record that cannot be read is evidence
/// that cannot be reported.
fn recorded(data_dir: &Path) -> Vec<AcceptedLogLoss> {
    match accepted_log_losses(data_dir) {
        Ok(losses) => losses,
        Err(e) => {
            error!(error = %e, "cannot read the record of accepted log losses; refusing to start");
            std::process::exit(1);
        }
    }
}

/// How many losses had been accepted and carried out before this start opened the store.
pub(crate) fn carried_out_before(data_dir: Option<&Path>) -> usize {
    data_dir.map_or(0, |dir| {
        recorded(dir).iter().filter(|loss| loss.applied).count()
    })
}

/// After the store is open: publish the two gauges and say what the record holds.
///
/// The gauges are values, set once per start and registered even when they are zero, so an
/// alert on them needs no sample from before the restart. `given` is `--accept-lost-log`.
pub(crate) fn report(
    prom: &PrometheusMetrics,
    data_dir: Option<&Path>,
    given: Option<&str>,
    carried_out_before: usize,
) {
    let losses = data_dir.map(recorded).unwrap_or_default();
    let count = IntGauge::with_opts(Opts::new(
        "log_losses_accepted",
        "Log losses this store's data directory has accepted (see log_loss.accepted)",
    ))
    .expect("gauge");
    let latest = IntGauge::with_opts(Opts::new(
        "log_loss_last_accepted_timestamp_seconds",
        "Unix time of the most recent accepted log loss; 0 when there is none",
    ))
    .expect("gauge");
    count.set(i64::try_from(losses.len()).unwrap_or(i64::MAX));
    let last_at = losses
        .iter()
        .map(|loss| loss.accepted_at)
        .max()
        .unwrap_or(0);
    latest.set(i64::try_from(last_at).unwrap_or(i64::MAX));
    prom.registry
        .register(Box::new(count))
        .expect("register log_losses_accepted");
    prom.registry
        .register(Box::new(latest))
        .expect("register log_loss_last_accepted_timestamp_seconds");

    let carried_out = losses.iter().filter(|loss| loss.applied).count();
    let accepted_now = carried_out > carried_out_before;
    if accepted_now {
        warn!(
            "started with the loss of a log accepted: the writes that were only in it are \
             gone. Remove --accept-lost-log; it accepts no other loss, and does nothing now"
        );
    } else if let Some(token) = given {
        warn!(
            token = %token,
            "--accept-lost-log was given and there was no such loss to accept; remove it"
        );
    }
    if let (Some(dir), Some(last)) = (data_dir, losses.last()) {
        warn!(
            accepted = losses.len(),
            latest_log = %last.log,
            latest_accepted_at_unix = last_at,
            record = %dir.join(ACCEPTED_LOG_LOSSES_FILE).display(),
            "this store has accepted the loss of a log before: it holds less than was \
             acknowledged to its clients"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge(prom: &PrometheusMetrics, name: &str) -> i64 {
        prom.registry
            .gather()
            .iter()
            .find(|family| family.name() == name)
            .unwrap_or_else(|| panic!("{name} is not exported"))
            .get_metric()[0]
            .get_gauge()
            .get_value() as i64
    }

    /// Both gauges are exported from the first scrape, at zero for a store that has accepted
    /// nothing (and for one with no data directory), so an alert on their value needs no
    /// sample from before a restart.
    #[test]
    fn the_gauges_are_exported_at_zero_for_a_store_that_accepted_nothing() {
        for data_dir in [None, Some(std::env::temp_dir())] {
            let prom = PrometheusMetrics::new();
            report(&prom, data_dir.as_deref(), None, 0);
            assert_eq!(gauge(&prom, "reverse_rusty_log_losses_accepted"), 0);
            assert_eq!(
                gauge(
                    &prom,
                    "reverse_rusty_log_loss_last_accepted_timestamp_seconds"
                ),
                0
            );
        }
    }

    /// The gauges are read from the record in the data directory, so a start that did not
    /// accept the loss itself (the one after a start that accepted it and failed) reports it.
    #[test]
    fn the_gauges_are_read_from_the_record_in_the_data_directory() {
        let dir = std::env::temp_dir().join(format!("rr_log_loss_report_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("data dir");
        let entries = "v1\t1700000000\tapplied\twal.log\tsegment-2-seq-5\n\
                       v1\t1700000500\tpending\twal.log\tsegment-4-seq-9\n";
        // The record ends with a line that counts and checksums its entries.
        let closing = format!(
            "end\t2\t{:08x}\n",
            reverse_rusty::storage::crc32(entries.as_bytes())
        );
        std::fs::write(
            dir.join(ACCEPTED_LOG_LOSSES_FILE),
            format!("{entries}{closing}"),
        )
        .expect("a record");
        assert_eq!(carried_out_before(Some(&dir)), 1);
        let prom = PrometheusMetrics::new();
        report(&prom, Some(&dir), None, 1);
        assert_eq!(gauge(&prom, "reverse_rusty_log_losses_accepted"), 2);
        assert_eq!(
            gauge(
                &prom,
                "reverse_rusty_log_loss_last_accepted_timestamp_seconds"
            ),
            1_700_000_500
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
