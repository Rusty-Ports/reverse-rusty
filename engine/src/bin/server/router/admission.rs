//! Request admission: how many requests the server works on at once (ADR-199).
//!
//! One function classifies a request and one set of pools admits it, in both server modes.
//! Every route passes through it, so a route cannot be unbounded by where it was added.
//!
//! The pools are disjoint. Some requests wait in flight for something another request, or
//! a long maintenance operation, must do first: document writes wait for the engine lock,
//! and a job-status long poll waits for the job's stream to be consumed. A pool those
//! requests shared with the requests they wait for could fill with waiters and never
//! drain. Classes that never share a slot cannot do that to each other.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::Response;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Matching and read requests a serving process works on at once.
pub(crate) const MAX_IN_FLIGHT_READS: usize = 256;

/// Each of the other classes has one slot for this many read slots.
const SMALLER_POOL_DIVISOR: usize = 4;

/// `/_metrics` requests answered at once.
const MAX_IN_FLIGHT_SCRAPES: usize = 8;

/// The class of a request: which pool it takes its slot from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// `/_health`: no slot. The handler bounds itself (ADR-144), and an orchestrator must
    /// get its answer from a server whose every slot is taken.
    Probe,
    /// `/_metrics`, so a full server can still be observed.
    Scrape,
    /// Matching and reads: search and percolate, document reads, points in time, and
    /// starting or streaming an exhaustive job.
    Read,
    /// Document writes: the requests that queue for the engine mutex or the coordinator's
    /// write serializer (`admit_write`, `admit_cluster_write`). A compaction, backup or
    /// rebuild holds that lock for as long as it takes, and they all wait meanwhile.
    Write,
    /// A job-status read, which may long-poll for a completion that is published only once
    /// the job's stream has been consumed.
    JobStatus,
    /// Everything else: administration, statistics, vocabulary, cluster operations, and
    /// any path no rule names.
    Other,
}

/// Classify a request by method and path.
pub(crate) fn admission_for(method: &Method, path: &str) -> Admission {
    let reads = method == Method::GET || method == Method::HEAD;
    match path {
        "/_health" => Admission::Probe,
        "/_metrics" => Admission::Scrape,
        "/_bulk" | "/_flush" => Admission::Write,
        "/_search" | "/v2/_search" | "/_mpercolate" | "/v2/_mpercolate" | "/v2/_pit"
        | "/_percolate/jobs" => Admission::Read,
        "/" if reads => Admission::Read,
        _ => {
            if path.starts_with("/_doc/") {
                return if reads {
                    Admission::Read
                } else {
                    Admission::Write
                };
            }
            match path.strip_prefix("/_percolate/jobs/") {
                Some(job) if job.ends_with("/stream") => Admission::Read,
                Some(_) if reads => Admission::JobStatus,
                // Cancelling a job is not a status read: it must not wait behind them.
                _ => Admission::Other,
            }
        }
    }
}

/// The server's request slots, one pool per class. Clones share them.
#[derive(Clone)]
pub(crate) struct RequestPools {
    reads: Arc<Semaphore>,
    writes: Arc<Semaphore>,
    job_status: Arc<Semaphore>,
    other: Arc<Semaphore>,
    scrapes: Arc<Semaphore>,
}

impl RequestPools {
    /// The pools a serving process uses.
    pub(crate) fn serving() -> Self {
        Self::sized(MAX_IN_FLIGHT_READS)
    }

    /// `reads` slots for matching and reads, and a quarter of that, at least one, for each
    /// of document writes, job-status reads and everything else.
    pub(crate) fn sized(reads: usize) -> Self {
        let smaller = || Arc::new(Semaphore::new(smaller_pool(reads)));
        Self {
            reads: Arc::new(Semaphore::new(reads)),
            writes: smaller(),
            job_status: smaller(),
            other: smaller(),
            scrapes: Arc::new(Semaphore::new(MAX_IN_FLIGHT_SCRAPES)),
        }
    }

    /// Wait for a slot of the pool `admission` names. A request that is dropped while it
    /// waits takes nothing.
    pub(crate) async fn admit(&self, admission: Admission) -> Option<OwnedSemaphorePermit> {
        let pool = match admission {
            Admission::Probe => return None,
            Admission::Scrape => &self.scrapes,
            Admission::Read => &self.reads,
            Admission::Write => &self.writes,
            Admission::JobStatus => &self.job_status,
            Admission::Other => &self.other,
        };
        Some(
            Arc::clone(pool)
                .acquire_owned()
                .await
                .expect("a request pool is never closed"),
        )
    }
}

/// The size of each smaller pool beside `reads` read slots.
fn smaller_pool(reads: usize) -> usize {
    (reads / SMALLER_POOL_DIVISOR).max(1)
}

/// The admission middleware: hold the request's slot until its response head is ready.
///
/// A request that finds no slot waits; it is not refused. None of its body has been read
/// and none of its work has started.
pub(crate) async fn admit(
    State(pools): State<RequestPools>,
    request: Request,
    next: Next,
) -> Response {
    let _slot = pools
        .admit(admission_for(request.method(), request.uri().path()))
        .await;
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(method: &Method, path: &str) -> Admission {
        admission_for(method, path)
    }

    #[test]
    fn document_writes_are_the_write_class() {
        for (method, path) in [
            (Method::PUT, "/_doc/7"),
            (Method::DELETE, "/_doc/7"),
            (Method::POST, "/_doc/7"),
            (Method::POST, "/_bulk"),
            (Method::POST, "/_flush"),
            // `/_flush` accepts GET for Elasticsearch clients, and flushes.
            (Method::GET, "/_flush"),
        ] {
            assert_eq!(class(&method, path), Admission::Write, "{method} {path}");
        }
    }

    #[test]
    fn matching_and_reads_are_the_read_class() {
        for (method, path) in [
            (Method::GET, "/"),
            (Method::HEAD, "/"),
            (Method::GET, "/_doc/7"),
            (Method::HEAD, "/_doc/7"),
            (Method::POST, "/_search"),
            (Method::GET, "/_search"),
            (Method::POST, "/v2/_search"),
            (Method::POST, "/_mpercolate"),
            (Method::POST, "/v2/_mpercolate"),
            (Method::POST, "/v2/_pit"),
            (Method::DELETE, "/v2/_pit"),
            (Method::POST, "/_percolate/jobs"),
            (Method::GET, "/_percolate/jobs/job-id/stream"),
            (Method::POST, "/_percolate/jobs/job-id/stream"),
        ] {
            assert_eq!(class(&method, path), Admission::Read, "{method} {path}");
        }
    }

    /// A status read may wait for the job's stream to be consumed, so the two are never in
    /// one pool; and a cancellation waits behind neither.
    #[test]
    fn a_job_status_read_shares_no_pool_with_its_stream_or_its_cancellation() {
        assert_eq!(
            class(&Method::GET, "/_percolate/jobs/job-id"),
            Admission::JobStatus
        );
        assert_eq!(
            class(&Method::HEAD, "/_percolate/jobs/job-id"),
            Admission::JobStatus
        );
        assert_eq!(
            class(&Method::DELETE, "/_percolate/jobs/job-id"),
            Admission::Other
        );
        assert_eq!(
            class(&Method::GET, "/_percolate/jobs/job-id/stream"),
            Admission::Read
        );
    }

    #[test]
    fn administration_and_unknown_paths_are_the_other_class() {
        for (method, path) in [
            (Method::GET, "/_stats"),
            (Method::GET, "/_cat/segments"),
            (Method::POST, "/_compact"),
            (Method::POST, "/_backup"),
            (Method::GET, "/_vocab"),
            (Method::PUT, "/_vocab"),
            (Method::PUT, "/_settings"),
            (Method::POST, "/_cluster/resync"),
            (Method::GET, "/_cluster/state"),
            (Method::POST, "/"),
            (Method::POST, "/_nothing_here"),
        ] {
            assert_eq!(class(&method, path), Admission::Other, "{method} {path}");
        }
    }

    #[test]
    fn the_probes_take_no_slot_of_any_request_pool() {
        for method in [Method::GET, Method::HEAD, Method::POST] {
            assert_eq!(class(&method, "/_health"), Admission::Probe);
            assert_eq!(class(&method, "/_metrics"), Admission::Scrape);
        }
    }

    #[test]
    fn each_smaller_pool_is_a_quarter_of_the_read_pool_and_at_least_one_slot() {
        assert_eq!(smaller_pool(MAX_IN_FLIGHT_READS), 64);
        assert_eq!(smaller_pool(4), 1);
        assert_eq!(smaller_pool(1), 1);
    }
}
