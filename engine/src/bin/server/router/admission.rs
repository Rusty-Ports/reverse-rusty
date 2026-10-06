//! Request admission: how many requests the server works on at once (ADR-199).
//!
//! One function classifies a request and one pool admits it, in both server modes. Every
//! route passes through it, so a route cannot be outside the pool by where it was added.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::Response;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How many requests the server works on at once, across every route but the probes.
pub(crate) const MAX_IN_FLIGHT_REQUESTS: usize = 256;

/// Document writes may hold one slot in this many.
const WRITE_SHARE: usize = 4;

/// `/_metrics` requests answered at once. They are outside the request pool.
const MAX_IN_FLIGHT_SCRAPES: usize = 8;

/// What a request needs before its handler runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// `/_health`: nothing. The handler bounds itself (ADR-144), and an orchestrator must
    /// get its answer from a server whose every slot is taken.
    Probe,
    /// `/_metrics`: a slot of the scrape pool only, so a full server can still be observed.
    Scrape,
    /// A document write: a slot of the write share, then a slot of the request pool.
    Write,
    /// Everything else: a slot of the request pool.
    Request,
}

/// Classify a request by method and path.
///
/// Document writes are the requests that queue for the engine mutex or the coordinator's
/// write serializer (`admit_write`, `admit_cluster_write`): `PUT`/`DELETE /_doc/{id}`,
/// `/_bulk` and `/_flush`. A compaction, backup or rebuild holds that lock for as long as it
/// takes, and every write that arrives meanwhile waits in flight. Bounding their share keeps
/// those waiting writes from taking the slots that searches, which need no lock, would use.
pub(crate) fn admission_for(method: &Method, path: &str) -> Admission {
    let reads = method == Method::GET || method == Method::HEAD;
    match path {
        "/_health" => Admission::Probe,
        "/_metrics" => Admission::Scrape,
        "/_bulk" | "/_flush" => Admission::Write,
        _ if path.starts_with("/_doc/") && !reads => Admission::Write,
        _ => Admission::Request,
    }
}

/// The server's request slots. Clones share them.
#[derive(Clone)]
pub(crate) struct RequestPool {
    requests: Arc<Semaphore>,
    writes: Arc<Semaphore>,
    scrapes: Arc<Semaphore>,
}

/// The slots one admitted request holds until its response is ready.
pub(crate) struct Slots {
    _write: Option<OwnedSemaphorePermit>,
    _slot: Option<OwnedSemaphorePermit>,
}

impl RequestPool {
    /// A pool of `max_in_flight` request slots, a quarter of which document writes may hold.
    pub(crate) fn new(max_in_flight: usize) -> Self {
        Self {
            requests: Arc::new(Semaphore::new(max_in_flight)),
            writes: Arc::new(Semaphore::new(write_share(max_in_flight))),
            scrapes: Arc::new(Semaphore::new(MAX_IN_FLIGHT_SCRAPES)),
        }
    }

    /// Wait for the slots `admission` needs. A request that is dropped while it waits takes
    /// nothing.
    pub(crate) async fn admit(&self, admission: Admission) -> Slots {
        match admission {
            Admission::Probe => Slots {
                _write: None,
                _slot: None,
            },
            Admission::Scrape => Slots {
                _write: None,
                _slot: Some(slot(&self.scrapes).await),
            },
            Admission::Request => Slots {
                _write: None,
                _slot: Some(slot(&self.requests).await),
            },
            Admission::Write => {
                // The write share first: a write that waits for it holds no request slot.
                let write = slot(&self.writes).await;
                Slots {
                    _write: Some(write),
                    _slot: Some(slot(&self.requests).await),
                }
            }
        }
    }
}

/// The request slots document writes may hold: a quarter of the pool, and at least one.
fn write_share(max_in_flight: usize) -> usize {
    (max_in_flight / WRITE_SHARE).max(1)
}

async fn slot(pool: &Arc<Semaphore>) -> OwnedSemaphorePermit {
    Arc::clone(pool)
        .acquire_owned()
        .await
        .expect("the request pool is never closed")
}

/// The admission middleware: hold the request's slots while its handler runs.
///
/// A request that finds no slot waits; it is not refused. It has had none of its body read
/// and none of its work started.
pub(crate) async fn admit(
    State(pool): State<RequestPool>,
    request: Request,
    next: Next,
) -> Response {
    let _slots = pool
        .admit(admission_for(request.method(), request.uri().path()))
        .await;
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(
                admission_for(&method, path),
                Admission::Write,
                "{method} {path}"
            );
        }
    }

    #[test]
    fn matching_and_reads_take_a_request_slot_only() {
        for (method, path) in [
            (Method::GET, "/"),
            (Method::GET, "/_doc/7"),
            (Method::HEAD, "/_doc/7"),
            (Method::POST, "/_search"),
            (Method::GET, "/_search"),
            (Method::POST, "/v2/_search"),
            (Method::POST, "/_mpercolate"),
            (Method::POST, "/v2/_mpercolate"),
            (Method::POST, "/v2/_pit"),
            (Method::POST, "/_percolate/jobs"),
            (Method::GET, "/_stats"),
            (Method::POST, "/_compact"),
            (Method::POST, "/_backup"),
            (Method::PUT, "/_vocab"),
            (Method::POST, "/_cluster/resync"),
            // A path no route serves is counted like any other request.
            (Method::POST, "/_nothing_here"),
        ] {
            assert_eq!(
                admission_for(&method, path),
                Admission::Request,
                "{method} {path}"
            );
        }
    }

    #[test]
    fn the_probes_are_outside_the_request_pool() {
        for method in [Method::GET, Method::HEAD, Method::POST] {
            assert_eq!(admission_for(&method, "/_health"), Admission::Probe);
            assert_eq!(admission_for(&method, "/_metrics"), Admission::Scrape);
        }
    }

    #[test]
    fn writes_may_hold_a_quarter_of_the_pool_and_at_least_one_slot() {
        assert_eq!(write_share(MAX_IN_FLIGHT_REQUESTS), 64);
        assert_eq!(write_share(4), 1);
        assert_eq!(write_share(2), 1);
        assert_eq!(write_share(1), 1);
    }
}
