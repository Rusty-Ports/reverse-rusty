//! Resize operation records (ADR-179): the bounded, in-memory registry behind
//! `POST /_cluster/resize` idempotency, `GET /_cluster/resize` progress, and the
//! autoscaler's accepted operations.
//!
//! The registry never touches the cluster lock, so a status read stays responsive
//! while a rebuild holds the exclusive guards. Records are process-local: a
//! restart forgets them. That is safe because resize targets are absolute and
//! the optional `if_placement_generation` precondition is checked against durable
//! serving state, so a retry after a restart either converges on the same
//! layout or fails its precondition rather than repeating a superseded change.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

use reverse_rusty::cluster::ResizeVerdict;

/// Retained records, newest last. Terminal records are evicted oldest-first.
pub(crate) const MAX_RETAINED_RESIZE_OPERATIONS: usize = 64;
/// Longest accepted caller-supplied operation ID.
pub(crate) const MAX_RESIZE_OPERATION_ID_LEN: usize = 64;

/// Who requested an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResizeOrigin {
    Api,
    Autoscaler,
}

/// Lifecycle of one operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResizeState {
    /// Accepted; waiting for admission and exclusive guards.
    Queued,
    /// Every guard is held and the rebuild has started; it cannot be cancelled.
    Running,
    /// Terminal success with an attested layout.
    Succeeded,
    /// Terminal failure after start, or a precondition rejection.
    Failed,
    /// Terminal: admission or a guard was not obtained before the deadline, so
    /// no rebuild began.
    NotStarted,
}

impl ResizeState {
    fn is_terminal(self) -> bool {
        !matches!(self, Self::Queued | Self::Running)
    }
}

/// The attested terminal layout of a successful operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResizeOutcome {
    pub(crate) old_num_shards: usize,
    pub(crate) num_shards: usize,
    pub(crate) rebuilt: usize,
    pub(crate) version: u64,
    pub(crate) placement_generation: u64,
}

/// The sanitized failure of an unsuccessful operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResizeFailure {
    #[serde(rename = "type")]
    pub(crate) error_type: String,
    pub(crate) reason: String,
}

/// One retained operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResizeOperation {
    pub(crate) operation_id: String,
    pub(crate) origin: ResizeOrigin,
    /// Requested target shard count.
    pub(crate) num_shards: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) if_placement_generation: Option<u64>,
    pub(crate) state: ResizeState,
    pub(crate) accepted_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) outcome: Option<ResizeOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<ResizeFailure>,
}

impl ResizeOperation {
    fn same_request(&self, origin: ResizeOrigin, num_shards: usize, if_gen: Option<u64>) -> bool {
        self.origin == origin
            && self.num_shards == num_shards
            && self.if_placement_generation == if_gen
    }
}

/// The admission decision for one request.
#[derive(Debug)]
pub(crate) enum ResizeAdmission {
    /// Execute under this operation ID.
    Execute(String),
    /// The same request already succeeded; return its recorded outcome.
    Replay(Box<ResizeOperation>),
    /// The same request is queued or running.
    InProgress(Box<ResizeOperation>),
    /// The ID names a different retained request.
    Conflict(Box<ResizeOperation>),
    /// Every retained record is still active; no slot is available.
    Full,
}

/// The latest autoscaler observation, for `GET /_cluster/resize`.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutoscaleStatus {
    pub(crate) observed_at_ms: u64,
    pub(crate) num_shards: usize,
    pub(crate) placement_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recommended: Option<usize>,
    pub(crate) max_selective_corpus: usize,
    #[serde(flatten)]
    pub(crate) verdict: ResizeVerdict,
}

#[derive(Default)]
struct Inner {
    records: VecDeque<ResizeOperation>,
    autoscale: Option<AutoscaleStatus>,
}

/// The bounded registry shared by the REST handlers and the autoscale loop.
pub(crate) struct ResizeOperations {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
    autoscale_enabled: bool,
}

pub(crate) fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Whether `id` is an acceptable caller-supplied operation ID.
pub(crate) fn valid_operation_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_RESIZE_OPERATION_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

impl ResizeOperations {
    pub(crate) fn new(autoscale_enabled: bool) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            next_id: AtomicU64::new(1),
            autoscale_enabled,
        }
    }

    pub(crate) fn autoscale_enabled(&self) -> bool {
        self.autoscale_enabled
    }

    fn generated_id(&self, origin: ResizeOrigin, now_ms: u64) -> String {
        let seq = self.next_id.fetch_add(1, Ordering::Relaxed);
        let prefix = match origin {
            ResizeOrigin::Api => "resize",
            ResizeOrigin::Autoscaler => "autoscale",
        };
        format!("{prefix}-{now_ms}-{seq}")
    }

    /// Admit one request. A caller-supplied ID makes the request idempotent: the same
    /// request replays a success, reports an active attempt, or re-executes after a failed or
    /// not-started attempt (whose absolute target makes a retry safe and lets it heal a
    /// post-swap commit failure). Different parameters under a retained ID conflict.
    pub(crate) fn admit(
        &self,
        operation_id: Option<String>,
        origin: ResizeOrigin,
        num_shards: usize,
        if_placement_generation: Option<u64>,
    ) -> ResizeAdmission {
        let now_ms = unix_ms_now();
        let mut inner = self.inner.lock();
        if let Some(id) = operation_id.as_deref() {
            if let Some(existing) = inner.records.iter_mut().find(|r| r.operation_id == id) {
                if !existing.same_request(origin, num_shards, if_placement_generation) {
                    return ResizeAdmission::Conflict(Box::new(existing.clone()));
                }
                match existing.state {
                    ResizeState::Succeeded => {
                        return ResizeAdmission::Replay(Box::new(existing.clone()));
                    }
                    ResizeState::Queued | ResizeState::Running => {
                        return ResizeAdmission::InProgress(Box::new(existing.clone()));
                    }
                    ResizeState::Failed | ResizeState::NotStarted => {
                        existing.state = ResizeState::Queued;
                        existing.accepted_at_ms = now_ms;
                        existing.started_at_ms = None;
                        existing.finished_at_ms = None;
                        existing.outcome = None;
                        existing.error = None;
                        return ResizeAdmission::Execute(id.to_string());
                    }
                }
            }
        }
        if inner.records.len() >= MAX_RETAINED_RESIZE_OPERATIONS {
            let Some(oldest_terminal) = inner.records.iter().position(|r| r.state.is_terminal())
            else {
                return ResizeAdmission::Full;
            };
            inner.records.remove(oldest_terminal);
        }
        let id = operation_id.unwrap_or_else(|| self.generated_id(origin, now_ms));
        inner.records.push_back(ResizeOperation {
            operation_id: id.clone(),
            origin,
            num_shards,
            if_placement_generation,
            state: ResizeState::Queued,
            accepted_at_ms: now_ms,
            started_at_ms: None,
            finished_at_ms: None,
            outcome: None,
            error: None,
        });
        ResizeAdmission::Execute(id)
    }

    fn update(&self, id: &str, apply: impl FnOnce(&mut ResizeOperation)) {
        let mut inner = self.inner.lock();
        if let Some(record) = inner.records.iter_mut().find(|r| r.operation_id == id) {
            apply(record);
        }
    }

    /// The rebuild started with every guard held.
    pub(crate) fn mark_running(&self, id: &str) {
        let now_ms = unix_ms_now();
        self.update(id, |r| {
            r.state = ResizeState::Running;
            r.started_at_ms = Some(now_ms);
        });
    }

    pub(crate) fn mark_succeeded(&self, id: &str, outcome: ResizeOutcome) {
        let now_ms = unix_ms_now();
        self.update(id, |r| {
            r.state = ResizeState::Succeeded;
            r.finished_at_ms = Some(now_ms);
            r.outcome = Some(outcome);
            r.error = None;
        });
    }

    pub(crate) fn mark_failed(&self, id: &str, failure: ResizeFailure) {
        let now_ms = unix_ms_now();
        self.update(id, |r| {
            r.state = ResizeState::Failed;
            r.finished_at_ms = Some(now_ms);
            r.error = Some(failure);
        });
    }

    pub(crate) fn mark_not_started(&self, id: &str, failure: ResizeFailure) {
        let now_ms = unix_ms_now();
        self.update(id, |r| {
            r.state = ResizeState::NotStarted;
            r.finished_at_ms = Some(now_ms);
            r.error = Some(failure);
        });
    }

    pub(crate) fn get(&self, id: &str) -> Option<ResizeOperation> {
        self.inner
            .lock()
            .records
            .iter()
            .find(|r| r.operation_id == id)
            .cloned()
    }

    /// Retained records, newest first.
    pub(crate) fn list(&self) -> Vec<ResizeOperation> {
        self.inner.lock().records.iter().rev().cloned().collect()
    }

    pub(crate) fn record_autoscale(&self, status: AutoscaleStatus) {
        self.inner.lock().autoscale = Some(status);
    }

    pub(crate) fn autoscale_status(&self) -> Option<AutoscaleStatus> {
        self.inner.lock().autoscale.clone()
    }
}

#[cfg(test)]
mod tests;
