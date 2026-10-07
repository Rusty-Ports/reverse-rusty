//! Where a shard node's shards report durability events (ADR-213).
//!
//! A shard reports through an event sink and keeps what it reported while it was being
//! opened until someone installs one. A shard node hosts several shards and creates them at
//! start-up, at adoption and at recovery, and it used to give none of them a sink: start-up
//! reports stayed in their buffers and later ones were dropped. So nothing a shard node's
//! shards said about durability reached a log or a metric.
//!
//! [`NodeEvents`] is the one place they report to. Every [`ServerState`](super::ServerState)
//! is built through a constructor that wires its shard here, the node counts durability
//! failures by operation for its metrics, and the binary installs the sink that logs them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::cluster::shard::{EventSink, LocalShard, Shard};
use crate::events::{DurabilityOp, EngineEvent};

/// Reports kept for a sink that has not been installed yet. Start-up reports are few; a node
/// that never installs a sink (a test, a library user) must not grow without bound.
const MAX_WAITING: usize = 256;

#[derive(Default)]
pub(super) struct NodeEvents {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    sink: Option<EventSink>,
    /// Reported before a sink was installed; handed to it when it is.
    waiting: Vec<EngineEvent>,
    /// Durability failures by operation, for the metrics exposition.
    failures: BTreeMap<&'static str, u64>,
}

impl NodeEvents {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Have `shard` report to this node: what it queued while it was opened, and everything
    /// it reports from now on.
    pub(super) fn wire(self: &Arc<Self>, shard: &LocalShard) {
        let node = Arc::clone(self);
        let forward: EventSink = Arc::new(move |event| node.report(event));
        for event in shard.set_event_sink(Arc::clone(&forward)) {
            forward(&event);
        }
    }

    /// Count the event and pass it on. A shard may call this while it holds its own locks, so
    /// nothing here calls back into a shard, and the sink runs outside this lock.
    fn report(&self, event: &EngineEvent) {
        let sink = {
            let mut state = self.lock();
            if let EngineEvent::DurabilityFailure { op, .. } = event {
                *state.failures.entry(op.as_str()).or_default() += 1;
            }
            let Some(sink) = &state.sink else {
                if state.waiting.len() < MAX_WAITING {
                    state.waiting.push(event.clone());
                }
                return;
            };
            Arc::clone(sink)
        };
        sink(event);
    }

    /// Install the node's sink and hand it what was reported before it existed.
    pub(super) fn install(&self, sink: EventSink) {
        let waiting = {
            let mut state = self.lock();
            state.sink = Some(Arc::clone(&sink));
            std::mem::take(&mut state.waiting)
        };
        // The sink moves into its slot above and what waited is delivered through this clone.
        let deliver = sink;
        for event in &waiting {
            deliver(event);
        }
    }

    /// Durability failures by operation, in a stable order. A lost log is always listed, at
    /// zero until it happens, so that an alert on its increase sees the first one.
    pub(super) fn durability_failures(&self) -> Vec<(&'static str, u64)> {
        let mut failures = self.lock().failures.clone();
        failures.entry(DurabilityOp::LogLost.as_str()).or_default();
        failures.into_iter().collect()
    }
}
