//! ADR-180 remote resize under control-plane faults and stale coordinators: the fence goes up before
//! any control-plane call, a failure before `Commit` reopens writes, an ambiguous commit keeps them
//! paused, leftover intents are finished or aborted, and a coordinator that still serves the
//! retired layout refuses to resolve the intent.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// Fault switches a test flips at any point; each `*_once` or `lose_*` switch fires once.
#[derive(Default)]
struct Faults {
    /// Apply `Commit`, then lose its reply and every later control read or proposal.
    lose_commit_reply: AtomicBool,
    /// Apply `Begin`, then lose only its reply.
    lose_begin_reply: AtomicBool,
    refuse_abort_once: AtomicBool,
    /// Refuse `MarkReady` without applying it.
    refuse_mark_ready_once: AtomicBool,
    /// Refuse `Commit` without applying it.
    refuse_commit_once: AtomicBool,
    /// Refuse `Commit` without applying it, then lose every later control read or proposal, so
    /// the coordinator cannot tell that the commit did not apply.
    lose_commit_request: AtomicBool,
    refuse_finish_once: AtomicBool,
    reads_broken: AtomicBool,
}

/// A control plane that injects [`Faults`] around an in-memory one, to exercise ambiguous and
/// failed control-plane outcomes.
struct Faulty {
    inner: Arc<reverse_rusty::cluster::InMemoryControlPlane>,
    faults: Arc<Faults>,
}

impl Faulty {
    /// Install the faulty plane; also returns the underlying plane, which a test can hand to
    /// startup resolution as if it were a restarted coordinator's.
    fn install(
        cluster: ClusterEngine,
    ) -> (
        ClusterEngine,
        Arc<Faults>,
        Arc<reverse_rusty::cluster::InMemoryControlPlane>,
    ) {
        let initial = cluster.control_state().expect("state");
        let faults = Arc::new(Faults::default());
        let inner = Arc::new(reverse_rusty::cluster::InMemoryControlPlane::new(initial));
        let cluster = cluster.with_control_plane(Box::new(Self {
            inner: Arc::clone(&inner),
            faults: Arc::clone(&faults),
        }));
        (cluster, faults, inner)
    }

    fn broken() -> reverse_rusty::cluster::ControlError {
        reverse_rusty::cluster::ControlError::Backend("injected control-plane fault".into())
    }
}

impl reverse_rusty::cluster::ControlPlane for Faulty {
    fn cluster_state(
        &self,
    ) -> Result<Arc<reverse_rusty::cluster::ClusterState>, reverse_rusty::cluster::ControlError>
    {
        if self.faults.reads_broken.load(Ordering::SeqCst) {
            return Err(Self::broken());
        }
        self.inner.cluster_state()
    }

    fn version(
        &self,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.version()
    }

    fn propose(
        &self,
        change: reverse_rusty::cluster::ClusterStateChange,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.propose(change)
    }

    fn propose_resize(
        &self,
        command: reverse_rusty::cluster::ResizeCommand,
    ) -> Result<reverse_rusty::cluster::MoveProposalResult, reverse_rusty::cluster::ControlError>
    {
        use reverse_rusty::cluster::ResizeCommand;
        let faults = &self.faults;
        let fire = |switch: &AtomicBool| switch.swap(false, Ordering::SeqCst);
        match &command {
            ResizeCommand::Commit { .. } if fire(&faults.lose_commit_reply) => {
                self.inner.propose_resize(command)?;
                faults.reads_broken.store(true, Ordering::SeqCst);
                Err(Self::broken())
            }
            ResizeCommand::Begin(_) if fire(&faults.lose_begin_reply) => {
                self.inner.propose_resize(command)?;
                Err(Self::broken())
            }
            ResizeCommand::Abort { .. } if fire(&faults.refuse_abort_once) => Err(Self::broken()),
            ResizeCommand::MarkReady { .. } if fire(&faults.refuse_mark_ready_once) => {
                Err(Self::broken())
            }
            ResizeCommand::Commit { .. } if fire(&faults.refuse_commit_once) => Err(Self::broken()),
            ResizeCommand::Commit { .. } if fire(&faults.lose_commit_request) => {
                faults.reads_broken.store(true, Ordering::SeqCst);
                Err(Self::broken())
            }
            ResizeCommand::Finish { .. } if fire(&faults.refuse_finish_once) => Err(Self::broken()),
            _ => {
                if faults.reads_broken.load(Ordering::SeqCst) {
                    return Err(Self::broken());
                }
                self.inner.propose_resize(command)
            }
        }
    }

    fn change_membership(
        &self,
        voters: Vec<NodeId>,
    ) -> Result<reverse_rusty::cluster::StateVersion, reverse_rusty::cluster::ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, reverse_rusty::cluster::ControlError> {
        self.inner.leader()
    }
}

#[test]
fn grpc_remote_resize_keeps_writes_paused_when_the_commit_outcome_is_unknown() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        ..
    } = fixture(2);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    faults.lose_commit_reply.store(true, Ordering::SeqCst);
    let request = RemoteResizeRequest {
        operation_id: 31,
        num_shards: 2,
        targets,
    };
    let failed = cluster.resize_remote(&request);
    assert!(failed.is_err(), "{failed:?}");
    // Consensus may already name the new layout, so writes on the old one must stay refused.
    let write = cluster.add_query(9_600_001, "zzambiguous widget");
    assert!(
        matches!(write, Err(ShardError::ControlPlane(_))),
        "an ambiguous commit must keep writes paused: {write:?}"
    );
    let remove = cluster.remove_query(1);
    assert!(
        matches!(remove, Err(ShardError::ControlPlane(_))),
        "{remove:?}"
    );
    // Reads stop too: another coordinator may already serve the committed new layout, so the
    // old layout's results could silently miss its writes.
    let read = cluster.percolate("1994 acme");
    assert!(
        refused_as_retired(&read),
        "the retired old nodes must refuse reads after an ambiguous commit: {read:?}"
    );

    // A retry cannot know that outcome either. Once the control plane is reachable again it must
    // be refused without lowering the fence it did not raise, or writes would land on the old
    // layout and vanish when startup routes to the committed one.
    faults.reads_broken.store(false, Ordering::SeqCst);
    let retried = cluster.resize_remote(&request);
    assert!(retried.is_err(), "{retried:?}");
    let write = cluster.add_query(9_600_002, "zzambiguous gadget");
    assert!(
        matches!(write, Err(ShardError::ControlPlane(_))),
        "a retry must not reopen writes after an ambiguous commit: {write:?}"
    );
}

#[test]
fn grpc_remote_resize_finishes_a_leftover_committed_intent_before_the_next_one() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(5);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    faults.refuse_finish_once.store(true, Ordering::SeqCst);
    let before = matches(&cluster, &titles);
    let first = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 41,
            num_shards: 2,
            targets: targets[..2].to_vec(),
        })
        .expect("first resize");
    assert!(
        !first.finished,
        "the injected fault leaves the intent committed"
    );
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_some());
    let second = cluster
        .resize_remote(&RemoteResizeRequest {
            operation_id: 42,
            num_shards: 3,
            targets: targets[2..].to_vec(),
        })
        .expect("the next resize finishes the leftover intent first");
    assert!(second.finished);
    assert_eq!(cluster.num_shards(), 3);
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_none());
    assert_eq!(matches(&cluster, &titles), before);
}

/// Whether `result` failed because a node was retired by a remote resize.
fn refused_as_retired<T: std::fmt::Debug>(result: &Result<T, ShardError>) -> bool {
    matches!(result, Err(error) if error.to_string().contains("retired by remote resize"))
}

#[test]
fn grpc_remote_resize_retires_the_old_nodes_before_committing() {
    let Fixture {
        rt,
        mut cluster,
        targets,
        queries,
        titles,
    } = fixture(2);
    let before = matches(&cluster, &titles);
    let old_endpoints: Vec<String> = cluster
        .control_state()
        .expect("state")
        .nodes
        .iter()
        .filter_map(|node| node.addr.clone())
        .collect();
    let prepared = cluster
        .prepare_remote_resize(&RemoteResizeRequest {
            operation_id: 51,
            num_shards: 2,
            targets,
        })
        .expect("prepare");
    // Consensus names the new layout, and the old nodes themselves refuse to serve: nothing can
    // answer from the superseded layout, whatever this coordinator's memory says.
    assert!(cluster.attest_committed_layout().is_err());
    let read = cluster.percolate("1994 acme");
    assert!(refused_as_retired(&read), "{read:?}");
    let retired = cluster.install_remote_resize(prepared).expect("install");
    cluster
        .attest_committed_layout()
        .expect("the installed layout is the committed one");
    assert_eq!(matches(&cluster, &titles), before);
    assert!(
        cluster
            .finish_remote_resize(retired)
            .expect("finish")
            .finished
    );

    // A fresh coordinator pointed at the retired nodes is refused outright.
    let norm = Arc::new(vocab());
    let stale = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        frozen_dict_over(&queries, &norm),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 3,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &old_endpoints[..3],
        rt.handle(),
        0xB1E0_0001,
    );
    assert!(
        matches!(&stale, Err(error) if error.to_string().contains("retired by remote resize")),
        "{:?}",
        stale.as_ref().err()
    );
}

#[test]
fn grpc_startup_resolution_returns_an_uncommitted_resizes_old_nodes_to_service() {
    let Fixture {
        rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let (mut cluster, faults, plane) = Faulty::install(cluster);
    let before = matches(&cluster, &titles);
    // `Commit` never applies, but the coordinator cannot tell: the old nodes stay retired and
    // writes stay paused.
    faults.lose_commit_request.store(true, Ordering::SeqCst);
    let failed = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 121,
        num_shards: 2,
        targets,
    });
    assert!(failed.is_err(), "{failed:?}");
    faults.reads_broken.store(false, Ordering::SeqCst);
    let read = cluster.percolate("1994 acme");
    assert!(refused_as_retired(&read), "{read:?}");

    // A second coordinator cannot resolve it while this one still holds the old nodes' leases.
    let security = reverse_rusty::cluster::ClientSecurity::default();
    let contested = reverse_rusty::cluster::recover_durable_resize(
        &*plane,
        rt.handle(),
        0xB1E0_0FFF,
        &security,
    );
    assert!(contested.is_err(), "{contested:?}");
    assert!(reverse_rusty::cluster::ControlPlane::cluster_state(&*plane)
        .expect("state")
        .moves
        .resize
        .is_some());

    // The owning coordinator's restart aborts the intent and returns the old nodes to service.
    let recovered = reverse_rusty::cluster::recover_durable_resize(
        &*plane,
        rt.handle(),
        0xB1E0_0001,
        &security,
    )
    .expect("startup resolution");
    assert_eq!(
        recovered,
        Some(reverse_rusty::cluster::ResizeRecovery::Aborted { operation_id: 121 })
    );
    assert_eq!(matches(&cluster, &titles), before);
}

#[test]
fn grpc_remote_resize_fences_before_any_control_plane_call_and_reopens_on_failure() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        ..
    } = fixture(2);
    let (cluster, faults, _) = Faulty::install(cluster);
    faults.reads_broken.store(true, Ordering::SeqCst);
    // A server releases its write serialization from this callback, so it must run before the
    // first control-plane or mesh call, and only once writes are already refused.
    let refused_when_fenced = std::cell::Cell::new(None);
    let failed = cluster.prepare_remote_resize_then(
        &RemoteResizeRequest {
            operation_id: 71,
            num_shards: 2,
            targets,
        },
        || {
            let write = cluster.add_query(9_700_001, "zzprefence widget");
            refused_when_fenced.set(Some(matches!(write, Err(ShardError::ControlPlane(_)))));
        },
    );
    assert!(failed.is_err(), "the control plane is unreachable");
    assert_eq!(refused_when_fenced.get(), Some(true));
    // Nothing was proposed, so the old layout is still the layout of record: writes reopen.
    faults.reads_broken.store(false, Ordering::SeqCst);
    cluster
        .add_query(9_700_002, "zzprefence gadget")
        .expect("writes reopen after a failure before Commit");
}

#[test]
fn grpc_remote_resize_recovers_from_a_lost_begin_reply() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    let before = matches(&cluster, &titles);
    // `Begin` applies but its reply is lost, and the cleanup abort fails too.
    faults.lose_begin_reply.store(true, Ordering::SeqCst);
    faults.refuse_abort_once.store(true, Ordering::SeqCst);
    let request = RemoteResizeRequest {
        operation_id: 61,
        num_shards: 2,
        targets,
    };
    assert!(cluster.resize_remote(&request).is_err());
    // No `Commit` was proposed, so writes reopen even though the intent is still recorded.
    cluster
        .add_query(9_600_101, "zzlostbegin widget")
        .expect("writes reopen after a failure before Commit");
    let leftover = cluster.control_state().expect("state").moves.resize;
    assert_eq!(leftover.map(|intent| intent.operation_id), Some(61));

    // Another operation is still refused while that intent exists...
    let other = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 62,
        ..request.clone()
    });
    assert!(other.is_err(), "{other:?}");
    cluster
        .remove_query(9_600_101)
        .expect("a refused resize reopens writes");
    // ...but a retry of the same operation aborts its own leftover intent and completes.
    let report = cluster
        .resize_remote(&request)
        .expect("same-operation retry");
    assert!(report.finished);
    assert_eq!(cluster.num_shards(), 2);
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_none());
    assert_eq!(matches(&cluster, &titles), before);
}

#[test]
fn grpc_remote_resize_installs_without_a_control_plane_call() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    let before = matches(&cluster, &titles);
    let prepared = cluster
        .prepare_remote_resize(&RemoteResizeRequest {
            operation_id: 91,
            num_shards: 2,
            targets,
        })
        .expect("prepare commits the new layout");
    // Installation runs under the exclusive cluster lock, so it must not need the control plane:
    // a round trip there could stall the runtime that request threads waiting on the lock hold.
    faults.reads_broken.store(true, Ordering::SeqCst);
    let retired = cluster
        .install_remote_resize(prepared)
        .expect("installation makes no control-plane call");
    assert_eq!(matches(&cluster, &titles), before);
    faults.reads_broken.store(false, Ordering::SeqCst);
    assert!(
        cluster
            .finish_remote_resize(retired)
            .expect("finish")
            .finished
    );
}

#[test]
fn grpc_remote_resize_serves_the_old_layout_again_after_a_refused_commit() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    let before = matches(&cluster, &titles);
    // `Commit` is refused without applying, and the read-back proves the old layout is still the
    // layout of record, so the reads stopped before the proposal serve again, as do writes.
    faults.refuse_commit_once.store(true, Ordering::SeqCst);
    let failed = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 111,
        num_shards: 2,
        targets,
    });
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(matches(&cluster, &titles), before);
    cluster
        .add_query(9_900_101, "zzrefusedcommit widget")
        .expect("writes reopen after a proven non-commit");
}

#[test]
fn grpc_remote_resize_old_nodes_refuse_reads_when_a_prepared_resize_is_abandoned() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        ..
    } = fixture(2);
    let prepared = cluster
        .prepare_remote_resize(&RemoteResizeRequest {
            operation_id: 101,
            num_shards: 2,
            targets,
        })
        .expect("prepare commits the new layout");
    // Dropping the committed preparation (for example, a cancelled caller) must not leave the
    // retired layout answering reads that could miss the committed layout's writes.
    drop(prepared);
    let read = cluster.percolate("1994 acme");
    assert!(
        refused_as_retired(&read),
        "the old nodes of an abandoned committed resize must refuse reads: {read:?}"
    );
}

#[test]
fn grpc_remote_resize_returns_retired_nodes_to_service_after_a_failure_before_commit() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        titles,
        ..
    } = fixture(2);
    let (mut cluster, faults, _) = Faulty::install(cluster);
    let before = matches(&cluster, &titles);
    // The old nodes are retired, then `MarkReady` fails: `Commit` was never proposed, so the old
    // layout is certainly still the layout of record and its nodes serve again.
    faults.refuse_mark_ready_once.store(true, Ordering::SeqCst);
    let failed = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 131,
        num_shards: 2,
        targets,
    });
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(matches(&cluster, &titles), before);
    cluster
        .add_query(9_900_131, "zzbeforecommit widget")
        .expect("writes reopen");
    assert!(cluster
        .control_state()
        .expect("state")
        .moves
        .resize
        .is_none());
}
