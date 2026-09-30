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
    refuse_finish_once: AtomicBool,
    reads_broken: AtomicBool,
}

/// A control plane that injects [`Faults`] around an in-memory one, to exercise ambiguous and
/// failed control-plane outcomes.
struct Faulty {
    inner: reverse_rusty::cluster::InMemoryControlPlane,
    faults: Arc<Faults>,
}

impl Faulty {
    fn install(cluster: ClusterEngine) -> (ClusterEngine, Arc<Faults>) {
        let initial = cluster.control_state().expect("state");
        let faults = Arc::new(Faults::default());
        let cluster = cluster.with_control_plane(Box::new(Self {
            inner: reverse_rusty::cluster::InMemoryControlPlane::new(initial),
            faults: Arc::clone(&faults),
        }));
        (cluster, faults)
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
    let (mut cluster, faults) = Faulty::install(cluster);
    faults.lose_commit_reply.store(true, Ordering::SeqCst);
    let failed = cluster.resize_remote(&RemoteResizeRequest {
        operation_id: 31,
        num_shards: 2,
        targets,
    });
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
    let (mut cluster, faults) = Faulty::install(cluster);
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

#[test]
fn grpc_remote_resize_recovery_refuses_a_coordinator_serving_the_retired_layout() {
    let Fixture {
        rt: _rt,
        mut cluster,
        targets,
        ..
    } = fixture(2);
    cluster
        .attest_committed_layout()
        .expect("the assembled layout is the committed one");
    let prepared = cluster
        .prepare_remote_resize(&RemoteResizeRequest {
            operation_id: 51,
            num_shards: 2,
            targets,
        })
        .expect("prepare");
    // Consensus now names the new layout while this coordinator still serves the old one.
    // Resolving the intent here would finish it and keep serving the retired layout.
    let recovered = cluster.recover_resize_intent();
    assert!(
        matches!(recovered, Err(ShardError::ControlPlane(_))),
        "{recovered:?}"
    );
    assert!(cluster.attest_committed_layout().is_err());
    assert!(
        cluster
            .control_state()
            .expect("state")
            .moves
            .resize
            .is_some(),
        "the refused recovery leaves the intent recorded"
    );
    let retired = cluster.install_remote_resize(prepared).expect("install");
    cluster
        .attest_committed_layout()
        .expect("the installed layout is the committed one");
    assert!(
        cluster
            .finish_remote_resize(retired)
            .expect("finish")
            .finished
    );
}

#[test]
fn grpc_remote_resize_fences_before_any_control_plane_call_and_reopens_on_failure() {
    let Fixture {
        rt: _rt,
        cluster,
        targets,
        ..
    } = fixture(2);
    let (cluster, faults) = Faulty::install(cluster);
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
    let (mut cluster, faults) = Faulty::install(cluster);
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
