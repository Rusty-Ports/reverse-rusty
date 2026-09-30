//! ADR-180 remote resize under control-plane faults and stale coordinators: an ambiguous commit
//! keeps writes paused, a leftover committed intent is finished first, and a coordinator that
//! still serves the retired layout refuses to resolve the intent.

use super::*;

/// A control plane that can apply a resize commit but lose its reply (and later reads), or refuse
/// one `Finish`, to exercise ambiguous-outcome handling.
struct Faulty {
    inner: reverse_rusty::cluster::InMemoryControlPlane,
    lose_commit_reply: std::sync::atomic::AtomicBool,
    reads_broken: std::sync::atomic::AtomicBool,
    refuse_finish_once: std::sync::atomic::AtomicBool,
}

impl Faulty {
    fn install(
        cluster: ClusterEngine,
        lose_commit_reply: bool,
        refuse_finish_once: bool,
    ) -> ClusterEngine {
        let initial = cluster.control_state().expect("state");
        cluster.with_control_plane(Box::new(Self {
            inner: reverse_rusty::cluster::InMemoryControlPlane::new(initial),
            lose_commit_reply: std::sync::atomic::AtomicBool::new(lose_commit_reply),
            reads_broken: std::sync::atomic::AtomicBool::new(false),
            refuse_finish_once: std::sync::atomic::AtomicBool::new(refuse_finish_once),
        }))
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
        if self.reads_broken.load(std::sync::atomic::Ordering::SeqCst) {
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
        use std::sync::atomic::Ordering;
        match &command {
            reverse_rusty::cluster::ResizeCommand::Commit { .. }
                if self.lose_commit_reply.swap(false, Ordering::SeqCst) =>
            {
                self.inner.propose_resize(command)?;
                self.reads_broken.store(true, Ordering::SeqCst);
                Err(Self::broken())
            }
            reverse_rusty::cluster::ResizeCommand::Finish { .. }
                if self.refuse_finish_once.swap(false, Ordering::SeqCst) =>
            {
                Err(Self::broken())
            }
            _ => {
                if self.reads_broken.load(Ordering::SeqCst) {
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
    let mut cluster = Faulty::install(cluster, true, false);
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
    let mut cluster = Faulty::install(cluster, false, true);
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
