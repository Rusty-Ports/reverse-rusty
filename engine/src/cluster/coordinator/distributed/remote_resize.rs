//! Remote blue/green resize onto fresh target nodes (ADR-180).
//!
//! The committed layout stays authoritative and readable while a complete new layout is built on
//! separate, empty shard servers:
//!
//! 1. **prepare** (`&self`): raise the resize write fence and drain in-flight mutations, record a
//!    `Preparing` intent, build the target layout at placement generation `N + 1`, stream the
//!    deduplicated live corpus into it, prove every target position, **retire** every node of the
//!    current layout at the storage layer (each retirement also proves its slots did not change
//!    since the export), record `Ready`, and commit the new shard count, generation, and
//!    assignments together;
//! 2. **install** (`&mut self`, no network call): swap the serving ring and shards to the
//!    committed layout and lower the fence;
//! 3. **finish** (`&self`): finish the intent.
//!
//! Layout authority is enforced by the data nodes, not by this coordinator's memory: from the
//! moment `Commit` may apply, the old nodes are durably retired and refuse every read and write
//! from any coordinator. A failure before `Commit` is proposed unretires them and reopens writes;
//! after that, they are unretired only when the control plane proves the commit did not apply.
//! Coordinator startup resolves whatever a crash, cancellation, or lost reply left behind.

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering;

use crate::cluster::control::{
    normalized_move_endpoint, ClusterState, ClusterStateChange, MoveCommandOutcome, NodeDescriptor,
    NodeId, ResizeCommand, ResizeIntentPhase,
};
use crate::cluster::remote::unretire_node;

use super::{ClusterConfig, ClusterEngine, ShardError};

mod load;
mod plan;
mod recovery;
mod retire;

use crate::cluster::coordinator::layout::Layout;
use plan::{expected_endpoints, member_endpoint, resize_intent};
pub use recovery::{recover_durable_resize, ResizeRecovery};
use std::sync::Arc;

/// One remote resize request.
#[derive(Clone, Debug)]
pub struct RemoteResizeRequest {
    /// Non-zero idempotency key recorded in the control-plane intent.
    pub operation_id: u64,
    /// Target shard count.
    pub num_shards: usize,
    /// Fresh, empty shard servers for the new layout. Position `p` is placed on
    /// `targets[p % targets.len()]`. None may host a slot of the current layout.
    pub targets: Vec<NodeDescriptor>,
}

/// The attested result of a remote resize.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteResizeReport {
    pub old_num_shards: usize,
    pub num_shards: usize,
    pub placement_generation: u64,
    /// Distinct live queries exported from the old layout.
    pub exported: u64,
    /// Physical rows loaded into the new layout (replicated rows count once per position).
    pub loaded: u64,
    /// Old-layout slots durably retired before the commit.
    pub retired_slots: usize,
    /// Whether the control-plane intent was finished. `false` leaves a committed intent that the
    /// next resize attempt or coordinator startup finishes.
    pub finished: bool,
}

/// A committed but not yet installed resize: the staged engine whose shards and ring become the
/// serving layout. The old layout's nodes are already retired, so dropping this value without
/// installing it leaves this coordinator failing loud until a restart routes to the committed
/// layout.
pub struct PreparedRemoteResize {
    operation_id: u64,
    staged: ClusterEngine,
    old_num_shards: usize,
    /// The new layout's complete membership, installed as the converged logical-id directory.
    logical_ids: Vec<u64>,
    loaded: u64,
    retired_slots: usize,
}

/// How far a remote resize got, which decides how a failure is resolved.
#[derive(Default)]
pub(super) struct ResizeProgress {
    /// Old-layout nodes a `Retire` was sent to (including one whose reply was lost).
    retired_endpoints: RefCell<Vec<String>>,
    /// Set just before `Commit` is proposed, the only step that can change the layout of record.
    commit_proposed: Cell<bool>,
}

/// An installed resize awaiting `Finish`.
pub struct RetiredRemoteLayout {
    operation_id: u64,
    old_num_shards: usize,
    exported: u64,
    loaded: u64,
    retired_slots: usize,
}

impl ClusterEngine {
    /// Run a complete remote resize while holding `&mut self` (ADR-180). A server that wants reads
    /// to continue during the copy calls [`Self::prepare_remote_resize`] under a shared lock and
    /// the install/finish steps separately.
    pub fn resize_remote(
        &mut self,
        request: &RemoteResizeRequest,
    ) -> Result<RemoteResizeReport, ShardError> {
        let prepared = self.prepare_remote_resize_in(&self.layout(), request)?;
        let retired = self.install_remote_resize(prepared)?;
        self.finish_remote_resize_in(&self.layout(), retired)
    }

    /// Build, prove, and commit the new layout while the old one keeps serving reads. Writes are
    /// refused from the moment the fence is raised until [`Self::install_remote_resize`].
    pub fn prepare_remote_resize(
        &self,
        request: &RemoteResizeRequest,
    ) -> Result<PreparedRemoteResize, ShardError> {
        self.prepare_remote_resize_in(&self.layout(), request)
    }

    pub(in crate::cluster::coordinator) fn prepare_remote_resize_in(
        &self,
        layout: &Layout,
        request: &RemoteResizeRequest,
    ) -> Result<PreparedRemoteResize, ShardError> {
        self.prepare_remote_resize_then_in(layout, request, || {})
    }

    /// [`Self::prepare_remote_resize`], running `on_fenced` once the write fence is raised and
    /// every in-flight mutation has drained, before any control-plane or mesh RPC. A server passes
    /// a closure that releases its own write serialization: request threads waiting on it must
    /// never hold up the runtime these RPCs need, and queued writers are then refused by the fence
    /// instead of waiting out the copy. `on_fenced` does not run when the request itself is
    /// invalid.
    pub fn prepare_remote_resize_then(
        &self,
        request: &RemoteResizeRequest,
        on_fenced: impl FnOnce(),
    ) -> Result<PreparedRemoteResize, ShardError> {
        self.prepare_remote_resize_then_in(&self.layout(), request, on_fenced)
    }

    pub(in crate::cluster::coordinator) fn prepare_remote_resize_then_in(
        &self,
        layout: &Layout,
        request: &RemoteResizeRequest,
        on_fenced: impl FnOnce(),
    ) -> Result<PreparedRemoteResize, ShardError> {
        let handle = self.handle.clone().ok_or_else(|| {
            ShardError::Config("remote resize requires a gRPC-connected cluster".into())
        })?;
        self.validate_remote_resize_request(layout, request)?;
        self.raise_resize_write_fence()?;
        on_fenced();
        let progress = ResizeProgress::default();
        self.begin_and_build(layout, &handle, request, &progress)
            .map_err(|failure| {
                self.fail_resize(layout, &handle, request.operation_id, failure, &progress)
            })
    }

    /// Record `Begin`, then build, prove, retire, and commit.
    fn begin_and_build(
        &self,
        layout: &Layout,
        handle: &tokio::runtime::Handle,
        request: &RemoteResizeRequest,
        progress: &ResizeProgress,
    ) -> Result<PreparedRemoteResize, ShardError> {
        self.register_resize_targets(&request.targets)?;
        let state = self.control_state()?;
        let state = self.finish_prior_resize(layout, state, request.operation_id)?;
        let intent = resize_intent(&state, request)?;
        let expected_endpoints = expected_endpoints(&state)?;
        let target_endpoints: Vec<String> = intent
            .desired
            .assignments
            .iter()
            .map(|assignment| member_endpoint(&intent, assignment.primary))
            .collect::<Result<_, _>>()?;

        // Exclude moves, GC, and other resizes on every participating endpoint for the copy.
        let mut footprint = expected_endpoints.clone();
        footprint.extend(target_endpoints.iter().cloned());
        let _ticket = self.move_ledger.reserve(&footprint);
        // The export reads live routing, while fingerprints and retirement target the committed
        // layout, so both must name the same nodes. Checked under the reservation, which keeps
        // every handoff and move over these endpoints out until the resize ends: an uncommitted
        // route change (a raw handoff or a map-only reassignment) is refused rather than leaving
        // a live node unretired.
        self.attest_committed_layout_in(layout).map_err(|error| {
            ShardError::ControlPlane(format!(
                "remote resize requires serving routing to match the committed layout: {error}"
            ))
        })?;

        // A durable layout may only move onto durable targets; a volatile one (tests, caches) may
        // move onto either.
        let source_durable = self.layout_is_durable(layout, handle, &expected_endpoints)?;

        match self.propose_resize(ResizeCommand::Begin(intent.clone()))? {
            MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => {}
            outcome => {
                return Err(ShardError::ControlPlane(format!(
                    "remote resize intent was not accepted ({outcome:?}): another resize or \
                     move is active, or the layout changed"
                )));
            }
        }
        let load::StagedBuild {
            staged,
            logical_ids,
            loaded,
            retired_slots,
        } = self.build_and_commit(
            layout,
            handle,
            request,
            &intent,
            &load::Layouts {
                expected: &expected_endpoints,
                targets: &target_endpoints,
            },
            source_durable,
            progress,
        )?;
        Ok(PreparedRemoteResize {
            operation_id: request.operation_id,
            staged,
            old_num_shards: state.num_shards as usize,
            logical_ids,
            loaded,
            retired_slots,
        })
    }

    /// Swap the serving ring and shards to a committed staged layout and reopen writes. It makes no
    /// network call: it runs under the exclusive cluster lock that request threads may wait on.
    /// Preparation already confirmed the commit (an applied proposal or a matching read-back), and
    /// the recorded intent keeps every other layout change out until `Finish`.
    pub fn install_remote_resize(
        &mut self,
        prepared: PreparedRemoteResize,
    ) -> Result<RetiredRemoteLayout, ShardError> {
        let PreparedRemoteResize {
            operation_id,
            staged,
            old_num_shards,
            logical_ids,
            loaded,
            retired_slots,
        } = prepared;
        let exported = logical_ids.len() as u64;
        let generation = staged.placement_generation();
        // The directory mirrors the rebuilt corpus exactly, as after an in-process rebuild: the new
        // layout was loaded coherently from a fixed snapshot while writes were fenced, so it
        // restores create-only admission and exhaustive-delivery convergence even when this
        // coordinator attached to populated shards without either. A failure leaves the retired
        // old nodes refusing every request, so nothing answers from the superseded layout.
        self.replace_logical_ids(logical_ids)?;
        let staged_layout = staged.layout();
        let current = self.layout();
        self.layout.store(Arc::new(Layout {
            norm: Arc::clone(&current.norm),
            dict: Arc::clone(&current.dict),
            vocab: current.vocab.clone(),
            ring: staged_layout.ring.clone(),
            shards: Arc::clone(&staged_layout.shards),
            source_files: staged_layout.source_files.clone(),
            handoffs: staged_layout.handoffs.clone(),
            generation,
        }));
        self.transport_metrics = staged.transport_metrics;
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.clear_pits();
        self.resize_write_fence.store(false, Ordering::Release);
        Ok(RetiredRemoteLayout {
            operation_id,
            old_num_shards,
            exported,
            loaded,
            retired_slots,
        })
    }

    /// Finish the intent. Best effort: the committed layout is already serving and its old nodes
    /// are retired, and a remaining intent is finished by the next attempt or coordinator startup.
    /// Takes the installed layout by value so one installation is finished once.
    #[allow(clippy::needless_pass_by_value)]
    pub fn finish_remote_resize(
        &self,
        retired: RetiredRemoteLayout,
    ) -> Result<RemoteResizeReport, ShardError> {
        self.finish_remote_resize_in(&self.layout(), retired)
    }

    // The shape of the public method it serves.
    #[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
    pub(in crate::cluster::coordinator) fn finish_remote_resize_in(
        &self,
        layout: &Layout,
        retired: RetiredRemoteLayout,
    ) -> Result<RemoteResizeReport, ShardError> {
        let RetiredRemoteLayout {
            operation_id,
            old_num_shards,
            exported,
            loaded,
            retired_slots,
        } = retired;
        let finished = matches!(
            self.propose_resize(ResizeCommand::Finish { operation_id }),
            Ok(MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied)
        );
        Ok(RemoteResizeReport {
            old_num_shards,
            num_shards: layout.ring.num_shards(),
            placement_generation: layout.generation.0,
            exported,
            loaded,
            retired_slots,
            finished,
        })
    }

    /// A previous resize may have committed and installed but failed to record `Finish`. Its
    /// intent would block every later resize and move, so finish it first when the served layout is
    /// exactly the one it committed (its old nodes were retired before it committed).
    fn finish_prior_resize(
        &self,
        layout: &Layout,
        state: ClusterState,
        operation_id: u64,
    ) -> Result<ClusterState, ShardError> {
        let Some(prior) = state.moves.resize.clone() else {
            return Ok(state);
        };
        // An earlier attempt of this same operation left its uncommitted intent behind (for
        // example after a lost reply). Nothing routes to its staged layout and only this
        // coordinator could commit it, so abort it and start over.
        if prior.operation_id == operation_id
            && !matches!(prior.phase, ResizeIntentPhase::Committed(_))
        {
            self.expect_resize_outcome(ResizeCommand::Abort { operation_id })?;
            return self.control_state();
        }
        let serving_committed = matches!(prior.phase, ResizeIntentPhase::Committed(_))
            && state.num_shards == prior.desired.num_shards
            && state.placement_generation == prior.desired.placement_generation
            && state.assignments == prior.desired.assignments
            && layout.ring.num_shards() == prior.desired.num_shards as usize
            && layout.generation.0 == prior.desired.placement_generation;
        if !serving_committed {
            return Err(ShardError::ControlPlane(format!(
                "another remote resize ({}) is still in progress; wait for it or restart the \
                 coordinator to resolve it",
                prior.operation_id
            )));
        }
        self.expect_resize_outcome(ResizeCommand::Finish {
            operation_id: prior.operation_id,
        })?;
        self.control_state()
    }

    fn validate_remote_resize_request(
        &self,
        layout: &Layout,
        request: &RemoteResizeRequest,
    ) -> Result<(), ShardError> {
        if request.operation_id == 0 {
            return Err(ShardError::Config(
                "remote resize operation id must be non-zero".into(),
            ));
        }
        if request.num_shards == 0 || request.num_shards > u32::MAX as usize {
            return Err(ShardError::Config(
                "remote resize shard count is out of range".into(),
            ));
        }
        if request.targets.is_empty() {
            return Err(ShardError::Config(
                "remote resize needs at least one target node".into(),
            ));
        }
        if self.replication_factor > 1 {
            return Err(ShardError::Config(
                "remote resize supports replication factor 1; rebalance replicas afterwards".into(),
            ));
        }
        if layout.handoffs.len() != layout.shards.len() || self.data_dir.is_some() {
            return Err(ShardError::Config(
                "remote resize requires a remote, assignment-routed cluster".into(),
            ));
        }
        // The write fence only pauses this coordinator's writers. Exclusive shard claims keep
        // every other coordinator off the source slots, so nothing can land after the export.
        if self.coordinator_id.is_none() {
            return Err(ShardError::Config(
                "remote resize requires an exclusive coordinator (connect_remote_exclusive); a \
                 shared coordinator cannot keep other writers off the source slots"
                    .into(),
            ));
        }
        if self.pending_repairs() != 0 {
            return Err(ShardError::ControlPlane(
                "remote resize requires every queued partial write to be repaired first; run \
                 resync and retry"
                    .into(),
            ));
        }
        let mut ids: Vec<NodeId> = request.targets.iter().map(|t| t.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != request.targets.len()
            || request
                .targets
                .iter()
                .any(|t| t.addr.as_deref().is_none_or(str::is_empty))
        {
            return Err(ShardError::Config(
                "remote resize targets need distinct ids and endpoints".into(),
            ));
        }
        Ok(())
    }

    /// Register unknown targets; refuse a target id already registered at another endpoint.
    fn register_resize_targets(&self, targets: &[NodeDescriptor]) -> Result<(), ShardError> {
        let state = self.control_state()?;
        for target in targets {
            let wanted = target.addr.as_deref().map(normalized_move_endpoint);
            match state.nodes.iter().find(|node| node.id == target.id) {
                Some(node) if node.addr.as_deref().map(normalized_move_endpoint) == wanted => {}
                Some(_) => {
                    return Err(ShardError::Config(format!(
                        "resize target node {} is registered at a different endpoint",
                        target.id.0
                    )));
                }
                None => {
                    self.control
                        .propose(ClusterStateChange::AddNode(target.clone()))
                        .map_err(|error| ShardError::ControlPlane(error.to_string()))?;
                }
            }
        }
        Ok(())
    }

    fn propose_resize(&self, command: ResizeCommand) -> Result<MoveCommandOutcome, ShardError> {
        self.control
            .propose_resize(command)
            .map(|result| result.outcome)
            .map_err(|error| ShardError::ControlPlane(error.to_string()))
    }

    /// Raise the write fence, then take the mutation barrier exclusively so every mutation that
    /// passed the fence check before it was raised has finished applying to the old layout.
    ///
    /// Refuses, leaving the fence untouched, when it is already raised: another resize is still
    /// preparing or awaiting install, or an earlier one failed after proposing `Commit` without
    /// proof that it did not apply. This attempt did not raise that fence and cannot know its
    /// outcome, so lowering it on failure could accept writes the committed layout never receives.
    fn raise_resize_write_fence(&self) -> Result<(), ShardError> {
        if self
            .resize_write_fence
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ShardError::ControlPlane(
                "writes are paused by another remote resize, or by an earlier one whose commit \
                 outcome is unresolved; wait for it, or restart the coordinator to resolve the \
                 recorded intent, before resizing again"
                    .into(),
            ));
        }
        drop(
            self.pit_open_barrier
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        Ok(())
    }

    /// Resolve a failed preparation.
    ///
    /// Only this coordinator's `Commit` can make the new layout the layout of record: startup aborts
    /// every uncommitted intent. So before `Commit` is proposed, the old layout is certainly still
    /// authoritative: the nodes this attempt retired are unretired, the intent is aborted, and
    /// writes reopen. A node whose unretire fails keeps refusing requests (failing loud, never
    /// answering wrongly) until coordinator startup lifts it.
    ///
    /// Once `Commit` was proposed, the old nodes stay retired, refusing every read and write from any
    /// coordinator, unless the control plane proves the commit did not apply: the abort was
    /// accepted and the committed layout is still the served one. Otherwise writes stay fenced and
    /// the retired nodes keep failing loud until a coordinator restart routes to the committed
    /// layout.
    fn fail_resize(
        &self,
        layout: &Layout,
        handle: &tokio::runtime::Handle,
        operation_id: u64,
        failure: ShardError,
        progress: &ResizeProgress,
    ) -> ShardError {
        let retired = progress.retired_endpoints.borrow().clone();
        let proven = !progress.commit_proposed.get() || {
            let aborted = matches!(
                self.propose_resize(ResizeCommand::Abort { operation_id }),
                Ok(MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied)
            );
            aborted
                && self.control_state().is_ok_and(|state| {
                    state.num_shards as usize == layout.ring.num_shards()
                        && state.placement_generation == layout.generation.0
                        && state.moves.resize.is_none()
                })
        };
        if !proven {
            self.report_unresolved(operation_id, &failure);
            return failure;
        }
        self.unretire_all(handle, operation_id, &retired);
        let _aborted = self.propose_resize(ResizeCommand::Abort { operation_id });
        self.resize_write_fence.store(false, Ordering::Release);
        failure
    }

    /// Lift `operation_id`'s retirement of every node in `endpoints`, reporting each failure.
    fn unretire_all(
        &self,
        handle: &tokio::runtime::Handle,
        operation_id: u64,
        endpoints: &[String],
    ) {
        let Some(coordinator_id) = self.coordinator_id else {
            return;
        };
        for endpoint in endpoints {
            if let Err(error) = unretire_node(
                endpoint,
                handle,
                &self.client_security,
                coordinator_id,
                operation_id,
            ) {
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::ReplicaDesync,
                    detail: format!(
                        "remote resize {operation_id} could not unretire {endpoint}; it refuses \
                         requests until coordinator startup lifts the retirement"
                    ),
                    error: error.to_string(),
                });
            }
        }
    }

    fn report_unresolved(&self, operation_id: u64, failure: &ShardError) {
        self.emit(crate::events::EngineEvent::DurabilityFailure {
            op: crate::events::DurabilityOp::ReplicaDesync,
            detail: format!(
                "remote resize {operation_id} failed with an unresolved outcome; writes stay \
                 paused and the retired old nodes refuse requests until a coordinator restart \
                 resolves the recorded intent"
            ),
            error: failure.to_string(),
        });
    }

    fn expect_resize_outcome(&self, command: ResizeCommand) -> Result<(), ShardError> {
        match self.propose_resize(command)? {
            MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(()),
            outcome => Err(ShardError::ControlPlane(format!(
                "remote resize transition was refused ({outcome:?})"
            ))),
        }
    }
}
