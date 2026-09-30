//! Remote blue/green resize onto fresh target nodes (ADR-180).
//!
//! The committed layout stays authoritative and readable while a complete new layout is built on
//! separate, empty shard servers:
//!
//! 1. **prepare** (`&self`): record a `Preparing` intent, raise the resize write fence and drain
//!    in-flight mutations, build the target layout at placement generation `N + 1`, stream the
//!    deduplicated live corpus into it, record per-position fingerprint evidence as `Ready`, and
//!    conditionally commit the new shard count, generation, and assignments together;
//! 2. **install** (`&mut self`): swap the serving ring and shards to the committed layout and
//!    lower the fence;
//! 3. **finish** (`&self`): fence the retired slots so a stale writer fails loud, then finish the
//!    intent.
//!
//! Any failure before the commit lowers the fence and aborts the intent, leaving the old layout
//! serving untouched. An ambiguous commit is resolved by reading the control state back rather
//! than guessing.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::cluster::control::{
    normalized_move_endpoint, ClusterState, ClusterStateChange, MoveCommandOutcome,
    MoveMemberIdentity, NodeDescriptor, NodeId, ResizeCommand, ResizeIntent, ResizeIntentPhase,
    ResizeLayout, ResizePositionEvidence, ShardAssignment, RESIZE_INTENT_VERSION,
};
use crate::cluster::remote::RemoteShard;

use super::{ClusterConfig, ClusterEngine, ShardError};

/// Queries buffered before each staged-layout load call.
const EXPORT_BATCH: usize = 4096;

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
    /// Old-layout slots fenced after the cutover.
    pub retired_slots: usize,
    /// Whether the control-plane intent was finished. `false` leaves a committed intent that the
    /// next resize attempt or coordinator startup finishes.
    pub finished: bool,
}

/// A committed but not yet installed resize: the staged engine whose shards and ring become the
/// serving layout.
pub struct PreparedRemoteResize {
    operation_id: u64,
    staged: ClusterEngine,
    old_num_shards: usize,
    exported: u64,
    loaded: u64,
    retired: Vec<(u32, String)>,
}

/// The old layout after installation, awaiting retirement.
pub struct RetiredRemoteLayout {
    operation_id: u64,
    old_num_shards: usize,
    exported: u64,
    loaded: u64,
    slots: Vec<(u32, String)>,
}

impl ClusterEngine {
    /// Run a complete remote resize while holding `&mut self` (ADR-180). A server that wants reads
    /// to continue during the copy calls [`Self::prepare_remote_resize`] under a shared lock and
    /// the install/finish steps separately.
    pub fn resize_remote(
        &mut self,
        request: &RemoteResizeRequest,
    ) -> Result<RemoteResizeReport, ShardError> {
        let prepared = self.prepare_remote_resize(request)?;
        let retired = self.install_remote_resize(prepared)?;
        self.finish_remote_resize(retired)
    }

    /// Build, prove, and commit the new layout while the old one keeps serving reads. Writes are
    /// refused from the moment the fence is raised until [`Self::install_remote_resize`].
    pub fn prepare_remote_resize(
        &self,
        request: &RemoteResizeRequest,
    ) -> Result<PreparedRemoteResize, ShardError> {
        let handle = self.handle.clone().ok_or_else(|| {
            ShardError::Config("remote resize requires a gRPC-connected cluster".into())
        })?;
        self.validate_remote_resize_request(request)?;
        self.register_resize_targets(&request.targets)?;
        let state = self.control_state()?;
        if state.num_shards as usize != self.ring.num_shards()
            || state.placement_generation != self.placement_generation().0
        {
            return Err(ShardError::ControlPlane(format!(
                "remote resize requires serving routing to match the committed layout: serving \
                 generation {}/{} shards, committed generation {}/{} shards",
                self.placement_generation().0,
                self.ring.num_shards(),
                state.placement_generation,
                state.num_shards
            )));
        }
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

        match self.propose_resize(ResizeCommand::Begin(intent.clone()))? {
            MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => {}
            outcome => {
                return Err(ShardError::ControlPlane(format!(
                    "remote resize intent was not accepted ({outcome:?}): another resize or \
                     move is active, or the layout changed"
                )));
            }
        }
        self.raise_resize_write_fence();
        match self.build_and_commit(&handle, request, &intent, &target_endpoints) {
            Ok((staged, exported, loaded)) => Ok(PreparedRemoteResize {
                operation_id: request.operation_id,
                staged,
                old_num_shards: state.num_shards as usize,
                exported,
                loaded,
                retired: expected_endpoints
                    .into_iter()
                    .enumerate()
                    .map(|(position, endpoint)| (position as u32, endpoint))
                    .collect(),
            }),
            Err(failure) => Err(self.fail_resize(request.operation_id, failure)),
        }
    }

    /// Swap the serving ring and shards to a committed staged layout and reopen writes.
    pub fn install_remote_resize(
        &mut self,
        prepared: PreparedRemoteResize,
    ) -> Result<RetiredRemoteLayout, ShardError> {
        let PreparedRemoteResize {
            operation_id,
            staged,
            old_num_shards,
            exported,
            loaded,
            retired,
        } = prepared;
        let generation = staged.placement_generation();
        let state = self.control_state()?;
        if state.placement_generation != generation.0
            || state.num_shards as usize != staged.ring.num_shards()
        {
            return Err(ShardError::ControlPlane(format!(
                "refusing to install a staged layout at generation {}/{} shards: the committed \
                 layout is generation {}/{} shards",
                generation.0,
                staged.ring.num_shards(),
                state.placement_generation,
                state.num_shards
            )));
        }
        self.ring = staged.ring;
        self.shards = staged.shards;
        self.handoffs = staged.handoffs;
        self.source_files = staged.source_files;
        self.transport_metrics = staged.transport_metrics;
        self.placement_generation
            .store(generation.0, Ordering::Release);
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
            slots: retired,
        })
    }

    /// Fence every retired slot so a stale writer fails loud, then finish the intent. Both are
    /// best effort: the committed layout is already serving, and a remaining intent is finished
    /// by the next attempt or coordinator startup.
    pub fn finish_remote_resize(
        &self,
        retired: RetiredRemoteLayout,
    ) -> Result<RemoteResizeReport, ShardError> {
        let RetiredRemoteLayout {
            operation_id,
            old_num_shards,
            exported,
            loaded,
            slots,
        } = retired;
        let state = self.control_state()?;
        let fence_generation = state.epoch.max(1);
        let mut fenced = 0;
        for (shard_id, endpoint) in &slots {
            match self.fence_retired_slot(endpoint, *shard_id, fence_generation) {
                Ok(()) => fenced += 1,
                Err(error) => self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::ReplicaDesync,
                    detail: format!(
                        "remote resize could not fence retired slot {shard_id} on {endpoint}; \
                         decommission that node before reusing it"
                    ),
                    error: error.to_string(),
                }),
            }
        }
        let finished = matches!(
            self.propose_resize(ResizeCommand::Finish { operation_id }),
            Ok(MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied)
        );
        Ok(RemoteResizeReport {
            old_num_shards,
            num_shards: self.ring.num_shards(),
            placement_generation: self.placement_generation().0,
            exported,
            loaded,
            retired_slots: fenced,
            finished,
        })
    }

    fn validate_remote_resize_request(
        &self,
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
        if self.handoffs.len() != self.shards.len() || self.data_dir.is_some() {
            return Err(ShardError::Config(
                "remote resize requires a remote, assignment-routed cluster".into(),
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
    fn raise_resize_write_fence(&self) {
        self.resize_write_fence.store(true, Ordering::Release);
        drop(
            self.pit_open_barrier
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    /// Lower the fence and abort the intent after a failure before a proven commit. If the abort
    /// cannot be proposed, the intent stays for startup recovery, which aborts it.
    fn fail_resize(&self, operation_id: u64, failure: ShardError) -> ShardError {
        if let Ok(state) = self.control_state() {
            let committed = state.moves.resize.as_ref().is_some_and(|intent| {
                intent.operation_id == operation_id
                    && matches!(intent.phase, ResizeIntentPhase::Committed(_))
            });
            if committed {
                // The new layout is durable; keep writes fenced until it is installed.
                return failure;
            }
        }
        let _aborted = self.propose_resize(ResizeCommand::Abort { operation_id });
        self.resize_write_fence.store(false, Ordering::Release);
        failure
    }

    fn build_and_commit(
        &self,
        handle: &tokio::runtime::Handle,
        request: &RemoteResizeRequest,
        intent: &ResizeIntent,
        target_endpoints: &[String],
    ) -> Result<(ClusterEngine, u64, u64), ShardError> {
        if self.pending_repairs() != 0 {
            return Err(ShardError::ControlPlane(
                "a partial write was queued before the resize fence; run resync and retry".into(),
            ));
        }
        let config = ClusterConfig {
            num_shards: request.num_shards,
            vnodes: self.vnodes,
            replication_factor: 1,
            per_shard: self.per_shard.clone(),
            include_broad: self.include_broad,
            remote_placement_generation: intent.desired.placement_generation,
            ..ClusterConfig::default()
        };
        let staged = Self::connect_remote_with_security_mode(
            Arc::clone(&self.norm),
            Arc::clone(&self.dict),
            Arc::clone(&self.tag_dict),
            &config,
            target_endpoints,
            handle,
            self.client_security.clone(),
            self.coordinator_id,
        )?;
        if staged.num_queries()? != 0 {
            return Err(ShardError::Config(
                "remote resize targets must be empty; wipe their data directories and retry".into(),
            ));
        }

        let mut loaded = vec![0u64; request.num_shards];
        let mut batch = Vec::with_capacity(EXPORT_BATCH);
        let mut load_error: Option<ShardError> = None;
        let exported = self.export_live_corpus(&mut |query| {
            batch.push((query.logical_id, query.version, query.dsl, query.tags));
            if batch.len() >= EXPORT_BATCH {
                let result = staged.load_resize_batch(&batch, &mut loaded);
                batch.clear();
                if let Err(error) = result {
                    load_error = Some(error);
                    return Err(ShardError::Protocol("staged layout load failed".into()));
                }
            }
            Ok(())
        });
        if let Some(error) = load_error {
            return Err(error);
        }
        let exported = exported?;
        staged.load_resize_batch(&batch, &mut loaded)?;

        let mut evidence = Vec::with_capacity(request.num_shards);
        for (position, endpoint) in target_endpoints.iter().enumerate() {
            let client = RemoteShard::connect_for_coordinator_with_security(
                endpoint,
                handle.clone(),
                self.dict.fingerprint(),
                self.tag_dict.fingerprint(),
                position as u32,
                self.coordinator_id,
                &self.client_security,
            )?;
            let (fingerprint_lo, fingerprint_hi, live_count) = client.content_fingerprint()?;
            if live_count != loaded[position] {
                return Err(ShardError::Protocol(format!(
                    "staged position {position} holds {live_count} rows but {} were loaded",
                    loaded[position]
                )));
            }
            evidence.push(ResizePositionEvidence {
                position: position as u32,
                fingerprint_lo,
                fingerprint_hi,
                live_count,
            });
        }
        self.expect_resize_outcome(ResizeCommand::MarkReady {
            operation_id: request.operation_id,
            evidence,
        })?;
        self.commit_resize(request.operation_id, &intent.desired)?;
        let loaded_total = loaded.iter().sum();
        Ok((staged, exported, loaded_total))
    }

    fn expect_resize_outcome(&self, command: ResizeCommand) -> Result<(), ShardError> {
        match self.propose_resize(command)? {
            MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(()),
            outcome => Err(ShardError::ControlPlane(format!(
                "remote resize transition was refused ({outcome:?})"
            ))),
        }
    }

    /// Commit, resolving an ambiguous proposal result by reading the committed layout back.
    fn commit_resize(&self, operation_id: u64, desired: &ResizeLayout) -> Result<(), ShardError> {
        let proposed = self.propose_resize(ResizeCommand::Commit { operation_id });
        if matches!(
            proposed,
            Ok(MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied)
        ) {
            return Ok(());
        }
        let state = self.control_state()?;
        if state.num_shards == desired.num_shards
            && state.placement_generation == desired.placement_generation
            && state.assignments == desired.assignments
        {
            return Ok(());
        }
        Err(match proposed {
            Ok(outcome) => {
                ShardError::ControlPlane(format!("remote resize commit was refused ({outcome:?})"))
            }
            Err(error) => error,
        })
    }

    fn fence_retired_slot(
        &self,
        endpoint: &str,
        shard_id: u32,
        generation: u64,
    ) -> Result<(), ShardError> {
        let handle = self
            .handle
            .clone()
            .ok_or_else(|| ShardError::Config("no runtime handle".into()))?;
        RemoteShard::connect_for_coordinator_with_security(
            endpoint,
            handle,
            self.dict.fingerprint(),
            self.tag_dict.fingerprint(),
            shard_id,
            self.coordinator_id,
            &self.client_security,
        )?
        .fence(generation)
        .map(|_| ())
    }
}

/// The primary endpoint of every current position, in position order.
fn expected_endpoints(state: &ClusterState) -> Result<Vec<String>, ShardError> {
    state
        .assignments
        .iter()
        .map(|assignment| {
            if !assignment.replicas.is_empty() {
                return Err(ShardError::Config(
                    "remote resize supports replication factor 1".into(),
                ));
            }
            state
                .nodes
                .iter()
                .find(|node| node.id == assignment.primary)
                .and_then(|node| node.addr.clone())
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "position {} is assigned to node {} with no registered endpoint",
                        assignment.position, assignment.primary.0
                    ))
                })
        })
        .collect()
}

fn member_endpoint(intent: &ResizeIntent, node: NodeId) -> Result<String, ShardError> {
    intent
        .members
        .iter()
        .find(|member| member.node == node)
        .map(|member| member.endpoint.clone())
        .ok_or_else(|| ShardError::Config(format!("target node {} has no endpoint", node.0)))
}

/// The complete intent: the committed layout, and the new layout with position `p` on
/// `targets[p % targets.len()]` at the next placement generation.
fn resize_intent(
    state: &ClusterState,
    request: &RemoteResizeRequest,
) -> Result<ResizeIntent, ShardError> {
    let num_shards = u32::try_from(request.num_shards)
        .map_err(|_| ShardError::Config("remote resize shard count is out of range".into()))?;
    let placement_generation = state
        .placement_generation
        .checked_add(1)
        .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
    let assignments = (0..num_shards)
        .map(|position| ShardAssignment {
            position,
            primary: request.targets[position as usize % request.targets.len()].id,
            replicas: Vec::new(),
        })
        .collect();
    let mut members: Vec<MoveMemberIdentity> = request
        .targets
        .iter()
        .take(request.num_shards)
        .map(|target| MoveMemberIdentity {
            node: target.id,
            endpoint: target
                .addr
                .as_deref()
                .map(normalized_move_endpoint)
                .unwrap_or_default(),
        })
        .collect();
    members.sort_by_key(|member| member.node);
    Ok(ResizeIntent {
        intent_version: RESIZE_INTENT_VERSION,
        operation_id: request.operation_id,
        expected: ResizeLayout {
            num_shards: state.num_shards,
            placement_generation: state.placement_generation,
            assignments: state.assignments.clone(),
        },
        desired: ResizeLayout {
            num_shards,
            placement_generation,
            assignments,
        },
        members,
        phase: ResizeIntentPhase::Preparing,
    })
}

/// What coordinator startup did with a recorded remote-resize intent (ADR-180).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResizeRecovery {
    /// An uncommitted intent was aborted; the previous layout remains authoritative.
    Aborted { operation_id: u64 },
    /// A committed intent was finished; the committed (new) layout is authoritative.
    Finished { operation_id: u64 },
}

/// Resolve a recorded remote-resize intent before routes are assembled (ADR-180). An uncommitted
/// intent is aborted: its staged layout was never routed, and the previous layout is still the
/// committed one. A committed intent is finished: consensus already names the new layout. The
/// retired slots cannot be fenced here without their data-node handles, so a finished recovery
/// reports them for operator decommissioning. Returns `None` when no intent is recorded; any
/// refused or failed transition fails startup rather than serving an ambiguous layout.
pub fn recover_durable_resize(
    control: &dyn crate::cluster::control::ControlPlane,
) -> Result<Option<ResizeRecovery>, ShardError> {
    let state = control
        .cluster_state()
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?;
    let Some(intent) = state.moves.resize.clone() else {
        return Ok(None);
    };
    let operation_id = intent.operation_id;
    let (command, recovery) = match intent.phase {
        ResizeIntentPhase::Preparing | ResizeIntentPhase::Ready(_) => (
            ResizeCommand::Abort { operation_id },
            ResizeRecovery::Aborted { operation_id },
        ),
        ResizeIntentPhase::Committed(_) => (
            ResizeCommand::Finish { operation_id },
            ResizeRecovery::Finished { operation_id },
        ),
    };
    let outcome = control
        .propose_resize(command)
        .map_err(|error| ShardError::ControlPlane(error.to_string()))?
        .outcome;
    match outcome {
        MoveCommandOutcome::Applied | MoveCommandOutcome::AlreadyApplied => Ok(Some(recovery)),
        outcome => Err(ShardError::ControlPlane(format!(
            "could not resolve remote-resize intent {operation_id} at startup ({outcome:?}); \
             inspect the control-plane state before serving"
        ))),
    }
}
