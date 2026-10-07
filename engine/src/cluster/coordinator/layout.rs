//! The serving layout: everything a read or a write is routed and matched by.
//!
//! A layout is the feature space (normalizer, dictionary, vocabulary), the ring, the shards
//! and the placement generation they were built under. These always change together: a
//! vocabulary change or a resize builds a whole new layout and replaces the old one
//! (ADR-046, ADR-078, ADR-180). Nothing else on the coordinator replaces any of them.

use std::sync::Arc;

use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::ownership::PlacementGeneration;
use crate::vocab::Vocab;

#[cfg(feature = "distributed")]
use super::super::handoff::HandoffShard;
use super::super::ring::HashRing;
use super::super::shard::{Shard, ShardError};

#[derive(Clone)]
pub(in crate::cluster::coordinator) struct Layout {
    /// The one shared feature space, frozen when the layout is built.
    pub(in crate::cluster::coordinator) norm: Arc<Normalizer>,
    pub(in crate::cluster::coordinator) dict: Arc<Dict>,
    /// The vocabulary behind the normalizer, if one was installed (ADR-046). `None` when
    /// the cluster was built directly from a `Normalizer`. Kept so a durable cluster can
    /// persist it and a re-learn can merge into it.
    pub(in crate::cluster::coordinator) vocab: Option<Arc<Vocab>>,
    pub(in crate::cluster::coordinator) ring: HashRing,
    /// Shared, so that a layout can be copied with one field changed.
    pub(in crate::cluster::coordinator) shards: Arc<Vec<Box<dyn Shard>>>,
    /// Per-shard source-sidecar basenames selected by the current coordinator manifest.
    /// Index-aligned with `shards`.
    pub(in crate::cluster::coordinator) source_files: Vec<String>,
    /// Per-position handoff handles (ADR-043), index-aligned with `shards`. Empty on the
    /// in-process path; the gRPC builders wrap each position's backing in a [`HandoffShard`]
    /// so a position can be re-pointed at a new owner at runtime. `handoffs[i]` and
    /// `shards[i]` share one `HandoffShard`.
    #[cfg(feature = "distributed")]
    pub(in crate::cluster::coordinator) handoffs: Vec<Arc<HandoffShard>>,
    /// Monotonic placement identity (ADR-109). Changes only with the layout, never on a
    /// checkpoint or a physical data movement.
    pub(in crate::cluster::coordinator) generation: PlacementGeneration,
}

impl Layout {
    pub(in crate::cluster::coordinator) fn num_shards(&self) -> usize {
        self.ring.num_shards()
    }

    /// Total physical query count across shards (a replicated or any-of query is counted
    /// once per shard holding it).
    pub(in crate::cluster::coordinator) fn num_queries(&self) -> Result<usize, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).sum()
    }

    pub(in crate::cluster::coordinator) fn shard_query_counts(
        &self,
    ) -> Result<Vec<usize>, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).collect()
    }
}

/// How long a layout change waits, once it has published, for the operations still running on
/// the layout it replaced. After that it leaves that layout's files for a later checkpoint.
const RETIRED_LAYOUT_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// One change of the layout in progress. Until it is dropped no other change, checkpoint,
/// flush or backup runs, and every write is refused. Reads go on, on the layout that is
/// published when they load.
pub(in crate::cluster::coordinator) struct LayoutChange<'a> {
    engine: &'a super::ClusterEngine,
    _maintenance: std::sync::MutexGuard<'a, ()>,
}

impl LayoutChange<'_> {
    /// The layout being replaced: the published one until this change publishes another.
    pub(in crate::cluster::coordinator) fn current(&self) -> Arc<Layout> {
        self.engine.layout.load_full()
    }

    /// Publish `next` in one step and return it. Operations that loaded the previous layout
    /// finish on it; it is released when the last of them returns.
    ///
    /// `with_it` runs just before the swap and is for what must change together with the
    /// layout (the logical-id directory). Both, the repair queue and the release of the old
    /// layout's point-in-time pins change on the exclusive side of the mutation barrier. A frozen read
    /// view, a point-in-time open and an exhaustive read hold that side for as long as they
    /// run, so each of them sees one layout, one directory and its own pins from its first
    /// step to its last.
    pub(in crate::cluster::coordinator) fn publish(
        &self,
        next: Layout,
        with_it: impl FnOnce() -> Result<(), ShardError>,
    ) -> Result<Arc<Layout>, ShardError> {
        let _quiet = self.engine.quiesce_mutations();
        with_it()?;
        let next = self.engine.publish_layout(next);
        // Queued repairs (ADR-047) describe where the old layout's shards disagree, by their
        // positions. The new layout was rebuilt from the live corpus and has no such
        // disagreement. Until this moment they must stay: an exhaustive read on the old
        // layout refuses to certify a result while one is queued.
        self.engine
            .pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        // ADR-113: the old shards' pins can only fail their generation gate now. Drop them
        // (which frees their slots) without resetting the id counter, so a stale cursor can
        // never name a later point in time.
        self.engine.clear_pits();
        Ok(next)
    }
}

impl Drop for LayoutChange<'_> {
    fn drop(&mut self) {
        self.engine.lower_write_fence();
    }
}

/// The shards a layout change is replacing, with their storage frozen (ADR-209). The new
/// layout is built in their directories and shares their segment numbers, log and checkpoint
/// sidecar, so from the moment it starts to be built nothing may write there through the old
/// shards: not a write that slipped the fence, not a seal, not a replica recovery. A change
/// that fails thaws them and they go on serving; one that publishes keeps them frozen for
/// good, and they only finish the reads that are still running on them.
pub(in crate::cluster::coordinator) struct FrozenShards {
    replaced: Arc<Layout>,
    thaw: bool,
}

impl FrozenShards {
    pub(in crate::cluster::coordinator) fn freeze(replaced: Arc<Layout>) -> Self {
        for shard in replaced.shards.iter() {
            shard.set_storage_frozen(true);
        }
        Self {
            replaced,
            thaw: true,
        }
    }

    /// The new layout is published: the replaced shards stay frozen.
    pub(in crate::cluster::coordinator) fn keep(mut self) {
        self.thaw = false;
    }
}

impl Drop for FrozenShards {
    fn drop(&mut self) {
        if self.thaw {
            for shard in self.replaced.shards.iter() {
                shard.set_storage_frozen(false);
            }
        }
    }
}

/// A mutation that holds the mutation barrier shared. Its layout stays the published one
/// until this is dropped. See [`ClusterEngine::admit_mutation`](super::ClusterEngine).
pub(in crate::cluster::coordinator) struct Admitted<'a> {
    pub(in crate::cluster::coordinator) layout: Arc<Layout>,
    _barrier: std::sync::RwLockReadGuard<'a, ()>,
}

impl super::ClusterEngine {
    /// The layout to run one operation under. Load it once, at the operation's entry, and
    /// hand it down: a rebuild may publish another at any moment, and an operation that
    /// routed by one layout and matched in another would be wrong.
    pub(in crate::cluster::coordinator) fn layout(&self) -> Arc<Layout> {
        self.layout.load_full()
    }

    /// Admit one mutation: take the mutation barrier shared, then load the layout.
    ///
    /// The order is what makes the layout safe to write to. A layout change first refuses new
    /// writes and then takes the barrier exclusively once, so it waits for every mutation
    /// admitted before it and publishes only after they have applied. A mutation that loaded
    /// the layout before it held the barrier could find, by the time it applied, that the
    /// layout had been replaced, and write to shards nothing reads any more.
    pub(in crate::cluster::coordinator) fn admit_mutation(&self) -> Admitted<'_> {
        let barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        self.pause_admission();
        Admitted {
            layout: self.layout.load_full(),
            _barrier: barrier,
        }
    }

    /// Begin a change of the layout: one at a time, with every write held out.
    ///
    /// It takes the maintenance lock, raises the write fence, and takes the mutation barrier
    /// exclusively once, so every mutation admitted before the fence has applied. From here
    /// the corpus is still, and the layout published now stays published until this change
    /// replaces it. A change that fails before it publishes leaves that layout serving.
    pub(in crate::cluster::coordinator) fn begin_layout_change(
        &self,
    ) -> Result<LayoutChange<'_>, ShardError> {
        let maintenance = self.maintenance();
        self.raise_resize_write_fence()?;
        Ok(LayoutChange {
            engine: self,
            _maintenance: maintenance,
        })
    }

    /// Swap in `next` and remember the layout it replaces until its last holder lets go.
    pub(in crate::cluster::coordinator) fn publish_layout(&self, next: Layout) -> Arc<Layout> {
        let next = Arc::new(next);
        let previous = self.layout.swap(Arc::clone(&next));
        self.retired_layouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::downgrade(&previous));
        next
    }

    /// Whether every layout that was replaced has been released by the operations that held
    /// it. Until then its files stay: an operation still running on it may open one.
    pub(in crate::cluster::coordinator) fn retired_layouts_released(&self) -> bool {
        let mut retired = self
            .retired_layouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retired.retain(|layout| layout.strong_count() > 0);
        retired.is_empty()
    }

    /// Give the operations still running on a replaced layout a moment to finish, so that the
    /// checkpoint that follows can remove its files. The caller holds no handle on it.
    pub(in crate::cluster::coordinator) fn await_retired_layouts(&self) {
        let deadline = std::time::Instant::now() + RETIRED_LAYOUT_GRACE;
        while !self.retired_layouts_released() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    /// Whether `layout` is the one published now.
    pub(in crate::cluster::coordinator) fn is_published(&self, layout: &Layout) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.layout.load()), layout)
    }

    /// Publish a copy of the current layout with `edit` applied. For assembly and for the
    /// operations that hold the engine exclusively.
    pub(in crate::cluster::coordinator) fn edit_layout(&mut self, edit: impl FnOnce(&mut Layout)) {
        let mut next = Layout::clone(&self.layout());
        edit(&mut next);
        self.layout.store(Arc::new(next));
    }

    /// Take the shards out, pass them through `wrap`, and publish the result. For a test that
    /// instruments the shards of an engine nothing else is using.
    #[cfg(test)]
    pub(in crate::cluster::coordinator) fn replace_shards(
        &mut self,
        wrap: impl FnOnce(Vec<Box<dyn Shard>>) -> Vec<Box<dyn Shard>>,
    ) {
        let mut emptied = Layout::clone(&self.layout());
        emptied.shards = Arc::new(Vec::new());
        let previous = self.layout.swap(Arc::new(emptied));
        let previous = Arc::try_unwrap(previous)
            .ok()
            .expect("no operation holds the layout");
        let shards = Arc::try_unwrap(previous.shards)
            .ok()
            .expect("no other layout shares the shards");
        self.edit_layout(|layout| layout.shards = Arc::new(wrap(shards)));
    }

    /// Run the test's hook between the two steps of an admission.
    #[cfg(test)]
    fn pause_admission(&self) {
        let hook = self
            .admission_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }
}
