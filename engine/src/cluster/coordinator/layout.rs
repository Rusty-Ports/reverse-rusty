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

/// How long a layout change waits, once it has published, for the searches still running on
/// the layout it replaced. After that it leaves that layout's files for a later checkpoint.
const RETIRED_LAYOUT_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// One change of the layout in progress. It holds the layout lock alone, so until it is
/// dropped nothing runs on the engine but searches, which go on, each on the layout that was
/// published when it loaded.
pub(in crate::cluster::coordinator) struct LayoutChange<'a> {
    engine: &'a super::ClusterEngine,
    _alone: std::sync::RwLockWriteGuard<'a, ()>,
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
    /// layout (the logical-id directory). The swap, the repair queue and the release of the
    /// old layout's point-in-time pins happen on the exclusive side of the mutation barrier.
    /// A frozen read view is a search, so it may be open while this change builds; it holds
    /// that side of the barrier for as long as it lives, and so sees one layout from its
    /// first step to its last.
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
        // disagreement.
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

/// An operation that is not a search, running under the layout lock held shared: no layout
/// change starts or finishes while this lives, so its layout is the published one throughout,
/// and so is everything that changes with a layout (the logical-id directory, the repair
/// queue, the control state, the files on disk).
pub(in crate::cluster::coordinator) struct Stable<'a> {
    pub(in crate::cluster::coordinator) layout: Arc<Layout>,
    _held: std::sync::RwLockReadGuard<'a, ()>,
}

/// A mutation: [`Stable`], and the mutation barrier held shared.
pub(in crate::cluster::coordinator) struct Admitted<'a> {
    pub(in crate::cluster::coordinator) layout: Arc<Layout>,
    _barrier: std::sync::RwLockReadGuard<'a, ()>,
    _held: std::sync::RwLockReadGuard<'a, ()>,
}

impl super::ClusterEngine {
    /// The layout for a **search**: an operation that only reads shard data, through this
    /// one layout, and may therefore run while the layout is being changed. Load it once, at
    /// the operation's entry, and hand it down: a change may publish another at any moment,
    /// and a search that routed by one layout and matched in another would be wrong.
    ///
    /// Everything that is not a search uses [`Self::stable`] or [`Self::admit_mutation`]. The
    /// rule test (`tests/layout_discipline.rs`) lists the files that may call this.
    pub(in crate::cluster::coordinator) fn layout(&self) -> Arc<Layout> {
        self.layout.load_full()
    }

    /// Run an operation that is not a search: hold the layout lock shared and load the
    /// layout under it. A layout change holds that lock alone, so the two never overlap, as
    /// when a change needed the engine to itself. Take it once, at the operation's entry, and
    /// before any other lock: a second shared hold under the first would wait behind a change
    /// that is waiting for the first.
    pub(in crate::cluster::coordinator) fn stable(&self) -> Stable<'_> {
        let held = self
            .layout_lock
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Stable {
            layout: self.layout.load_full(),
            _held: held,
        }
    }

    /// [`Self::stable`] for an operation that has a deadline or can be cancelled: `keep_waiting`
    /// is asked after each attempt that found the lock taken, and its error ends the wait. Without it such an operation
    /// would sit out a whole rebuild after its caller had given up.
    pub(in crate::cluster::coordinator) fn stable_while(
        &self,
        mut keep_waiting: impl FnMut() -> Result<(), ShardError>,
    ) -> Result<Stable<'_>, ShardError> {
        const POLL: std::time::Duration = std::time::Duration::from_millis(5);
        loop {
            // Try first: a deadline that has already passed still gets the lock when it is
            // free, as a zero timeout means "do not wait", not "do not start".
            match self.layout_lock.try_read() {
                Ok(held) => {
                    return Ok(Stable {
                        layout: self.layout.load_full(),
                        _held: held,
                    })
                }
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    return Ok(Stable {
                        layout: self.layout.load_full(),
                        _held: poisoned.into_inner(),
                    })
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
            keep_waiting()?;
            std::thread::sleep(POLL);
        }
    }

    /// [`Self::stable`] that gives up at `deadline`.
    #[cfg(feature = "distributed")]
    pub(in crate::cluster::coordinator) fn stable_by(
        &self,
        deadline: std::time::Instant,
    ) -> Option<Stable<'_>> {
        self.stable_while(|| {
            if std::time::Instant::now() >= deadline {
                Err(ShardError::DeadlineExceeded)
            } else {
                Ok(())
            }
        })
        .ok()
    }

    /// Admit one mutation: the layout lock shared, then the mutation barrier shared, then the
    /// layout. The mutation holds both until it has applied.
    pub(in crate::cluster::coordinator) fn admit_mutation(&self) -> Admitted<'_> {
        let held = self
            .layout_lock
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        self.pause_admission();
        Admitted {
            layout: self.layout.load_full(),
            _barrier: barrier,
            _held: held,
        }
    }

    /// Whether `layout` is the one published now.
    pub(in crate::cluster::coordinator) fn is_published(&self, layout: &Layout) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.layout.load()), layout)
    }

    /// Begin a change of the layout. It waits for every operation that is not a search to
    /// finish and keeps new ones out, so the corpus is still and the layout published now
    /// stays published until this change replaces it. A change that fails before it publishes
    /// leaves that layout serving.
    ///
    /// It refuses, without waiting, while a remote resize is copying the corpus: that copy
    /// holds the layout lock shared for its whole run, and a change queued behind it would
    /// hold every other operation back until the copy was done. Checking for that copy and
    /// asking for the lock are one step ([`Self::layout_admission`]), so a copy cannot start
    /// in between.
    pub(in crate::cluster::coordinator) fn begin_layout_change(
        &self,
    ) -> Result<LayoutChange<'_>, ShardError> {
        let _admission = self.layout_admission();
        self.ensure_resize_write_fence_open()?;
        Ok(self.begin_cutover())
    }

    /// Held by a layout change from its check for a remote copy until it has the layout
    /// lock, and by a remote resize from before it takes the layout lock shared until its
    /// write fence is up. Either the change sees the fence and is refused, or it has the
    /// lock before the copy can start and the copy waits for it.
    pub(in crate::cluster::coordinator) fn layout_admission(
        &self,
    ) -> std::sync::MutexGuard<'_, ()> {
        self.layout_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The layout lock alone, for the cutover of a remote resize, whose own fence is up.
    pub(in crate::cluster::coordinator) fn begin_cutover(&self) -> LayoutChange<'_> {
        LayoutChange {
            _alone: self
                .layout_lock
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            engine: self,
        }
    }

    /// Swap in `next` and remember the layout it replaces until its last holder lets go.
    pub(in crate::cluster::coordinator) fn publish_layout(&self, next: Layout) -> Arc<Layout> {
        let next = Arc::new(next);
        let previous = self.layout.swap(Arc::clone(&next));
        let mut retired = self
            .retired_layouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An in-memory engine never runs the cleanup that would drop the ones already gone.
        retired.retain(|layout| layout.strong_count() > 0);
        retired.push(Arc::downgrade(&previous));
        next
    }

    /// Whether every layout that was replaced has been released by the searches that held
    /// it. Until then its files stay: a search still running on it may open one.
    pub(in crate::cluster::coordinator) fn retired_layouts_released(&self) -> bool {
        let mut retired = self
            .retired_layouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retired.retain(|layout| layout.strong_count() > 0);
        retired.is_empty()
    }

    /// Give the searches still running on a replaced layout a moment to finish, so that the
    /// checkpoint that follows can remove its files. The caller holds no handle on it.
    pub(in crate::cluster::coordinator) fn await_retired_layouts(&self) {
        let deadline = std::time::Instant::now() + RETIRED_LAYOUT_GRACE;
        while !self.retired_layouts_released() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
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
