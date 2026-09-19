use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteCall {
    Insert(u64),
    Delete(u64),
    Bulk,
}

pub(super) type WriteHook = Arc<dyn Fn(usize, WriteCall) -> Result<(), ShardError> + Send + Sync>;

pub(super) fn instrument(cluster: &mut ClusterEngine, hook: WriteHook) {
    let shard_count = cluster.shards.len();
    cluster.shards = std::mem::take(&mut cluster.shards)
        .into_iter()
        .enumerate()
        .zip(std::iter::repeat_n(hook, shard_count))
        .map(|((position, inner), hook)| {
            Box::new(ObservedShard {
                inner,
                position,
                hook,
            }) as Box<dyn Shard>
        })
        .collect();
}

pub(super) fn pause(gate: &FirstAppendGate) {
    *gate.entered.0.lock().expect("entered") = true;
    gate.entered.1.notify_all();
    let released = gate.release.0.lock().expect("release");
    let (released, _) = gate
        .release
        .1
        .wait_timeout_while(released, Duration::from_secs(10), |released| !*released)
        .expect("wait for release");
    assert!(*released, "test must release the paused shard operation");
}

struct ObservedShard {
    inner: Box<dyn Shard>,
    position: usize,
    hook: WriteHook,
}

impl Shard for ObservedShard {
    fn percolate_filtered(
        &self,
        title: &str,
        broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.inner.percolate_filtered(title, broad, pred)
    }

    fn percolate_filtered_owned(
        &self,
        title: &str,
        broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.inner
            .percolate_filtered_owned(title, broad, pred, context, position)
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.inner
            .percolate_filtered_ranked(title, broad, pred, spec)
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        self.inner.num_queries()
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.inner.class_counts()
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        (self.hook)(self.position, WriteCall::Bulk)?;
        self.inner.ingest_extracted(items)
    }

    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        (self.hook)(self.position, WriteCall::Insert(logical))?;
        self.inner
            .insert_extracted_with_tags(ex, logical, version, text, tags)
    }

    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        (self.hook)(self.position, WriteCall::Insert(logical))?;
        self.inner
            .insert_extracted_with_placement(ex, logical, version, text, tags, placement)
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        (self.hook)(self.position, WriteCall::Delete(logical))?;
        self.inner.delete_by_logical_id(logical)
    }

    fn flush(&self) -> Result<(), ShardError> {
        self.inner.flush()
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        self.inner.seal_for_checkpoint()
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.inner.segment_filenames()
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        self.inner.next_seg_id()
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        self.inner.translog_tail(from)
    }
}
