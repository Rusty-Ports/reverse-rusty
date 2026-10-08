//! ADR-214: a shard keeps the segment files it has replaced until the coordinator has
//! committed a manifest that no longer names them.

use super::*;

fn durable(tag: &str) -> (PathBuf, ClusterConfig) {
    let dir = scratch_dir(tag);
    let cfg = ClusterConfig {
        num_shards: 2,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    (dir, cfg)
}

/// A corpus with enough distinct terms that its rows are spread over the shards.
fn corpus() -> Vec<(u64, String)> {
    (1..=200u64)
        .map(|id| (id, format!("seedterm{id} seedword{}", id % 90)))
        .collect()
}

/// Delete most of what was built and write a little, so that a flush has a memtable to seal
/// and each shard's built segment is over its holes threshold: the flush then compacts, and
/// the compaction replaces the segment the manifest names.
fn delete_most_and_write_a_little(cluster: &ClusterEngine) {
    for id in 1..=150u64 {
        cluster.remove_query(id).expect("remove");
    }
    for id in 1000..1040u64 {
        cluster
            .add_query(id, &format!("liveterm{id} liveword{}", id % 30))
            .expect("write");
    }
}

fn matched(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut ids = cluster.percolate(title).expect("percolate");
    ids.sort_unstable();
    ids
}

/// The `.seg` files in each shard's directory.
fn segment_files(dir: &std::path::Path, shards: usize) -> Vec<Vec<String>> {
    (0..shards)
        .map(|shard| {
            let mut names: Vec<String> = std::fs::read_dir(shard_dir(dir, shard).join("segments"))
                .expect("a shard's segments")
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| {
                    std::path::Path::new(name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
                })
                .collect();
            names.sort();
            names
        })
        .collect()
}

/// The segment files the committed manifest names, per shard.
fn committed_files(dir: &std::path::Path) -> Vec<Vec<String>> {
    let mut registry = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
        .expect("read manifest")
        .segment_registry;
    for files in &mut registry {
        files.sort();
    }
    registry
}

fn every_committed_file_is_on_disk(dir: &std::path::Path, shards: usize) -> bool {
    let on_disk = segment_files(dir, shards);
    committed_files(dir)
        .iter()
        .zip(&on_disk)
        .all(|(named, held)| named.iter().all(|name| held.contains(name)))
}

fn open(dir: &std::path::Path, cfg: &ClusterConfig, what: &str) -> ClusterEngine {
    match ClusterEngine::open(dir.to_path_buf(), vocab(), Some(cfg)) {
        Ok(cluster) => cluster,
        Err(error) => panic!("{what}: {error:?}"),
    }
}

/// Deletions, a flush, and a kill before the next checkpoint. The deletions push each shard
/// over its holes threshold, so the flush compacts, and the compaction replaces the segment
/// the manifest names. The shard used to remove that file there and then; the manifest still
/// named it, and the cluster could not reopen ("attaching shard segments: No such file or
/// directory"). The file is kept, the cluster reopens, and the deletions hold from the log.
#[test]
fn a_kill_after_a_flush_that_compacted_leaves_a_cluster_that_reopens() {
    let (dir, cfg) = durable("kept_after_compaction");
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster");
    let committed = committed_files(&dir);
    delete_most_and_write_a_little(&cluster);
    cluster.flush().expect("flush");
    assert_ne!(
        segment_files(&dir, cfg.num_shards),
        committed,
        "precondition: the flush replaced a segment the manifest names"
    );
    assert!(
        every_committed_file_is_on_disk(&dir, cfg.num_shards),
        "a shard removed a segment file the committed manifest still names"
    );
    // A kill: no checkpoint and no shutdown.
    drop(cluster);

    let reopened = open(&dir, &cfg, "after the kill");
    assert!(
        matched(&reopened, "seedterm3 seedword3").is_empty(),
        "a deletion did not hold"
    );
    assert_eq!(matched(&reopened, "seedterm160 seedword70"), vec![160]);
    assert_eq!(
        matched(&reopened, "liveterm1005 liveword15"),
        vec![1005],
        "a write from before the kill"
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A checkpoint rewrites every segment that holds a deletion, and then writes the manifest.
/// If that write fails, the manifest that stays committed is the old one, and it names the
/// segments from before the rewrite. They were removed at the rewrite, and the cluster could
/// not reopen. They are kept until a manifest that no longer names them is committed.
#[test]
fn a_checkpoint_whose_manifest_cannot_be_written_leaves_a_cluster_that_reopens() {
    let (dir, cfg) = durable("kept_after_failed_checkpoint");
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster");
    cluster
        .remove_query(3)
        .expect("remove a query that is in a base segment");
    let blocker = dir
        .join(CLUSTER_MANIFEST_FILE)
        .with_extension("cmanifest.tmp");
    std::fs::create_dir_all(&blocker).expect("block the manifest write");
    let failed = cluster.checkpoint().is_err();
    std::fs::remove_dir_all(&blocker).expect("unblock");
    assert!(
        failed,
        "precondition: the checkpoint could not write its manifest"
    );
    assert!(
        every_committed_file_is_on_disk(&dir, cfg.num_shards),
        "a failed checkpoint removed a segment file the committed manifest still names"
    );
    drop(cluster);

    let reopened = open(&dir, &cfg, "after the failed checkpoint");
    assert!(matched(&reopened, "seedterm3 seedword3").is_empty());
    assert_eq!(matched(&reopened, "seedterm4 seedword4"), vec![4]);
    // The next checkpoint goes through, and from then on nothing names the old files.
    reopened.checkpoint().expect("checkpoint");
    assert_eq!(
        segment_files(&dir, cfg.num_shards),
        committed_files(&dir),
        "files that no manifest names were left behind"
    );
    drop(reopened);
    let again = open(&dir, &cfg, "after the checkpoint");
    assert!(matched(&again, "seedterm3 seedword3").is_empty());
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Keeping a replaced file is not keeping it for ever. Once the coordinator has committed a
/// manifest that names the replacements, it releases what the shards replaced, and each
/// shard's directory holds exactly the files the manifest names.
#[test]
fn replaced_files_are_removed_once_a_manifest_no_longer_names_them() {
    let (dir, cfg) = durable("released_after_commit");
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus()).expect("durable cluster");
    let built = committed_files(&dir);
    delete_most_and_write_a_little(&cluster);
    cluster.flush().expect("flush");
    let kept = segment_files(&dir, cfg.num_shards);
    assert!(
        kept.iter()
            .zip(&built)
            .any(|(held, named)| held.len() > named.len()),
        "precondition: a replaced file and its replacement are both on disk: {kept:?}"
    );

    cluster.checkpoint().expect("checkpoint");
    assert_ne!(
        committed_files(&dir),
        built,
        "the manifest names the replacements"
    );
    assert_eq!(
        segment_files(&dir, cfg.num_shards),
        committed_files(&dir),
        "the replaced files were not removed after the commit"
    );
    drop(cluster);
    let reopened = open(&dir, &cfg, "after the checkpoint");
    assert_eq!(matched(&reopened, "seedterm160 seedword70"), vec![160]);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

fn seg_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("segments"))
        .expect("segments")
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
        })
        .count()
}

/// A replica's files are in no manifest (it is rebuilt from its primary on reopen), and no
/// sweep looks at its directory. It keeps what it replaced like any shard of an in-process
/// cluster, and the coordinator's release after its commit is what removes it.
#[test]
fn a_replicas_replaced_files_are_released_with_the_commit() {
    let (dir, mut cfg) = durable("released_on_replicas");
    cfg.replication_factor = 2;
    let cluster = ClusterEngine::build(vocab(), &cfg, &corpus()).expect("replicated cluster");
    let replicas: Vec<PathBuf> = (0..cfg.num_shards)
        .map(|shard| replica_dir(&dir, shard, 1))
        .collect();
    let built: Vec<usize> = replicas.iter().map(|replica| seg_count(replica)).collect();
    delete_most_and_write_a_little(&cluster);
    cluster.flush().expect("flush");
    let kept: Vec<usize> = replicas.iter().map(|replica| seg_count(replica)).collect();
    assert!(
        kept.iter().zip(&built).any(|(now, was)| now > was),
        "precondition: a replica holds a replaced file and its replacement: {built:?} -> {kept:?}"
    );
    cluster.checkpoint().expect("checkpoint");
    let released: Vec<usize> = replicas.iter().map(|replica| seg_count(replica)).collect();
    assert!(
        released.iter().zip(&kept).all(|(now, was)| now <= was)
            && released.iter().zip(&kept).any(|(now, was)| now < was),
        "a replica's replaced files were not released with the commit: {kept:?} -> {released:?}"
    );
    assert_eq!(
        segment_files(&dir, cfg.num_shards),
        committed_files(&dir),
        "the primaries hold exactly what the manifest names"
    );
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}
