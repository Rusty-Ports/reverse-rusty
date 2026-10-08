# ADR-215 — A build does not build over what it finds

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A durable in-process cluster is created by `ClusterEngine::build`: it creates one directory
per shard, loads the corpus into them, and commits by writing `cluster_manifest.bin`. The
server chooses between building and opening by whether that manifest exists.

A build that stops before the manifest (a crash, an out-of-memory kill during a large first
load, a disk that fills) leaves shard directories with checkpoint files and segments, and no
manifest. The next start sees no cluster and builds again in the same directory.

`build` did not look at what the directory held, and it created each shard with the
constructor a shard node uses, which says: "if a checkpoint sidecar is already present in the
dir, this is a node restarting over its own prior data". That is right for a node. In a build
it restored the rows the unfinished attempt had left, and the corpus was then loaded a second
time on top of them. The build failed ("logical-id enumeration covers 1 of 2 live queries")
after it had written a manifest for the doubled state, and later starts opened that state
with create-only writes refused. Reproduced. On Kubernetes that is a crash loop followed by a
degraded cluster that stays so until the volume is wiped by hand.

The same code had a second reading that is worse. A directory with shard state and no
manifest may be a cluster that has **lost its manifest**. Building over it mixed the surviving
corpus with the seed. And `build` called on a directory that held a whole cluster restored
every shard, loaded the new corpus on top and replaced the manifest.

The in-process resize already knew the hazard for a new shard position, and cleans the
directory first ("its checkpoint sidecar would self-restart `new_durable` into an old
corpus").

## Decision

1. **A build says what it is doing.** Before it creates the first shard directory it writes a
   mark, `build.incomplete`, in the data directory, and the mark is on disk before anything
   else is. The mark is cleared once the build has committed.
2. **The mark is written only into a directory that holds nothing of a cluster**: no
   manifest, no cluster log, no file under an entry named like a shard directory. So shard
   state beside a mark was made by a build that did not finish, and nothing was ever served
   from it. (A shard directory that holds only directories holds no data. There is nothing
   in it to restore or to lose, and a build takes it as it takes an empty data directory.)
3. **A build that finds the mark and no manifest starts again.** It removes every shard
   directory (the earlier attempt may have had more shards than this one) and builds from the
   beginning. The corpus comes from the same place it came from the first time.
4. **Everything else a build can find is refused**, and a refusal changes nothing in the
   directory:
   - *a manifest*: the directory holds a cluster. A build does not replace one; it is opened.
   - *shard state or a cluster log, no manifest, no mark*: not left by a build that says what
     it is doing. With the cluster log there it was a working cluster and its manifest is
     gone. Without it, it is a first start that an earlier release did not finish, or a
     cluster that has lost its manifest and its log. The error says which, and what to do.
   - *the mark and a cluster log, no manifest*: the log is created after the manifest, so the
     marked build had committed and the manifest has since been lost.
5. **A manifest ends the mark.** A build that stops between its manifest and its last step
   leaves a cluster with a mark beside it. The start that follows opens the cluster, and the
   open clears the mark as soon as it has read the manifest. A mark that outlived an open
   would say that a cluster's shards may be thrown away.
6. **A build creates its shards, and never takes one up.** `LocalShard::create_durable`
   refuses a directory that holds a checkpoint file. The node's constructor, which restores,
   is not called by a build. After decisions 2 to 4 a build cannot meet such a directory;
   this turns a mistake there into an error instead of a doubled corpus.

## What changes for a deployment

- An in-process cluster whose first start is killed during the initial load starts again
  from the beginning at the next start, with nothing to clean up.
- A data directory that holds shard directories and no manifest, left by a first start under
  an earlier release, is refused at start with an error that says so. Empty the directory
  and start again: nothing was ever served from it. Before, the start failed in a different
  way and left a doubled corpus.
- A data directory whose manifest has been lost is refused instead of being built over.
  Restore it from a backup.
- A library caller that called `ClusterEngine::build` twice on one directory gets an error
  the second time. Open the cluster, or remove the directory.
- One new file in the data directory while a build runs, `build.incomplete`. No format
  change. An earlier release ignores the file.

## Alternatives considered

- **Build in a temporary directory and rename it into place**, as etcd creates its WAL
  directory. The data directory is usually a mount point, which cannot be renamed, and the
  shard directories are many: renaming them one by one needs a rule for a crash between two
  renames, which is the mark again under another name.
- **Remove whatever is in the directory when no manifest is found.** RocksDB and Lucene do
  this for files their commit record does not name. It cannot tell an unfinished build from a
  cluster that has lost its manifest, and the second is the one case where the shard
  directories are the only copy of the data.
- **Refuse every non-empty directory and leave the cleaning to the operator**, as
  PostgreSQL's `initdb` and MySQL's `--initialize` do. Their initialisation is run once by
  hand. This one runs at every start of a server that is given a seed file, under a
  supervisor that restarts it, so a refusal is a crash loop that needs a person; and the
  build can prove which leftovers are its own.
- **Have the failed build remove what it created.** `initdb` does, from an exit handler. It
  covers the failures a process observes and not the ones this is about (a kill), so the
  start that follows needs the rule either way. One rule is enough.
- **Clear the mark at the end of a successful open** instead of at its start. An open that is
  refused for another reason (a lost log, ADR-213) would leave the mark beside a manifest, and
  an operator who then removed the manifest would have the next build throw the shards away.

## Consequences

- A first start that cannot finish (the corpus does not fit in memory) still cannot finish;
  it now fails the same way each time and leaves nothing behind.
- The mark is a file in the data directory. Removing it by hand turns "an unfinished build"
  into "shard state with no manifest and no mark", which is refused: the safe direction.
  Creating one by hand beside a cluster's shard directories, after removing its manifest and
  its log, makes the next build remove them. That takes three deliberate steps.
- A remote cluster's first load is a bulk load into shard nodes and has its own mark
  (ADR-196). This decision is about the in-process cluster.

## Proven

- `cluster/coordinator/tests/interrupted_build.rs`: a build whose manifest cannot be written
  leaves shard state, no manifest and its mark; the next build, with fewer shards, stores
  exactly what a build in an empty directory stores, admits a new query, leaves no mark and
  no directory of the first attempt, and the cluster reopens (the second build failed
  before, with a doubled corpus). Shard state with no manifest and no mark is refused, with
  and without the cluster log, and the directory is unchanged. A build in a directory that
  holds a cluster is refused and the cluster is unchanged. An open clears the mark of a build
  that stopped after its manifest. A mark beside a cluster log is refused. A build that is
  refused leaves no mark, and one that is taken and then fails has left it. Shard directories
  that hold no file do not stop a build, and one file under one does. `build` does not call
  the constructor that restores a shard.
- `tests/cluster_durability_oracle/vocab_stale_sources.rs`: the existing test of a build
  whose source store cannot be written still reaches that write. Its fixture is a directory
  where each shard's `sources.dat` should go, which holds no file, and it now asserts the
  durability error itself. (Review found that the first form of this change refused the
  fixture as shard state, and that the test went on passing because its directory's name
  contained the word it looked for.)
- `cluster/shard/tests/recovery.rs`: creating a shard in a directory that holds one is
  refused; the node's constructor restores it.
- Mutation checks, each after an unmutated baseline: listed in the pull request.

## Prior art

How other systems treat a first initialisation that was interrupted, and a directory with
data and no commit record (sources read 2026-10-08).

- **RocksDB, etcd, Lucene: the commit is one rename, so an unfinished initialisation looks
  like an empty directory and is redone.** RocksDB makes `CURRENT` last ("Make "CURRENT" file
  that points to the new manifest file"), and with no `CURRENT` and `create_if_missing` it
  creates a new database and its obsolete-file scan removes the leftovers. etcd: "keep
  temporary wal directory so WAL initialization appears atomic", and a stale temporary
  directory is removed first. Lucene with no `segments_N` creates a fresh index.
- **PostgreSQL and MySQL: an explicit initialisation refuses a directory that is not
  empty.** `initdb`: "directory "%s" exists but is not empty"; the documentation: "this is to
  prevent accidentally overwriting an existing installation". On a failure it observes,
  `initdb` "removes any files it might have created"; MySQL tells the operator to ("The newly
  created data directory %s by --initialize is unusable. You can remove it."). Neither can do
  so after a kill.
- **Telling "never finished" from "lost its commit record" takes a witness.** With
  rename-as-commit the two look the same, and the systems that distinguish them use something
  beside the data files. ZooKeeper uses a one-shot `initialize` file that is consumed at the
  first start and means "an empty database is expected here"; without it, transaction logs
  with no snapshot are refused ("No snapshot found, but there are log entries. Something is
  broken!"). Elasticsearch uses the recovery source in the cluster state: an expected shard
  with no commit is an error ("should exist, but doesn't"), and an empty-store recovery that
  finds a commit removes it ("its a leftover (possibly dangling) … better to clean it than
  use same data"). RocksDB has a weak one: creating a new database is refused when write-ahead
  log files are present ("While creating a new Db, wal_dir contains existing log file").
- **Building over data with no commit record** is what RocksDB, Lucene and etcd do; the
  refusers are ZooKeeper, Elasticsearch, PostgreSQL and MySQL.

What is taken from them: the redo of the first group, which a supervised server needs; the
refusal of the second, for everything that is not provably an unfinished build; and
ZooKeeper's kind of witness to tell the two apart. The mark differs from ZooKeeper's file in
who writes it: the build itself, and only into a directory it has found empty of cluster
state, so it cannot be left over from anything else. The cluster-log check is RocksDB's
heuristic, used where it is exact (the log is created after the manifest).

**See also:** ADR-032 (the manifest as the cluster's commit), ADR-072 (why a durable shard
writes a checkpoint file at once), ADR-078 (the resize that cleans a new position's
directory), ADR-196 (the mark of an unfinished bulk load on a shard node), ADR-213 (the
manifest before the log, and what epoch 0 means).
