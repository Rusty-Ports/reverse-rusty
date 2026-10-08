# ADR-221 — Every step of a durable operation has a name, and a test fails each one

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A durable operation is a sequence of filesystem steps. A cluster checkpoint writes and renames
a source sidecar and a checkpoint record for every shard, writes segments, publishes the
manifest, rewrites the log and removes what was replaced: 70 steps for three shards. Whether
it is safe depends on what is true between the steps, and nothing in the repository could
stop at one.

So each window was covered only when someone thought of it. ADR-178's unopenable cluster (a
failed control update, then a write, then a crash) passed a green gate and was found by
analysis. Each such fix added a hook of its own: by now three thread-local switches, two
interleaving hooks and two `break_writes_for_test`, each failing one step for one test. The
real-process kill lane (ADR-088) kills at moments it does not record.

## Decision

1. **The functions that perform durable steps say so.** `fault::step(name, path)` is called
   before a create, a rename, an append, a truncate, a remove or a control proposal;
   `fault::sync(file, path)` and `fault::sync_dir_of(entry)` perform the two kinds of sync.
   In a build that ships these are the plain operations and an empty inline function.
2. **A test opens a scope on a directory.** `fault::Scope::open(dir)` records every step
   taken under `dir`, on whichever thread, and can plan one fault: the n-th step equal to a
   given one fails, once, without being performed. Scopes are found by path, so tests that
   run side by side do not see each other's faults, and worker threads are covered. Under a
   scope a sync is recorded and not performed: a test that never loses power cannot tell, and
   the matrix below would otherwise spend minutes in `fsync`.
3. **The steps are where durable I/O already funnels:** `durable_rename`, manifest
   publication, the segment and source-sidecar writers, the cluster log (append, checkpoint),
   a shard's checkpoint record, the publication of an empty log, tail repair, the
   coordinator's removals, the build mark, and the control proposal of a layout change.
4. **A matrix enumerates.** `cluster/coordinator/tests/crash_matrix.rs` takes each operation of
   a durable in-process cluster (add, upsert, remove, checkpoint, a resize up, a resize down,
   an alias import, a build), runs it once under a scope to learn its steps, and then for
   every step runs it again with that step failing and goes three ways: crash at once; keep
   serving, write, crash; retry, crash. A crash is the engine dropped with no checkpoint.
   What reopens must hold every acknowledged write and no acknowledged remove, take a write
   and a checkpoint, and reopen the same again. Nobody picks the windows: a step added to an
   operation is in the list the next time the test runs, and a test pins the set of step
   names so that one which disappears is noticed.
5. **Test builds of the crate only, for now.** The facility is compiled under `cfg(test)`.
   The stage that kills a process at a named step will put it behind a cargo feature for the
   tool that needs it, with a gate lane that builds it.

## What the first run found

Of the 210 checkpoint cases, 146 failed. A checkpoint seals each shard, which replaces the
shard's source sidecar in place, and commits the manifest only after every shard is sealed.
A process that dies between the first sidecar rename and the manifest rename leaves a sidecar
that already holds the writes of the log's tail beside committed segments that do not. The
next start enumerated each shard's ids before replaying the tail, found the two disagree,
took the shard for a partial store and left the coordinator's id directory unseeded. Reads
were right. Every insert-only add was refused until a checkpoint and a second restart.

After the tail is replayed the two describe the same queries again. The open now tries the
enumeration again there and seeds the directory. A store that is short of sources for another
reason still is, stays degraded and is reported, as before.

## What changes for a deployment

A cluster that is killed during a checkpoint takes creates again as soon as it restarts. It
used to refuse them, with a message about remote shards, until an operator checkpointed and
restarted once more. Nothing else changes; release builds contain none of the facility.

## Alternatives considered

- **The `fail` crate.** Its points are program points selected by name from a process-global
  registry; a name nobody reaches is a silent no-op, tests must be serialized, and nothing
  enumerates. The part that is needed here is fifty lines and has no dependency.
- **Thread-local switches, as before.** They do not reach the thread that seals a shard.
- **A complete filesystem trait.** It is what the lost-unsynced-data model below needs, and it
  is a change to about a hundred call sites. The funnels give most of the coverage now.
- **The matrix as a test outside the crate, built with the feature.** It costs the gate a
  second build of the library. Inside the crate it costs nothing and runs in seconds.
- **Process death at each step first.** That is the faithful crash and it needs a child
  process for every step. The errors-in-process matrix is the shape ADR-178 had (a step
  fails and the engine goes on serving), runs in the ordinary lane, and found a defect on its
  first run.

## Consequences

- What the matrix models is a step that fails and a process that goes on, and a crash as a
  drop. It does not tear a system call and it does not lose data that was written and not
  synced: every byte written before the fault is there on reopen. A missing `fsync` cannot be
  seen by it.
- A durable step that does not go through the three functions is invisible. Still outside
  them: the write-ahead log's own appends and reset, the control store, the shard node's
  retirement, drop, adoption and recovery files, backups, and the accepted-log-loss record.
- A new durable function has to call them. Review is what holds that for now; a gate lane
  that refuses a direct `rename` or `sync_all` in library code comes with the filesystem
  layer.
- The one-off rename switch of the cluster log is gone; its test uses a scope. The others
  go as their operations are brought in.

## Later stages

1. The single-node engine's operations (flush, both merges, backup, a vocabulary rebuild) in
   the same matrix.
2. Process death at a named step: `crashwriter` built with the facility behind a cargo
   feature, a step chosen by an environment variable, `abort` there, reopen in the parent. The same markers give the
   SIGKILL lane (ADR-088) a way to know where a kill landed.
3. A coordinator killed in the middle of a checkpoint or a resize, which ADR-088 deferred.
4. Lost unsynced data: remember each file's length at its last sync and each directory's
   entries since its last sync, and on a crash truncate and undo, as LevelDB's and RocksDB's
   fault-injection filesystems do. This is the stage that needs every write to go through one
   layer.
5. The shard node's sequences, and the sidecar as part of the commit instead of a file
   replaced before it.

## Proven

- **The matrix.** Eight operations and 437 steps: 355 on an existing cluster, each failed
  three ways, and 82 of a build. That is 1,147 cases in 17 seconds. All pass.
- **It fails without the fix:** with the second enumeration removed, 146 checkpoint cases
  fail.
- **It reproduces ADR-178:** with the commit fence disabled, 189 resize and alias-import
  cases fail, most with `PlacementDecisionMismatch` on reopen. The hand-written tests of that
  ADR are not what catches it.
- **Other mutants it catches:** the log truncated before the manifest is published; the
  cluster log left appending through its old handle after a failed rename (four cases, the
  one window); a shrink that removes shard directories before it commits; the sidecar sweep
  removing the committed generation; a build that does not start again over its own mark; the
  manifest recorded as committed before it is written. Three that it does not catch are not
  crash windows: a sweep while a reader holds a replaced layout, the in-memory epoch advanced
  early, and a control failure that the following attestation reports anyway.
- The whole library suite and the cluster durability and persistence suites pass with the
  steps in place.

## Prior art

Sources read 2026-10-08.

| System | A crash point is | Loses unsynced data | Knows a point was reached |
|---|---|---|---|
| TiKV `fail` | named program point, `FAILPOINTS="name=action"` | no | no: an unreached name is silent |
| etcd gofail | named point generated from comments | no | yes: a registered list and hit counts |
| PostgreSQL 17 injection points | named point with an attached callback | no | partly |
| SQLite | the N-th I/O operation, for every N until the workload completes cleanly | yes | by construction |
| RocksDB, LevelDB | named sync points and random kill points; a fault-injection filesystem | yes | not tracked |
| FoundationDB | chosen by a deterministic simulator | yes | probes |
| ALICE (OSDI 2014), CrashMonkey (OSDI 2018) | states derived from a recorded trace of system calls or blocks, bounded small workloads | yes | derived from the trace |

What is taken from them: put the points where durable I/O already funnels and give them
names; enumerate every one a workload reaches, as SQLite does, and keep the workload small,
as CrashMonkey does; address a point by its name, its path and its occurrence, because a
global count is not stable when shards work in parallel; make an unreached point fail the
test, which is the pitfall of `fail`; and keep a real kill lane beside it, as RocksDB does.
What is not yet taken is the loss of unsynced data, which ALICE found to be where
applications most often go wrong.

**See also:** ADR-088 (the real-process kill lane), ADR-178 (the commit fence the matrix
reproduces), ADR-197 (a checkpoint excludes mutations), ADR-215 (the build mark).
