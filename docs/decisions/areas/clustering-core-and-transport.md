# Clustering — core & transport decisions

> [Architecture decision hub](../../DECISIONS.md)

The multi-shard correctness core, remote shard seam, shared feature space, and durable shard topology.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [027](../adr-027-in-process-multi-shard-core.md) | In-process multi-shard core | Uses one frozen dictionary, anchor sharding, and content routing to preserve lossless retrieval across K shards. | Accepted |
| [029](../adr-029-grpc-shardserver-shard-seam.md) | gRPC `ShardServer` + local↔remote `trait Shard` | Gives local and remote shards one fallible interface, with tonic transport behind the optional `distributed` feature. | Accepted |
| [030](../adr-030-dict-fingerprint-handshake.md) | Dict-fingerprint handshake + fallible construction | Rejects mismatched feature dictionaries at connect time and makes cluster construction fail loudly. | Accepted |
| [031](../adr-031-externalized-coordinator-log.md) | Externalized coordinator log (`trait ClusterLog`) | Records ordered mutations in a CRC-framed coordinator log so the cluster can rebuild after restart. | Accepted |
| [032](../adr-032-per-shard-durable-segments.md) | Per-shard durable compiled segments | Stores compiled segments per shard and reopens them by attach-and-mmap under one coordinator commit. | Accepted |
| [033](../adr-033-shared-nothing-storage.md) | Shared-nothing cluster storage | Chooses local segments, peer recovery, and Raft instead of a shared object store. | Accepted |
| [034](../adr-034-cross-process-dict-shipping.md) | Cross-process dict shipping over gRPC | Ships the frozen dictionary during adoption so a data node need not rebuild the corpus. | Accepted |
| [176](../adr-176-remote-logical-id-directory.md) | Remote logical-ID directory | Reconstructs create-only admission from bounded shard snapshots while keeping convergence evidence separate. | Accepted |
| [177](../adr-177-per-id-cluster-write-locks.md) | Per-ID cluster write locks | Removes stripe collisions while bounding lock storage to active callers and preserving log/apply and bulk exclusion. | Accepted |
| [183](../adr-183-cluster-rpc-runtime-isolation.md) | Cluster RPC runtime isolation | Drives cluster RPCs on a dedicated runtime and moves write-handler lock waits to blocking threads, so concurrent writes can no longer deadlock a remote coordinator. | Accepted |
| [197](../adr-197-checkpoint-excludes-mutations.md) | Checkpoint excludes mutations | Makes the public cluster `checkpoint`, `flush` and `backup_to` take the exclusive side of the mutation barrier, so a write in flight can neither be truncated out of the log before it reaches its shard nor land in both a committed segment and the log tail. | Accepted |
| [206](../adr-206-coordinator-writes-share-admission.md) | Coordinator writes share admission | Turns the server's one write mutex into a reader-writer admission lock: writes and searches that return sources share it, so writes run beside each other, and whole-cluster operations take it alone. | Accepted |
| [207](../adr-207-cluster-writers-wait-outside-the-search-pool.md) | A cluster writer waits outside the search pool | Gives the coordinator's search pool a gate: a request holds it shared while its work is in the pool, and a vocabulary change or resize holds it alone before it asks for the cluster's write lock, so no pool worker waits for a writer that is waiting for the pool. | Superseded by 210 |
| [208](../adr-208-the-coordinator-serves-from-a-published-layout.md) | The coordinator serves from a published layout | Moves the normalizer, dictionary, ring, shards and placement generation into one immutable layout that is published atomically; every operation loads it once and a rebuild publishes a new one. First step of serving through a rebuild. | Accepted |
| [209](../adr-209-only-a-search-runs-beside-a-layout-change.md) | Only a search runs beside a layout change | Rebuilds take shared access to the engine: a layout change holds a layout lock alone, every operation that is not a search holds it shared, and the searches that skip it are an allow-list a test enforces. A replaced layout's files stay until no search runs on it. | Accepted |
| [210](../adr-210-the-coordinator-serves-beside-a-rebuild.md) | The coordinator serves beside a rebuild | Removes the server's lock around the engine and the search pool's gate: a search and a read answer during a vocabulary change or resize, a rebuild holds the topology guard and write admission alone, and administrative changes get their own admission slot so health and metrics are not queued behind one. Supersedes 207. | Accepted |
| [224](../adr-224-a-write-is-under-way-on-every-shard-of-a-step-at-once.md) | A write is under way on every shard of a step at once | `Shard::start_write` lets a remote shard send a request and return; the coordinator starts each step of a mutation on all its shards and then reads the answers in order. A write to remote shards costs a round trip for each step, not for each shard; the order between steps, the error reported and in-process clusters are unchanged. |
| [225](../adr-225-what-a-rebuild-costs-and-who-frees-the-layout-it-replaced.md) | What a rebuild costs, and who frees the layout it replaced | A rebuild keeps the time of each of its parts (`ClusterEngine::last_rebuild`) and `clusterbench rebuild` measures one on a serving coordinator: about five seconds for a million queries on the reference machine, searches not slowed, writes waiting for all of it, 1.5 to 2.5 times the resident memory. A replaced layout is freed on a thread of its own, so a search is never the one that frees a corpus (the longest search beside a 2M rebuild went from 205–293 ms to 18–84 ms). |
| [185](../adr-185-reader-atomic-cluster-upsert.md) | Reader-atomic cluster upsert | Replaces a query atomically on each shard and fences only placement-moving upserts, so an unfenced reader never sees neither version or both. | Accepted |
| [192](../adr-192-shard-local-engine-settings.md) | Shard-local engine settings | Gives `shardserver` its own durability and storage flags, makes a remote coordinator refuse or warn about the ones it cannot honour, and marks them in `/_settings`. | Accepted |
| [193](../adr-193-inbound-request-size.md) | Inbound request size | Sends a bulk bucket as one staged load of bounded messages and makes the shard node's inbound limit a named setting, so bootstrap and dictionary shipping no longer stop at tonic's 4 MiB default. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
