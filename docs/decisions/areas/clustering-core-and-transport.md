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
| [207](../adr-207-cluster-writers-wait-outside-the-search-pool.md) | A cluster writer waits outside the search pool | Gives the coordinator's search pool a gate: a request holds it shared while its work is in the pool, and a vocabulary change or resize holds it alone before it asks for the cluster's write lock, so no pool worker waits for a writer that is waiting for the pool. | Accepted |
| [185](../adr-185-reader-atomic-cluster-upsert.md) | Reader-atomic cluster upsert | Replaces a query atomically on each shard and fences only placement-moving upserts, so an unfenced reader never sees neither version or both. | Accepted |
| [192](../adr-192-shard-local-engine-settings.md) | Shard-local engine settings | Gives `shardserver` its own durability and storage flags, makes a remote coordinator refuse or warn about the ones it cannot honour, and marks them in `/_settings`. | Accepted |
| [193](../adr-193-inbound-request-size.md) | Inbound request size | Sends a bulk bucket as one staged load of bounded messages and makes the shard node's inbound limit a named setting, so bootstrap and dictionary shipping no longer stop at tonic's 4 MiB default. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
