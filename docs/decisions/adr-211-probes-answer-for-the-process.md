# ADR-211 — Probes answer for the process

> [Engine quality & operations decisions](areas/engine-quality-and-operations.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted · **Amends:**
> [ADR-144](adr-144-health-api-contract.md), [ADR-084](adr-084-kubernetes-helm-health.md)

## Problem

The Helm chart pointed the coordinator's liveness probe and its readiness probe at
`GET /_health`, with no `timeoutSeconds` and no startup probe.

`/_health` answers for the whole cluster. It calls every shard and the control plane, waits
for an admission slot that a stats scan can hold, and is red (503) when any of them does not
answer. That is what an operator wants from it. It is the wrong thing for the kubelet to act
on:

- **Liveness.** A shard that was down for a minute failed six probes, and the kubelet
  restarted the coordinator, which had nothing wrong with it. The restart fixed nothing, and
  the new process could not start, because assembling the cluster needs every shard. The
  kubelet's default probe timeout is one second, so a shard that was merely slow, or a slow
  administrative read holding the admission slot, could do the same.
- **Readiness.** After three failed probes the coordinator left the Service. The chart runs
  one coordinator, so one shard's outage became an outage of the whole API, although the
  coordinator could still have answered every request that did not need that shard.
- **Startup.** Neither the coordinator nor a shard serves its probe port until it has
  assembled or opened what it serves, and liveness was counting from the first second.
- **Termination.** A liveness kill inherits the pod's `terminationGracePeriodSeconds`, which
  for the coordinator is an hour (sized for a rebalance drain).

## Decision

1. **Two probe routes that answer for the process and nothing else**, in both modes:
   `GET`/`HEAD /_health/live` and `GET`/`HEAD /_health/ready`. They run on the main listener
   and the async runtime, take no permit, and read nothing from the engine or the network.
   `GET` answers `{"status":"alive"}` or `{"status":"ready"}`. They need no credentials,
   like `/_health`, because a kubelet cannot send any.
2. **`/_health` is unchanged** and stays the operator's status endpoint: strict, waitable,
   red when a shard or the control plane does not answer (ADR-144). Watch it and alert on
   it. The kubelet no longer acts on it.
3. **The chart's coordinator probes are values, with these defaults:**
   - liveness on `/_health/live`, with an explicit timeout and its own
     `terminationGracePeriodSeconds`;
   - readiness on `/_health/ready`. A deployment that wants the coordinator out of the
     Service whenever any shard is down sets `coordinator.probes.readiness.path` back to
     `/_health`;
   - a startup probe on the liveness route, whose budget covers assembly.
4. **Every probe in the chart sets `timeoutSeconds`**, and the shards have a startup probe
   whose budget covers opening a durable store.
5. **`deploy/check-probes.sh` renders the chart and fails** if the coordinator's liveness or
   startup probe points at `/_health`, if a probe has no timeout, if the coordinator or the
   shards have no startup probe, or if the coordinator's liveness or startup probe has no
   termination grace of its own. It runs in the `helm chart` CI job.

The two routes answer the same way today. The listener binds only once the engine or the
cluster is assembled, and a graceful shutdown closes it before anything else, so a process
that answers at all is both alive and ready. They are two routes because they are two
contracts: readiness may come to say more, and liveness must not.

## What changes for a deployment

- **With a shard down, the coordinator is not restarted and stays in the Service.** A request
  that needs the missing shard fails loudly, as it always has; a request that does not is
  served. Before, the whole API went away after about thirty seconds and the coordinator was
  restarted after about a minute.
- **`/_health` still goes red** for the missing shard. Anything that watches it sees the same
  thing as before.
- **A coordinator has five minutes to assemble** before the kubelet gives up on it
  (`coordinator.probes.startup`), and a shard ten minutes to open its store
  (`shard.probes.startup`). Liveness no longer has an `initialDelaySeconds`; the startup
  probe replaces it.
- **Not changed:** a coordinator still cannot *start* while a shard is unreachable, because
  assembly connects to every shard. This decision stops the kubelet from causing that
  restart; it does not make the restart survivable.

## Alternatives considered

- **Keep readiness on `/_health`.** It is what Kubernetes' documentation describes for a pod
  that "can only respond with error messages" when a backend is gone. A coordinator with one
  of N shards down is not that pod. None of the comparable routers surveyed ties readiness
  to every backend (see Prior art), and with one replica the cost is the whole API. It
  remains one value away.
- **A `tcpSocket` liveness probe**, with no new route. The kernel completes a TCP handshake
  into the listen backlog even when the process's runtime is stuck, so it detects only a
  dead process. An HTTP answer needs the runtime to turn.
- **Make liveness detect more** (a watchdog, a check of the engine). The sources recommend
  against it: every extra condition is another way to restart a process that a restart will
  not help.
- **One route for both probes.** Kubernetes' documentation allows it. Two routes cost
  nothing and keep the readiness contract free to change.
- **`/livez` and `/readyz`**, the Kubernetes API server's names. There is no single
  convention; the sub-paths keep the probes beside `/_health` in an API that is otherwise
  shaped like Elasticsearch's.

## Consequences

- A coordinator that is cut off from every shard still answers ready. With one replica that
  changes nothing; with several it would keep a useless replica in the Service. Readiness is
  the place to add that, without touching liveness.
- `/_health`'s shard probe still shares an admission slot with stats scans and vocabulary
  reads, so a slow one of those still makes `/_health` answer at its deadline. It no longer
  restarts anything.
- The repair queue of a remote coordinator is in memory (ADR-125), so a coordinator restart
  during a shard outage used to discard it at the moment it was filling. Not being restarted
  keeps it.

## Proven

- `handlers/cluster/tests/health.rs`: with a control plane that fails, `/_health` is 503 and
  both probe routes answer 200 with their bodies, uncached, and bodyless for `HEAD`; with
  the administrative slot held and every health permit taken, `/_health` waits and both
  probes answer within half a second; other methods are refused with `Allow: GET, HEAD`.
- `handlers/admin/probes_tests.rs`: the same in single-node mode.
- `auth.rs`: under `--auth-protect-reads`, `GET` and `HEAD` on both routes are open, other
  methods are not, and no other path under `/_health/` is.
- `deploy/local-smoke.sh` and `deploy/cluster-smoke.sh`, which run in CI against the built
  server, ask both routes on the server's own router in single-node, in-process cluster and
  remote coordinator modes.
- Six mutations of the routes each fail a test: a probe that takes a health permit; either
  route needing credentials under `--auth-protect-reads`; a probe that answers every method;
  and either router missing a probe route (the smoke scripts fail).
- `deploy/check-probes.sh` passes on the chart for four sets of values, and fails for each
  of six planted regressions: liveness on `/_health`; a shard probe and a control probe
  without a timeout; no coordinator startup probe; no shard startup probe; liveness without
  its own termination grace.
- `deploy/k8s-smoke.sh` has a fault leg, run on a `kind` cluster: one shard is taken away for
  100 seconds, longer than a liveness probe's whole failure budget. Both probe routes go on
  answering, `/_health` reports 503, the coordinator's restart count does not change and it
  stays Ready; when the shard comes back `/_health` returns to green and the query ingested
  earlier is still matched. Run on 2026-10-07 on `kind` v0.32 (Kubernetes 1.34): it passes.
  With the probes set back to `/_health` (`coordinator.probes.liveness.path` and
  `readiness.path`), the same run fails: within two minutes of the shard going away the
  coordinator had been restarted twice and was in `CrashLoopBackOff`.

## Prior art

- **Kubernetes** (*Liveness, Readiness, and Startup Probes*): "Liveness probes must be
  configured carefully to ensure that they truly indicate unrecoverable application failure,
  for example a deadlock", and "Incorrect implementation of liveness probes can lead to
  cascading failures." A startup probe should check "the same endpoint as the liveness
  probe". Defaults: `timeoutSeconds` 1, `periodSeconds` 10, `failureThreshold` 3. A
  probe-level `terminationGracePeriodSeconds` otherwise inherits the pod's
  ([concepts](https://kubernetes.io/docs/concepts/workloads/pods/probes/)).
- **Spring Boot**: the liveness probe "should not depend on health checks for external
  systems", and when every instance is unready a Service "does not accept any incoming
  connections"
  ([actuator endpoints](https://github.com/spring-projects/spring-boot/blob/main/documentation/spring-boot-docs/src/docs/antora/modules/reference/pages/actuator/endpoints.adoc)).
- **Routers do not tie readiness to every backend.** The Elasticsearch operator's readiness
  probe is a check that the node has joined a cluster, with no health colour, and its
  documentation says the probe is not influenced by the load on the cluster.
  Vitess's `vtgate` reports healthy whatever its tablets are doing. The Solr operator probes
  the node's own state. CockroachDB's `/health?ready=1` fails only when the node is
  draining, decommissioning or cannot reach a majority.
- **Names.** The Kubernetes API server uses `/livez` and `/readyz` (and deprecated
  `/healthz`); Spring Boot uses `/actuator/health/liveness` and `/readiness`; CockroachDB a
  query parameter. There is no one convention.

**See also:** ADR-062 (bearer-token auth and what stays open), ADR-084 (the chart and gRPC
health), ADR-125 (the in-memory repair queue), ADR-144 (the `/_health` contract), ADR-210
(a rebuild no longer makes `/_health` wait).
