# ADR-201 — `--include-broad` sets the default scope on every surface

> [Distributed v1 graduation decisions](areas/distributed-v1-graduation.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

A stored query in class C, or an accepted class D query, is matched only when the request's
scope includes the broad lane. A server started with `--include-broad` makes that the default,
and the operator documents, the CLI help and the Helm and Compose comments all describe the flag
as server-wide.

It applied to the compatibility routes only. `/_search` and `/_mpercolate` resolve an omitted
`include_broad` from the flag. `/v2/_search`, `/v2/_mpercolate` and exhaustive jobs take
`query_scope` from the request alone and fell back to `standard` (ADR-107 reserved that
default). An operator who turned broad on for the server and then moved a consumer to a v2 route
or to jobs, without adding `query_scope: "with_broad"` to every request, lost every class C and
accepted class D candidate from that consumer's results. Nothing failed: the response is a
valid, smaller candidate set, and the only sign is the echoed `query_scope`.

Reverse Rusty is the recall stage of a two-stage matcher. A candidate that is silently absent
is the one failure the system is built to prevent, and this one was caused by configuration.

## Decision

1. **An omitted scope is the server's default on every surface.** `AppState` and
   `ClusterAppState` answer `default_query_scope()` from `--include-broad`, and v2 search, v2
   batch and exhaustive jobs resolve an omitted `query_scope` through it, as the compatibility
   routes resolve an omitted `include_broad`. The failure metrics label the same resolved scope.
2. **A request that names a scope gets that scope,** in either direction: `standard` on a
   server whose default includes broad, `with_broad` on one whose default does not.
3. **The response says which scope ran,** as before: v2 responses and job status echo
   `query_scope`.

This amends the reserved v2 default in ADR-107 and ADR-108: it is `standard` unless the server
was started with `--include-broad`.

## Alternatives considered

- **Keep the fixed `standard` default and fix the documents,** with a startup warning that the
  flag does not reach v2 or jobs. It keeps ADR-107 intact and one request body meaning the same
  on every server. The trap then stays open, guarded by a log line an integrator does not read.
- **Reject a v2 or job request that names no scope on a server with the flag on.** Loud, but it
  breaks every existing v2 client of such a server to tell it something the server can resolve.
- **A second flag for the native default.** Two flags for one question, and the pair can
  disagree, which is the present problem with one flag.

## Consequences

- **Behaviour change, only for a server started with `--include-broad`:** v2 search, v2 batch
  and exhaustive jobs now include class C and accepted class D candidates when the request
  names no scope, and echo `with_broad`. Result sets grow accordingly. A server without the
  flag is unchanged.
- The same request body can mean different scopes on servers configured differently, as it
  already does on the compatibility routes. The echoed scope shows which one ran.
- The flag is fixed for the life of a process, so a point-in-time continuation and a retried job
  resolve an omitted scope to the same value. A job's `event_id` fingerprint hashes the resolved
  scope: a request without a scope and one naming the server's default are the same job.
- The compatibility responses still carry no scope, and the job completion frame does not
  repeat it. Adding both is on the roadmap ("Effective scope on every response").

## Proven

- `bin/server/handlers/search/tests/default_scope.rs`, on a corpus with broad-lane queries and
  a title they change the answer for: without a scope, v2 search and v2 batch return exactly the
  broad-inclusive set on a server with the flag and the standard set without it, and a job
  records the same scope; a named scope wins in both directions on all three; and the
  compatibility route and the v2 route return the same ids under either server default (they
  differed with the flag on).
- `bin/server/handlers/cluster/tests/default_scope.rs`: the coordinator's v2 search, v2 batch
  and jobs echo the server default without a scope and the named scope with one.

**See also:** ADR-107 (the result contract and its reserved defaults), ADR-108, ADR-073 (the
compatibility `include_broad` override), ADR-131 (exhaustive jobs).
