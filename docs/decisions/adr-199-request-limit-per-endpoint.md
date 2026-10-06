# ADR-199 — The request limit is per endpoint, on purpose

> [Engine quality & operations decisions](areas/engine-quality-and-operations.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Both HTTP servers end their router with `tower::limit::ConcurrencyLimitLayer::new(256)`, applied
through `Router::layer`. ADR-062, ADR-099, ADR-144, the threat model and a code comment describe
the result as one server-wide cap: "the 256 in-flight slots that protect the engine".

It is not one cap. `Router::layer` gives every route, and every method of a route, its own clone
of the layer, and each clone of that layer makes its own semaphore. The limit is 256 per
endpoint. The single-node server has 32 routes and the coordinator 47.

So the record and the code disagreed, and nothing tested either reading: both routers were built
inline in the startup code, and the server's tests assembled stub routers without the limiter.

The obvious repair is to make the code match the record with one shared pool. That was built
here, twice, and each version was found to stall the server, because some requests wait in
flight for a request on another endpoint:

1. **Document writes wait for the engine lock.** A compaction, backup or vocabulary rebuild
   holds it for as long as it takes, and every `PUT`, `DELETE`, `/_bulk` and `/_flush` that
   arrives meanwhile waits (ADR-183, ADR-191). In a shared pool a few hundred writer connections
   hold every slot for the length of the maintenance, and searches, which need no lock, wait
   behind them.
2. **A job-status read waits for the job's stream.** `GET /_percolate/jobs/{id}` may long-poll
   for a completion that is published only once the stream has been read. In a pool full of
   status polls the stream request is never admitted, and each poll runs to its timeout.
3. **A coordinator job holds the write serializer until its stream is read.** Source-enriched
   searches wait for that lock. In a pool they share with the stream, they fill it and the
   stream that would release them is never admitted.
4. **Checkpoints and backups wait for the same lock,** and in a pool they share with
   `DELETE /_percolate/jobs/{id}`, the cancellation that would release it cannot run.

The first was found while building the shared pool and given a bounded share of it. Review found
the second. The pools were then split by class of request, and review found the third and the
fourth inside two of the classes. Each case is a request that can finish only after a request on
a different endpoint is admitted. A pool that two endpoints share is safe only if no such pair
exists between them, the API has several, and the next one added would be silent.

## Decision

1. **The limit stays per endpoint:** 256 requests in flight for each route and method. A request
   beyond its endpoint's limit waits. This is the behaviour the servers have always had.
2. **No pool is shared between endpoints.** A request is delayed only by requests on its own
   endpoint, so a set of waiting requests can never keep out the request they are waiting for.
   The code says so where the layer is applied, and names the layer that must not replace it.
3. **The record is corrected** instead of the code: this ADR, dated notes on ADR-052, ADR-062,
   ADR-099 and ADR-144, the threat model and the server reference.
4. **The routers are built by functions** (`router::build_router`,
   `cluster_mode::router::build_cluster_router`) that take the limit, so tests drive the layer
   stack the server serves, and the limit, its isolation and the position of auth are tested.

## Alternatives considered

- **One pool for every route.** What the record described. Cases 1 to 4 above.
- **One pool with bounded shares for the requests known to wait,** or **separate pools per class
  of request.** Both were built. Each depends on a complete list of which requests wait for
  which, and review extended the list both times.
- **Refuse at saturation instead of waiting.** In a shared pool this turns the stall into a
  refusal of the one request that would end it; the waiters still hold the pool.
- **A byte budget for in-flight request bodies.** This is the bound a count was standing in for:
  what protects the process is the memory its requests hold, not their number. It needs no pool
  shared between endpoints for handlers, and it is on the roadmap.

## Consequences

- No behaviour change.
- The number of requests in flight is bounded per endpoint, not per server: 256 times the number
  of endpoints at most. The documents now say so.
- Memory is not bounded by the limit. The document, bulk and search routes accept bodies of up
  to 100 MB each, and each of those endpoints admits 256.
- A new endpoint gets its own limit without anyone listing it.
- `main.rs` and `cluster_mode.rs` no longer hold the route tables.

## Proven

- `bin/server/router/tests.rs` and `bin/server/handlers/cluster/tests/request_limit.rs`, on the
  real routers, with requests held in flight by a body the test withholds: under a limit of one,
  a second search waits for the first; with the search endpoint full, another search route and
  a write are admitted, and `GET /`, `/_stats`, `/_health` and `/_metrics` answer; a request
  without the token is refused at once while its endpoint is full.
- `bin/server/router/tests.rs`, with a real exhaustive job and a limit of one: while a status
  read long-polls, the job's stream is admitted and read, and the poll reports the job complete
  well inside its timeout (case 2).

**See also:** ADR-062 (auth outside the limiter), ADR-099 (the match-pool bound), ADR-144
(`/_health` admission), ADR-183 and ADR-191 (writes wait on a semaphore, off the runtime),
ADR-131 (exhaustive jobs).
