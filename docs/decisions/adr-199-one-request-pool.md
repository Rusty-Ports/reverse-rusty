# ADR-199 — One request pool for the whole server, with a bounded share for document writes

> [Engine quality & operations decisions](areas/engine-quality-and-operations.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Both HTTP servers ended their router with `tower::limit::ConcurrencyLimitLayer::new(256)`,
applied through `Router::layer`. ADR-062, ADR-099, ADR-144, the threat model and a code comment
describe the result as one server-wide cap: "the 256 in-flight slots that protect the engine".

It was never one cap. `Router::layer` gives every route, and every method of a route, its own
clone of the layer, and each clone of that layer makes its own semaphore. The limit was 256 per
endpoint. The single-node server has 32 routes and the coordinator 47, and seven of them accept
100 MB bodies, so about 1,800 heavy requests could be in flight where the record says 256.

Nothing tested the cap. Both routers were built inline in the startup code, so the server's
tests assembled stub routers that left the limiter out.

Making the layer share one semaphore is a one-line change, and on its own it would have
introduced a new failure. Document writes (`PUT`/`DELETE /_doc/{id}`, `/_bulk`, `/_flush`) run
one at a time behind the engine mutex or the coordinator's write serializer, and a compaction,
backup or vocabulary rebuild holds that lock for as long as it takes. Every write that arrives
meanwhile waits in flight (ADR-183, ADR-191). With one undivided pool, a writer fleet of a few
hundred connections would hold every slot for the length of the maintenance, and searches,
which need no lock, would wait behind them. Separate pools per route had hidden that.

## Decision

1. **One admission function, on every route, in both modes.** `router::admission::admit` is a
   middleware around the whole router. It decides from the method and path what a request
   needs. A route cannot be outside the pool because of where in the builder it was added, and
   a path no rule names is counted like any other request.
2. **One pool of 256 request slots** (`MAX_IN_FLIGHT_REQUESTS`). A request holds its slot until
   its response head is ready. A streamed response body is sent after the slot is free.
3. **Document writes may hold a quarter of the pool** (64 slots). A write takes a slot of the
   write share first and a request slot second, so a write that waits for the share holds no
   request slot. The class is the set of requests that queue on `admit_write` and
   `admit_cluster_write`: `/_bulk`, `/_flush`, and `/_doc/{id}` with any method but `GET` and
   `HEAD`.
4. **The probes are outside the pool.** `/_health` takes nothing: it bounds itself (eight at
   once, then 429, ADR-144) and an orchestrator must get an answer from a server whose every
   slot is taken. `/_metrics` takes a slot of its own pool of eight, so a full server can still
   be observed.
5. **A request that finds no slot waits.** It is not refused. None of its body has been read
   and none of its work has started, and it is dropped if its client goes away.
6. **Auth stays outside admission** (ADR-062), and the request id outside both.
7. **The routers are built by functions** (`router::build_router`,
   `cluster_mode::router::build_cluster_router`) that take the pool size, so tests drive the
   stack the server serves.

## Alternatives considered

- **`GlobalConcurrencyLimitLayer` through `Router::layer`, with the probe routes added after
  it.** The smallest change, and the first one built here. A route's membership then depends
  on its position in a 200-line builder, and one undivided pool has the write problem above.
- **Keep a pool per route and correct the documents.** Each route stays bounded, but no
  server-wide number exists, and the total grows with every route added.
- **Separate pools for reads and writes.** It isolates both ways. A read flood that fills the
  pool is an overloaded server, where waiting in arrival order is the fair answer for writes
  too. Writes parked behind maintenance happen on a healthy server, so that is the direction
  that needs a bound, and one pool keeps the documented total true.
- **Refuse at saturation** (503 or 429) instead of waiting. A waiting request costs a
  connection and a parsed head, and its client's own timeout decides how long it waits.
  Refusing would change the answer every client sees in a burst. It can be added behind a flag
  if an operator needs it.
- **A `--max-in-flight-requests` flag.** Not added: the constant is one line, and there is no
  guidance yet for sizing it against the match pool and the body limit.
- **A byte budget for in-flight request bodies.** That is the bound on memory; a count is not.
  It is a larger change and is left as follow-up work.

## Consequences

- **Behaviour change.** At most 256 requests are in flight across all routes, where each route
  had 256 of its own. A server that relied on more than that in total now makes the excess
  wait.
- More than 64 document writes at once wait at admission. They ran one at a time behind the
  lock before, with at most 32 on blocking threads, so write throughput is unchanged.
- A read flood that fills the pool delays writes and administrative requests until slots free.
- `/_metrics` answers at most eight scrapes at once, where it had 256 slots of its own.
- The pool bounds the number of requests, not their memory: 256 bodies of up to 100 MB each is
  still the worst case. The number of waiting requests is not bounded either; each is one
  connection.
- ADR-062, ADR-099, ADR-144 and ADR-052 described a server-wide limit that did not exist until
  this change. Each carries a dated note.

## Proven

- `bin/server/router/tests.rs` and `bin/server/handlers/cluster/tests/request_limit.rs`, on the
  real routers, with requests held in flight by a body the test withholds: two requests on two
  routes fill a pool of two and a request on a third route waits until one finishes (it ran at
  once with a pool per route); `/_health` and `/_metrics` answer while every slot is taken; a
  request without the token is refused at once while every slot is taken; with one write slot
  in a pool of four, five more writes wait and three searches are all admitted.
- `bin/server/router/admission.rs`: the write class is exactly the document writes, also
  `GET /_flush`; a document read, a search, an administrative request and an unknown path take
  a request slot only; the probes take none; the write share is a quarter and at least one.

**See also:** ADR-062 (auth outside admission), ADR-099 (the match-pool bound), ADR-144
(`/_health` admission), ADR-183 and ADR-191 (writes wait on a semaphore, off the runtime).
