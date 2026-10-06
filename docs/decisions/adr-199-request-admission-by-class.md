# ADR-199 — Request admission by class: one function, disjoint bounded pools

> [Engine quality & operations decisions](areas/engine-quality-and-operations.md) ·
> [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Problem

Both HTTP servers ended their router with `tower::limit::ConcurrencyLimitLayer::new(256)`,
applied through `Router::layer`. ADR-062, ADR-099, ADR-144, the threat model and a code comment
describe the result as one server-wide cap: "the 256 in-flight slots that protect the engine".

It was never one cap. `Router::layer` gives every route, and every method of a route, its own
clone of the layer, and each clone of that layer makes its own semaphore. The limit was 256 per
endpoint. The single-node server has 32 routes and the coordinator 47, so the number of requests
in flight was bounded only at several thousand, where the record says 256.

Nothing tested the cap. Both routers were built inline in the startup code, so the server's
tests assembled stub routers that left the limiter out.

The obvious repair, one semaphore shared by every route, is a one-line change. It is also
wrong, because some requests wait in flight for something another request must do:

- **Document writes** (`PUT`/`DELETE /_doc/{id}`, `/_bulk`, `/_flush`) run one at a time behind
  the engine mutex or the coordinator's write serializer. A compaction, backup or vocabulary
  rebuild holds that lock for as long as it takes, and every write that arrives meanwhile waits
  (ADR-183, ADR-191). A writer fleet of a few hundred connections would hold every slot of a
  shared pool for the length of the maintenance, and searches, which need no lock, would wait
  behind them.
- **A job-status read** (`GET /_percolate/jobs/{id}`) may long-poll for the job's completion,
  which is published only once the job's stream has been read. In a shared pool full of status
  polls, the stream request that would let them finish cannot be admitted, and each poll runs
  to its timeout.

The per-route pools had hidden both. A shared pool is safe only if every such dependency is
found and given a carve-out, and the next one added to the API would be a silent hazard.

## Decision

1. **One admission function, on every route, in both modes.** `router::admission::admit` is a
   middleware around the whole router. It decides from the method and path which class a
   request belongs to. A route cannot be unbounded because of where in the builder it was
   added, and a path no rule names lands in the class for everything else.
2. **Each class has its own pool, and no slot is shared between classes.** A class whose
   requests are waiting can fill its own pool and nothing else:

   | Class | Requests | Slots |
   |---|---|---|
   | Read | search and percolate (`/_search`, `/_mpercolate`, both `/v2` forms), `/v2/_pit`, `GET`/`HEAD /_doc/{id}`, `GET /`, starting a job and reading a job's stream | 256 |
   | Write | `/_bulk`, `/_flush`, `/_doc/{id}` with any other method | 64 |
   | Job status | `GET`/`HEAD /_percolate/jobs/{id}` | 64 |
   | Other | everything else: statistics, vocabulary, settings, backup, compaction, cluster operations, cancelling a job, unknown paths | 64 |
   | Scrape | `/_metrics` | 8 |
   | Probe | `/_health` | none: it admits eight itself and answers 429 beyond that (ADR-144) |

   A server therefore works on at most 456 requests at once, and at most 256 of them match.
3. **A request that finds no slot waits.** It is not refused. None of its body has been read
   and none of its work has started, and it is dropped if its client goes away. A request holds
   its slot until its response head is ready; a streamed body is sent after the slot is free.
4. **Auth stays outside admission** (ADR-062), and the request id outside both.
5. **The routers are built by functions** (`router::build_router`,
   `cluster_mode::router::build_cluster_router`) that take the pools, so tests drive the stack
   the server serves.

## Alternatives considered

- **One pool for every route** (`GlobalConcurrencyLimitLayer`, with the probe routes outside
  it). This is what the record described, and the first thing built here. It has the two
  failures above. A second version gave document writes a bounded share of the one pool; review
  then found the job-status case, which that version did not cover. Two dependencies found in
  one day, by two readers, is evidence that the list is not complete.
- **Keep a pool per route and correct the documents.** It isolates every route from every
  other, which is safe, but the bound is 256 times the number of endpoints and grows with each
  route added. Classes keep the isolation where requests depend on one another and give a
  number that means something.
- **Refuse at saturation** (503 or 429) instead of waiting. A waiting request costs a
  connection and a parsed head, and its client's own timeout decides how long it waits.
  Refusing would change the answer every client sees in a burst. It can be added behind a flag
  if an operator needs it.
- **Flags for the pool sizes.** Not added: there is no guidance yet for sizing them against the
  match pool and the body limit.
- **A byte budget for in-flight request bodies.** That is the bound on memory; a count is not.
  It is a larger change and is left as follow-up work.

## Consequences

- **Behaviour change.** A class of requests is now bounded across its routes: 256 matching and
  read requests in total, where each of those routes had 256 of its own; 64 document writes,
  64 job-status reads and 64 other requests, where each route had 256.
- More than 64 document writes at once wait at admission. They ran one at a time behind the
  lock before, with at most 32 on blocking threads, so write throughput is unchanged.
- A search flood does not delay writes or administrative requests, and writes parked behind
  maintenance do not delay searches. An operator can still act on a saturated server.
- `/_metrics` answers at most eight scrapes at once, where it had 256 slots of its own.
- A route added later is in the Other class until the classifier names it. That is the safe
  default; a new high-volume read route must be added to the Read class.
- The pools bound the number of requests, not their memory: a body may be up to 100 MB on the
  document, bulk and search routes. The number of waiting requests is not bounded either; each
  is one connection.
- ADR-062, ADR-099, ADR-144 and ADR-052 described a server-wide limit that did not exist. Each
  carries a dated note.

## Proven

- `bin/server/router/tests.rs` and `bin/server/handlers/cluster/tests/request_limit.rs`, on the
  real routers, with requests held in flight by a body the test withholds: two reads on two
  routes fill a read pool of two and a read on a third route waits until one finishes (it ran
  at once with a pool per route); `/_health` and `/_metrics` answer while the read pool is
  full; a request without the token is refused at once while the pool it would use is full;
  with one write slot, five more writes wait and four searches are all admitted; with the read
  pool full, a write and a statistics read are admitted.
- `bin/server/router/tests.rs`, with a real exhaustive job and one read slot: while a status
  read long-polls, the job's stream is admitted and read, and the poll then reports the job
  complete well inside its timeout.
- `bin/server/router/admission.rs`: the class of every kind of route, including `GET /_flush`
  (a write), a job's stream (a read), its status (job status) and its cancellation (other);
  and the pool sizes.

**See also:** ADR-062 (auth outside admission), ADR-099 (the match-pool bound), ADR-144
(`/_health` admission), ADR-183 and ADR-191 (writes wait on a semaphore, off the runtime),
ADR-131 (exhaustive jobs).
