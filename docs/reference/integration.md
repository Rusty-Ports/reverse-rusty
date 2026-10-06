# Recall-first integration

> [Documentation hub](../README.md) · [REST API hub](api.md) · [Query DSL](dsl.md)

Reverse Rusty is built to be the first stage of a two-stage matcher: it returns every stored
query that *could* match a title, and a precise second stage narrows that set. A false positive
costs the second stage one check. A false negative is lost for good, because nothing downstream
can restore a candidate it never saw.

The engine keeps that promise for the candidates it is asked for. This page is about asking for
all of them. Three defaults can each leave candidates out of a response without any error, and
an integration that follows only the quickstart hits all three.

| What can go missing | Why | What to do |
|---|---|---|
| Broad-lane queries (class C, accepted class D) | They are matched only when the request's scope includes the broad lane, and the default scope does not | [Choose the scope](#1-choose-the-scope) |
| Hits beyond the page | A response is one page: 1000 hits on `/_search` and `/_mpercolate`, 100 on the v2 routes | [Get every candidate](#2-get-every-candidate) |
| A hit at a page boundary | Offset paging re-matches on every request, so a write between two requests can shift the pages | [Get every candidate](#2-get-every-candidate) |

## 1. Choose the scope

Every stored query has a cost class, decided when it is written
([matching design §4](../design/matching.md)):

| Class | What it is | Matched by default |
|---|---|---|
| A, B | a selective anchor exists | yes |
| H | hot anchor, when `--hot-anchor-threshold` is set | yes |
| C | only a very common term anchors it (`new`, `pro`) | **no** |
| D | only exclusions (`-refurbished`), stored only with `--accept-class-d` | **no** |

Classes C and D are the broad lane. A request sees them only if its scope includes it:

| Surface | Per request | When the request names nothing |
|---|---|---|
| `/_search`, `/_mpercolate` | `"include_broad": true` | the server's `--include-broad` |
| `/v2/_search`, `/v2/_mpercolate`, `/_percolate/jobs` | `"query_scope": "with_broad"` | the server's `--include-broad` ([ADR-201](../decisions/adr-201-server-default-scope-on-every-surface.md)) |
| the library | `match_title(.., include_broad)` | — |

**For a recall-first consumer: start the server with `--include-broad`, or send the scope on
every request.** Sending it on every request is the stronger of the two, because it does not
depend on how a server was started. The v2 routes and job status echo the scope that ran as
`query_scope`; check it once in an integration test.

What it costs: with the broad lane on, candidates per title grow with the corpus, because a
query anchored on a very common term is a candidate for every title that has the term. That is
second-stage work, not lost recall.

Which class a query lands in depends on the corpus, not only on its text: a term is "very
common" relative to the queries present when the top-64 mask was assigned, which happens once,
at the first bulk load ([ADR-188](../decisions/adr-188-mask-assigned-once.md)). The same query
can be class B on one deployment and class C on another. Do not assume a query is default-visible because it
looks selective; ask for the broad scope, or check.

To see what a deployment holds, `GET /_stats` reports `class_counts` (rows per class,
[stats reference](api/observability/stats.md)). A non-zero `c` or `d` means some stored queries
are matched only in the broad scope. A write does not report the class of the query it stored.

A negation-only query is rejected at write time unless the server runs with `--accept-class-d`;
the response says so. If the corpus has such queries, turn the setting on before loading it.

## 2. Get every candidate

A response is a page. Compare what came back with what matched:

- `/_search` and `/_mpercolate`: `hits.total` is the full match count, and `hits.hits` holds at
  most `size` of them (default 1000).
- `/v2/_search` and `/v2/_mpercolate`: `size` defaults to 100, and `hits.total` is
  `{"value", "relation"}`. `"relation": "gte"` means the count stopped at
  `track_total_hits_up_to` and more exist.

When `hits.total` is larger than the page, use one of these, in this order of preference:

1. **An exhaustive job**, [`POST /_percolate/jobs`](api/percolate/exhaustive-jobs.md). It
   delivers every match of one snapshot as a stream, ends with a completion record carrying the
   exact total and a checksum, and works on every topology including a remote coordinator. Treat
   the result as complete only after the completion record.
2. **A point in time with a cursor**, [`POST /v2/_pit`](api/percolate/pit.md) and
   `search_after` on `/v2/_search`. Pages come from one frozen snapshot, with no gaps and no
   repeats. Not available on a coordinator with remote shards (501).
3. **One request large enough to hold everything**: `/_search` with `size` at least `hits.total`.
   One request is one snapshot, so the set is consistent.

**Do not page `/_search` or `/_mpercolate` with `from` while the corpus is being written.** Each
request matches against the snapshot current at that moment. A query deleted or added between
two requests shifts every later hit by one position, so a hit at the boundary is returned twice
or not at all. With no writes in between, offset paging is exact.

## 3. Vocabulary and punctuation

Recall also depends on the title and the query being read the same way. The normalizer decides
what a token is: how punctuation splits or folds (`O'Brien` and `OBrien`), which words are
synonyms, which phrases are one feature. Those are configuration, set by the vocabulary a store
was created with, and they are covered in the [query DSL reference](dsl.md) and the
[percolator workload note](../research/percolator-workload.md), which also lists the settings
that reproduce an existing matcher's behaviour. Decide them before loading a corpus: a durable
store keeps the vocabulary it recorded ([ADR-184](../decisions/adr-184-recorded-feature-model.md)).

## Checklist

- [ ] The server runs with `--include-broad`, or every request names its scope.
- [ ] An integration test asserts the echoed `query_scope` on the v2 routes it uses.
- [ ] `--accept-class-d` is on if the corpus has negation-only queries.
- [ ] The consumer compares `hits.total` with the hits it received, on every response.
- [ ] Results larger than a page come from an exhaustive job, a point in time, or one request
      sized to the total, never from `from` paging during writes.
- [ ] The vocabulary and punctuation settings were chosen before the corpus was loaded.
