# `PUT /_vocab` — Replace vocabulary

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Replace the engine's vocabulary. Existing stored queries are **automatically recompiled** under the
new normalizer before the new snapshot is published, so the change takes effect immediately with
zero false negatives. Standalone mode performs the replacement and recompile under its engine writer
lock. Coordinator mode performs one blue/green re-placement under the cluster write lock and
checkpoints a durable cluster before success. Both return the same response shape;
`recompiled` reports how many live queries were rebuilt.

The request is strict and synchronous:

- only query-free `PUT` is accepted;
- a body and `Content-Type: application/json` (or an `application/*+json` vendor media type) are
  required;
- the complete `Vocab` object rejects malformed JSON and unknown top-level fields;
- body extraction is capped at 16 MiB and must complete within 5 seconds; and
- every response includes `Cache-Control: no-store` and uses the standard JSON error envelope.

The decoded request waits asynchronously for the server's single administrative-work slot. The
O(corpus) replacement then runs on a blocking worker, so lock acquisition and compilation do not
occupy a Tokio request worker. The operation has no cancellable execution timeout: after admission,
it runs to a terminal result and publishes a coherent snapshot even if the client disconnects.
Concurrent stats and vocabulary read/write operations serialize through the same bounded slot.

> **Durability:** on a successful response, the recompiled queries have committed like a flush,
> and the same manifest commit records the new vocabulary and its feature-model fingerprint
> (ADR-184). A restart restores it from the manifest on single-node and cluster alike, with or
> without `--vocab-file`: the file only seeds a new store, and a restart with a file that differs
> from the recorded vocabulary keeps the recorded one and logs a warning. If a standalone recompile
> becomes live but its durable commit
> fails, the process publishes that coherent live state but returns
> `503 persistence_unavailable` instead of `acknowledged: true`; the old manifest remains
> authoritative for restart. A durable coordinator checkpoint failure is likewise not
> acknowledged even though the blue/green state may already be live; inspect `GET /_vocab` before
> deciding how to recover. Until a retry or any successful checkpoint commits that live state, the
> coordinator returns `503 durability_unavailable` for adds and upserts so a crash cannot strand
> them behind the older manifest; reads and removes continue (ADR-178).

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms":[{"token":"pkg","canonical":"term:package","kind":"generic"}],"phrases":[],"equivalences":[],"punctuation":[],"number_context":[],"aliases":{"entries":[]}}'
```

```json
{
  "took": 37,
  "took_ms": 37.428,
  "acknowledged": true,
  "recompiled": 1280
}
```

`took` is the whole-route elapsed time in integer milliseconds and `took_ms` preserves fractional
milliseconds. Invalid transport or JSON is 400/408/413/415 as applicable, an invalid vocabulary is
400 `vocab_error`, closed admission or unhealthy/degraded persistence is 503, and a blocking-worker
or impossible incomplete rebuild failure is 500. A non-local coordinator still returns 400 because
the current remote shard protocol does not ship the replacement normalizer.

**Declaring equivalences (ADR-054).** The optional `equivalences` block is a list of groups of
surface forms treated as the same entity (e.g. `[["ns", "north star"], ["pkg", "package"]]`). Unlike
`synonyms` (which *collapse* a form to a canonical via the normalizer), equivalences are applied by
**expansion**: a query requiring one form is widened to an any-of over the group, so it matches a
title bearing any form. Expansion only grows a query's match set, so it is **false-negative-safe** —
a wrong/uncertain equivalence can only add bounded false positives, never drop a true match. Each form
should resolve to a single entity (glue a multi-token form as a phrase first); a form that doesn't is
skipped. Applying the change recompiles existing queries through the expansion. The phrase that
glues a form is a separate rule with its own effect: a declared collapse phrase makes a query that
spells the form out require the words adjacent. To relate a multi-word form without that, put the
group in the alias registry: a title that carries the form's words apart still matches (ADR-205).

**Declaring punctuation rules (ADR-058).** The optional `punctuation` block reclassifies how individual
characters are handled in byte-cleaning. Each rule is `{"ch": "<char>", "class": "<fold|split|keep|marker>"}`:
`fold` deletes the character so its neighbors **join** into one token (so `O'Brien`, `O-Brien`, and
`OBrien` all become `obrien` — closing a recall gap for punctuation-only spelling differences), `split`
makes it a word boundary, `keep` leaves it literally in place, and `marker` emits it as its own token. The
default — `.` is `keep`, `#`/`/` are `marker`, everything else is `split` — is reproduced exactly when the
block is omitted. The same table applies to both queries and
titles, so the lossless-cover contract is preserved under any configuration.

**Number-context words (ADR-069).** The optional `number_context` array lists tokens that demote an
immediately-following number to a generic term (`model 1995` → `term:1995`, never `year:1995`).
Omitted or empty means position-insensitive typing, so a four-digit year is `year:N` everywhere.
Like every vocabulary change, applying a custom list recompiles stored queries under the new typing;
the same list runs over queries and titles.
