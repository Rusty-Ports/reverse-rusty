# `GET /_cluster/resize` — Resize operations and autoscaler state

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

These read-only routes report the resize operations this coordinator process has retained and the
latest observation of the opt-in governed resize loop (ADR-179). They read neither the cluster nor
the guards a rebuild holds, so they respond while one runs.

```bash
curl localhost:9200/_cluster/resize
curl localhost:9200/_cluster/resize/grow-to-16
```

## `GET /_cluster/resize`

```json
{
  "operations": [
    {
      "operation_id": "grow-to-16",
      "origin": "api",
      "num_shards": 16,
      "if_placement_generation": 4,
      "state": "succeeded",
      "accepted_at_ms": 1790000000000,
      "started_at_ms": 1790000000012,
      "finished_at_ms": 1790000004210,
      "outcome": {
        "old_num_shards": 8,
        "num_shards": 16,
        "rebuilt": 1200000,
        "version": 47,
        "placement_generation": 5
      }
    }
  ],
  "autoscaler": {
    "enabled": true,
    "last_observation": {
      "observed_at_ms": 1790000060000,
      "num_shards": 16,
      "placement_generation": 5,
      "recommended": 17,
      "max_selective_corpus": 910000,
      "verdict": "cooling_down",
      "target": 17,
      "remaining_ms": 840000
    }
  }
}
```

`operations` lists retained records newest first. The registry keeps the 64 most recent, evicting
only terminal records, and a process restart forgets them.

| Field | Meaning |
|---|---|
| `operation_id` | Caller-supplied or generated (`resize-…` for API requests, `autoscale-…` for the loop). |
| `origin` | `api` or `autoscaler`. |
| `num_shards` | Requested target shard count. |
| `if_placement_generation` | The precondition, when one was supplied. The loop always supplies its observed generation. |
| `state` | `queued` (waiting for admission or guards), `running` (the rebuild started and cannot be cancelled), `succeeded`, `failed`, or `not_started` (nothing started before the manager deadline). |
| `accepted_at_ms`, `started_at_ms`, `finished_at_ms` | Unix-epoch milliseconds; absent until the phase is reached. |
| `outcome` | Present on success: the attested transition, final control `version`, and `placement_generation`. |
| `error` | Present on failure or not-started: a sanitized `{type, reason}`. |
| `uncommitted_generation` | Present when a failed attempt swapped the serving layout but did not commit it. Retrying the same operation (same ID and parameters) may pass its precondition at this generation to finish the commit; durable writes stay paused until it does. |

`autoscaler.enabled` reflects `--autoscale-resize-interval-secs`. `last_observation` is absent until
the loop's first observation. Its `verdict` is one of:

- `idle` — no growth is recommended;
- `at_ceiling` — the layout already sits at the configured maximum;
- `deferred` — growth is recommended but has not persisted for the required observations;
- `cooling_down` — the cooldown since the last layout change has not elapsed;
- `ineffective` — the previous automatic resize did not relieve the hottest shard enough, so
  growth is held until the recommendation clears or the layout changes by other means;
- `accept` — this observation started an operation.

Verdict-specific fields (`target`, `observations`, `required`, `remaining_ms`,
`before_max_selective`, `after_max_selective`, `min_relief_percent`, `from`, `to`,
`if_placement_generation`) accompany the verdict.

## `GET /_cluster/resize/{operation_id}`

Returns one record in the shape above, or `404 resize_operation_not_found` when no retained record
has that ID.

## Strictness

Both routes accept no query parameters (`400 validation_error`), return `Cache-Control: no-store`,
and are observed under the `cluster_resize` metric label. The single-operation route accepts only
`GET` (`405` with `Allow: GET`). Operation execution is documented under
[`POST /_cluster/resize`](resize.md).

---

Back to the [REST API reference](../../api.md).
