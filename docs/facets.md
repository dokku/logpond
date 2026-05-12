# Facets

A **facet** is a field whose distinct values are summarized in the Search view's sidebar, with a count per value. The sidebar lets you see what dimensions exist in the data and one-click-filter by them. Click `level: error (1,013)` in the sidebar and the search bar fills in `level:error`, the result list re-runs, and you are looking at just the errors.

Logpond ships with four built-in facets and lets you declare any number of custom ones, either statically in config or interactively through the admin UI.

## Built-in facets

These four are always available and cannot be disabled:

| Facet     | Underlying field | Default cap |
| --------- | ---------------- | ----------- |
| `service` | `service`        | 100         |
| `level`   | `level`          | 10          |
| `host`    | `host`           | 50          |
| `source`  | `source`         | 50          |

The **cap** is the maximum number of distinct values the sidebar will show for that facet. Values are ordered by count descending. If the field has more distinct values than the cap, the sidebar shows the top N and a "+ N more" affordance. You can change the caps in config (`facets.builtin.<name>.cap`); the fields themselves and their display labels are not configurable for built-ins.

## Custom facets

To add a facet on `attributes.user.id` or any other field, declare it as a custom facet. There are two ways.

### In config (YAML or JSON)

```yaml
facets:
  custom:
    - name: user_id
      field: attributes.user.id
      display_label: User ID
      cardinality_cap: 25
      value_type: string
```

Or with the JSON env var:

```bash
LOGPOND_FACETS_JSON='[{"name":"user_id","field":"attributes.user.id","display_label":"User ID","cardinality_cap":25}]'
```

The fields:

- **`name`** is the unique identifier. It is what the sidebar section's heading uses internally and what `GET /api/facets` returns. It is not part of the search syntax - you still filter on the underlying field path with `@`.
- **`field`** is the dotted JSON path. Core columns (`service`, `level`, `host`, `source`, `message`) and `attributes.<...>` paths are both valid.
- **`display_label`** is what the sidebar header shows. Defaults to a title-cased version of `name`.
- **`cardinality_cap`** caps how many distinct values appear. Defaults to `50` (overridden by `facets.default_cardinality_cap`).
- **`value_type`** controls how a value parses when you click it. `string` (default), `number`, or `bool`. Click `42` on a `number`-typed facet and you get `@user.id:42`; click it on a `string`-typed facet and you get `@user.id:"42"`.

Config-defined facets are loaded at startup and on `POST /api/admin/reload`. The `facets.custom` key is reloadable, so you can add a facet without restarting.

### In the admin UI (or via API)

For ad-hoc facets - the case of "I just realized I want to see request_id values" - the admin view has a "+ Add facet" button that POSTs to `/api/facets`. The same endpoint is available directly:

```bash
curl -X POST http://localhost:8080/api/facets \
  -H 'Content-Type: application/json' \
  -d '{"name":"request_id","field":"attributes.request_id","cardinality_cap":50}'
```

UI-managed facets are stored in the SQLite catalog and survive restarts. Edit with `PATCH /api/facets/<name>`; delete with `DELETE /api/facets/<name>`.

### When config and UI facets collide

If a config-defined facet and a UI-defined facet share a name, **config wins**. The UI facet is hidden and the admin view shows a warning so you know the conflict exists. The reason is consistency: an operator who put a facet in `config.yaml` for a reason should not see it silently overridden by something a previous admin clicked.

To resolve, either rename one or remove the loser. UI facets are deleted with `DELETE /api/facets/<name>`; config facets are removed from the YAML or env var and a reload applied.

## How facets are computed

When you run a query, Logpond computes the result list and every enabled facet's value counts in a single DuckDB scan per segment. The facet aggregation is `GROUP BY <field> ORDER BY count DESC LIMIT <cap>`.

Facet values are derived from **the N most recent segments overlapping the time range**, where N is `facets.segment_sample_size` (default 24, range 1-168). The active segment and segments that fit entirely inside the time range are always included; older segments contribute up to N total.

This sampling is deliberate. A facet computed across every segment in the time range would be exhaustive but slow on a large time range. With the sample bound, facet derivation stays under the budget that lets sidebar values feel instant.

The flip side: a value that only exists in segments outside the sample window will not appear in the sidebar. It is still queryable - type the value into the search bar manually and the result list will show matching events. The sidebar is a discovery aid, not an index.

## Cardinality overflow

If a facet has more distinct values than its cap, the sidebar shows the top N by count plus an indicator: "+ 1,238 more (showing top 25)." The API response sets `truncated: true` and includes `truncated_count: 1238`. The truncated values are still queryable; they just are not in the sidebar.

Configuring a facet on a unique-per-row field (a UUID, a request ID with no repeats) is allowed but discouraged - you will get exactly `cardinality_cap` essentially random values, which is rarely useful.

## When changes take effect

- Adding, editing, or deleting a facet via the admin UI / `/api/facets` takes effect for the next query immediately.
- Editing `facets.custom` in config and reloading (`POST /api/admin/reload`) takes effect for the next query.
- Built-in facet caps (`facets.builtin.<name>.cap`) hot-reload the same way.
- The sample size (`facets.segment_sample_size`) hot-reloads.

## A worked example

You ship Dokku app logs to Logpond. Out of the box the sidebar gives you `service`, `level`, `host`, `source`. You want to filter by container and by HTTP endpoint as well. Add two facets:

```yaml
facets:
  custom:
    - name: container
      field: attributes.container_name
      display_label: Container
      cardinality_cap: 50
    - name: endpoint
      field: attributes.http.endpoint
      display_label: Endpoint
      cardinality_cap: 100
```

Apply with `POST /api/admin/reload`. The Search view's sidebar grows two new sections. Click `Endpoint: /api/users (1,420)` and the search bar shows `@http.endpoint:/api/users`, with the result list filtered.

The autocomplete dropdown also picks up these fields - typing `@end` will suggest `@http.endpoint` because the autocomplete service samples attribute keys from the same N-segment window used for facet computation.

## Inspecting current facets

```bash
curl http://localhost:8080/api/facets
```

Each entry's `kind` is `builtin`, and `source` tells you whether it came from config (`config`) or the admin UI (`ui`):

```json
{
  "facets": [
    { "name": "service", "field": "service",    "kind": "builtin", "source": null,    "cardinality_cap": 100 },
    { "name": "user_id", "field": "attributes.user.id", "kind": "custom",  "source": "ui", "cardinality_cap": 25 }
  ]
}
```

The full endpoint reference is in [API Reference](api-reference.md#post-apifacets).
