# API reference

Logpond exposes a JSON HTTP API at `/api/*`, an ingest endpoint at `/ingest/<source>`, a Prometheus metrics endpoint at `/metrics`, a health endpoint at `/healthz`, and a WebSocket live stream at `/api/query/stream`.

These `/api/*` endpoints are the supported public contract for scripting. The web UI also calls a `/ui/*` set of endpoints that return HTML fragments for HTMX swapping; those are an internal detail and may change between minor versions - do not script against them.

## Conventions

- **Content type.** JSON request and response bodies are `application/json` unless noted. The ingest endpoint also accepts `application/x-ndjson`, `application/ndjson`, and an empty Content-Type (Vector compatibility).
- **Timestamps.** RFC 3339, microsecond precision, UTC.
- **Errors.** Uniform envelope:

  ```json
  {
    "error": {
      "code": "invalid_filter",
      "message": "Filter operator 'fuzzy_match' is not supported.",
      "details": { "field": "filters[2].op" }
    }
  }
  ```

  Stable error codes: `unknown_source`, `invalid_filter`, `invalid_query_syntax`, `filter_too_deep`, `invalid_time_range`, `time_range_too_large`, `buffer_full`, `segment_not_found`, `segment_busy`, `s3_unavailable`, `script_unavailable`, `rehydrate_unsupported`, `manifest_invalid`, `cursor_invalidated`, `field_not_reloadable`, `facet_conflict`, `bad_request`, `payload_too_large`, `too_many_clients`, `internal_error`.
- **Redirects.** Trailing slashes redirect with `308`. Wrong method returns `405` with `Allow`.
- **CORS.** `GET` and `OPTIONS` allow any origin. Mutations are same-origin only.

## `POST /ingest/:source_name`

Ingest a batch of NDJSON events from a producer (typically Vector).

**Request headers:** `Content-Type: application/x-ndjson` (or `application/json`, `application/ndjson`, or empty). Optional `Content-Encoding: gzip`.

**Body:** NDJSON, decompressed size capped at 32 MB.

```
{"timestamp":"2026-05-12T14:32:01.123Z","level":"info","service":"api","message":"user 42 logged in","user_id":42}
{"timestamp":"2026-05-12T14:32:01.250Z","level":"error","service":"api","message":"db connection refused","err":"ECONNREFUSED"}
```

**Response `202 Accepted`:**

```json
{ "accepted": 2, "skipped": 0 }
```

**Errors:** `404 unknown_source`, `413 payload_too_large`, `415` (bad content type), `429 buffer_full` with `Retry-After: 1`.

## `POST /api/parse-query`

Parse a search-bar string into the canonical filter tree without executing.

**Body:**

```json
{ "q": "service:api (level:error OR @duration_ms:>1000) -host:debug-*" }
```

**Response `200 OK`:**

```json
{
  "filter": {
    "op": "and",
    "children": [
      { "field": "service", "op": "eq", "value": "api" },
      { "op": "or", "children": [
        { "field": "level", "op": "eq", "value": "error" },
        { "field": "attributes.duration_ms", "op": "gt", "value": 1000 }
      ] },
      { "op": "and", "not": true, "children": [
        { "field": "host", "op": "starts_with", "value": "debug-" }
      ] }
    ]
  },
  "search": null,
  "warnings": []
}
```

**Errors:** `400 invalid_query_syntax` (with a column number in `details`).

## `POST /api/query`

Execute a query across the time range. The body accepts one of three filter forms:

**Form A - search-bar string:**

```json
{
  "time_range": { "from": "2026-05-12T10:00:00Z", "to": "2026-05-12T11:00:00Z" },
  "q": "service:api level:(error OR warn) \"connection refused\"",
  "facets": ["service", "level", "host", "user_id"],
  "sort": [{ "field": "timestamp", "order": "desc" }],
  "limit": 100,
  "cursor": null
}
```

**Form B - canonical filter tree:**

```json
{
  "time_range": { "from": "...", "to": "..." },
  "filter": { "op": "and", "children": [
    { "field": "service", "op": "eq", "value": "api" },
    { "field": "level",   "op": "in", "value": ["error", "warn"] }
  ] },
  "search": "connection refused"
}
```

**Form C - legacy flat filters:**

```json
{
  "time_range": { "from": "...", "to": "..." },
  "filters": [
    { "field": "service", "op": "eq", "value": "api" }
  ]
}
```

`q`, `filter`, and `filters` are mutually exclusive (one or none). `facets`, `sort`, `limit`, and `cursor` are optional. Limit defaults to 100, max 1000 (silently clamped). The time range is required and its span cannot exceed `query.max_time_range` (default 7 days).

**Response `200 OK`:**

```json
{
  "events": [
    {
      "timestamp": "2026-05-12T10:58:14.221000Z",
      "service": "api",
      "level": "error",
      "message": "db connection refused",
      "host": "app-1",
      "source": "default",
      "attributes": { "user_id": 42, "request_id": "r-882" },
      "raw": "{...}"
    }
  ],
  "next_cursor": "eyJ0cyI6...",
  "stats": { "rows_scanned": 143821, "rows_returned": 100, "segments_read": 2, "duration_ms": 312 },
  "facets": {
    "service": {
      "values": [ { "value": "api", "count": 12420 }, { "value": "worker", "count": 3801 } ],
      "cardinality_cap": 100, "truncated": false, "sample_segments": 24
    },
    "level": {
      "values": [ { "value": "info", "count": 14210 }, { "value": "warn", "count": 2100 } ],
      "cardinality_cap": 10, "truncated": false, "sample_segments": 24
    }
  }
}
```

**Errors:** `400 invalid_filter`, `400 invalid_query_syntax`, `400 time_range_too_large`, `400 filter_too_deep`, `410 cursor_invalidated`.

## `POST /api/query/count`

Return just the matching event count for a query. Body shape matches `/api/query`; `sort`, `limit`, `cursor`, and `facets` are ignored.

**Response `200 OK`:**

```json
{ "count": 1243, "exact": true, "duration_ms": 42 }
```

Counts above 10,000 short-circuit:

```json
{ "count": 10000, "exact": false, "lower_bound": true, "duration_ms": 18 }
```

## `POST /api/search-suggest`

Context-aware autocomplete suggestions for a partial query.

**Body:**

```json
{ "q": "service:api level:", "cursor_pos": 18, "time_range": { "from": "now-1h", "to": "now" } }
```

**Response - value context (`200 OK`):**

```json
{
  "context": "value",
  "field": "level",
  "suggestions": [
    { "text": "info",  "kind": "value", "count": 14210 },
    { "text": "warn",  "kind": "value", "count": 2100 },
    { "text": "error", "kind": "value", "count": 1013 }
  ]
}
```

**Response - field context:**

```json
{
  "context": "field",
  "prefix": "le",
  "suggestions": [
    { "text": "level", "kind": "core", "description": "Log level" }
  ]
}
```

**Response - combinator context:**

```json
{
  "context": "combinator",
  "suggestions": [
    { "text": "AND", "kind": "combinator" },
    { "text": "OR",  "kind": "combinator" },
    { "text": "NOT", "kind": "combinator" }
  ]
}
```

## `GET /api/query/stream` (WebSocket)

Stream matching events as they ingest. See [Live Tail](live-tail.md) for the full protocol. Connection is HTTP GET upgraded to WebSocket; subscription messages take the same `q`/`filter`/`filters` shape as `/api/query`. Close codes: `4001 filter_required`, `4002 invalid_filter`, `4003 slow_client`, `4004 too_many_clients`.

## `GET /api/segments`

List segments with optional state and time filters. Backs the Admin view.

**Query parameters:** `state` (repeatable), `from`, `to`, `source`, `limit`, `offset`.

**Response `200 OK`:**

```json
{
  "segments": [
    {
      "id": "202605121400",
      "state": "sealed",
      "time_start": "2026-05-12T14:00:00Z",
      "time_end": "2026-05-12T15:00:00Z",
      "row_count": 3598221,
      "size_bytes": 1841299832,
      "size_compressed": 183921002,
      "local_path": "/data/segments/sealed/202605121400.parquet",
      "s3_url": null,
      "archive_ref": null,
      "source_names": ["default", "nginx"],
      "sealed_at": "2026-05-12T15:00:04Z",
      "archived_at": null,
      "rehydrated_at": null,
      "evict_after": null,
      "overlaps": []
    }
  ],
  "total": 482, "limit": 100, "offset": 0
}
```

Possible `state` values: `active`, `sealed`, `archived`, `rehydrated`, `lost`.

## `POST /api/archive`

Trigger an archive job. Body has exactly one of `segment_id` or `older_than`.

```json
{ "segment_id": "202605121400" }
```

```json
{ "older_than": "30d" }
```

**Response `202 Accepted`:**

```json
{ "job_id": "arc-7c1f", "segments": ["202605121400"], "status_url": "/api/jobs/arc-7c1f" }
```

**Errors:** `400 bad_request`, `404 segment_not_found`, `501` (when `archive.backend: none`), `503 s3_unavailable` or `503 script_unavailable`.

## `POST /api/rehydrate`

Pull archived segments back to local disk.

```json
{
  "time_range": { "from": "2026-04-12T00:00:00Z", "to": "2026-04-13T00:00:00Z" },
  "ttl_days": 7,
  "persistent": false
}
```

**Response `202 Accepted`:**

```json
{
  "job_id": "reh-3a02",
  "segments": ["202604120000", "..."],
  "segment_count": 24,
  "total_bytes": 4423800000,
  "estimated_seconds": 90,
  "status_url": "/api/jobs/reh-3a02"
}
```

**Errors:** `404 segment_not_found`, `501 rehydrate_unsupported` (when the configured backend cannot retrieve), `503`.

## `DELETE /api/rehydrated/:id`

Evict a rehydrated segment from local disk. The archive copy is untouched.

**Response:** `204 No Content`. **Errors:** `404`, `409 segment_busy`.

## `POST /api/import`

Sideload a Parquet plus its manifest as a rehydrated segment.

**Request:** `multipart/form-data` with `parquet` and `manifest` parts. Optional `?persistent=true` query parameter. Body size cap 5 GB.

**Response `201 Created`:**

```json
{
  "segment_id": "202604120100",
  "state": "rehydrated",
  "row_count": 3601001,
  "size_bytes": 1842500000,
  "evict_after": "2026-05-19T12:00:00Z",
  "persistent": false,
  "overlaps": []
}
```

**Errors:** `400 manifest_invalid`, `400 bad_request` (missing PAR1 header or SHA mismatch), `409` (segment ID already present), `413 payload_too_large`.

## `POST /api/saved-queries`

Save a query by name for re-use.

```json
{
  "name": "api errors last 24h",
  "freeze_times": false,
  "query": { "time_range": { "from": "now-24h", "to": "now" }, "q": "service:api level:error" }
}
```

**Response `201 Created`:**

```json
{ "id": "sq-9a01", "name": "api errors last 24h", "created_at": "2026-05-12T14:35:00Z" }
```

**Errors:** `409` on name collision.

## `GET /api/saved-queries`

List saved queries.

```json
{
  "saved_queries": [
    { "id": "sq-9a01", "name": "...", "query": { "..." : "..." }, "created_at": "..." }
  ]
}
```

## `DELETE /api/saved-queries/:id`

Remove a saved query.

**Response:** `204` or `404`.

## `GET /api/facets`

List configured facets, including built-ins.

```json
{
  "facets": [
    { "name": "service", "field": "service",            "kind": "builtin", "source": null, "cardinality_cap": 100, "value_type": "string", "display_label": "Service" },
    { "name": "user_id", "field": "attributes.user.id", "kind": "custom",  "source": "ui",  "cardinality_cap": 25,  "value_type": "string", "display_label": "User ID", "created_at": "...", "updated_at": "..." }
  ]
}
```

## `POST /api/facets`

Add a custom facet (UI-managed).

```json
{ "name": "user_id", "field": "attributes.user.id", "display_label": "User ID", "cardinality_cap": 25, "value_type": "string" }
```

**Response `201 Created`** (same shape as the list entry). **Errors:** `409 facet_conflict` if the name is taken.

## `PATCH /api/facets/:name`

Edit a UI-managed facet. Body is a partial update.

```json
{ "cardinality_cap": 50, "display_label": "User" }
```

**Errors:** `409` when the named facet is built-in or config-defined (those are not editable through the API).

## `DELETE /api/facets/:name`

Delete a UI-managed facet.

**Response:** `204`, `404`, or `409`.

## `POST /api/admin/reload`

Apply hot-reloadable config changes.

**Body:** empty or `{"dry_run": true}`.

```json
{ "reloaded": true, "changed_fields": ["retention.max_age", "facets.custom"], "warnings": [] }
```

**Errors:** `422 field_not_reloadable` when the new config changes a key that requires a restart.

## `POST /api/admin/retention/run`

Run the retention evaluator manually.

```json
{ "dry_run": false }
```

**Response `200 OK`:**

```json
{
  "evaluated": 482,
  "actions": [
    { "segment_id": "202604120100", "action": "archive_then_delete", "reason": "age > 30d" }
  ]
}
```

## `POST /api/admin/archive/verify`

Compare the catalog against the archive backend.

**Body:** empty or `{ "segment_ids": ["..."] }`.

**S3 response:**

```json
{ "backend": "s3", "scanned": 458, "orphaned_parquets": [], "dangling_manifests": [], "missing_parquets": [] }
```

**Script response:**

```json
{
  "backend": "script",
  "scanned": 458,
  "verified": 455,
  "failed": [
    { "segment_id": "202604120100", "exit_code": 1, "note": "restic: snapshot abc123 not found" }
  ]
}
```

**Errors:** `501`, `503`.

## `GET /api/admin/archive/capabilities`

Report the script backend's probed capabilities (relevant only when `archive.backend: script`).

```json
{ "backend": "script", "archive": true, "verify": true, "retrieve": false }
```

## `POST /api/admin/archive/probe`

Re-probe the script backend's capabilities. Useful after editing the script.

**Response:** same shape as `GET /api/admin/archive/capabilities`.

## `GET /api/admin/config`

Return the effective merged config with all secrets redacted.

```json
{
  "data_dir": "/data",
  "archive": {
    "backend": "s3",
    "s3": { "bucket": "logs-archive", "access_key_id": "***REDACTED***", "secret_access_key": "***REDACTED***" }
  }
}
```

## `GET /api/jobs/:id`

Poll job state. Used by the admin UI to display archive, rehydrate, and verify progress.

```json
{
  "job_id": "reh-3a02",
  "type": "rehydrate",
  "state": "running",
  "progress": { "completed": 14, "total": 24, "bytes_done": 2580000000, "bytes_total": 4423800000 },
  "started_at": "2026-05-12T14:40:00Z",
  "finished_at": null,
  "error": null,
  "script_stdout": null,
  "script_stderr": null
}
```

`state` is one of `pending`, `running`, `completed`, `failed`.

## `GET /healthz`

Liveness probe. Returns `200` when the service is healthy and `503` with a `reason` when it is not.

```json
{ "status": "ok", "uptime_seconds": 84221, "version": "1.0.0" }
```

## `GET /metrics`

Prometheus exposition. See [Metrics and Health](metrics-and-health.md) for the names that matter.
