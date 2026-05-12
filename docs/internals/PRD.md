# Logpond — Product Requirements Document

**Status:** Draft v1.8
**Owner:** TBD
**Last updated:** 2026-05-12

---

## 1. Summary

Logpond is a single-container log aggregation service designed for small, resource-constrained deployments. It ingests JSON logs over HTTP (shipped by Vector), stores them in an embedded columnar database, exposes a web UI for searching/filtering/sorting, and automatically archives aged data to S3-compatible object storage *or* via a user-provided script, with rehydration support.

The product targets developers running side-project or small production workloads on a Dokku host, where existing log aggregation stacks (Elasticsearch, Loki, ClickHouse, Datadog) are either too heavy for the host, too expensive, or both. Logpond aims to provide a "Datadog-lite" experience in under 256MB of RAM, including a Datadog-style search bar, faceted sidebar, and autocompleting query syntax.

## 2. Design philosophy

Where a trade-off exists between **end-user friendliness** and **strict correctness**, this document prefers end-user friendliness, with the correctness cost called out inline using a **⚖ Trade-off** callout.

The trade-off callouts make these choices visible so future maintainers and operators can revisit them without re-discovering the rationale. Every trade-off callout in the body has a corresponding entry in §16; the index is the canonical list.

## 3. Goals & non-goals

### 3.1 Goals

- Run as a single container on a 2GB Dokku host alongside other applications, consuming less than 256MB of resident memory under steady-state load.
- Ingest at least 1,000 JSON log events per second over HTTP without backpressure under normal conditions.
- Provide a web UI with a Datadog-style search experience: a single search bar with autocompleting query syntax, a faceted sidebar showing top values for common fields, and a results list. Support OR / nested boolean expressions in the search syntax.
- Support light, dark, and system-following theme modes in the web UI.
- Ship the web UI with no JavaScript build step and no SPA framework. The full client-side dependency footprint must fit in approximately 60KB gzipped.
- Enforce configurable retention by age and/or total size.
- Archive aged log segments to S3-compatible object storage *or* via a user-provided script.
- Allow operators to rehydrate archived segments for querying through the same UI, or sideload archives downloaded out-of-band.
- Deploy as a standard Dockerfile-based Dokku application configured primarily via environment variables, with no required external dependencies beyond an S3-compatible bucket (or archive script).
- Integrate cleanly with Dokku's built-in Vector logging functionality so app logs flow into Logpond with usable filter dimensions.

### 3.2 Non-goals (v1)

- Metrics, traces, or alerting on log content.
- Multi-tenancy, role-based access control, or any authentication beyond what a reverse proxy provides.
- Distributed or multi-node deployment.
- Ingest protocols other than HTTP/NDJSON (gRPC, OTLP, syslog, etc.).
- Server-side log parsing for arbitrary formats — parsing is delegated to Vector.
- Self-hosted UI customization, dashboards, or report generation.

## 4. Target user & use case

The intended user is a developer or small operations team running one or more applications on a single Dokku host. They ship application logs via Dokku's built-in Vector integration and want to see recent logs across all their apps, search and filter when investigating an issue, keep recent logs hot for fast access without paying a managed-service bill, push older logs to cheap object storage (or anywhere a script can put them), and pull old logs back for forensics without a separate analysis environment.

They are familiar with Datadog's log explorer UI (or willing to learn a similar one). They are comfortable editing a YAML config file when necessary, but prefer environment-variable-driven deployment.

## 5. Assumptions & constraints

- Peak ingest rate is approximately 1,000 events per second, with an assumed average event size of 500 bytes.
- The host has 2GB of total RAM and is shared with other Dokku applications; Logpond's memory budget is 256MB.
- Logs are shipped exclusively by Vector and arrive as JSON. Vector's disk buffer is treated as the durability layer for in-flight events.
- An S3-compatible object store is available *or* the operator has a script capable of archiving a single Parquet file to wherever they want it.
- Authentication is handled outside the service in v1.
- A single operator administers the service; concurrent admin actions are not a design concern.

## 6. Terminology

- **Source (ingest source).** A configured HTTP ingest endpoint, identified by `source_name`, with an associated field extraction mapping.
- **`source` column.** A column on every stored event whose value is the `source_name` of the ingest endpoint that received it.
- **Segment.** A time-bucketed unit of storage covering a fixed time window (default one hour).
- **Active segment.** A segment currently accepting writes. There can be more than one briefly during window transitions.
- **Sealed segment.** A past segment whose time window has closed, stored as a compressed Parquet file on local disk.
- **Archived segment.** A sealed segment that has been uploaded to S3 or successfully processed by an archive script.
- **Rehydrated segment.** An archived segment that has been downloaded back to local disk for querying.
- **Event time.** The `timestamp` field on a log event as extracted by the source's field mapping.
- **Ingest time.** The wall-clock time at which the service parsed the event from the HTTP request body. Used as a fallback when event time can't be extracted.
- **Archive backend.** The configured mechanism for persisting sealed segments off the local disk. Either `s3` or `script`. Mutually exclusive.
- **Catalog.** A SQLite database that is the authoritative index of all segments and their metadata.
- **Facet.** A field whose distinct values are summarized in the sidebar with per-value event counts, used as a one-click filter. Logpond has built-in facets (`service`, `level`, `host`, `source`) and supports custom facets configured by the operator.
- **Filter tree.** The internal canonical representation of a query's boolean structure: a tree of `and`/`or`/`not` groups with leaf predicates at the bottom. The search bar parses to this; the API accepts this directly.

## 7. Functional requirements

### 7.1 Ingest

**Endpoint.** `POST /ingest/:source_name` accepts NDJSON request bodies. The set of valid `source_name` values is defined in the configuration. Requests to unconfigured sources return HTTP 404.

**Source name format.** Source names must match `^[a-z0-9][a-z0-9_-]{0,63}$` (lowercase alphanumeric, underscore, hyphen; up to 64 chars; must start with alphanumeric).

**Line format.** Each line is a JSON object. Line terminators are `\n` or `\r\n`. Empty lines are silently skipped. Lines must be parseable JSON objects; non-conforming lines are skipped with a counter incremented in metrics, and the rest of the batch is accepted.

> **⚖ Trade-off.** A strict service would 400 the whole batch on the first bad line. We prefer to accept the good lines and skip the bad ones so that one malformed event doesn't lose a whole Vector batch on retry. The cost: silently skipped lines are invisible unless the operator looks at the metric. Mitigated by emitting a service-log warning (rate-limited) and the `logpond_ingest_skipped_lines_total` metric.

**Body size limits.** Decompressed body capped at 32MB. Larger requests return 413 Payload Too Large.

**Field extraction.** Each configured source specifies an ordered list of candidate JSON paths per core field (`timestamp`, `level`, `message`, `service`, `host`). Resolution rules:

- A candidate "resolves" if the path returns a non-null scalar or — for `timestamp` only — a string parseable as RFC 3339 or a number interpretable as a Unix timestamp.
- For `timestamp`: candidates returning arrays, objects, or unparseable values are skipped. If all fail, the service uses **ingest time** and sets `attributes.logpond_timestamp_fallback = true`.
- For `level`: candidates returning strings are normalized. Candidates returning numbers are interpreted as syslog severity levels (0–7 mapped to the four normalized values). If all fail, level is set to `info` and `attributes.logpond_level_fallback = true` is set.
- For `message`: candidates may return any scalar; non-string values are stringified.
- For `service` and `host`: candidates must return strings; other types skipped. If all fail, the column is left null.

**Level normalization.** Strings are matched case-insensitively against a fixed table:

| Input (case-insensitive)                              | Normalized |
| ----------------------------------------------------- | ---------- |
| `trace`, `debug`, `dbg`, `0`, `1`                     | `debug`    |
| `info`, `information`, `notice`, `2`, `3`             | `info`     |
| `warn`, `warning`, `4`                                | `warn`     |
| `error`, `err`, `critical`, `crit`, `fatal`, `emerg`, `alert`, `panic`, `5`, `6`, `7` | `error` |
| (anything else)                                       | `info`, with `attributes.logpond_level_unrecognized = "<original>"` |

The original value is preserved in `attributes.logpond_level_original` when normalization changed it.

> **⚖ Trade-off.** Collapsing severities makes filtering simple at the cost of losing gradations. The escape hatch is `attributes.logpond_level_original` for users who need the original.

**Timestamp fallback semantics.** Ingest time is captured at parse time, not flush time.

**Attributes column.** Contains top-level fields not extracted into core columns. Extracted fields are removed from `attributes` to avoid duplication. The original full line is preserved in `raw`.

> **⚖ Trade-off.** Removing extracted fields from `attributes` keeps the column small and avoids ambiguity (which field is canonical?), at the cost that users querying `attributes.level` will find nothing — they must query the `level` column. The UI's field autocomplete steers users toward the right column, and the original full event line lives in `raw` so no data is lost.

**Compression.** `Content-Encoding: gzip` accepted; other encodings return 415.

**Batch acceptance semantics.** 202 with `{"accepted": N, "skipped": M}`. Vector's disk buffer is the durability layer.

**Backpressure.** Sustained backpressure → 429 with `Retry-After: 1`. Never silently dropped.

### 7.2 Storage layout

**Segments.** Time-bucketed; default 1h. The window is configurable at startup and must be a whole-number divisor of 24 hours (5m, 10m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, 24h).

**Window alignment.** Aligned to UTC wall-clock boundaries. Internal time is UTC.

**Segment ID format.** `YYYYMMDDhhmm` (12 chars, UTC) representing the start of the window.

**Active segment.** Normally one. Briefly two during transitions (the new one taking writes, the old one sealing).

**Event placement.** By event time, not ingest time.

**Late arrivals.** Events whose event time falls into an already-sealed segment go to the current active segment with `attributes.logpond_late = true` and `attributes.logpond_intended_segment = "<id>"`.

> **⚖ Trade-off.** Reopening sealed segments would defeat their immutability. We accept that late arrivals are queryable by event time but live in a different file than the rest of their time window.

**Sealing.** A goroutine wakes every 60s (configurable) and seals segments whose window closed at least 60s ago. Sealing exports to Parquet (zstd-3), writes atomically (temp → fsync → rename), updates the catalog, deletes the source DuckDB file.

**Crash recovery.** On startup, the service scans `/data/segments/` and reconciles with the catalog (active DuckDB without entry → register; sealed Parquet without entry → register; catalog entry without file → mark `lost`; both DuckDB and Parquet for same ID → keep Parquet, archive DuckDB to `/data/orphans/`).

**Row schema.** Every segment uses this schema:

| Column      | Type        | Notes                                                                |
| ----------- | ----------- | -------------------------------------------------------------------- |
| `timestamp` | `TIMESTAMP` | Microsecond precision, UTC (Parquet: `isAdjustedToUTC=true`).        |
| `service`   | `VARCHAR`   | Extracted via source config; nullable.                               |
| `level`     | `VARCHAR`   | One of `debug`, `info`, `warn`, `error`.                             |
| `message`   | `VARCHAR`   | Extracted from common message-like fields; nullable.                 |
| `host`      | `VARCHAR`   | Extracted; nullable.                                                 |
| `source`    | `VARCHAR`   | The ingest endpoint name; always set.                                |
| `attributes`| `VARCHAR`   | JSON-encoded string; Parquet logical type `JSON`.                    |
| `raw`       | `VARCHAR`   | The original NDJSON line as received.                                |

### 7.3 Search and filter expressions

Logpond's primary search interface is a Datadog-style search bar that parses a single-line query string into an internal filter tree. The same filter tree can also be supplied directly to the API for programmatic use.

#### 7.3.1 Filter tree (canonical form)

The internal canonical form is a tree of boolean operators with leaf predicates:

**Group node.** A logical combination of children:

```json
{
  "op": "and",
  "not": false,
  "children": [ /* one or more nodes */ ]
}
```

- `op` is `"and"` or `"or"`. Required.
- `not` is an optional boolean (default false). When true, the group's result is negated.
- `children` is a non-empty array of group nodes and/or leaf nodes. Nesting depth is capped at 32 (`filter_too_deep`).

**Leaf node.** A field predicate:

```json
{
  "field": "level",
  "op":    "in",
  "value": ["error", "warn"],
  "case_sensitive": false
}
```

- `field` is the column or attribute path.
- `op` is one of `eq`, `neq`, `in`, `not_in`, `contains`, `starts_with`, `gt`, `lt`, `gte`, `lte`, `exists`.
- `value` is required for all ops except `exists`.
- `case_sensitive` is optional, default false (string ops only).

#### 7.3.2 Datadog-style search syntax

The search bar accepts a single line of query text. The grammar:

```
query        := disjunction
disjunction  := conjunction ( "OR" conjunction )*
conjunction  := factor ( ("AND")? factor )*           # AND is implicit when omitted
factor       := negation | atom
negation     := ("NOT" | "-") atom
atom         := group | predicate | term
group        := "(" disjunction ")"
predicate    := field ":" value-expression
field        := core-field | "@" attribute-path
core-field   := "service" | "level" | "host" | "source" | "message"
attribute-path := identifier ("." identifier)*
value-expression := value | "(" value-list ")" | range-expression | wildcard
value-list   := value ( "OR" value )+ | value ( "," value )+
range-expression := "[" value "TO" value "]" | "{" value "TO" value "}"   # [] inclusive, {} exclusive
wildcard     := bare-token containing "*" or "?"
value        := bare-token | quoted-string
term         := bare-token | quoted-string             # free-text search (matches message + raw)
```

Keywords `AND`, `OR`, `NOT`, `TO` are case-insensitive but conventionally uppercased in the UI.

**Field naming notes.** The `core-field` production lists the five fields reachable without the `@` prefix. The `timestamp` and `raw` columns are not directly usable as predicates in search-bar syntax — `timestamp` filtering happens via the time-range picker (not the search bar), and `raw` is searched implicitly via free-text terms. Custom facets are reached using their underlying field path with the `@` prefix (e.g., `@user.id:42`); the facet's display name is not part of the syntax. Bare identifiers that aren't in the `core-field` list and aren't preceded by `@` are treated as free-text terms.

Examples (each shown with its filter tree translation):

**Simple field equality.**

```
service:api
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "service", "op": "eq", "value": "api" }
] } }
```

**Implicit AND between predicates.**

```
service:api level:error
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "service", "op": "eq", "value": "api" },
  { "field": "level",   "op": "eq", "value": "error" }
] } }
```

**Multiple values for one field (parenthesized OR list).**

```
level:(error OR warn)
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "level", "op": "in", "value": ["error", "warn"] }
] } }
```

**Multiple values (comma form, equivalent).**

```
level:(error, warn)
```

→ same as above.

**Negation.**

```
service:api -level:debug
```

(also accepted as `service:api NOT level:debug`) →

```json
{ "filter": { "op": "and", "children": [
  { "field": "service", "op": "eq", "value": "api" },
  { "op": "and", "not": true, "children": [
    { "field": "level", "op": "eq", "value": "debug" }
  ] }
] } }
```

A negated single predicate could also be folded into a `neq` leaf; the parser uses the wrapped-NOT form so the tree shape exactly mirrors the source query.

**Attribute path with `@` prefix.**

```
@user.id:42
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "attributes.user.id", "op": "eq", "value": 42 }
] } }
```

The `@` prefix is sugar for the `attributes.` prefix. Numeric-looking values are parsed as numbers; quote them (`@user.id:"42"`) to force string.

**Free-text search.**

```
"connection refused"
```

→

```json
{ "search": "connection refused" }
```

Unquoted bare tokens that aren't `field:value` predicates are also treated as free-text search terms, joined with spaces. Mixed predicate + free-text:

```
service:api "connection refused"
```

→

```json
{
  "filter": { "op": "and", "children": [
    { "field": "service", "op": "eq", "value": "api" }
  ] },
  "search": "connection refused"
}
```

**Wildcards.**

```
service:api-*
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "service", "op": "starts_with", "value": "api-" }
] } }
```

Trailing `*` → `starts_with`. Other wildcard positions (leading, middle, `?` for single-char) are documented as supported but in v1 compile to a slower full-scan substring match using `contains`. The UI surfaces a "this query will be slow" warning when a non-prefix wildcard is detected.

**Ranges.**

```
@duration_ms:[1000 TO 5000]
```

→

```json
{ "filter": { "op": "and", "children": [
  { "op": "and", "children": [
    { "field": "attributes.duration_ms", "op": "gte", "value": 1000 },
    { "field": "attributes.duration_ms", "op": "lte", "value": 5000 }
  ] }
] } }
```

`[a TO b]` is inclusive; `{a TO b}` is exclusive (translates to `gt` / `lt`). Open ranges use `*`: `[1000 TO *]` → `gte 1000`.

**Existence check.**

```
@user.id:*
```

→

```json
{ "filter": { "op": "and", "children": [
  { "field": "attributes.user.id", "op": "exists" }
] } }
```

**Nested grouping with OR/AND.**

```
service:api AND (level:error OR (level:warn AND @duration_ms:>1000))
```

→

```json
{ "filter": {
  "op": "and",
  "children": [
    { "field": "service", "op": "eq", "value": "api" },
    { "op": "or", "children": [
      { "field": "level", "op": "eq", "value": "error" },
      { "op": "and", "children": [
        { "field": "level", "op": "eq", "value": "warn" },
        { "field": "attributes.duration_ms", "op": "gt", "value": 1000 }
      ] }
    ] }
  ]
} }
```

The `field:>value` shorthand (without surrounding brackets) is sugar for `field:[value TO *]` with the inclusive flag tracking `>` vs `>=`: `>`, `>=`, `<`, `<=` are all supported.

**Operator precedence.** From tightest to loosest:

1. Parentheses (highest — explicit grouping).
2. Predicate (`field:value`).
3. `NOT` / `-`.
4. `AND` (explicit or implicit — both at the same precedence level).
5. `OR` (lowest).

Operators of the same level associate left-to-right.

**Reserved characters.** `:`, `(`, `)`, `[`, `]`, `{`, `}`, `"`, `\`, and unescaped whitespace are reserved. Use double quotes to include them in values. Within quoted strings, `\"` escapes a quote and `\\` escapes a backslash.

**Empty query.** An empty search bar means "all events in the time range" — the API receives no `filter` and no `search`, and the time range governs the result set.

#### 7.3.3 Parsing endpoint

The parser is available as `POST /api/parse-query`, which transforms a search-bar string into a filter tree without executing the query. This is used by the UI for live preview and validation, and is available to scripts and CLI tools.

The same parsing also runs implicitly inside `POST /api/query` when the caller passes a `q` field instead of `filter`. Either form is accepted:

```json
{ "time_range": { ... }, "q": "service:api level:error" }
```

is equivalent to

```json
{ "time_range": { ... }, "filter": { "op": "and", "children": [ /* parsed tree */ ] } }
```

Passing both `q` and `filter` is an error (`bad_request` with `details.conflict: "q and filter are mutually exclusive"`).

#### 7.3.4 Filter semantics

- String operators (`eq`, `neq`, `contains`, `starts_with`) are case-insensitive by default; per-leaf `case_sensitive: true` opts into case-sensitive matching. The search-bar syntax has no inline case-sensitive marker in v1 — power users wanting case sensitivity use the API or check a "case sensitive" toggle in the UI.
- `in`/`not_in` accept arrays; null in the array matches null cells; mixed-type arrays allowed.
- `exists` returns true for any non-null value (including empty strings, empty arrays, zero, false). Missing keys and explicit nulls return false.
- `gt`/`lt`/`gte`/`lte` on `timestamp` as timestamps; on string core columns as strings; on attributes by the value's stored JSON type.
- Lenient type coercion on attribute filters: numeric strings ↔ numbers, `"true"`/`"false"` ↔ booleans. Coercion failures don't match (no query-level error).

> **⚖ Trade-off.** Lenient coercion means `@user_id:42` matches rows where `user_id` is `42`, `"42"`, or `42.0`. Strict typing would surface upstream inconsistencies; we prefer the matches happen because Vector ships data from many languages with varying JSON conventions.

**Free-text search.** Top-level `search` (or unquoted text in the search bar) does case-insensitive substring match against `message` and `raw`. Implicitly AND-combined with any filter tree.

**Sort.** Defaults to `[{"field":"timestamp","order":"desc"}]`. Sortable fields: `timestamp`, `service`, `level`, `host`, `source`. Sorting on `attributes.*`, `message`, or `raw` is rejected with 400.

**Pagination.** Cursor-based using `(timestamp, segment_id, intra_segment_row_number)`. Cursors invalidate if their segments are evicted (`cursor_invalidated`).

**Limit.** Default 100, max 1000 (silently clamped).

**Unbounded queries.** Forbidden. Time range required and span ≤ 7 days (configurable via `query.max_time_range`).

> **⚖ Trade-off.** The 7-day cap is a backstop against accidental "show me everything" queries that would scan months of segments. A power user with a real need can raise the cap in config. The UI surfaces this limit clearly in the time-range picker.

### 7.4 Facets

Facets are summaries of distinct values for selected fields, with counts, displayed in the Search view's sidebar. They serve two purposes: discovering what values exist in the data, and one-click filtering by clicking a value.

#### 7.4.1 Built-in facets

Four facets are always available:

| Facet name | Source field | Default cardinality cap |
| ---------- | ------------ | ----------------------- |
| `service`  | `service`    | 100                     |
| `level`    | `level`      | 10                      |
| `host`     | `host`       | 50                      |
| `source`   | `source`     | 50                      |

Built-in facets cannot be disabled but their cardinality caps and display labels can be customized via config.

#### 7.4.2 Custom facets

Operators may declare additional facets in two ways:

1. **Static config.** YAML or `LOGPOND_FACETS_JSON` env var defining facets at startup. See §7.11.
2. **Admin UI.** The Admin view's Facets pane lets operators add/edit/remove custom facets at runtime. Changes are persisted in the catalog and take effect immediately for new queries.

A facet definition:

```json
{
  "name":          "user_id",
  "field":         "attributes.user.id",
  "display_label": "User ID",
  "cardinality_cap": 25,
  "value_type":    "string"
}
```

- `name` — unique identifier; the sidebar shows the value, and the search syntax can reference the facet as `@<field>:<value>` (the underlying field path, not the facet name).
- `field` — the dotted field path (core column or `attributes.*` path).
- `display_label` — what the sidebar shows as the section header. Defaults to a title-cased version of `name`.
- `cardinality_cap` — max number of distinct values to surface per query. Defaults to 50.
- `value_type` — optional. `string` (default), `number`, `bool`. Controls how values are parsed when clicked into the search bar.

UI-managed facets are stored in the catalog and survive restarts. Config-defined facets are loaded at startup or reload; if a config facet conflicts in name with a UI facet, the config wins and the UI facet is hidden (surfaced as a warning in the admin UI).

> **⚖ Trade-off.** Allowing UI-managed facets adds catalog state and the risk of operators leaving stale facet definitions around. We accept this because the typical workflow ("I just realized I want to see request_id values") shouldn't require editing config and reloading.

#### 7.4.3 Facet computation

Facet values are computed alongside query results in the same DuckDB query, using `GROUP BY <field>` for each enabled facet with a `LIMIT <cardinality_cap>` ordered by count desc.

**Source segments.** Facet values are derived from the N most recent sealed segments overlapping the current query time range, where N is configurable (default 24, range 1–168). For shorter time ranges, all overlapping segments are included regardless of N.

> **⚖ Trade-off.** Facets are a sample, not an exhaustive index. A value that exists only in older segments may not appear in the sidebar — but it can still be typed manually in the search bar. The sample boundary makes facet derivation fast and predictable.

**Refresh cadence.** Facets are re-derived on every query execution. The DuckDB query unions result-set retrieval with facet aggregation in a single scan, so the marginal cost is small (typically <30% of the result-set query time).

**Cardinality overflow.** When a field has more distinct values than its cardinality cap, the top N by count are returned and a `truncated_count` indicator is set. The UI displays a "+ N more (showing top X)" affordance.

**High-cardinality fields.** Configuring a facet on a unique-per-row field is allowed but discouraged. Operator discretion.

### 7.5 Search bar autocomplete

The search bar offers context-aware autocomplete as the user types. Suggestions come from a `POST /api/search-suggest` endpoint that returns ranked completions for a given cursor position in a partial query.

**Suggestion contexts:**

- **Start of token or after a combinator.** Suggest field names, drawn from built-in facets, custom facets, and recently-seen attribute keys. Attribute keys are sampled from the same N-segment window used for facet computation (default 24, configurable via `facets.segment_sample_size`).
- **After a field name (no colon yet).** Suggest `:` as the next character; offer combinators if the predicate is already complete.
- **After `field:`.** Suggest values for that field. For fields configured as facets, the values come directly from the current query's facet computation (already cached for the visible sidebar). For other fields, values are sampled from the same N-segment window.
- **Inside a grouping parenthesis or value list.** Continue suggesting values, plus `OR`/`,` to add another value.
- **At any whitespace position.** Suggest combinators (`AND`, `OR`, `NOT`).

**Debounce.** Suggest requests are debounced 50ms after the last keystroke (short enough to feel instant, long enough to coalesce rapid typing).

**Live result count.** As the user types, the UI also debounces (300ms — longer because counting is heavier) and asks the server for a count of matching events via `POST /api/query/count`. Counts above 10,000 short-circuit to `"≈ 10,000+ events"`.

**Quick-completion shortcuts.** Tab accepts, Enter executes, Up/Down navigate, Escape closes, Ctrl-Space reopens.

**Performance budget.** `search-suggest` and `query/count` must complete in <100ms p95.

### 7.6 Live tail

**Endpoint.** `GET /api/query/stream` (HTTP GET upgraded to WebSocket).

**Subscription.** Client sends a subscription message containing `filter` (the tree form), `search`, or `q`. At least one leaf predicate or non-empty `search`/`q` is required.

**Subscription updates.** New subscription messages replace the current filter; the stream continues without disconnecting.

**Latency.** Effective end-to-end ≤ 2s. "Near-real-time."

**Rate cap.** 100 events/sec per client default. Excess events dropped, counter increments.

**Slow clients.** Drop events; close with 4003 after 30s.

**Connection cap.** 20 globally; 4004 on overflow.

**Heartbeat.** Every 30s. UI marks stale after 60s.

### 7.7 Web UI

The UI is server-rendered HTML with HTMX driving server interactions and Alpine.js for small bits of client state. See §11 for the full frontend stack rationale and §12 for the visual style guide. The UI provides three views:

- **Search.** Search bar with autocomplete (top), faceted sidebar (left), results table (center). Time-range picker above the search bar.
- **Live tail.** Search bar with autocomplete (top, required), streaming results.
- **Admin.** Segment list, retention policy, archive backend status, facet configuration pane, manual archive/rehydrate/evict/import controls.

**Theme support.** Three modes — Auto (default, follows `prefers-color-scheme`), Light, Dark. Stored in `localStorage` as `logpond.theme`. Default for new sessions configurable via `theme.default`. See §12 for palette details.

**Timestamp display.** Browser local time with timezone abbreviation by default; toggle to UTC. Server-rendered HTML emits UTC ISO in a `data-ts` attribute; a small JS helper renders the display string client-side.

**Result pagination.** 100 rows initial; "Load more" appends 100. No infinite scroll.

### 7.8 Retention

**Policy axes.** `max_age` and `max_size` (independent; either or both required).

**Size accounting.** Sealed + rehydrated local segments. Active and archived-only are not counted.

> **⚖ Trade-off.** Counting rehydrated segments toward `max_size` can trigger deletion of normal sealed segments after a large rehydration. We honor the cap to protect the disk.

**Protected window.** Active segments and segments sealed within the last hour are never deleted.

**Evaluation cadence.** Every 5 minutes (configurable).

**Manual override.** `POST /api/admin/retention/run` with optional dry-run.

### 7.9 Archival

Two mutually exclusive backends, set via `archive.backend` (`s3`, `script`, or `none`).

#### 7.9.1 Common archive behavior

- The manifest file is the commit marker. Catalog is updated only after manifest creation succeeds.
- Archive operations run automatically (from retention) or manually.
- Failed archives leave the segment in `sealed` state for retry.
- The catalog tracks archive state via `s3_url` (S3 backend) or `archive_ref` (script backend), with `archived_at` set on success.

#### 7.9.2 S3 backend

**Configuration.** Endpoint URL, bucket, prefix, region, credentials. Required IAM: `s3:PutObject`, `s3:GetObject`, `s3:HeadObject`, `s3:ListBucket`. Service never deletes from S3.

**Object key layout.** `{prefix}/segments/YYYY/MM/DD/HH/segment-{id}.parquet` and `manifest.json` alongside.

**Idempotency.** SHA-256 metadata header (`x-amz-meta-parquet-sha256`); no-op on hash match, overwrite on mismatch with warning.

**Commit ordering.** Upload Parquet → HEAD verify → write manifest.

**Resumability.** Failed uploads leave no manifest; next retention cycle retries. Multi-part upload for objects >64MB.

**Verify endpoint.** `POST /api/admin/archive/verify` walks the prefix and reports orphans/dangling/missing.

#### 7.9.3 Script backend

**Contract.** Configured executable invoked as a subprocess.

**Invocation modes:**

1. `script archive --segment-id <id> --parquet-path <path> --manifest-path <path>`
2. `script verify --segment-id <id>` or `script verify --all`
3. `script retrieve --segment-id <id> --output-parquet <path> --output-manifest <path>`

A script that doesn't support a mode exits 64 (`EX_USAGE`). Unsupported modes are surfaced in the admin UI.

**Environment variables passed to the script:**

| Variable                       | Value                                                       |
| ------------------------------ | ----------------------------------------------------------- |
| `LOGPOND_SEGMENT_ID`           | Segment ID                                                  |
| `LOGPOND_SEGMENT_TIME_START`   | RFC 3339 segment window start                               |
| `LOGPOND_SEGMENT_TIME_END`     | RFC 3339 segment window end                                 |
| `LOGPOND_SEGMENT_ROW_COUNT`    | Row count                                                   |
| `LOGPOND_SEGMENT_PARQUET_SHA256` | SHA-256 of the Parquet (hex)                              |
| `LOGPOND_MANIFEST_VERSION`     | Manifest schema version (`1`)                               |
| `LOGPOND_INVOCATION_ID`        | Unique ID for correlation                                   |
| `LOGPOND_MODE`                 | `archive`, `verify`, or `retrieve`                          |
| Any `archive.script.env.*` config | Passed through verbatim                                  |

The same values are also passed as command-line flags.

**Exit codes:**

| Code  | Meaning                                                                         |
| ----- | ------------------------------------------------------------------------------- |
| 0     | Success.                                                                        |
| 1–63  | Generic failure. Service retries on next retention cycle.                       |
| 64    | Mode not supported. Service marks this mode unavailable.                        |
| 65    | Permanent failure. Service does not retry; operator must intervene.             |
| 75    | Temporary failure. Service retries on next cycle.                               |

These follow `sysexits.h` conventions.

**Stdout/stderr.** Captured to service logs (stdout=info, stderr=warn), tagged with invocation ID, stored in the `jobs` table for admin UI display.

**Verify mode.** Exit 0 = retrievable, non-zero = not retrievable.

**Retrieve mode.** Must write Parquet to `--output-parquet` and manifest to `--output-manifest`. Service validates SHA-256 against manifest after exit 0.

**Timeout.** Default 600s. SIGTERM, then SIGKILL after 30s grace.

**Concurrency.** One invocation at a time globally.

**Archive ref.** Script's final stdout line prefixed `LOGPOND_ARCHIVE_REF=` is stored in `archive_ref`; opaque to the service, shown in admin UI.

> **⚖ Trade-off.** Scripts give operators full control over destinations at the cost of harder verification and process-startup overhead. The verify mode contract recovers most verification; startup overhead is fine at archive rates of one segment per hour.

### 7.10 Rehydration & sideload

**In-place rehydrate.** `POST /api/rehydrate` with a time range. Service identifies overlapping archived segments and retrieves via S3 download or script retrieve mode. Catalog updated to `rehydrated` with `evict_after = now + ttl`.

**Already-local rehydrate.** No-op + TTL bump.

**Rehydrate availability.** If the backend doesn't support retrieve, returns 501 with `rehydrate_unsupported`.

**Sideload.** `POST /api/import` with multipart Parquet + manifest. Validates schema version and `parquet_sha256`. Always works regardless of backend.

**Time-overlap on sideload.** Accepted; UI flags overlap with a yellow indicator.

> **⚖ Trade-off.** Rejecting overlapping sideloads would prevent merging logs from different environments. We accept; the UI warning surfaces the situation.

**Watched directory.** `/data/import/` polled every 30s. Files picked up with a `.ready` marker. On success: moved to `imported/{id}/`. On failure: `failed/{id}/` with `error.txt`.

**TTL and persistence.** Default 7-day TTL. Persistence flags clear the TTL.

### 7.11 Configuration

Two sources, combinable:

- **YAML file.** Default `/etc/logpond/config.yaml`; override with `LOGPOND_CONFIG`.
- **Environment variables.** Take precedence. Naming: `LOGPOND_<UPPERCASE_DOTTED_PATH>`.

Source and facet definitions can be set as single JSON env vars (`LOGPOND_SOURCES_JSON`, `LOGPOND_FACETS_JSON`).

#### 7.11.1 Full configuration reference

| Config field                         | Env var                                 | Default                  | Reloadable |
| ------------------------------------ | --------------------------------------- | ------------------------ | ---------- |
| `listen_address`                     | `LOGPOND_LISTEN_ADDRESS`                | `0.0.0.0`                | No         |
| `port`                               | `LOGPOND_PORT` (or Dokku `PORT`)        | `8080`                   | No         |
| `data_dir`                           | `LOGPOND_DATA_DIR`                      | `/data`                  | No         |
| `segment_window`                     | `LOGPOND_SEGMENT_WINDOW`                | `1h`                     | No         |
| `sealing_interval`                   | `LOGPOND_SEALING_INTERVAL`              | `60s`                    | No         |
| `log_level`                          | `LOGPOND_LOG_LEVEL`                     | `info`                   | Yes        |
| `memory_limits.duckdb`               | `LOGPOND_MEMORY_LIMITS_DUCKDB`          | `128MB`                  | No         |
| `memory_limits.ring_buffer`          | `LOGPOND_MEMORY_LIMITS_RING_BUFFER`     | `50MB`                   | No         |
| `query.max_time_range`               | `LOGPOND_QUERY_MAX_TIME_RANGE`          | `7d`                     | Yes        |
| `facets.segment_sample_size`         | `LOGPOND_FACETS_SEGMENT_SAMPLE_SIZE`    | `24`                     | Yes        |
| `facets.default_cardinality_cap`     | `LOGPOND_FACETS_DEFAULT_CARDINALITY_CAP`| `50`                     | Yes        |
| `facets.builtin.service.cap`         | `LOGPOND_FACETS_BUILTIN_SERVICE_CAP`    | `100`                    | Yes        |
| `facets.builtin.level.cap`           | `LOGPOND_FACETS_BUILTIN_LEVEL_CAP`      | `10`                     | Yes        |
| `facets.builtin.host.cap`            | `LOGPOND_FACETS_BUILTIN_HOST_CAP`       | `50`                     | Yes        |
| `facets.builtin.source.cap`          | `LOGPOND_FACETS_BUILTIN_SOURCE_CAP`     | `50`                     | Yes        |
| `facets.custom` (YAML)               | `LOGPOND_FACETS_JSON`                   | (none)                   | Yes        |
| `live_tail.rate_cap`                 | `LOGPOND_LIVE_TAIL_RATE_CAP`            | `100`                    | Yes        |
| `live_tail.max_clients`              | `LOGPOND_LIVE_TAIL_MAX_CLIENTS`         | `20`                     | Yes        |
| `retention.max_age`                  | `LOGPOND_RETENTION_MAX_AGE`             | (unset)                  | Yes        |
| `retention.max_size`                 | `LOGPOND_RETENTION_MAX_SIZE`            | (unset)                  | Yes        |
| `retention.archive_before_delete`    | `LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE` | `true`                 | Yes        |
| `retention.evaluation_interval`      | `LOGPOND_RETENTION_EVALUATION_INTERVAL` | `5m`                     | Yes        |
| `rehydration.ttl_days`               | `LOGPOND_REHYDRATION_TTL_DAYS`          | `7`                      | Yes        |
| `archive.backend`                    | `LOGPOND_ARCHIVE_BACKEND`               | `none`                   | No         |
| `archive.s3.endpoint`                | `LOGPOND_ARCHIVE_S3_ENDPOINT`           | (unset)                  | Yes        |
| `archive.s3.bucket`                  | `LOGPOND_ARCHIVE_S3_BUCKET`             | (unset)                  | Yes        |
| `archive.s3.prefix`                  | `LOGPOND_ARCHIVE_S3_PREFIX`             | `logpond/`               | Yes        |
| `archive.s3.region`                  | `LOGPOND_ARCHIVE_S3_REGION`             | `auto`                   | Yes        |
| `archive.s3.access_key_id`           | `LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID`      | (unset)                  | Yes        |
| `archive.s3.secret_access_key`       | `LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY`  | (unset)                  | Yes        |
| `archive.script.path`                | `LOGPOND_ARCHIVE_SCRIPT_PATH`           | (unset)                  | Yes        |
| `archive.script.timeout`             | `LOGPOND_ARCHIVE_SCRIPT_TIMEOUT`        | `600s`                   | Yes        |
| `archive.script.env.*`               | `LOGPOND_ARCHIVE_SCRIPT_ENV__*`         | (none)                   | Yes        |
| `sources` (YAML)                     | `LOGPOND_SOURCES_JSON`                  | one `default` source     | Yes        |
| `theme.default`                      | `LOGPOND_THEME_DEFAULT`                 | `auto`                   | Yes        |

`facets.segment_sample_size` controls the N most recent sealed segments scanned for facet values and autocomplete.

`facets.builtin.<name>.cap` sets per-built-in caps; custom facets specify their own in the `facets.custom` array entry.

#### 7.11.2 Reloadable vs. non-reloadable

Reloadable fields change via `SIGHUP` or `POST /api/admin/reload`. Non-reloadable require restart.

#### 7.11.3 Theme default

`theme.default` controls the default for new sessions. Existing `localStorage` preferences always win.

## 8. Non-functional requirements

### 8.1 Performance

| Metric                                | Target                                      |
| ------------------------------------- | ------------------------------------------- |
| Sustained ingest rate                 | 1,000 events/sec at steady state            |
| Burst ingest rate                     | 5,000 events/sec for up to 10 seconds       |
| Ingest endpoint p99 latency           | < 50ms at sustained rate                    |
| Query latency, 1-hour range, core-column filters | < 500ms p95 (incl. facets)       |
| Query latency, 24-hour range, core-column filters | < 2s p95 (incl. facets)         |
| Query latency, attribute-filter dominant queries | best-effort; may be 2–5× slower |
| Search-suggest latency                | < 100ms p95                                 |
| Query count latency                   | < 100ms p95                                 |
| Search-bar parse latency              | < 5ms p99                                   |
| Live tail end-to-end delay            | < 2s from ingest to browser                 |
| Segment seal duration                 | < 5s for a one-hour segment at target rate  |
| UI first contentful paint             | < 500ms on a local network                  |
| UI total client asset size (gzipped)  | < 60KB                                      |

### 8.2 Resource footprint

| Component                      | Steady     | Peak       |
| ------------------------------ | ---------- | ---------- |
| DuckDB active segment + query buffers | 80MB | 120MB      |
| Ingest ring buffer             | 5–10MB     | 50MB       |
| Go runtime + HTTP servers + catalog + UI | 30–50MB | 50MB |
| Query execution headroom       | 20MB       | 40MB       |
| **Total**                      | **150–200MB** | **~256MB** |

`GOMEMLIMIT=240MiB`; DuckDB `memory_limit=128MB`.

**Memory pressure.** Watchdog sheds live-tail clients (oldest first) when RSS exceeds 95% of `GOMEMLIMIT` for >30s.

**Script subprocesses.** Not counted in the 256MB budget. Typical 20–80MB; one at a time globally.

Other targets:

| Resource              | Target                                       |
| --------------------- | -------------------------------------------- |
| CPU (steady-state)    | < 10% of one core at 1,000 events/sec        |
| CPU (during seal)     | < 50% of one core                            |
| Disk write throughput | < 5MB/sec at target ingest rate              |
| Disk usage, active segment | ~1.8GB per hour (uncompressed DuckDB)   |
| Disk usage, sealed segment | ~180MB per hour (Parquet + zstd-3)      |
| Disk usage per day, sealed only | ~4.3GB at target rate              |
| Disk usage per day, total | ~6.1GB peak before retention             |

### 8.3 Durability & data integrity

- Events accepted with 202 are buffered in RAM; flush to DuckDB every 1s (or at 50% capacity). WAL fsync every 5s.
- Vector's disk buffer is the durability source of truth.
- Sealed segments written atomically (temp → fsync → rename → catalog → delete).
- Catalog uses SQLite transactions; recovery reconciles catalog and filesystem on startup.

> **⚖ Trade-off.** 5s fsync trades a small RAM-loss window against lower I/O.

### 8.4 Operability

- Structured JSON logs to stdout.
- `GET /healthz` → 200 / 503. `GET /metrics` → Prometheus format.
- Startup <5s, no external service required.
- Both archive backends optional. `archive.backend: none` disables archival UI.

### 8.5 Compatibility & portability

- Multi-arch container (`linux/amd64`, `linux/arm64`).
- Parquet: zstd 1–22, default 3.
- Independently versioned manifest and Parquet schema.
- Timestamps `TIMESTAMP_MICROS` with `isAdjustedToUTC=true`.

## 9. Architecture overview

Single Go binary with three subsystems: ingest server, query/UI server, lifecycle manager. They share the catalog and an in-process job queue.

The archive subsystem is pluggable behind an `ArchiveBackend` interface (`S3Backend`, `ScriptBackend`).

On-disk artifacts:

- `/data/segments/active/{id}.duckdb`
- `/data/segments/sealed/{id}.parquet`
- `/data/rehydrated/{id}.parquet`
- `/data/catalog.db`
- `/data/import/`
- `/data/orphans/`
- `/data/script-work/`

## 10. Data model

### 10.1 Log event row

See §7.2.

### 10.2 Catalog schema (SQLite)

```sql
CREATE TABLE segments (
  id              TEXT PRIMARY KEY,
  state           TEXT NOT NULL,           -- active|sealed|archived|rehydrated|lost
  time_start      TIMESTAMP NOT NULL,
  time_end        TIMESTAMP NOT NULL,
  row_count       INTEGER NOT NULL,
  size_bytes      INTEGER NOT NULL,
  size_compressed INTEGER,
  local_path      TEXT,
  s3_url          TEXT,
  archive_ref     TEXT,
  manifest_sha256 TEXT,
  parquet_sha256  TEXT,
  source_names    TEXT,                    -- JSON array
  created_at      TIMESTAMP NOT NULL,
  sealed_at       TIMESTAMP,
  archived_at     TIMESTAMP,
  rehydrated_at   TIMESTAMP,
  evict_after     TIMESTAMP
);

CREATE INDEX idx_segments_time ON segments(time_start, time_end);
CREATE INDEX idx_segments_state ON segments(state);

CREATE TABLE saved_queries (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  query_json TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE jobs (
  id            TEXT PRIMARY KEY,
  type          TEXT NOT NULL,             -- archive|rehydrate|verify
  state         TEXT NOT NULL,             -- pending|running|completed|failed
  payload_json  TEXT NOT NULL,
  progress_json TEXT,
  script_stdout TEXT,
  script_stderr TEXT,
  started_at    TIMESTAMP,
  finished_at   TIMESTAMP,
  error_json    TEXT
);

CREATE INDEX idx_jobs_state ON jobs(state);

CREATE TABLE custom_facets (
  name              TEXT PRIMARY KEY,
  field             TEXT NOT NULL,
  display_label     TEXT,
  cardinality_cap   INTEGER,
  value_type        TEXT NOT NULL DEFAULT 'string',
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP NOT NULL,
  source            TEXT NOT NULL          -- 'ui' or 'config'
);
```

### 10.3 Manifest format

```json
{
  "manifest_version": 1,
  "schema_version": 1,
  "segment_id": "202605121400",
  "time_start": "2026-05-12T14:00:00Z",
  "time_end": "2026-05-12T15:00:00Z",
  "row_count": 3600000,
  "size_bytes": 1843200000,
  "size_compressed": 184320000,
  "compression": "zstd",
  "compression_level": 3,
  "source_names": ["default", "nginx"],
  "parquet_filename": "segment-202605121400.parquet",
  "parquet_sha256": "...",
  "persistent": false
}
```

## 11. Frontend stack

Logpond's web UI is built on a deliberately minimal stack: **server-rendered HTML with HTMX for server interactions, Alpine.js for small bits of client state, and Open Props with a small custom stylesheet for visuals.** No JavaScript build step, no SPA framework, no bundler.

### 11.1 Why this stack

The PRD constraints — under-256MB memory budget, single-container deployment, single-Dockerfile-push Dokku flow — make a Node-based build pipeline a poor fit. The intended user is also unlikely to want to debug a webpack config to make a small change. The chosen stack composes from three small libraries delivered as plain `<script>`/`<link>` tags, served from the Go binary's embedded static assets.

> **⚖ Trade-off.** Choosing no SPA framework and no build step means more imperative code in vanilla JS and Alpine for complex components (the Advanced filter-tree editor being the worst case) than would be needed in React/Svelte. We accept this in exchange for deployment simplicity (single Dockerfile push, no Node in CI), smaller asset payload, and a smaller cognitive footprint for contributors. §11.6 documents the escape hatch if a single component proves intractable.

The total client-side dependency footprint is approximately:

| Asset                                   | Size (gzipped) |
| --------------------------------------- | -------------- |
| HTMX core                               | ~14KB          |
| HTMX WebSocket extension                | ~2KB           |
| Alpine.js                               | ~15KB          |
| Open Props (cherry-picked)              | ~10KB          |
| Logpond custom CSS                      | ~6–10KB        |
| Logpond custom JS                       | ~4–6KB         |
| Lucide icons (used subset, inlined SVG) | ~3KB           |
| **Total**                               | **~55KB**      |

This meets the approximately-60KB target (the budget is not contractual).

### 11.2 What each library does

**HTMX** handles all server interactions that aren't WebSocket-driven. The pattern is: an HTML element has `hx-post`, `hx-get`, or similar attributes; HTMX intercepts the appropriate event (click, keyup, form submit), makes the HTTP request, and swaps the response HTML into a designated target element. The server returns HTML fragments, not JSON, for these endpoints.

Fragment-returning endpoints live under the `/ui/` URL prefix to make the distinction explicit. They are not part of the public API contract and may change between minor versions; programmatic clients use the JSON `/api/*` endpoints (§13). The specific bindings:

- **Search bar autocomplete.** `hx-post="/ui/search-suggest" hx-trigger="keyup changed delay:50ms" hx-target="#suggestions"`. The server returns a small `<ul>` of suggestion `<li>` elements, replacing the previous list.
- **Live result count.** `hx-post="/ui/query/count" hx-trigger="keyup changed delay:300ms" hx-target="#count-indicator"`. The server returns `<span>≈ 1,243 events</span>`.
- **Result list.** The Search form submits with `hx-post="/ui/query"` to load the results as HTML. "Load more" appends via `hx-swap="beforeend"`.
- **Facet sidebar checkbox toggles.** Each checkbox is wired (via Alpine) to rewrite the search bar's value and trigger a re-search through the same `/ui/query` endpoint.
- **Admin view actions.** Archive, evict, import use `hx-post="/ui/archive"`, `hx-delete="/ui/rehydrated/:id"`, `hx-post="/ui/import"` respectively, with the response replacing the row or the surrounding table.

The query endpoints in §13.4 of the API spec serve JSON to programmatic clients; the `/ui/` versions wrap the same logic and render the response via Go templates.

**HTMX's WebSocket extension** drives the Live tail view. The element has `hx-ext="ws" ws-connect="/api/query/stream"`. Incoming messages are server-rendered HTML rows that HTMX prepends to the result list. The subscription protocol (which is JSON, per §13.7) is sent via a separate hidden form on subscribe and on filter changes. The live tail uses the public WebSocket endpoint directly because there's no useful HTML-fragment wrapper for streaming — the server still renders HTML per event, and HTMX inserts those fragments as they arrive.

**Alpine.js** handles client-side state that doesn't need a server round-trip:

- **Theme toggle.** A `x-data="{theme: localStorage.getItem('logpond.theme') || 'auto'}"` wraps the top bar; the toggle button switches between `auto`/`light`/`dark` and writes the chosen value into `localStorage`. For `light` and `dark`, the value is also written to `<html data-theme="...">`. For `auto`, the `data-theme` attribute is *removed* so the `prefers-color-scheme` media query takes effect.
- **Time-zone toggle.** Same pattern with `localStorage.getItem('logpond.tz')` driving timestamp re-rendering.
- **Facet sidebar local state.** Which sections are expanded/collapsed, which values are checked. The checked state mirrors what's in the search bar; clicking a checkbox composes the new search-bar string and triggers HTMX to re-run the query.
- **Search bar suggestion navigation.** Keyboard handling for Up/Down/Tab/Enter/Esc within the suggestion dropdown.
- **Live tail controls.** Pause auto-scroll, clear results, stop tail — local state with simple event handlers.
- **The Advanced filter-tree editor.** Adding/removing groups and leaves. The tree is rendered server-side initially; subsequent mutations are Alpine-driven, with the resulting tree serialized into a hidden form field on submit.

Alpine's reactivity model handles all of these without a separate state-management library. Each component is a small `x-data` block scoped to its parent element.

**Open Props** provides design tokens (CSS custom properties for spacing, colors, type, shadows) consumed by the custom stylesheet. See §12 for full visual style details.

### 11.3 Asset delivery

All client-side assets are embedded in the Go binary using `embed.FS` and served from `/static/`. The Go server sets long `Cache-Control` headers (one year with content-hashed filenames) so browser caching is effective. Pre-compressed `.gz` and `.br` variants are stored alongside originals for serving with `Content-Encoding`.

No CDN dependency. All assets ship inside the container.

### 11.4 Browser support

Modern evergreen browsers only: latest two major versions of Chrome, Firefox, Safari, and Edge. Edge cases (older mobile Safari, IE) are out of scope; the UI degrades gracefully (HTML still renders without JS, but interactivity is lost) but is not tested.

### 11.5 No-JavaScript fallback

The Search view's core functionality (issue a query, see results) works without JavaScript via a traditional form POST to `/ui/query` + full-page response. Autocomplete, live count, the facet sidebar's reactivity, and Live tail require JavaScript and are noted as such in the UI when JS is disabled. This is not a primary supported configuration; it's a graceful-degradation property of the chosen stack.

### 11.6 Escape hatch

If a future component proves intractable with HTMX + Alpine (the Advanced filter-tree editor is the most likely candidate), the team may introduce **Preact** (3KB) with **htm** template literals for that component specifically. Preact mounts to a single DOM element, so it can coexist with the rest of the HTMX-driven UI without restructuring anything else. This is an explicitly-allowed v1 path, not a future-only consideration.

## 12. Visual style

### 12.1 Aesthetic target

Logpond's UI is a developer tool, not a consumer app or a marketing site. The target is **information-dense, calm, and quick to scan**. Visual references:

- **Datadog Log Explorer** for the layout and search-pattern conventions.
- **Linear** for spacing, type hierarchy, and dark-mode palette discipline.
- **Grafana Loki Explore** for layout reference, though Logpond should be visually quieter (less chrome).
- **GitHub's diff and search views** for table density and monospace usage.

What Logpond explicitly is *not*: card-grid marketing layouts, Material-style heavy elevation, animated illustrations, or generous whitespace that wastes vertical screen real estate.

### 12.2 Type

**Sans-serif (primary).** The system font stack: `system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif`. No web fonts are downloaded — this saves bytes, matches the operator's OS, and renders fast.

**Monospace.** The system mono stack: `ui-monospace, "SF Mono", "Cascadia Mono", "Roboto Mono", Consolas, monospace`. Used for: timestamps, the `raw` field in expanded result rows, attribute values in expanded rows, the search bar input itself (so users see exactly which characters are present), and any code-like UI element.

**Sizes.** Most of the UI is at 13–14px, not 16px. Open Props' built-in `--font-size-*` scale doesn't match these density-tuned values directly (it goes `12px → 16px → 17.6px → 20px → ...`), so Logpond defines its own semantic font-size variables in the custom stylesheet:

| Variable                | Value     | Used for                                             |
| ----------------------- | --------- | ---------------------------------------------------- |
| `--logpond-text-xs`     | `0.75rem` (12px) | Timestamps in collapsed rows; metadata          |
| `--logpond-text-sm`     | `0.8125rem` (13px) | Result row text; facet sidebar values         |
| `--logpond-text-base`   | `0.875rem` (14px) | Body text; search bar input                    |
| `--logpond-text-md`     | `1rem` (16px) | Section headers                                    |

Defining custom font tokens (rather than mapping onto `--font-size-*`) is a deliberate choice: Open Props' scale is tuned for content-focused sites, and forcing the dense developer-tool aesthetic onto it would either misuse the tokens or require offsetting the root font size. Custom semantic tokens keep the meaning clear in CSS.

**Line heights.** `1.4` for body, `1.25` for collapsed rows where vertical density matters. These map roughly to Open Props' `--font-lineheight-2` (1.375) and `--font-lineheight-1` (1.25); the custom stylesheet uses literal values rather than tokens because the small difference matters for row alignment.

### 12.3 Color

Logpond consumes Open Props' palettes (`--gray-*`, `--blue-*`, `--red-*`, etc.) directly. The custom stylesheet does not invent new color values; it composes Open Props tokens into semantic role variables.

**Light-mode surfaces:**

| Role                  | Token                       |
| --------------------- | --------------------------- |
| Page background       | `--gray-0` (near-white)     |
| Panel background      | `--gray-1`                  |
| Subtle border         | `--gray-2`                  |
| Strong border         | `--gray-4`                  |
| Body text             | `--gray-10`                 |
| Muted text            | `--gray-7`                  |

**Dark-mode surfaces:**

| Role                  | Token                       |
| --------------------- | --------------------------- |
| Page background       | `--gray-12` (near-black)    |
| Panel background      | `--gray-11`                 |
| Subtle border         | `--gray-9`                  |
| Strong border         | `--gray-7`                  |
| Body text             | `--gray-1`                  |
| Muted text            | `--gray-5`                  |

**Theme switching.** Light is the default; dark is applied via either user system preference or explicit override. The CSS is structured as three layers:

```css
:root {
  /* light tokens (default) */
}

@media (prefers-color-scheme: dark) {
  :root {
    /* dark tokens — applied when no manual override */
  }
}

[data-theme="dark"] {
  /* dark tokens — applied for manual override regardless of system */
}

[data-theme="light"] {
  /* light tokens — applied for manual override regardless of system */
}
```

The Auto mode is the *absence* of `data-theme` on `<html>`; the JS toggle adds or removes the attribute. This means CSS-only auto-mode (no JS) works correctly with system preferences, and the JS only handles the manual overrides.

**Accent color: a single blue.** Used for the primary "Search" button, focus rings, the live tail's ● live indicator, count badges, and the underline on active nav links. One accent throughout — additional hues make the UI feel like a paint-by-numbers.

| Mode  | Token       |
| ----- | ----------- |
| Light | `--blue-6`  |
| Dark  | `--blue-4`  |

Both tokens are tuned for WCAG AA contrast against their respective surface colors.

**Log level chips.** The one place additional hues are mandatory. Filled chips only for `warn` and `error` — `info` and `debug` are text-only so a screen of info messages doesn't look alarmed.

| Level | Light chip                                    | Dark chip                                     |
| ----- | --------------------------------------------- | --------------------------------------------- |
| debug | `--gray-6` text, transparent bg               | `--gray-5` text, transparent bg               |
| info  | `--blue-7` text, transparent bg               | `--blue-4` text, transparent bg               |
| warn  | `--orange-6` text, `--orange-0` bg            | `--orange-3` text, transparent bg, `--orange-9` left-border |
| error | `--red-0` text, `--red-7` bg                  | `--red-2` text, `--red-9` bg                  |

This is closer to Linear's restraint than Datadog's color-everything approach.

**Status indicators.** ● green (reachable), ● yellow (degraded), ● red (failing). Used for archive backend status, live tail connection state, and similar one-dot indicators.

| State    | Light                | Dark                 |
| -------- | -------------------- | -------------------- |
| Healthy  | `--green-6`          | `--green-4`          |
| Degraded | `--yellow-6`         | `--yellow-4`         |
| Failing  | `--red-6`            | `--red-4`            |

**Never color alone.** Every error or warning indicator pairs the color with text, an icon, or a shape.

### 12.4 Spacing and layout

**Base unit: 4px.** Logpond uses Open Props' `--size-px-*` scale (which is pixel-exact) rather than the rem-based `--size-*` scale, because density-tuned UIs benefit from spacing that doesn't shift when users change their browser font size. The relevant tokens:

| Token        | Value | Common uses                                     |
| ------------ | ----- | ----------------------------------------------- |
| `--size-px-1` | 4px  | Tightest gaps (chip padding, badge margins)     |
| `--size-px-2` | 8px  | Adjacent elements within a row, filter gaps     |
| `--size-px-3` | 16px | Between major sections; sidebar/results gap     |
| `--size-px-4` | 20px | (occasionally) larger panel gaps                |
| `--size-px-5` | 24px | Page edge padding on wide layouts               |
| `--size-px-7` | 32px | Above/below large section breaks                |

> **⚖ Trade-off.** Pixel-exact spacing (`--size-px-*`) gives predictable density but ignores user browser-zoom for spacing. Font sizes remain in rem so text *does* scale with user zoom, which is the more important accessibility property. The layout density stays constant; only the text inside it grows.

**Common gaps:**

| Context                          | Gap                            |
| -------------------------------- | ------------------------------ |
| Adjacent elements within a row   | 8px (`--size-px-2`)            |
| Within a facet sidebar item      | 8px                            |
| Between filter inputs in a group | 8px                            |
| Between major sections           | 16px (`--size-px-3`)           |
| Page edge padding                | 16–24px (`--size-px-3`/`--size-px-5`)|

**Row heights:**

| Element                       | Height                         |
| ----------------------------- | ------------------------------ |
| Collapsed result row          | 28px                           |
| Facet sidebar item            | 26px                           |
| Form control (input, button)  | 32px                           |
| Search bar                    | 40px (larger interaction target)|
| Admin segment table row       | 36px (more room for actions)   |

**Layout primitives.** Flexbox for horizontal arrangements; CSS Grid for the Search view's three-column layout (sidebar | results | footer). No grid framework — the layouts are simple enough that a handful of grid declarations cover everything.

### 12.5 Borders, shadows, radii

**Borders, not shadows.** Section separation is via 1px borders using the semantic border tokens (`--border-subtle`, `--border-strong`). Shadows are visual noise in a dense UI.

**Exception: dropdowns.** The autocomplete suggestion list, the theme toggle popover, and modal dialogs use Open Props' `--shadow-2` because they need to visually float above the rest of the page.

**Radii.** Open Props' built-in `--radius-*` scale (`--radius-1: 2px`, `--radius-2: 5px`, `--radius-3: 1rem`) doesn't match the small, consistent 4px/8px values that the developer-tool aesthetic calls for. Logpond defines two semantic radius variables in the custom stylesheet:

| Variable                  | Value | Used for                          |
| ------------------------- | ----- | --------------------------------- |
| `--logpond-radius-control`| 4px   | Buttons, inputs, chips, panels    |
| `--logpond-radius-modal`  | 8px   | Modal dialogs                     |

No fully-rounded "pill" shapes anywhere — they feel out of place in a developer-tool aesthetic.

### 12.6 Iconography

Icons are from **Lucide**, inlined as SVG (no icon font, no separate file requests). Approximately 15–20 distinct icons across the whole UI, totaling ~3KB gzipped.

Where icons appear:

| Location                       | Icon                          |
| ------------------------------ | ----------------------------- |
| Theme toggle                   | sun / moon / monitor (auto)   |
| Search bar left edge           | search (magnifier)            |
| Search bar right edge (clear)  | x                             |
| Result row expand/collapse     | chevron-right / chevron-down  |
| Sidebar section expand/collapse| chevron-down / chevron-right  |
| Admin: archive action          | upload-cloud                  |
| Admin: rehydrate action        | download-cloud                |
| Admin: evict action            | trash-2                       |
| Admin: edit action             | pencil                        |
| Admin: add facet               | plus                          |
| Live tail: start               | play (filled)                 |
| Live tail: pause               | pause                         |
| Live tail: stop                | square                        |
| Status dots (●)                | unicode bullet (not SVG)      |

Icons render at 16px next to 13–14px text. Don't add icons for their own sake; if a button label is clear, no icon is needed.

### 12.7 Forms

**Inputs.** Thin 1px border (`--border-subtle`), 4px radius (`--logpond-radius-control`), no shadow. Focus ring uses the accent color: `outline: 2px solid var(--accent); outline-offset: 1px;`.

**Buttons.** Three types only:

- **Primary** (filled with accent color): the main action on a view ("Search", "Save query", "Archive now"). One per screen, ideally.
- **Secondary** (transparent fill, 1px border, body text color): all non-primary actions. The default button.
- **Destructive** (transparent fill, red border, red text): delete, evict.

**Checkboxes.** Browser-native via `accent-color: var(--accent);`. Don't reinvent.

**Dropdowns.** Native `<select>` for simple choices (time-range presets). Custom popover for autocomplete and the theme toggle (where the native control doesn't fit the design).

### 12.8 Motion

Developer tools that animate transitions feel slow. Logpond defaults to **no motion**, with three specific exceptions:

| Animation                          | Duration / Easing      |
| ---------------------------------- | ---------------------- |
| Sidebar section expand/collapse    | 150ms ease-out         |
| Autocomplete dropdown fade-in      | 80ms linear            |
| Live tail new-row highlight (briefly highlighted then fade) | 1000ms ease-out |

The theme switch is **instant** — no transition. Half-faded color shifts look broken; immediate switching feels deliberate.

All motion respects `@media (prefers-reduced-motion: reduce)` — animations become instant.

### 12.9 Accessibility

These are not optional, even in v1:

- **Keyboard navigation everywhere.** Tab order matches visual order. Search-bar autocomplete is fully keyboard-navigable (Up/Down/Enter/Tab/Esc/Ctrl-Space).
- **Focus rings always visible** when navigating by keyboard. Don't suppress them.
- **WCAG AA contrast** for all text/background combinations. Open Props' default token combinations are designed to meet AA in both modes; verify any custom combinations.
- **ARIA labels** on icon-only buttons.
- **Skip-to-content link** at the top of each page (visible on focus, otherwise hidden).
- **Semantic HTML.** `<button>` for buttons, `<a>` for links. Never `<div onclick>`.
- **Form labels.** Every input has a `<label>` (visible or visually-hidden where space requires).

### 12.10 Empty and error states

**Empty results.** "No events match." plus a one-line hint ("Try a wider time range or remove a filter.") plus, in muted text, the active query summary so the operator can see what they searched.

**Loading.** A thin 1px indeterminate progress bar at the top of the affected area (results list, facet sidebar). No full-screen spinners — they hide previous results and feel slow.

**Inline error.** A small panel with the error code, message, and details. Red left-border, otherwise quiet. The surrounding UI remains usable.

**WebSocket disconnect.** The live tail's status indicator shifts to ● disconnected; a small inline notice offers a "Reconnect" button. Previous events stay visible.

### 12.11 Density vs. spacious mode

Logpond does not provide a "comfortable" / "compact" toggle in v1. The density chosen above is the only mode. If user feedback strongly requests it, a future version may add a single toggle that swaps spacing tokens.

## 13. API specification

### 13.1 General conventions

- JSON bodies; `application/x-ndjson` for ingest.
- RFC 3339 timestamps with microsecond precision.
- Uniform error envelope:

  ```json
  {
    "error": {
      "code": "invalid_filter",
      "message": "Filter operator 'fuzzy_match' is not supported.",
      "details": { "field": "filters[2].op" }
    }
  }
  ```

- Stable error codes: `unknown_source`, `invalid_filter`, `invalid_query_syntax`, `filter_too_deep`, `invalid_time_range`, `time_range_too_large`, `buffer_full`, `segment_not_found`, `segment_busy`, `s3_unavailable`, `script_unavailable`, `rehydrate_unsupported`, `manifest_invalid`, `cursor_invalidated`, `field_not_reloadable`, `facet_conflict`, `bad_request`, `payload_too_large`, `too_many_clients`, `internal_error`.
- Trailing slashes redirect (308). Wrong method returns 405 with `Allow`.
- CORS: GET/OPTIONS allow `*`; mutations same-origin only.

The Go server also exposes HTML-fragment-returning sibling endpoints under `/ui/` (e.g., `/ui/query`, `/ui/search-suggest`, `/ui/query/count`, `/ui/archive`, `/ui/rehydrated/:id`, `/ui/import`) that the HTMX-driven UI consumes. These are not part of the public API contract and may change between minor versions; programmatic clients use the JSON `/api/*` endpoints exclusively.

### 13.2 `POST /ingest/:source_name`

**Purpose.** Accept a batch from Vector.

**Request headers.** `Content-Type: application/x-ndjson` or `application/json`. Optional `Content-Encoding: gzip`.

**Request body.** NDJSON.

```
{"timestamp":"2026-05-12T14:32:01.123Z","level":"info","service":"api","message":"user 42 logged in","user_id":42}
{"timestamp":"2026-05-12T14:32:01.250Z","level":"error","service":"api","message":"db connection refused","err":"ECONNREFUSED"}
```

**Response: 202 Accepted.**

```json
{ "accepted": 2, "skipped": 0 }
```

**Other responses.** 404 (`unknown_source`), 413 (`payload_too_large`), 415, 429 (`buffer_full` with `Retry-After: 1`).

### 13.3 `POST /api/parse-query`

**Purpose.** Parse a search-bar string into the canonical filter tree without executing.

**Request body.**

```json
{ "q": "service:api (level:error OR @duration_ms:>1000) -host:debug-*" }
```

**Response: 200 OK.**

```json
{
  "filter": {
    "op": "and",
    "children": [
      { "field": "service", "op": "eq", "value": "api" },
      {
        "op": "or",
        "children": [
          { "field": "level", "op": "eq", "value": "error" },
          { "field": "attributes.duration_ms", "op": "gt", "value": 1000 }
        ]
      },
      {
        "op": "and",
        "not": true,
        "children": [
          { "field": "host", "op": "starts_with", "value": "debug-" }
        ]
      }
    ]
  },
  "search": null,
  "warnings": []
}
```

**Response: 400 Bad Request.** Malformed syntax (`invalid_query_syntax` with column number).

### 13.4 `POST /api/query`

**Purpose.** Execute a search across stored log segments.

**Request body.** Either form:

**Form A — search-bar string.**

```json
{
  "time_range": { "from": "2026-05-12T10:00:00Z", "to": "2026-05-12T11:00:00Z" },
  "q":          "service:api level:(error OR warn) \"connection refused\"",
  "facets":     ["service", "level", "host", "user_id"],
  "sort":       [{ "field": "timestamp", "order": "desc" }],
  "limit":      100,
  "cursor":     null
}
```

**Form B — canonical filter tree.**

```json
{
  "time_range": { "from": "2026-05-12T10:00:00Z", "to": "2026-05-12T11:00:00Z" },
  "filter": {
    "op": "and",
    "children": [
      { "field": "service", "op": "eq", "value": "api" },
      { "field": "level",   "op": "in", "value": ["error", "warn"] }
    ]
  },
  "search":     "connection refused",
  "facets":     ["service", "level", "host", "user_id"],
  "sort":       [{ "field": "timestamp", "order": "desc" }],
  "limit":      100,
  "cursor":     null
}
```

**Form C — legacy flat filters.**

```json
{
  "time_range": { ... },
  "filters":    [
    { "field": "service", "op": "eq", "value": "api" }
  ]
}
```

`q`, `filter`, and `filters` are mutually exclusive (one or none).

**Response: 200 OK.**

```json
{
  "events": [
    {
      "timestamp":  "2026-05-12T10:58:14.221000Z",
      "service":    "api",
      "level":      "error",
      "message":    "db connection refused",
      "host":       "app-1",
      "source":     "default",
      "attributes": { "user_id": 42, "request_id": "r-882", "err": "ECONNREFUSED" },
      "raw":        "{...}"
    }
  ],
  "next_cursor": "eyJ0cyI6...",
  "stats": {
    "rows_scanned":  143821,
    "rows_returned": 100,
    "segments_read": 2,
    "duration_ms":   312
  },
  "facets": {
    "service": {
      "values": [
        { "value": "api",     "count": 12420 },
        { "value": "worker",  "count": 3801 }
      ],
      "cardinality_cap": 100,
      "truncated":       false,
      "sample_segments": 24
    },
    "level": {
      "values": [
        { "value": "info",  "count": 14210 },
        { "value": "warn",  "count": 2100 },
        { "value": "error", "count": 1013 }
      ],
      "cardinality_cap": 10,
      "truncated":       false,
      "sample_segments": 24
    },
    "user_id": {
      "values": [
        { "value": "42",  "count": 821 },
        { "value": "99",  "count": 412 }
      ],
      "cardinality_cap": 25,
      "truncated":       true,
      "truncated_count": 1240,
      "sample_segments": 24
    }
  }
}
```

**Other responses.** 400 (`invalid_filter`, `invalid_query_syntax`, `time_range_too_large`, `filter_too_deep`), 410 (`cursor_invalidated`).

### 13.5 `POST /api/query/count`

**Purpose.** Return only the result count for a query.

**Request body.** Same as `/api/query`; `sort`, `limit`, `cursor`, `facets` are ignored if present.

**Response: 200 OK.**

```json
{ "count": 1243, "exact": true, "duration_ms": 42 }
```

Above 10,000:

```json
{ "count": 10000, "exact": false, "lower_bound": true, "duration_ms": 18 }
```

### 13.6 `POST /api/search-suggest`

**Purpose.** Context-aware autocomplete.

**Request body.**

```json
{ "q": "service:api level:", "cursor_pos": 18, "time_range": { "from": "now-1h", "to": "now" } }
```

**Response: 200 OK (value context).**

```json
{
  "context": "value",
  "field":   "level",
  "suggestions": [
    { "text": "info",  "kind": "value", "count": 14210 },
    { "text": "warn",  "kind": "value", "count": 2100 },
    { "text": "error", "kind": "value", "count": 1013 }
  ]
}
```

**Response: 200 OK (field context).**

```json
{
  "context": "field",
  "prefix":  "le",
  "suggestions": [
    { "text": "level", "kind": "core", "description": "Log level" }
  ]
}
```

**Response: 200 OK (combinator context).**

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

### 13.7 `GET /api/query/stream` (WebSocket)

**Purpose.** Stream matching events in near-real-time.

**Connection.** HTTP GET upgraded to WebSocket.

**Client → server subscribe.** Accepts `q`, `filter`, or `filters`:

```json
{ "q": "service:api level:error" }
```

**Server → client messages.**

```json
{ "type": "event", "event": { /* same shape as /api/query */ } }
```

```json
{ "type": "status", "subscribed": true, "rate_capped": false, "events_dropped": 0, "filter_summary": "service = api AND level = error" }
```

**Close codes.** 4001 `filter_required`, 4002 `invalid_filter`, 4003 `slow_client`, 4004 `too_many_clients`, 1011, 1001.

### 13.8 `GET /api/segments`

**Purpose.** List segments. Backs the Admin view.

**Query parameters.** `state` (repeatable), `from`, `to`, `source`, `limit`, `offset`.

**Response: 200 OK.**

```json
{
  "segments": [
    {
      "id":              "202605121400",
      "state":           "sealed",
      "time_start":      "2026-05-12T14:00:00Z",
      "time_end":        "2026-05-12T15:00:00Z",
      "row_count":       3598221,
      "size_bytes":      1841299832,
      "size_compressed": 183921002,
      "local_path":      "/data/segments/sealed/202605121400.parquet",
      "s3_url":          null,
      "archive_ref":     null,
      "source_names":    ["default", "nginx"],
      "sealed_at":       "2026-05-12T15:00:04Z",
      "archived_at":     null,
      "rehydrated_at":   null,
      "evict_after":     null,
      "overlaps":        []
    }
  ],
  "total": 482, "limit": 100, "offset": 0
}
```

### 13.9 `POST /api/archive`

**Request.** Exactly one of `segment_id` or `older_than`.

**Response: 202 Accepted.**

```json
{ "job_id": "arc-7c1f", "segments": ["202605121214"], "status_url": "/api/jobs/arc-7c1f" }
```

**Other responses.** 400, 404, 503 (`s3_unavailable` or `script_unavailable`), 501 (when `archive.backend: none`).

### 13.10 `POST /api/rehydrate`

**Request.**

```json
{ "time_range": { "from": "2026-04-12T00:00:00Z", "to": "2026-04-13T00:00:00Z" }, "ttl_days": 7, "persistent": false }
```

**Response: 202 Accepted.**

```json
{
  "job_id":            "reh-3a02",
  "segments":          ["202604120000", "..."],
  "segment_count":     24,
  "total_bytes":       4423800000,
  "estimated_seconds": 90,
  "status_url":        "/api/jobs/reh-3a02"
}
```

**Other responses.** 404, 501 (`rehydrate_unsupported`), 503.

### 13.11 `DELETE /api/rehydrated/:id`

**Response.** 204, 404, or 409 (`segment_busy`).

### 13.12 `POST /api/import`

**Request.** `multipart/form-data` with `parquet` and `manifest` parts. Query param `persistent`. Max 5GB.

**Response: 201 Created.**

```json
{
  "segment_id":  "202604120100",
  "state":       "rehydrated",
  "row_count":   3601001,
  "size_bytes":  1842500000,
  "evict_after": "2026-05-19T12:00:00Z",
  "persistent":  false,
  "overlaps":    []
}
```

**Other responses.** 400, 409, 413.

### 13.13 `POST /api/saved-queries`

**Request.**

```json
{
  "name":         "api errors last 24h",
  "freeze_times": false,
  "query": {
    "time_range": { "from": "now-24h", "to": "now" },
    "q":          "service:api level:error"
  }
}
```

**Response: 201 Created.**

```json
{ "id": "sq-9a01", "name": "api errors last 24h", "created_at": "2026-05-12T14:35:00Z" }
```

**409** on name collision.

### 13.14 `GET /api/saved-queries`

**Response: 200 OK.**

```json
{ "saved_queries": [ { "id": "sq-9a01", "name": "...", "query": { ... }, "created_at": "..." } ] }
```

### 13.15 `DELETE /api/saved-queries/:id`

**Response.** 204 or 404.

### 13.16 `GET /api/facets`

**Response: 200 OK.**

```json
{
  "facets": [
    {
      "name":              "service",
      "field":             "service",
      "display_label":     "Service",
      "cardinality_cap":   100,
      "value_type":        "string",
      "kind":              "builtin",
      "source":            null
    },
    {
      "name":              "user_id",
      "field":             "attributes.user.id",
      "display_label":     "User ID",
      "cardinality_cap":   25,
      "value_type":        "string",
      "kind":              "custom",
      "source":            "ui",
      "created_at":        "2026-05-10T14:22:00Z",
      "updated_at":        "2026-05-10T14:22:00Z"
    }
  ]
}
```

### 13.17 `POST /api/facets`

**Request.**

```json
{ "name": "user_id", "field": "attributes.user.id", "display_label": "User ID", "cardinality_cap": 25, "value_type": "string" }
```

**Response: 201 Created.** Same shape as the list entry.

**Response: 409 Conflict.** `facet_conflict` — name already in use.

### 13.18 `PATCH /api/facets/:name`

**Request.** Partial.

```json
{ "cardinality_cap": 50, "display_label": "User" }
```

**Response: 200 OK.** Updated facet.

**Response: 409.** When facet is built-in or config-defined.

### 13.19 `DELETE /api/facets/:name`

**Response.** 204, 404, or 409.

### 13.20 `POST /api/admin/reload`

**Request.** Empty or `{"dry_run": true}`.

**Response: 200 OK.**

```json
{ "reloaded": true, "changed_fields": ["retention.max_age", "facets.custom"], "warnings": [] }
```

**Response: 422.** `field_not_reloadable`.

### 13.21 `POST /api/admin/retention/run`

**Request.** `{"dry_run": false}`.

**Response: 200 OK.**

```json
{
  "evaluated": 482,
  "actions": [
    { "segment_id": "202604120100", "action": "archive_then_delete", "reason": "age > 30d" }
  ]
}
```

### 13.22 `POST /api/admin/archive/verify`

**Request.** Empty or `{"segment_ids": ["..."]}`.

**Response: 200 OK (S3 backend).**

```json
{ "backend": "s3", "scanned": 458, "orphaned_parquets": [], "dangling_manifests": [], "missing_parquets": [] }
```

**Response: 200 OK (script backend).**

```json
{
  "backend":  "script",
  "scanned":  458,
  "verified": 455,
  "failed":   [
    { "segment_id": "202604120100", "exit_code": 1, "note": "restic: snapshot abc123 not found" }
  ]
}
```

**Other responses.** 501, 503.

### 13.23 `GET /api/jobs/:id`

**Response: 200 OK.**

```json
{
  "job_id":        "reh-3a02",
  "type":          "rehydrate",
  "state":         "running",
  "progress":      { "completed": 14, "total": 24, "bytes_done": 2580000000, "bytes_total": 4423800000 },
  "started_at":    "2026-05-12T14:40:00Z",
  "finished_at":   null,
  "error":         null,
  "script_stdout": null,
  "script_stderr": null
}
```

### 13.24 `GET /healthz`

**Response.** 200 or 503.

```json
{ "status": "ok", "uptime_seconds": 84221, "version": "1.0.0" }
```

### 13.25 `GET /metrics`

Prometheus format. Notable v1 metrics include `logpond_ingest_events_total`, `logpond_ingest_skipped_lines_total`, `logpond_query_duration_seconds`, `logpond_facet_compute_duration_seconds`, `logpond_search_suggest_duration_seconds`, `logpond_query_count_duration_seconds`, `logpond_search_bar_parse_duration_seconds`, `logpond_archive_script_invocations_total{mode,exit}`, `logpond_segments{state}`, `logpond_disk_bytes{state}`, `logpond_facets{kind,source}`, `logpond_live_tail_clients`.

## 14. Web UI sketches

### 14.1 Top bar (all views)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  Logpond   [ Search ] [ Live tail ] [ Admin ]   Archive: ● s3   Disk: 4.2G   ☼/☾ ⌄    │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Theme toggle on the right cycles through Auto/Light/Dark; click for an explicit dropdown. Archive status: ● green / yellow / red; hover for backend type.

### 14.2 Search view (`GET /`)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  [ top bar ]                                                                           │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   Time range:  [ Last 1 hour      ▼ ]   From [ 2026-05-12 13:35 ]  To [ now ]          │
│                                                                                        │
│   ╭───────────────────────────────────────────────────────────────────────────────╮    │
│   │ 🔍 service:api level:(error OR warn) "connection refused"                ✕    │    │
│   ╰───────────────────────────────────────────────────────────────────────────────╯    │
│   ≈ 1,243 events                                          [ Search ]  [ Save query ]   │
│                                                                                        │
│   ──────────────────────────────────────────────────────────────────────────────────   │
│                                                                                        │
│  ┌─────────────────────┐ ┌─────────────────────────────────────────────────────────┐   │
│  │ FACETS              │ │ 314 matches in 312ms · 2 segments scanned · 143k rows   │   │
│  │                     │ │                                                         │   │
│  │ ▼ Service       100 │ │ ▸ 14:58:14.221  ERROR  api    app-1  db connection ref… │   │
│  │   ☑ api      12,420 │ │ ▸ 14:57:09.118  ERROR  api    app-2  db connection ref… │   │
│  │   ☐ worker    3,801 │ │ ▼ 14:55:33.882  WARN   api    app-1  slow query: select │   │
│  │   ☐ frontend  1,102 │ │     attributes:                                         │   │
│  │   + 4 more          │ │       user_id:     42                                   │   │
│  │                     │ │       request_id:  r-882                                │   │
│  │ ▼ Level          10 │ │       duration_ms: 4120                                 │   │
│  │   ☐ info     14,210 │ │     raw: {"timestamp":"2026-05-12T14:55:33.882Z",...}   │   │
│  │   ☑ warn      2,100 │ │ ▸ 14:54:17.005  ERROR  api    app-1  timeout waiting    │   │
│  │   ☑ error     1,013 │ │ ▸ ...                                                   │   │
│  │   ☐ debug        82 │ │                                                         │   │
│  │                     │ │ [ Load more ]                                           │   │
│  │ ▼ Host           50 │ │                                                         │   │
│  │   ☐ app-1     7,402 │ │ ─────────────────────────────────────────────────────── │   │
│  │   ☐ app-2     5,108 │ │ ⓘ  Older than 30d not in local storage.                 │   │
│  │   + 1 more          │ │    [ Rehydrate Apr 1–14 (~2.6 GB) ]                     │   │
│  │                     │ └─────────────────────────────────────────────────────────┘   │
│  │ ▼ Source          5 │                                                               │
│  │   ☑ dokku    23,140 │                                                               │
│  │                     │                                                               │
│  │ ▼ User ID        25 │                                                               │
│  │   ☐ 42          821 │                                                               │
│  │   ☐ 99          412 │                                                               │
│  │   + 1,238 more      │                                                               │
│  │                     │                                                               │
│  │ Facets sampled from │                                                               │
│  │ 24 segments         │                                                               │
│  └─────────────────────┘                                                               │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Behaviors:

- Search bar with HTMX-driven autocomplete (suggestion dropdown beneath, not shown in sketch). Debounce: 50ms for suggestions, 300ms for the live count indicator.
- Live `≈ N events` indicator updates 300ms after the last keystroke via `/ui/query/count`.
- Facet checkboxes drive the search bar — clicking adds `field:value` or rewrites to `field:(a OR b)`.
- Truncated facets show "+ N more"; clicking expands the list inline.
- Sidebar footer notes facet sample size.
- Result rows collapsed by default to one 28px line; click to expand.
- Browser local timestamps with timezone abbreviation; UTC tooltip on hover.
- Rehydrate banner when the time range falls outside locally-resident segments.
- Optional "Advanced ⌄" toggle near the search bar reveals the canonical filter-tree editor for power users; two-way bound to the search bar.

### 14.3 Live tail view (`GET /tail`)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  [ top bar ]                                                                           │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   ╭───────────────────────────────────────────────────────────────────────────────╮    │
│   │ 🔍 service:api level:error                                              ✕     │    │
│   ╰───────────────────────────────────────────────────────────────────────────────╯    │
│                                                                                        │
│   [ ● Start tail ]   Rate cap: 100/s   Status: ● live   Dropped: 0                     │
│                                                                                        │
│   ──────────────────────────────────────────────────────────────────────────────────   │
│                                                                                        │
│   ▸ 14:58:14.221  ERROR  api      app-1   db connection refused                  ← new │
│   ▸ 14:58:13.118  ERROR  api      app-2   db connection refused                        │
│   ▸ 14:58:11.882  ERROR  api      app-1   timeout waiting for upstream                 │
│   ▸ ...                                                                                │
│                                                                                        │
│   [ ⏸ Pause auto-scroll ]   [ Clear ]   [ ⏹ Stop tail ]                                │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Same search-bar autocomplete as Search. Filter changes mid-stream send a replacement subscription; stream uninterrupted. No facet sidebar (facets derived from sealed segments would be stale for streaming). Start tail is disabled while the search bar is empty.

### 14.4 Admin view (`GET /admin`)

**S3 backend variant:**

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  [ top bar ]                                                                           │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   Retention policy                                                                     │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│     Max age:          30d                                                              │
│     Max disk size:    20 GB                                                            │
│     Archive before delete: yes                                                         │
│     Next evaluation:  in 2m 41s   [ Run now ]   [ Dry run ]                            │
│                                                                                        │
│   Archive backend                                                                      │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│     Type:        S3 (Cloudflare R2)                                                    │
│     Endpoint:    https://abc.r2.cloudflarestorage.com                                  │
│     Bucket:      logs-archive   Prefix: logpond/                                       │
│     Status:      ● reachable (last check 30s ago)                                      │
│     [ Verify archive ]                                                                 │
│                                                                                        │
│   Facets                                          [ + Add facet ]                      │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│   Name        Field                       Label       Cap     Source     Action        │
│   service     service                     Service     100     builtin    [Edit cap]    │
│   level       level                       Level       10      builtin    [Edit cap]    │
│   host        host                        Host        50      builtin    [Edit cap]    │
│   source      source                      Source      50      builtin    [Edit cap]    │
│   endpoint    attributes.http.endpoint    Endpoint    100     config     —             │
│   user_id     attributes.user.id          User ID     25      ui         [Edit][Del]   │
│   region      attributes.region           Region      20      ui         [Edit][Del]   │
│                                                                                        │
│   Segment sample size: 24   [ Edit ]                                                   │
│                                                                                        │
│   Storage                                                                              │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│     Active segment:      1.4 GB    (current hour: 2026-05-12 14:00–15:00 UTC)          │
│     Sealed local:        2.6 GB    (24 segments)                                       │
│     Rehydrated:          1.1 GB    (6 segments, evicts in 4–7 days)                    │
│     Archived only:       —         (458 segments off-host)                             │
│     Total local:         5.1 GB    (of 20 GB cap)                                      │
│                                                                                        │
│   Segments                          [ Filter: All ▼ ]   [ + Import segment ]           │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│   ID              State        Time range                Rows     Size    Action       │
│   202605121400   active        14:00–15:00 (current)     1.39M    1.4G    —            │
│   202605121300   sealed        13:00–14:00               3.60M    181M    [Archive]    │
│   ...                                                                                  │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

**Script backend variant** (Archive backend section replaces the S3 version):

```
│   Archive backend                                                                      │
│   ─────────────────────────────────────────────────────────────────────────────────    │
│     Type:        Script                                                                │
│     Path:        /etc/logpond/scripts/archive.sh                                       │
│     Capabilities: archive ✓   verify ✓   retrieve ✗                                    │
│     Status:      ● ready (last invocation 5m ago, exit 0)                              │
│     [ Verify archive ]   [ Test invocation ]                                           │
```

When `retrieve` is unsupported (as shown above), the `[ Get ]` action on archived segments in the segment table is disabled with a tooltip explaining the operator should sideload manually.

Facets pane lists all facets with kind (builtin/config/ui) and source. UI facets show Edit/Del; built-in facets show Edit cap; config facets show — (edit the source config).

## 15. Deployment on Dokku

### 15.1 Overview

Logpond runs as a standard Dokku app. Dokku's `logs` plugin includes built-in Vector support: setting a `vector-sink` (globally or per-app) starts a Vector container that ships Docker container logs to the configured sink. We configure that sink to point at Logpond's HTTP ingest endpoint.

The Dokku Vector source emits one event per log line with these fields:

| Field                       | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `message`                   | The log line itself                                          |
| `container_name`            | Docker container name (Dokku-generated, like `web.1.<hash>`) |
| `container_id`              | Docker container ID                                          |
| `image`                     | Image name                                                   |
| `host`                      | The Dokku host's hostname                                    |
| `label.com.dokku.app-name`  | The Dokku app name (e.g., `myapp`)                           |
| `timestamp`                 | Time Docker received the log line                            |

### 15.2 Deploying Logpond as a Dokku app

**Step 1: Create the app and storage.**

```bash
dokku apps:create logpond
dokku domains:set logpond logpond.example.com

dokku storage:ensure-directory logpond
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond:/data
```

**Step 2: Set environment variables.**

Minimum useful configuration with S3 (Cloudflare R2):

```bash
dokku config:set logpond \
  LOGPOND_LOG_LEVEL=info \
  LOGPOND_RETENTION_MAX_AGE=30d \
  LOGPOND_RETENTION_MAX_SIZE=10GB \
  LOGPOND_ARCHIVE_BACKEND=s3 \
  LOGPOND_ARCHIVE_S3_ENDPOINT=https://abc.r2.cloudflarestorage.com \
  LOGPOND_ARCHIVE_S3_BUCKET=logs-archive \
  LOGPOND_ARCHIVE_S3_PREFIX=logpond/ \
  LOGPOND_ARCHIVE_S3_REGION=auto \
  LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID=... \
  LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY=... \
  LOGPOND_SOURCES_JSON='[{"name":"dokku","extract":{"timestamp":["timestamp"],"message":["message"],"service":["label.com.dokku.app-name"],"host":["host"]}}]' \
  LOGPOND_FACETS_JSON='[{"name":"container","field":"attributes.container_name","display_label":"Container","cardinality_cap":50}]'
```

`LOGPOND_SOURCES_JSON` defines a source named `dokku` that extracts `service` from `label.com.dokku.app-name` — this is what makes Dokku app names show up as the `service` facet.

For script-backend instead of S3:

```bash
dokku config:set logpond \
  LOGPOND_ARCHIVE_BACKEND=script \
  LOGPOND_ARCHIVE_SCRIPT_PATH=/etc/logpond/scripts/archive.sh \
  LOGPOND_ARCHIVE_SCRIPT_TIMEOUT=600s \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=sftp:backup.example.com:/srv/logpond \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

**Step 3: Deploy.**

```bash
git init logpond-deploy && cd logpond-deploy
echo "FROM ghcr.io/<owner>/logpond:1.8.0" > Dockerfile
git add . && git commit -m "deploy logpond"
git remote add dokku dokku@dokku.example.com:logpond
git push dokku main
```

**Step 4 (script backend only): mount the script.**

```bash
sudo mkdir -p /var/lib/dokku/data/storage/logpond-scripts
sudo cp ~/my-archive.sh /var/lib/dokku/data/storage/logpond-scripts/archive.sh
sudo chmod +x /var/lib/dokku/data/storage/logpond-scripts/archive.sh
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond-scripts:/etc/logpond/scripts
```

If the script needs additional tools, extend the base image:

```dockerfile
FROM ghcr.io/<owner>/logpond:1.8.0
RUN apk add --no-cache restic
```

**Step 5: Verify.**

```bash
curl https://logpond.example.com/healthz
open https://logpond.example.com/
```

### 15.3 Configuring Dokku to ship logs to Logpond

**Step 1: Set the Vector sink.**

```bash
dokku logs:set --global vector-sink \
  "http://?uri=http://logpond.example.com/ingest/dokku&encoding[codec]=json&framing[method]=newline_delimited&compression=gzip&batch[max_events]=500&batch[timeout_secs]=1"
```

The DSN options:

- `uri=http://logpond.example.com/ingest/dokku` — the Logpond ingest endpoint. The `dokku` suffix matches the source name in `LOGPOND_SOURCES_JSON`.
- `encoding[codec]=json` — JSON events, one per line.
- `framing[method]=newline_delimited` — NDJSON framing.
- `compression=gzip` — compress.
- `batch[max_events]=500&batch[timeout_secs]=1` — batch up to 500 events or 1s.

**Step 2: Start the Vector container.**

```bash
dokku logs:vector-start
```

**Step 3: Verify shipping.**

```bash
dokku logs:vector-logs --tail
curl https://logpond.example.com/metrics | grep logpond_ingest
```

Open the Search view, set time range to "Last 15 minutes," and events should appear with `service` set to Dokku app names.

### 15.4 Avoiding the Logpond → Logpond feedback loop

The global vector-sink ships logs from every Docker container, including Logpond. Three ways to break the loop:

**A. Per-app sinks (recommended).**

```bash
dokku logs:set web-app vector-sink "http://?uri=http://logpond..."
dokku logs:set api-app vector-sink "http://?uri=http://logpond..."
# do not set vector-sink on the logpond app itself
```

**B. Blackhole Logpond's logs.**

```bash
dokku logs:set logpond vector-sink "blackhole://?"
```

**C. Quiet Logpond's logging.**

```bash
dokku config:set logpond LOGPOND_LOG_LEVEL=warn
```

Per-app sinks (A) is the cleanest.

### 15.5 Filter dimensions in practice

With the configuration above, the Search view's sidebar shows:

- **Service** — the Dokku app name (`web-app`, `api-app`, etc.).
- **Level** — from app structured logs or fallback `info`.
- **Host** — Dokku host hostname.
- **Source** — always `dokku` with this config.
- **Container** (custom facet from §15.2) — Dokku process container name.

A typical investigation query in the search bar:

```
service:api-app level:(error OR warn) @container_name:web*
```

If the underlying app emits structured JSON, those fields become queryable as `@field` paths.

### 15.6 Upgrading

```bash
echo "FROM ghcr.io/<owner>/logpond:1.9.0" > Dockerfile
git add . && git commit -m "upgrade to 1.9.0"
git push dokku main
```

Catalog, segments, and saved queries are forward-compatible between minor versions.

### 15.7 Backup considerations

The `/data` volume contains segments, the catalog, and rehydrated segments. To back up:

- Sealed and archived segments are already off-host — no separate backup needed.
- The active segment is volatile; loss bounded by Vector's disk buffer.
- The catalog (`/data/catalog.db`) is the only file that needs explicit backup. Small (<100MB typically). Back up `/var/lib/dokku/data/storage/logpond/catalog.db` with your regular host backup.

Catalog rebuild from scratch is supported: on startup, Logpond scans `/data/segments/` and reconciles. Archived segments are rediscovered via the backend's list mechanism. Slower than restoring the catalog file.

## 16. Resolved trade-offs index

- **Ingest skips bad lines** instead of failing the batch (§7.1).
- **Level normalization collapses severities** into four buckets (§7.1).
- **Extracted fields removed from `attributes`** instead of duplicated (§7.1).
- **Late arrivals written to current segment** instead of re-opening sealed segment (§7.2).
- **Lenient type coercion** on attribute filters (§7.3.4).
- **Time-range cap of 7 days** instead of unbounded scans (§7.3.4).
- **Facets sampled from N most recent overlapping segments** instead of full scans (§7.4.3).
- **UI-managed facets stored in the catalog** instead of config-only (§7.4.2).
- **Rehydrated segments count toward `max_size`** instead of being exempt (§7.8).
- **Sideload overlapping segments** accepted instead of rejected (§7.10).
- **Script backends sacrifice service-side verification** for operator flexibility (§7.9.3).
- **5s fsync interval** instead of per-write fsync (§8.3).
- **No build step / no SPA framework** — accepts more imperative JS for complex components in exchange for deployment simplicity (§11.1).
- **Pixel-exact spacing** ignores user browser-zoom for layout density (§12.4).

## 17. Open questions

- **Event size assumption.** Sizing assumes ~500 bytes/event average. Real-world Dokku logs may average higher.
- **DuckDB FTS extension availability.** Needs verification during prototyping.
- **zstd-3 compression CPU cost.** Re-evaluate under realistic load.
- **Attribute-filter query latency.** Validate with users.
- **Watched directory polling interval (30s).** Tune based on operator feedback.
- **Archive script probe mechanism.** Re-evaluate whether to require probes for capability detection.
- **Facet sample size default (24).** May need adjustment based on real-world segment counts.
- **High-cardinality facets.** No automatic detection; may need a future heuristic.
- **Search-bar autocomplete on touch devices.** Tab-completion doesn't translate; UX needs validation on mobile.
- **Advanced filter-tree editor complexity.** May trigger the Preact + htm escape hatch (§11.6).

## 18. Future enhancements (post-v1)

- Per-source authentication tokens and multi-tenant data isolation.
- Authentication for the web UI (basic auth, OAuth, SSO).
- Per-segment FTS indexes for faster substring search.
- Configurable attribute promotion (selected `attributes.*` fields stored as top-level columns).
- Automatic facet discovery.
- Additional ingest protocols (OTLP, syslog, raw socket).
- Server-side parsing for sources where running Vector is impractical.
- Alerting on saved-query result counts.
- Multi-node clustering.
- Field-level access control and PII redaction at ingest.
- Export to CSV/Parquet from search results.
- Multiple archive backends configured simultaneously.
- Saved facet sets (preset configurations of which facets are shown / expanded by default).
- Density toggle (compact / comfortable spacing).

## 19. Success criteria

The v1 release is considered successful if:

1. A reference deployment on a 2GB Dokku host can sustain 1,000 events/sec ingest with under 256MB resident memory over a 7-day soak test.
2. Queries spanning one hour of data on core-column filters return in under 500ms p95, including facet computation.
3. Search-suggest and live-count endpoints return in under 100ms p95.
4. The full retention → archive → rehydrate cycle works end-to-end against AWS S3, Cloudflare R2, MinIO, and at least one example archive script (restic).
5. The published Parquet output is readable by `duckdb` and `pyarrow` without the service running.
6. A new user can stand up the service on Dokku and ship logs from their Dokku apps in under 30 minutes following §15.
7. The Datadog-style search bar is usable without consulting the syntax docs for common queries.
8. The UI respects the operating system's light/dark preference on first load and lets the user override.
9. Custom facets configured via the admin UI persist across restarts and surface in autocomplete and the sidebar immediately.
10. The total client-side asset payload stays under approximately 60KB gzipped, and the UI loads with no build step in the deployment pipeline.
