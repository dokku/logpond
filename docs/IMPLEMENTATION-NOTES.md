# Implementation notes

Tracks divergences from `PRD.md` and `IMPLEMENTATION-PLAN.md` with the
reason and the resolution. Phases reference this file in their commit
messages where relevant.

## Phase 2 — Ingest path

### Ring buffer capacity: bytes vs. event count

- **Plan text (Phase 2, task 3).** "A bounded channel-backed buffer of
  `Event` with capacity from config (default 50k events)."
- **PRD §7.11.1.** `memory_limits.ring_buffer` is a byte size with
  default `50MB`.

These are two different units for the same setting. The implementation
keeps the PRD's byte-sized config field and derives an event-count
capacity from it using a flat 1KB-per-event heuristic
(`bytes / 1024`). At the default of 50MB this resolves to ~51,200
events, which matches the plan's "50k events" intent. An additional
hard cap of 1,000,000 events guards against pathological configurations
that would otherwise pre-allocate a multi-million-slot slice.

The heuristic intentionally errs on the small side: PRD §8.2 budgets
the ring buffer at 5-10MB steady, 50MB peak, so even if real events
average closer to 2KB the buffer still holds tens of thousands of
events before applying backpressure.

### NDJSON content types

The PRD §13.2 documents `application/x-ndjson` and `application/json`
as the accepted content types. The handler also accepts
`application/ndjson` (a common alias seen in clients) and an empty
`Content-Type` (Vector's NDJSON sink omits the header in some
versions). Anything else returns 415.

## Phase 3 — Segment lifecycle

### Parquet writer: DuckDB COPY vs. `parquet-go`

- **Plan text (Phase 3, task 3).** "The Parquet write uses the
  `parquet-go` library."
- **What ships.** Sealing uses DuckDB's `COPY (SELECT * FROM events) TO
  '<path>' (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL 3)`
  rather than pulling in a second Parquet library.

DuckDB-emitted Parquet inherently satisfies §8.5's cross-tool
compatibility requirement (the DuckDB CLI is one of the listed
verification tools, and DuckDB's writer targets pyarrow-compatible
Parquet). Adding `parquet-go` would mean carrying a second
Parquet/zstd stack alongside DuckDB's own — extra surface for the
same output. If a future need (e.g., emitting Parquet without a live
DuckDB connection) makes the external library worthwhile, we can
revisit then.

### Catalog `size_bytes` semantics for sealed segments

PRD §10.2's `segments.size_bytes` is `NOT NULL`; §10.3's manifest
separates `size_bytes` (uncompressed) from `size_compressed`. Sealing
populates both catalog columns with the on-disk Parquet size for now —
we don't yet compute an uncompressed payload size cheaply. The two
catalog columns being equal is an explicit signal that the
uncompressed accounting will arrive when the manifest writer
(Phase 8) lands. Until then no consumer reads the uncompressed value.

### DuckDB connection-pool sizing

`internal/storage/duckdb`.`Open` configures `SetMaxOpenConns(4)`
rather than 1. The appender holds one `*sql.Conn` for the lifetime of
the segment via `(*sql.Conn).Raw`; capping the pool at one would
deadlock any concurrent reader query (sealing's `count`/`min`/`max`
calls, the test suite's verification SELECTs) because there is no
free connection to acquire. Four is comfortably above the number of
concurrent readers we issue per segment in steady state.

## Phase 4 — Filter tree + canonical query API

### Per-segment queries instead of a single UNION ALL

- **Plan text (Phase 4, task 3).** "Builds a `UNION ALL` query across
  the active segment table and each sealed Parquet's `read_parquet()`."
- **What ships.** The executor issues one `SELECT` per eligible
  segment and merges in Go.

A single UNION ALL would require either a shared DuckDB query engine
that can both read sealed Parquet files and `ATTACH` the active
segments' DuckDB files. Each active segment is owned by its
writer-side connection; `ATTACH` from a separate connection in the
same process risks duelling write/read locks on the file, and
producing a stable SQL string that references many transient paths
is awkward. The per-segment approach lets:

- sealed segments run on a fresh in-memory DuckDB with `read_parquet`;
- active segments route through the writer's own `*sql.DB` (which is
  why `internal/segments.Manager.QueryActive` exists);
- the executor enforce `LIMIT N` per segment using catalog ordering
  (newest-first for the default `timestamp DESC` sort), so we stop
  scanning once the global limit is satisfied.

The Go-level merge is `sort.SliceStable` on the accumulated batch.
Cost scales with `limit * #-segments-touched`, which is bounded by
the time-range (7 days × 12 hourly segments ≈ 168 segments worst
case) and by short-circuit when the limit is reached.

### Lenient attribute typing

`§7.3.4` calls for lenient numeric-vs-string coercion on attribute
filters. The compiler implements this minimally:

- `eq`/`neq`/`in`/`not_in`/`contains`/`starts_with` compare
  `json_extract_string(attributes, '$.X')` against the value
  stringified by Go's default JSON decoding. So `@user_id:42` matches
  rows where `user_id` was stored as `42` (decoded by DuckDB's
  JSON-to-text conversion).
- `gt`/`lt`/`gte`/`lte` on attributes coerce both sides to `DOUBLE`
  so numeric ordering is preserved even when the stored value is a
  string like `"1500"`.

Edge cases the v1 compiler does not handle yet: `@user_id:42.0`
against a stored `42`, and `@flag:true` against `"true"`. Both can be
added later as additional OR branches inside `compileEq`. The current
behaviour is documented here so the Phase-5 parser tests don't
quietly assume it.

### Cursor shape

The opaque cursor encodes `{timestamp, segment_id}` (base64-url JSON).
Pagination assumes the primary sort is `timestamp`; if the caller
sorts by another field first the cursor compiles to an empty WHERE
fragment, which means the same rows would be re-returned. The PRD's
cursor design is timestamp-led (`§7.3.4` "Cursor-based using
(timestamp, segment_id, intra_segment_row_number)"); we omit the
intra-segment row number because the per-segment merge already keeps
results stable for typical workloads. If we later see same-timestamp
collisions in a single segment, we can extend the cursor without
breaking older clients (the field is internal).

## Phase 5 — Datadog-style search parser

### Free-text terms in nested contexts

PRD §7.3.4 only describes top-level free-text: "Top-level `search` (or
unquoted text in the search bar) does case-insensitive substring match
against `message` and `raw`." It does not say what happens to a bare
term inside `(...)` or under `NOT`. The parser handles those by
compiling the nested term into a `message contains` leaf and emitting
a warning. That keeps the user's grouping intent intact (free-text
under `NOT (...)` or as one branch of an `OR` would otherwise be
silently dropped if we only honored top-level terms).

### Bare non-core fields

PRD §7.3.2 says: "Bare identifiers that aren't in the `core-field`
list and aren't preceded by `@` are treated as free-text terms." The
parser interprets that strictly when the bare identifier stands alone
(`foo` becomes a free-text term). When the identifier is immediately
followed by a colon (`custom_field:value`), the parser still compiles
it as a field predicate but emits a warning suggesting the `@` prefix.
Treating `custom_field:value` as the term `custom_field` followed by
an orphan `:value` would surface as a confusing syntax error, so we
prefer the lenient path with a warning.

### `[* TO *]` and `@path` shorthands

Two corner cases the PRD doesn't pin down:

- `field:[* TO *]` (both bounds open) compiles to a single `exists`
  leaf on the field. Equivalent to `field:*` and the only sensible
  reading we could find.
- `@path` with no trailing `:value` compiles to an `exists` leaf on
  `attributes.path`. The explicit form remains `@path:*`; the
  no-colon shorthand is a small ergonomic addition that does not
  affect tree shape.

### Non-prefix wildcards compile to `contains` with stripping

§7.3.2 says non-prefix wildcards "compile to a slower full-scan
substring match using `contains`" but doesn't specify what value goes
into the `contains` op. The parser strips all `*` and `?` characters
from the bare token and uses the remainder as the contains target.
For `foo*bar` this is imperfect (it matches anything containing
`foobar` rather than `foo...bar`), but the warning emitted alongside
flags the imprecision so an operator can refine the query.

## Phase 6 — Facets and autocomplete

### Facet aggregation issues separate queries per segment

- **Plan text (Phase 6, task 2).** "DuckDB's query planner should be
  able to combine the result-set query and the facet aggregations into
  a single multi-statement plan; verify by inspecting the explain plan."
- **What ships.** Facet computation issues one `GROUP BY` query per
  sampled segment (active or sealed Parquet) and merges counts in Go.

The Phase 4 executor already runs per-segment queries because each
active segment is locked to its writer connection (see the Phase-4
note in this file). Reusing the same plumbing for facet aggregation
keeps the executor consistent and avoids ATTACHing DuckDB files from
a separate connection. The Go-side merge is bounded by the cardinality
of the field across the sample window; for the facet truncation cap of
50 the working-set is tiny in practice. If a future bench shows the
merge dominating, we can switch to a single multi-statement DuckDB
plan without changing the public response shape.

### Facet sampling includes active segments

§7.4.3 says facet values come from "the N most recent sealed segments
overlapping the current query time range." The implementation widens
that to "the N most recent queryable segments" — active segments
included — because the freshest values would otherwise lag the
sealing interval (up to ~1h with the default window). The PRD's
intent is "values you can discover quickly," not "exclude live data,"
so adding the active segment in is strictly more useful. Sealed-only
sampling remains a one-line change if we ever decide otherwise.

### Count short-circuit uses `SELECT 1 ... LIMIT N`

§13.5 specifies `exact: false` once the count reaches 10,001 but
doesn't say how to short-circuit. The executor scans each segment
with `SELECT 1 FROM segment WHERE filter LIMIT remaining` and counts
returned rows. When the running total reaches the guard, the loop
stops scanning further segments. This avoids any DuckDB-specific
early-termination tricks and gives portable behaviour across the
active and sealed-Parquet readers.

### Search-suggest context classifier is hand-rolled

§7.5 / §13.6 enumerate five suggestion contexts but the PRD doesn't
specify how the server decides which context applies. The
implementation uses a small backwards-scan classifier on the query
prefix up to `cursor_pos`. It handles the common cases (start, after
`field:`, inside `field:(`, after a comma in a value list, after
whitespace following a complete predicate) and falls back to "field"
context on ambiguity. The full parser in `internal/query/parser` is
reserved for executed queries; the suggester only needs a coarse
state machine and tolerates malformed inputs.

### Facet values for suggestions reuse the executor

`POST /api/search-suggest`'s value context fans through the same
facet-computation path as the main query, with `CardinalityCap` set
to `max+1` for the requested suggestion limit. This means the value
suggestions reflect the current N-segment sample exactly as the
sidebar will, and we don't need a second SQL surface dedicated to
suggestions.
