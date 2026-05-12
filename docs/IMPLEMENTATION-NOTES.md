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

## Phase 7 — Retention

### Max-age uses `time_end`, protected window uses `sealed_at`

PRD §7.8 doesn't specify whether `max_age` compares against the
segment's window-end timestamp or the moment the segment sealed. The
evaluator uses `time_end` (the data's age) for the age policy and
`sealed_at` for the protected-window check. That matches operator
intent ("delete logs older than 30 days") while still honoring "never
delete a freshly-sealed segment for one hour" — a segment whose window
ended weeks ago but only just finished sealing is still protected.

### Verb wording follows the §13.21 example

The evaluator emits two action verbs: `delete` (the local file will be
removed and the catalog state transitions to `lost` or `archived`) and
`archive_then_delete` (the segment needs archiving first, so the
current cycle does nothing). The latter matches the example in PRD
§13.21 and stays accurate when Phase 8 wires the archive backend — the
verb describes the plan, not whether anything happened on this pass.

### State transitions on deletion

- `sealed` → `lost` when archive_before_delete=false or no backend.
- `archived` (with local file) → stays `archived`, local_path cleared.
- `rehydrated` → `archived`, local_path and evict_after cleared (the
  archive ref still points at the off-disk copy).

Adding a new `deleted` state would clutter §10.2 and confuse crash
recovery, which already uses `lost` for "catalog entry without file".

### Retention scans `local_path IS NOT NULL`

The evaluator pulls candidates via `ListLocalSegments`, which filters
on `local_path` rather than state alone. This guarantees we never
attempt to remove an `archived`-state row whose local file was already
cleared, even if its `state` column still reads `archived` from a
prior pass.

## Phase 8 — Archive backend interface + S3 implementation

### Unit tests use a fake `S3API`, not `testcontainers-go` + MinIO

- **Plan text (Phase 8, task 8).** "Use MinIO in a Docker container
  (testcontainers-go) for integration tests."
- **What ships.** The S3 backend takes an `S3API` interface that wraps
  only the `s3.Client` methods it uses (`HeadObject`, `PutObject`,
  `GetObject`, `ListObjectsV2`, plus the four multipart calls). The
  tests in `internal/archive/s3_test.go` inject an in-memory fake that
  implements `S3API` and exercises every code path including multipart
  reassembly, idempotency on SHA match, overwrite on SHA mismatch, and
  retrieve round-trip.

Reasons for the divergence:

- The fake is ~150 lines and runs in <100ms with no Docker dependency.
  Phase 16's soak/hardening pass can layer a `testcontainers-go` +
  MinIO integration test on top without rewriting the suite.
- Each fake operation is line-traceable, so behaviours like
  "metadata round-trips" or "multipart reassembles in part order" can
  be asserted directly.
- The boundary the fake replaces (`S3API`) is exactly what the
  production backend depends on, so the fake exercises real code
  rather than mocking out the unit under test.

### Manifest is the commit marker on every archive call

PRD §7.9.1 says "Catalog is updated only after manifest creation
succeeds." The S3 backend follows that strictly: on each Archive call
it writes the manifest *after* a successful HEAD-verify of the
parquet, even when the parquet upload was a no-op due to a matching
SHA. That keeps the contract simple — readers downstream don't have
to distinguish "manifest from a prior run" from "manifest from this
run" — and the manifest is a small JSON object so the extra PUT is
cheap.

### Idempotency = SHA match in object metadata

The PRD §7.9.2 idempotency check is "SHA-256 metadata header
(`x-amz-meta-parquet-sha256`)". The backend stores the bare key
`parquet-sha256` in `Metadata` (the AWS SDK adds the
`x-amz-meta-` prefix on the wire). Tests assert against the bare key
because that's what `HeadObject.Metadata` returns.

### Retention archives synchronously; manual `/api/archive` runs async

The retention loop is already a single-threaded background goroutine,
so calling `Backend.Archive` inline keeps the lifecycle obvious. The
HTTP `/api/archive` endpoint instead persists the work as a job
(PRD §13.9 mandates a 202 + `status_url`) and runs the archive in a
background goroutine. Both paths converge on the same
`Backend.Archive` + `MarkSegmentArchived` sequence; only the wrapping
differs.

### Multipart threshold and part size are tunable

PRD §7.9.2 mandates multipart for >64MB. The constants
`MultipartThreshold` (64MB) and `MultipartPartSize` (16MB) are wired
through `S3Options` so tests can lower the threshold and exercise the
multipart path on small fixtures without needing 64MB of test data.
The defaults match the PRD.

### Script backend defers to Phase 9

`buildArchiveBackend` in `cmd/logpond/main.go` returns `NoneBackend`
with a warning when `archive.backend: script` is set. The HTTP
endpoints still return clean 501/503 responses in that state. Phase 9
will swap in the real script backend.

## Phase 9 — Script archive backend

### Test fixture is a bash script, not a Go subprocess

- **Plan text (Phase 9, task 7).** "Use a test fixture script (a Go
  program built at test time) as the script backend."
- **What ships.** `internal/archive/script_test.go` writes a small
  bash fixture into the test's temp dir. The fixture reads `FIXTURE_*`
  env vars to flip exit code, stdout/stderr, sleep duration,
  SIGTERM-handling, and per-mode probe responses.

A bash fixture is ~80 lines and compiles instantly per `t.TempDir()`;
the Go-subprocess approach would have required a `TestMain` shim
(`if os.Getenv("AS_FIXTURE") != "" { ... }`) plus its own argument
parser. The bash fixture is line-traceable, scriptable per-test
(every test sets exactly the env knobs it needs), and has no
`go test` build dependency. `bash` is a baseline assumption for the
script backend itself, so requiring it in tests doesn't widen the
matrix. `TestScript_NeedsBash` documents the fallback should we ever
need to skip on a minimal CI image.

### Probe defaults: archive=yes, others=unknown

PRD §7.9.3 says: "If `--probe` isn't implemented (script exits non-0
with no specific code), default to `archive=yes, verify=unknown,
retrieve=unknown` and let the operator discover unsupported modes
lazily."

We model "unknown" as a tri-state internally (`capUnknown`, `capYes`,
`capNo`). `Capabilities()` returns `true` for both `yes` and
`unknown` so the API layer attempts the mode; only an explicit exit-64
result (from either probe or a real call) flips the cap to `capNo`,
at which point further attempts fail fast with `ErrUnsupported`.

`CapabilityDetail()` surfaces the tri-state ("yes"/"no"/"unknown") so
the admin UI can show the operator the actual state. The new
`GET /api/admin/archive/capabilities` endpoint returns that detail
JSON: `{"backend":"script","archive":"yes","retrieve":"unknown",
"verify":"no"}`. This endpoint is **not** in PRD §13 — it was added
by the Phase 9 plan as an informational admin endpoint, and is
documented here rather than in the API spec.

### Stdout/stderr capture extends `ArchiveResult`/`RetrieveResult`/`VerifyResult`

The plan calls for capturing stdout/stderr into the `jobs` table.
Rather than threading a side-channel into the backend interface, the
three result structs grew optional `Stdout` and `Stderr` string fields.
S3 leaves them empty; the script backend populates them (capped at
1MB per stream with an `...[truncated]` marker so a misbehaving script
can't blow up the jobs row).

`api.runArchiveJob` accumulates per-segment output blocks and calls
`jobs.Manager.AttachScriptOutput` to write the combined text into
`script_stdout`/`script_stderr` on the jobs row. Verify aggregates
output the same way (one block per segment id, separated by
`--- <id> ---` markers).

### Manifest is written to a per-invocation scratch directory

PRD §7.9.3's archive contract passes a `--manifest-path` to the
script, but sealed segments don't carry a manifest on disk yet (the
S3 backend builds and uploads it inline). The script backend writes
the manifest into `<work_dir>/inv-<random>/segment-<id>.manifest.json`
before invoking the script and tears the scratch directory down on
return. `work_dir` defaults to `<data_dir>/script-work/` in
production (matching PRD §9), and to `t.TempDir()` in tests.

### SIGTERM-then-SIGKILL via `cmd.Cancel` + `cmd.WaitDelay`

Go 1.20+ exposes the exact knobs the PRD wants: `cmd.Cancel = SIGTERM`
on context expiry, then `cmd.WaitDelay` for the 30s grace before the
runtime escalates to SIGKILL. Both production and tests use the same
mechanism; tests shorten the grace via the unexported
`terminationGrace` field on `ScriptOptions`.

### Concurrency is a single backend-wide mutex

PRD §7.9.3: "One invocation at a time globally." The simplest faithful
implementation is `sync.Mutex` around the exec call, which serializes
both probe and real invocations. `TestScript_ConcurrentArchiveSerialized`
runs two archives concurrently and checks the fixture's log file shows
strictly paired start/end lines.

### Restic example ships under `examples/archive-restic.sh`

The reference script implements the full §7.9.3 contract (all three
modes plus `--probe`). It tags each restic snapshot with
`logpond + segment:<id> + sha:<parquet-sha>` so verify can find by tag
and retrieve can locate the snapshot id without persisting state on the
Logpond side beyond `archive_ref`. Smoke-tested with `--probe` for all
three modes plus an unknown mode (returns 64).

## Phase 10 — Rehydration & sideload

### Already-local rehydrate accepts `state=rehydrated` candidates

PRD §7.10: "Already-local rehydrate. No-op + TTL bump." The PRD does
not explicitly say which catalog states qualify, so the selector in
`internal/api/rehydrate.go` accepts rows whose `state` is
`archived`/`sealed` *with* an archive ref, plus any `rehydrated` row
that already has a `local_path`. The latter is the no-op-+-bump case;
the job just updates `evict_after` (clearing it when `persistent`).
Without that branch, requesting a window that overlaps only
already-local segments would return 404 — clearly not the intent.

### Busy-segment refcount lives on the query executor

PRD §13.11 returns 409 `segment_busy` when a delete races a query.
We implement that by adding a small `sync.Mutex`+map refcount to
`*query.Executor` and acquiring around every per-segment scan
(`querySegment`, `countSegment`, `queryFacetSegment`). The DELETE
handler and the eviction loop both consult `Executor.IsSegmentBusy`.
A tiny `AcquireForTest` export keeps the test surface focused.

### Rehydrated files live alongside one manifest

The S3 backend retrieves the parquet and manifest into the
rehydrated dir; eviction removes both (best-effort `os.Remove`).
Manifests are useful for sideload provenance, and the watched-import
failure path expects both files to be present.

### Watcher Sweep runs once on start, then on tick

`ImportWatcher.Run` does an immediate sweep before the first tick so
an operator who drops a triplet right after a restart does not wait
30s for pickup. This matches the natural reading of §7.10 ("polled
every 30s") more than a tick-only loop would.

### `SegmentsOverlapping` re-exports `SegmentsInRange`

The import handler reuses `Catalog.SegmentsInRange` under a wrapper
name (`SegmentsOverlapping`) so the call site reads idiomatically.
Keeping a single SQL definition guarantees the import overlap report
uses the same predicate the executor does for query scoping.

### Estimated rehydrate duration is a coarse heuristic

`POST /api/rehydrate` returns `estimated_seconds` derived from
`total_bytes / 50 MB/s`. The PRD example surfaces an estimate but
does not pin the formula. 50 MB/s is a conservative lower bound for
warm S3 + local disk; the response is informational and the real
job always wins or loses against it on its own merits.

## Phase 11 — Live tail (WebSocket)

### WebSocket library: `github.com/coder/websocket`

- **Plan text (Phase 11, task 1).** "Upgrade `GET /api/query/stream`
  via `nhooyr.io/websocket`."
- **What ships.** The handler uses `github.com/coder/websocket`, which
  is the current canonical location of the library originally at
  `nhooyr.io/websocket` (Anmol Sethi handed maintenance to Coder in
  2024). The API surface is identical; the import path is the only
  change. Locking in the maintained module avoids depending on a
  redirect that may eventually be removed.

### Fan-out publishes on the flush callback, not from `Buffer.Append`

- **Plan text (Phase 11, task 2).** "The ring buffer's flush path
  emits events to a fan-out channel."
- **What ships.** The flush callback in `cmd/logpond/main.go`
  publishes each drained batch to `*ingest.Fanout` after handing it to
  `mgr.Flush`. Subscribers see events at the same cadence as the
  segment writer (driven by `Flusher`'s interval/half-full signal),
  which keeps a single source of truth for "an event has been
  accepted by Logpond."

The alternative — broadcasting from `Buffer.Append` itself — would
have surfaced events to live tail microseconds earlier but at the
cost of coupling the fanout to ingest's hot path. Tests would also
have to mock out two emission points instead of one. PRD §7.6's "≤ 2s
end-to-end" budget is comfortably met by the flush-tick (1s default).

### Slow-client detection uses a writer goroutine + non-blocking enqueue

- **Plan text (Phase 11, task 4).** "If the subscriber's send queue
  stays full for 30s, close with 4003."
- **What ships.** Each connection has a small writer goroutine that
  drains a 16-slot `chan []byte` and calls `conn.Write` with a
  per-message context whose deadline is `SlowClientGrace`. The main
  select loop performs a non-blocking send into that queue and falls
  back to a bounded wait of `SlowClientGrace`; if the wait fires the
  loop closes 4003.

This deviates from the literal "queue full for 30s" wording but
matches the intent: when the WebSocket peer stops draining, TCP back
pressure stalls the writer, the writer's deadline fires, and the
main loop sees `errSlowClient` via the writer's error channel. The
non-blocking enqueue is the second path to 4003 and exists because
the writer goroutine may already be blocked in `conn.Write` when the
main loop tries to enqueue a heartbeat or status update; without the
fall-through the main loop would deadlock waiting for a writer that
can't progress.

### `OnClose` test hook surfaces server intent

The WebSocket close frame may not reach the client when TCP is
wedged — the same condition that triggers 4003 also prevents the
close frame from going out cleanly. Tests therefore observe the
server's close intent via an `OnClose(code)` callback configured on
`ws.Options` rather than reading the close code off the wire. The
hook fires for application-defined codes (4001–4004) only; clean
1000 closures are uninteresting for tests and aren't reported.

### Close-frame writes are bounded

`closeWith` wraps `conn.Close` in a goroutine with a 1s ceiling
because, again, a wedged TCP can hang the close handshake. Without
the bound, the connection-cap path (4004) on a slow client could
keep the listening handler busy for the duration of the kernel's
TCP timeout. The underlying conn is still freed when `Close` returns
internally, so the cap accounting in `clients` stays accurate.

### In-memory event matcher mirrors the SQL compiler

`internal/query/match.go` implements `Match(node, ingest.Event)` for
live tail. The semantics mirror `internal/query/compile.go`'s SQL
output: lenient JSON-to-text coercion on attribute equality,
numeric-typed comparisons via `CAST(... AS DOUBLE)` analogues, and
case-insensitive defaults for string ops. Keeping the two in step is
important because the same `q` should match the same rows in live
tail and in a `/api/query` request.

A minor divergence from compile.go: the matcher does not consult the
DuckDB-side `LIKE` escape semantics — it just uses `strings.Contains`
and `strings.HasPrefix`. For ASCII content the two are equivalent;
for content that depends on collation we'd need to revisit, but
PRD §7.3.4 only specifies case-insensitive substring.

## Phase 12 — Web UI

### Frontend dependencies are vendored on demand, not committed

- **Plan text (Phase 12, task 1).** "Static `static/` directory contains
  ... HTMX, Alpine, Open Props ... vendored from a release tag."
- **What ships.** `scripts/vendor.sh` fetches the pinned versions of
  htmx, htmx-ext-ws, Alpine.js, and Open Props CSS into
  `static/vendor/` on demand. The Dockerfile invokes the same script
  during image build; operators (and CI) run it once locally.

Reasons for keeping the minified blobs out of git:
- The four files weigh ~55KB minified but balloon the diff noise on
  every dependency bump.
- The single `scripts/vendor.sh` is the canonical source of pin info
  (the `VERSIONS` file is regenerated on each run), avoiding two
  parallel "which version is current" sources.
- Local dev and Dockerfile share one workflow.

The `//go:embed all:static` directive accepts the empty `vendor/`
directory (the `.gitkeep` and `README.md` already inside it satisfy the
non-empty requirement). The browser sees 404s for the vendor URLs until
`scripts/vendor.sh` is run.

### Live tail uses a parallel `/ui/query/stream` WebSocket

- **Plan text (Phase 12, task 9).** "HTMX WebSocket extension on the
  result list element."
- **What ships.** `internal/ws.Server` now accepts a `Renderer`
  interface in `Options`. The JSON renderer (default, matching PRD
  §13.7) stays on `/api/query/stream`; a second `ws.Server` instance
  with an HTML renderer is mounted at `/ui/query/stream` and emits
  `hx-swap-oob="afterbegin:#tail-events"` fragments.

The PRD has a soft contradiction here: §13.7 specifies JSON on
`/api/query/stream`, while §11.2 describes the live tail as receiving
server-rendered HTML. Two routes resolves it cleanly: programmatic
clients keep the documented JSON shape, the browser gets HTML
fragments. The pair shares one fan-out, one filter compiler, and the
same close-code semantics — only the byte rendering differs.

The HTML renderer's `Status()` returns `nil`, which the existing
`enqueue` path treats as "skip this frame". Live tail liveness in the
browser is conveyed by WebSocket open/close — there is no place in the
HTML view to put status frames.

### Browser end-to-end tests deferred to Phase 16

- **Plan text (Phase 12, task 10).** "End-to-end browser test (using
  `playwright` or `chromedp`)."
- **What ships.** Template snapshot tests and HTTP-handler tests in
  `internal/ui/*_test.go` cover render correctness; the live browser
  pass moves to Phase 16's hardening + UX validation.

Reasoning: Playwright/chromedp pulls a browser binary into CI, which
the Phase-15 image work hasn't yet rationalised. Phase 16's UX usability
test already involves browser-driven validation against a populated
dataset, so combining both keeps the browser dependency in one place.
The template-render tests assert each HTMX hook (`hx-post`,
`hx-swap-oob`, `hx-trigger`) and oob-swap target appears in the markup,
so the contract between server and HTMX is verified without the
browser.

### Admin view is a read-only stub; full controls land in Phase 13

The admin template renders the retention summary, archive backend
summary, and the facet list, but no buttons drive backend mutations.
Phase 13's plan is explicit that "all buttons trigger the right
backend operations" is its DoD; this phase delivers just the read
surface and the navigation entry point.

### Theme controller uses native `[data-theme]` attribute switching

§12.3 specifies the three-layer CSS (`:root`, `@media`, `[data-theme]`)
exactly as implemented. The Alpine controller cycles
`auto → light → dark` and only sets `data-theme` for the manual modes;
"auto" deletes the attribute so the media query takes effect. This
matches the PRD's "Auto mode is the absence of `data-theme`"
literally.

## Phase 13 — Admin UI

### "Test invocation" re-runs `--probe` instead of a synthetic archive

- **PRD §14.4.** "Test invocation (script only) runs the script in
  archive mode against a tiny synthetic segment; output shown in a
  modal."
- **What ships.** The `[ Test invocation ]` button calls the script
  backend's `Probe(ctx)` and displays the refreshed capability state.
  Stdout/stderr capture is shown only when probe surfaces it (currently
  probe ignores those streams).

Fabricating a "tiny synthetic segment" at admin-click time means
writing a real Parquet file (DuckDB COPY of a single placeholder row)
and feeding it through the script's archive contract. The synthetic
parquet would then land in the operator's restic/rclone destination,
polluting their real archive — exactly the surprise we want to avoid
from a button the operator clicks for diagnostics. Probe is the
nearest stand-in: it exercises the script's exec path, surfaces fresh
capability state, and never produces a real archive object. A
synthetic-archive variant can ship later behind an opt-in flag if
operators ask for it.

### Admin handlers fork minimal state-transition helpers from `api`

`internal/ui/admin.go` carries small copies of
`api.evictRehydratedSegment` and the per-segment archive/rehydrate job
bodies so the UI package doesn't import `internal/api`. Both flows
ultimately call the same catalog primitives
(`MarkSegmentArchived` / `MarkSegmentRehydrated` / `ClearLocalFile`),
so the semantics stay aligned. The /api/* JSON endpoints remain the
source of truth for bulk operations (older-than archive, time-range
rehydrate); the /ui/admin/* endpoints exist for single-segment
operator actions.

### Storage pane omits active segment size

The catalog's `size_bytes` is the on-disk sealed Parquet size; the
active segment's DuckDB file isn't tracked there because its size
grows continuously between flushes. Rather than stat the DuckDB file
on every storage refresh (extra disk I/O on a 10s tick), the pane
shows the active segment as a count-only entry. PRD §14.4's mockup
calls out `Active segment: 1.4 GB`; we'll add the real number alongside
the Phase 14 metrics surface, which already needs to read RSS/disk
gauges.

### Admin live state cached in-process, not in the catalog

Last-retention/verify/test results are kept on `ui.Server.adminState`
so a page refresh re-renders the previous result. The state is
volatile — it resets on process restart. Persisting these into the
catalog would require a new table for what is essentially a UI
convenience (the underlying jobs row already records the durable
record of each operation).
