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
