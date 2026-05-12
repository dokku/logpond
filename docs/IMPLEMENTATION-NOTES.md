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
