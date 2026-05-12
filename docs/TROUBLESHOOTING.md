# Troubleshooting

This guide collects the operational symptoms operators are most likely to hit and what to check first. The PRD (`docs/PRD.md`) remains the authoritative spec - this page is a pointer index for incident response.

## Ingest

### 429 from `/ingest/<source>`

Logpond's in-memory ring buffer is full and the segment writer can't drain it fast enough. The PRD §7.1 contract is to apply backpressure via 429 with `Retry-After: 1`; Vector and most well-behaved clients retry automatically.

Check:

- `logpond_ingest_buffer_fill_ratio` on `/metrics` - sustained > 0.5 means the buffer is bordering on full.
- `logpond_segment_flush_duration_seconds` - if flush p95 is rising, disk is the bottleneck.
- `df -h` on the data dir - DuckDB will block when the volume is full.

Tunables (PRD §7.11.1):

- `memory_limits.ring_buffer` (default 50MB) - the buffer is sized in bytes; raise if RAM allows.
- `segments.flush_interval` (default 1s) - lower means smaller, more frequent writes; useful on slow disks.

### 415 `bad_request` "Content-Type must be application/x-ndjson..."

A client is sending an unrecognized content type. Logpond accepts `application/x-ndjson`, `application/json`, `application/ndjson`, and an empty content type (Vector's NDJSON sink omits the header in some versions). Anything else is rejected.

### 413 `payload_too_large`

A single ingest body decoded to more than 32MB. Split the upload into smaller batches. (PRD §7.1 caps the decoded body at 32MB.)

## Query

### Queries returning no events, but live tail shows traffic

- Check the time range in the UI - the search bar's default `5m` excludes anything older.
- Confirm the source is configured. `GET /api/admin/sources` (Phase 13 UI) lists every source the binary saw at startup.
- Try `service:*` to rule out the search predicate.

### Query latency spikes after a seal

PRD §8.1 budgets 5s for a one-hour segment seal at the target ingest rate. During that window the query layer skips the sealing segment (Phase 4 executor); queries fall back to sealed Parquet only. If you see spikes longer than 5s, check `logpond_segment_seal_duration_seconds` and disk IOPS.

### Facets show stale values

Facet sampling reads the N most recent queryable segments (PRD §7.4.3 + the Phase 6 implementation note widens this to include active segments). If a custom facet was just added and doesn't appear in the sidebar, hit `POST /api/admin/reload` - the facet registry is the main reloadable surface.

## Archive

### `archive_unavailable` 503 on `POST /api/archive`

`archive.backend: none` is configured. The endpoint is wired but the backend is no-op; `GET /api/admin/archive/capabilities` will confirm. Set `LOGPOND_ARCHIVE_BACKEND=s3` or `LOGPOND_ARCHIVE_BACKEND=script` and restart.

### Script archive returning `rehydrate_unsupported`

The script's `--probe` reported retrieve=no (exit 64). Run the script manually with `LOGPOND_MODE=retrieve --probe` to confirm. Once the script implements retrieve, hit `POST /api/admin/archive/probe` to refresh the cached capability.

### "Script backend: Path ... must be absolute / must be normalized"

The Phase 16 security audit tightened script-path validation: `archive.script.path` must be an absolute, normalized path (no `..`, no `//`). Update the config to a fully qualified path inside the container.

### Archive subprocess sees no AWS credentials / no LOGPOND_* secrets

This is intentional (Phase 16 task 7). The script subprocess inherits the parent environment minus the `AWS_*` and `LOGPOND_*` namespaces. Pass any credentials the script needs via `archive.script.env.*` config keys (also exposed as `LOGPOND_ARCHIVE_SCRIPT_ENV__<KEY>` env). PATH, HOME, USER, TZ, and other non-Logpond vars still pass through.

## Rehydrate / sideload

### `POST /api/import` returns 400 "not a Parquet file: missing PAR1 header"

The Phase 16 audit added a magic-bytes check before the SHA-256 verify. The uploaded `parquet` part either isn't a Parquet file or was truncated in transit. Re-upload, and verify locally with `scripts/verify-parquet.sh <path>` first.

### Watched-import drops a triplet to `import/failed/<id>/error.txt`

`error.txt` contains the rejection reason - usually a `parquet_sha256` mismatch between the manifest and the file. The watcher uses `segments.ParquetSHA256` to compute the hash; recompute manually with `sha256sum segment-<id>.parquet` and compare to the manifest.

## Live tail (WebSocket)

### Closed with code 4003

Slow client. Logpond closes a tail connection if a write into the per-connection queue stalls past `slow_client_grace` (PRD §7.6 / Phase 11 note). Browsers usually self-heal; if it persists, check the network or the browser tab's CPU load.

### Closed with code 4004

Connection cap hit. The default is 32 concurrent tail subscribers; the cap is `live_tail.max_clients`.

## Memory pressure

The Phase 14 watchdog samples Go runtime in-use memory and sheds the oldest live-tail subscriber when it stays > 95% of `GOMEMLIMIT` for 30s. If subscribers are getting dropped without explanation, check `logpond_memory_inuse_bytes` and `GOMEMLIMIT` (default 240MiB; lower in `dokku config` if your host can't afford the full budget).

## Health & metrics

| Endpoint                | Returns                                                                          |
| ----------------------- | -------------------------------------------------------------------------------- |
| `GET /healthz`          | 200 with uptime + version; 503 with `reason` when the health probe fails.        |
| `GET /metrics`          | Prometheus exposition (PRD §13.25).                                              |
| `GET /api/admin/config` | Effective config with secrets redacted (Phase 14 / PRD §13.20).                  |

## When to file a bug

Open an issue if:

- A documented success criterion in PRD §19 fails on a clean install.
- An ingested event is silently dropped (not 4xx, not in any segment).
- The UI renders inconsistent state between facets and the result list for the same query/range.

Include the output of `GET /api/admin/health` (Phase 14), `GET /api/admin/config`, and the last ~50 lines of stdout JSON logs.
