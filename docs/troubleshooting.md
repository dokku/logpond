# Troubleshooting

A symptom-indexed guide to the things that go wrong most often. Each section starts with what an operator typically sees, explains what is actually happening, and lists what to check.

For the underlying mechanics see the linked topic pages. For metrics names and what to scrape see [Metrics and Health](metrics-and-health.md).

## Ingest

### `429 Too Many Requests` from `/ingest/<source>`

Logpond's in-memory ingest buffer is full and the segment writer cannot drain it fast enough. The response includes `Retry-After: 1`, and Vector (or any well-behaved client) retries automatically. Brief 429s during a burst are normal and not a cause for alarm.

If 429s are sustained, check:

- `logpond_ingest_buffer_fill_ratio`. Anything above 0.5 sustained means the writer is the bottleneck.
- `logpond_segment_flush_duration_seconds` p95. Rising values point at slow disk.
- `df -h` on the data directory. DuckDB blocks when the volume is full.

Tunables that help:

- Raise `memory_limits.ring_buffer` from `50MB` if the host has spare RAM. Bigger buffer absorbs longer bursts.
- Lower `segments.flush_interval` from `1s` if the disk is fast - smaller, more frequent writes can reduce contention. On slow disks, do the opposite.

### `415 Unsupported Media Type`

A client is sending an unrecognized `Content-Type`. Logpond accepts `application/x-ndjson`, `application/json`, `application/ndjson`, and an empty Content-Type (some Vector versions omit the header). Anything else is rejected. Set the header on the client side; if you cannot, configure the producer to send NDJSON with no header.

### `413 Payload Too Large`

One ingest body decoded to more than 32 MB. Split the upload into smaller batches - 50 events per request is a healthy default. The 32 MB limit caps decompressed body size, so a heavily-compressed gzip body that decompresses past the limit still trips it.

### Events accepted but missing from the UI

The endpoint returns `{"accepted": N, "skipped": 0}` but the UI shows nothing in the time range. Likely causes:

- **Time range too narrow.** The default search-bar time range is "Last 5 minutes." If events are timestamped in the future or in the past, they will not show. Widen the range or check `attributes.logpond_timestamp_fallback` to see if Logpond fell back to ingest time.
- **Wrong source name.** The search bar's `source` facet shows what is configured. Confirm with `GET /api/admin/sources`.
- **Filtered out.** Try `service:*` to remove all predicates and confirm events exist.

## Query

### Queries return no events, but live tail shows traffic

Live tail watches the ingest path; queries scan stored segments. If live tail works and queries do not, the difference is usually the time range or the predicate. Widen the time range; try `service:*` to remove all field predicates and check whether events exist at all.

If they do exist but a specific filter excludes them, run `POST /api/parse-query` with your query string to see how it parsed. The output often reveals an unexpected interpretation (a free-text term where you meant a predicate, for example).

### Query latency spikes after a seal

Sealing an hourly segment takes up to 5 seconds at target ingest rate. During that window the executor skips the sealing segment and queries fall back to the already-sealed Parquet files. Spikes longer than 5 seconds point at disk pressure.

Check `logpond_segment_seal_duration_seconds` p95 and disk IOPS. A slow disk affects both the seal and concurrent queries.

### Facets show stale values

Facets are sampled from the N most recent segments overlapping the time range (`facets.segment_sample_size`, default 24). A value that only exists in older segments will not appear in the sidebar - that is by design, to keep facet computation fast. The value is still queryable; just type it in the search bar manually.

If a custom facet you just added does not show up, hit `POST /api/admin/reload`. Facets registered through the admin UI take effect on the next query without a reload.

## Archive

### `503 archive_unavailable` from `POST /api/archive`

`archive.backend: none` is configured. The endpoint exists but is wired to a no-op backend. Check with `GET /api/admin/archive/capabilities` to confirm. To enable, set `LOGPOND_ARCHIVE_BACKEND=s3` (or `script`) and restart.

### Script backend reports `retrieve` unsupported

The script's `--probe` returned exit 64 for the retrieve mode (or the probe has not been refreshed since you implemented retrieve). Confirm by running the script manually:

```bash
./archive.sh retrieve --probe
echo $?
```

Exit `0` means it supports retrieve; `64` means it does not. If your script now supports retrieve, refresh the cached capability:

```bash
curl -X POST http://localhost:8080/api/admin/archive/probe
```

### `Script backend: Path ... must be absolute / must be normalized`

`archive.script.path` must be an absolute path with no `..` and no `//`. Edit the config (or `dokku config:set logpond LOGPOND_ARCHIVE_SCRIPT_PATH=/full/path/to/script.sh`) to a fully qualified path inside the container.

### Archive subprocess does not see expected credentials

The script subprocess inherits the parent environment **minus** the `AWS_*` and `LOGPOND_*` namespaces. This prevents leaking Logpond's own S3 credentials into a script that does not need them. Pass any variables the script needs through `archive.script.env.*` config keys (or `LOGPOND_ARCHIVE_SCRIPT_ENV__<KEY>` env vars):

```bash
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=...
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

`PATH`, `HOME`, `USER`, `TZ`, and other non-Logpond variables still pass through.

## Rehydrate and import

### `POST /api/import` returns `400 not a Parquet file: missing PAR1 header`

Logpond checks the file's magic header (`PAR1`) before computing the SHA-256. The uploaded `parquet` part either is not actually a Parquet file or was truncated in transit. Re-upload, and verify the file locally first:

```bash
scripts/verify-parquet.sh path/to/segment.parquet
```

The script tries DuckDB, pyarrow, and pandas and reports per-tool success. If none of them can read the file, fix the file before retrying the upload.

### Watched-import dropped a triplet into `import/failed/<id>/`

Look at `error.txt` in that directory. The most common cause is a `parquet_sha256` mismatch between the manifest and the file - the SHA-256 in the manifest does not match the actual Parquet's hash. Recompute manually:

```bash
sha256sum segment-202604120100.parquet
```

Compare with `parquet_sha256` in the manifest JSON. If they disagree, either the manifest was generated from a different file or the file was modified after the manifest was written. Regenerate the manifest from the current file and retry.

## Live tail

### Connection closed with code `4003`

Slow client. The server's per-client send queue stalled past the grace period (30 seconds). Browsers usually self-heal on the next page focus; if it keeps happening, check the tab's CPU load or the network path.

### Connection closed with code `4004`

The global subscriber cap is hit. The default is 20 concurrent live-tail subscribers (`live_tail.max_clients`). Raise it carefully - each subscriber consumes memory and a goroutine. The right answer is often to share a tail rather than open a new one.

### Connection closed with code `4001`

`filter_required`: your subscription had no leaf predicates. Live tail rejects unfiltered streams because they would saturate the rate cap immediately. Send a subscription with at least one `field:value` predicate or a free-text term.

## Memory pressure

The watchdog drops the oldest live-tail subscriber when in-use memory stays above 95 % of `GOMEMLIMIT` for 30 seconds. If subscribers vanish without explanation, check:

- `logpond_memory_inuse_bytes` over time.
- The current `GOMEMLIMIT` value (default 240 MiB; lower in `dokku config:set` if your host has less to give).
- The metrics in [Metrics and Health](metrics-and-health.md) for what is using the memory.

The watchdog never sheds ingest or query work; only live-tail subscribers. So if every subscriber is being shed, something else (a huge query, a sealing segment) is using the memory and tightening the live-tail max-clients is not the fix.

## Where to look

| Question                                      | Command                                       |
| --------------------------------------------- | --------------------------------------------- |
| Is the service up?                            | `curl http://localhost:8080/healthz`          |
| What's the effective config (secrets hidden)? | `curl http://localhost:8080/api/admin/config` |
| Recent jobs (archive, rehydrate, verify)?     | The Admin view, or `GET /api/jobs/<id>`.      |
| Current segment list?                         | `curl http://localhost:8080/api/segments`     |
| All metrics in one place?                     | `curl http://localhost:8080/metrics`          |

## When to file a bug

Open an issue if:

- The `/healthz` endpoint returns 503 with a reason you cannot reconcile from this page.
- An event was returned `202 Accepted` by the ingest endpoint but does not appear in any segment afterwards.
- The UI displays inconsistent state between the facet sidebar and the result list for the same query and time range.
- A documented error code returns with no useful `details`.

Include in the report:

- `GET /api/admin/config` (secrets are redacted automatically).
- `GET /healthz`.
- The last ~50 lines of stdout JSON logs (`docker logs <container>` or `dokku logs logpond`).
- The exact request and response that reproduced the issue, if applicable.
