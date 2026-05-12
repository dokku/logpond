# Metrics and health

Logpond exposes two operations endpoints:

- **`GET /healthz`** for liveness probes and the front-of-rack health check.
- **`GET /metrics`** in Prometheus text format for everything else - throughput, latency, archive activity, memory pressure.

These are the right hooks for "is it up?" and "is it well?" respectively. The Admin view of the UI is also useful, but it pulls from the same metrics, so for any automated alerting wire your Prometheus / Grafana at `/metrics` directly.

## `/healthz`

```bash
curl http://localhost:8080/healthz
```

Returns `200 OK` when the service is healthy:

```json
{ "status": "ok", "uptime_seconds": 84221, "version": "1.0.0" }
```

Returns `503` with a `reason` when the health probe fails. The probe fails when the catalog cannot be read, the data directory is unwritable, or a similar fatal-but-recoverable condition holds. A `503` from `/healthz` should take the instance out of rotation, not page someone immediately - retention and similar background work continues, and the service often recovers on the next cycle.

For Dokku's `CHECKS` health-check plugin, `/healthz` is the right URL.

## `/metrics`

Prometheus exposition. Scrape on a 15-second interval for a typical small deployment. The metrics divide cleanly into a few groups:

### Ingest

| Metric                                    | What it tells you                                                                  |
| ----------------------------------------- | ---------------------------------------------------------------------------------- |
| `logpond_ingest_events_total`             | Counter, total events accepted. Rises with traffic; the rate is your QPS.          |
| `logpond_ingest_skipped_lines_total`      | Counter, lines that failed to parse. A growing value means a producer is broken.   |
| `logpond_ingest_request_duration_seconds` | Histogram, per-request latency. Watch p99 - the budget is under 50 ms sustained.   |
| `logpond_ingest_buffer_fill_ratio`        | Gauge, 0-1. Anything above ~0.5 sustained means the writer is not keeping up.      |

### Query

| Metric                                       | What it tells you                                                          |
| -------------------------------------------- | -------------------------------------------------------------------------- |
| `logpond_query_duration_seconds`             | Histogram. p95 should be under 500 ms for 1-hour core-column queries.      |
| `logpond_facet_compute_duration_seconds`     | Histogram. The facet-aggregation slice of a query.                         |
| `logpond_search_suggest_duration_seconds`    | Histogram. Autocomplete p95 budget is 100 ms.                              |
| `logpond_query_count_duration_seconds`       | Histogram. Live-count indicator latency, same 100 ms p95 budget.           |
| `logpond_search_bar_parse_duration_seconds`  | Histogram. The Datadog-style parser. p99 budget is 5 ms.                   |

### Segments and disk

| Metric                                  | What it tells you                                                            |
| --------------------------------------- | ---------------------------------------------------------------------------- |
| `logpond_segments{state}`               | Gauge per state (`active`, `sealed`, `archived`, `rehydrated`).              |
| `logpond_disk_bytes{state}`             | Gauge per state. The sum of these against your disk capacity is your headroom. |
| `logpond_segment_seal_duration_seconds` | Histogram. p95 budget is 5 s for an hourly segment at target rate.           |
| `logpond_segment_flush_duration_seconds`| Histogram. Per-flush write latency. Rising p95 means disk is the bottleneck. |

### Archive

| Metric                                                       | What it tells you                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `logpond_archive_invocations_total{result}`                  | Counter per outcome (`ok`, `fail`).                          |
| `logpond_archive_script_invocations_total{mode,exit}`        | Counter per script mode and exit code. Useful for debugging script failures. |

### Live tail and facets

| Metric                       | What it tells you                                                |
| ---------------------------- | ---------------------------------------------------------------- |
| `logpond_live_tail_clients`  | Gauge, current subscriber count. Connection cap is 20 by default.|
| `logpond_facets{kind,source}`| Gauge per built-in / config / UI facet.                          |

### Memory and process

The standard Go process collector ships with `process_resident_memory_bytes`, `process_cpu_seconds_total`, and friends. The Logpond-specific:

| Metric                          | What it tells you                                                       |
| ------------------------------- | ----------------------------------------------------------------------- |
| `logpond_memory_inuse_bytes`    | Gauge, Go runtime in-use memory. Compared against `GOMEMLIMIT`.         |

## The memory watchdog

Logpond runs with `GOMEMLIMIT=240MiB` by default (the budget that fits within the 256 MB peak the resource budget allows for). A goroutine watches `logpond_memory_inuse_bytes`. When the value stays above **95 %** of `GOMEMLIMIT` for **30 seconds**, the watchdog sheds the oldest live-tail subscriber. The shed continues every 10 seconds as long as the threshold holds.

This is shedding, not crashing. The dropped client gets close code `1011` (server error) and reconnects on its own. The reason for choosing live-tail subscribers first is that each one holds memory for buffered events; ingest and query share the same pools and shedding them would lose data or break in-flight queries.

If you find the watchdog firing routinely:

- Lower `live_tail.max_clients` so you cap exposure before the watchdog has to step in.
- Tighten the live-tail filters - busy unfiltered subscriptions are the usual culprit.
- Raise `GOMEMLIMIT` if the host genuinely has more memory to give. Set the environment variable on the container; nothing in Logpond's YAML config controls it.

## What to scrape and alert on

For a small deployment, four metrics carry most of the signal:

```promql
# QPS
rate(logpond_ingest_events_total[1m])

# p99 ingest latency
histogram_quantile(0.99, sum(rate(logpond_ingest_request_duration_seconds_bucket[5m])) by (le))

# Buffer pressure
logpond_ingest_buffer_fill_ratio

# RSS
process_resident_memory_bytes
```

Suggested alerts:

- p99 ingest latency above 50 ms for 5 minutes.
- Buffer fill ratio above 0.7 for 5 minutes.
- RSS above 240 MB for 10 minutes (the watchdog will already be active).
- `logpond_segment_seal_duration_seconds` p95 above 5 s.
- Archive failure rate above zero (`rate(logpond_archive_invocations_total{result="fail"}[10m]) > 0`).

For full soak-test instrumentation see [Soak Test](soak-test.md).
