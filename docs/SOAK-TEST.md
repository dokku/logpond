# Soak test playbook

PRD §19 success criterion 1 is "1,000 events/sec sustained for 7 days under 256MB RSS on a 2GB host." Phase 16 ships the tooling; this page is the operator playbook for executing the run.

## Prerequisites

- A 2GB / 1-vCPU host with at least 50GB of free disk (worst-case 6.1GB/day for 7 days = 43GB before retention).
- Logpond deployed per [`DEPLOY-DOKKU.md`](DEPLOY-DOKKU.md), reachable over the network the load generator uses.
- An archive backend configured (S3 or script). The soak verifies the full retention → archive cycle, not just ingest.
- `retention.max_age` set to 24h or shorter so the retention cycle exercises archive + delete during the run.

## The run

From a separate host (the load generator is CPU-light but you don't want it competing with the SUT for memory):

```bash
go run ./cmd/loadgen \
  -url https://logpond.example.com \
  -source dokku \
  -rate 1000 \
  -duration 168h \
  -report-every 5m \
  >loadgen.log 2>&1 &
```

The driver writes a progress line every 5 minutes with running totals and rolling latency percentiles. Tail `loadgen.log` to spot regressions early.

## What to watch

Scrape these on a 15s interval (Prometheus + Grafana on the SUT works fine):

| Metric                                          | Target                                  |
| ----------------------------------------------- | --------------------------------------- |
| `logpond_ingest_request_duration_seconds`       | p99 < 50ms                              |
| `logpond_ingest_events_total`                   | Rises smoothly at ~1,000/sec            |
| `logpond_ingest_buffer_fill_ratio`              | < 0.5 in steady state                   |
| `logpond_memory_inuse_bytes`                    | < 256MB                                 |
| `logpond_segment_seal_duration_seconds`         | < 5s p95                                |
| `logpond_archive_invocations_total{result="ok"}` | Climbs with each retention pass        |
| `process_resident_memory_bytes`                 | < 256MB                                 |
| `process_cpu_seconds_total` rate                | < 0.10 / sec (10% of one core)          |

Disk:

```bash
watch -n 60 df -h /var/lib/dokku/data/storage/logpond
```

If RSS or buffer-fill rises monotonically over hours, stop the run and capture a heap profile (`/debug/pprof/heap`) for analysis. The PRD §8.2 budget has no room for unbounded growth.

## Burst test

The burst phase is a one-shot:

```bash
go run ./cmd/loadgen \
  -url https://logpond.example.com \
  -burst-rate 5000 -burst-duration 10s \
  -rate 0
```

Expected outcome:

- The first second or two surface a small number of 429s (the backpressure path; PRD §7.1).
- The load generator's `backpressure_429` counter is non-zero but bounded.
- Once the buffer drains, ingest p99 returns to < 50ms within ~30s.

Vector retries 429s automatically with backoff; the load generator does not retry, which is why a 429 count > 0 is expected.

## Query bench during the soak

Once the soak has accumulated a few hours of data, run the bench from another shell on the load-generator host:

```bash
go run ./cmd/querybench -url https://logpond.example.com -iterations 50
```

Each case prints PASS/FAIL against PRD §8.1's per-case p95 target. Re-run periodically through the week; a regression mid-soak is worth investigating before it compounds.

## Cross-tool Parquet verification

Roughly every 24h, pick one freshly-sealed segment and verify it reads in DuckDB, pyarrow, and pandas:

```bash
seg=$(curl -s https://logpond.example.com/api/segments | jq -r '.segments[] | select(.state=="sealed") | .id' | head -1)
scp logpond.example.com:/var/lib/dokku/data/storage/logpond/segments/sealed/${seg}.parquet /tmp/
scripts/verify-parquet.sh /tmp/${seg}.parquet
```

The script reports per-tool PASS / FAIL / SKIPPED. PRD §19 criterion 5 requires at least DuckDB and pyarrow to read every sealed file.

## When the soak ends

After 168 hours (7 days), capture:

- The final `loadgen.log` (total accepted, skipped, 429s, p50/p95/p99 over the full run).
- A 24h Grafana export of the metrics above.
- The Prometheus `process_resident_memory_bytes` series - the 95th percentile of RSS across the run is the headline number for PRD §19 criterion 1.
- Output of `scripts/verify-parquet.sh` against three random sealed segments and one rehydrated segment.

Append the summary to `docs/IMPLEMENTATION-NOTES.md` under a "Phase 16 - soak run" subsection, and flip the Phase 16 box in `docs/IMPLEMENTATION-STATUS.md` if every criterion held.

## Rolling back a failed soak

If the run fails before 168h:

1. Capture the failure window's metrics + a stdout log slice ± 5 minutes around the failure.
2. Stop the load generator and let Logpond drain (buffer + flush should complete within a minute).
3. File the regression with the metrics window attached. Do not retry the soak on the same host until the cause is understood; an unbounded-growth bug will repeat.
