# Soak test

The Logpond release process gates new versions on a 7-day continuous load run at 1,000 events per second on a 2 GB host, with resident memory staying under 256 MB the whole time. This page is the operator playbook for running that soak, including the tooling that ships in `cmd/loadgen` and `cmd/querybench`.

You do not need to run the soak to use Logpond. This page is for releases - cutting a tag, validating a configuration change, or qualifying a new host - and for anyone diagnosing a slow leak.

## Prerequisites

- A 2 GB / 1-vCPU host with at least 50 GB of free disk. The worst case is roughly 6.1 GB per day, so seven days needs 43 GB before retention kicks in.
- Logpond deployed and reachable from the load generator. See [Dokku Deployment](dokku-deployment.md) for the standard setup.
- An archive backend configured (S3 or script). The soak validates the full retention -> archive -> rehydrate cycle, not just ingest. With `archive.backend: none` the run still works but skips the archive path.
- `retention.max_age` set to 24h or shorter, so the retention cycle exercises archive plus delete during the run rather than only at the end.

## The sustained run

The load generator is a Go program in this repo. Run it from a separate host so it does not compete with Logpond for memory:

```bash
go run ./cmd/loadgen \
  -url https://logpond.example.com \
  -source dokku \
  -rate 1000 \
  -duration 168h \
  -report-every 5m \
  >loadgen.log 2>&1 &
```

What each flag does:

- `-url` is Logpond's base URL.
- `-source` matches a configured source name. Use whatever your real Vector sink uses.
- `-rate 1000` is the sustained events-per-second target.
- `-duration 168h` is seven days.
- `-report-every 5m` prints a progress line every five minutes with running totals and rolling latency percentiles.

Tail `loadgen.log` from another shell to spot regressions early. Progress lines look roughly like:

```
[run] elapsed=42m rate=1000.0/s accepted=2520000 skipped=0 p50=8.2ms p95=14.1ms p99=22.7ms 429s=0
```

## What to watch

Scrape Logpond's `/metrics` endpoint on a 15-second interval and watch these against the budget:

| Metric                                                        | Target                              |
| ------------------------------------------------------------- | ----------------------------------- |
| `logpond_ingest_request_duration_seconds` p99                 | < 50 ms                             |
| `logpond_ingest_events_total` rate                            | smooth around 1,000/s               |
| `logpond_ingest_buffer_fill_ratio`                            | < 0.5 in steady state               |
| `logpond_memory_inuse_bytes`                                  | < 256 MB                            |
| `logpond_segment_seal_duration_seconds` p95                   | < 5 s                               |
| `logpond_archive_invocations_total{result="ok"}`              | climbs with each retention pass     |
| `process_resident_memory_bytes`                               | < 256 MB                            |
| `rate(process_cpu_seconds_total[5m])`                         | < 0.10 (10 % of one core)           |

Disk on the host:

```bash
watch -n 60 df -h /var/lib/dokku/data/storage/logpond
```

If resident memory or buffer-fill rises monotonically over hours - not a brief excursion but a clear upward trend - stop the run and capture a heap profile for analysis:

```bash
curl -o heap.pprof http://localhost:8080/debug/pprof/heap
```

The memory budget has no room for unbounded growth, so a monotonic rise is the signal of a leak.

## The burst test

The burst phase confirms backpressure works. It is a one-shot, not part of the 7-day run:

```bash
go run ./cmd/loadgen \
  -url https://logpond.example.com \
  -burst-rate 5000 -burst-duration 10s \
  -rate 0
```

Setting `-rate 0` disables the sustained phase so only the burst runs.

Expected outcome:

- The first second or two surface a small number of `429`s as the ingest buffer fills and the writer applies backpressure.
- The load generator's `backpressure_429` counter is non-zero but bounded.
- Once the burst stops and the buffer drains, ingest p99 returns to under 50 ms within about 30 seconds.

Vector retries `429`s automatically with backoff; the load generator does not retry, which is why a positive 429 count is expected and fine.

## Query bench during the soak

Once the soak has accumulated a few hours of data, run the query benchmark from another shell on the load-generator host. It exercises the representative-query latency suite and reports PASS/FAIL against the per-case budget:

```bash
go run ./cmd/querybench -url https://logpond.example.com -iterations 50
```

Re-run periodically through the week. A mid-soak regression is worth investigating before it compounds - either the data shape is degrading query performance, or a slow leak in the query path is widening latencies.

## Cross-tool Parquet verification

Roughly every 24 hours, pick one freshly-sealed segment and verify it reads in all three target tools (DuckDB, pyarrow, pandas):

```bash
seg=$(curl -s https://logpond.example.com/api/segments | jq -r '.segments[] | select(.state=="sealed") | .id' | head -1)
scp logpond.example.com:/var/lib/dokku/data/storage/logpond/segments/sealed/${seg}.parquet /tmp/
scripts/verify-parquet.sh /tmp/${seg}.parquet
```

The script reports per-tool PASS / FAIL / SKIPPED. The release-gating criterion is that DuckDB and pyarrow both read every sealed file; pandas is reported but not required.

## When the run ends

After 168 hours, capture these for the release record:

- The final `loadgen.log` - total accepted, total skipped, total 429s, plus p50, p95, and p99 latencies over the full run.
- A 24-hour Grafana export of the metrics above.
- The Prometheus `process_resident_memory_bytes` series - the 95th percentile of resident memory across the run is the headline number.
- Output of `scripts/verify-parquet.sh` against three random sealed segments and one rehydrated segment.

If every criterion held, the release ships. If any did not, the failure window's metrics plus a stdout-log slice from five minutes around the failure are the evidence for the regression report.

## Rolling back a failed soak

If the run fails before 168 hours:

1. Capture the failure window's metrics and a stdout log slice (`docker logs <container>` or `dokku logs logpond`) covering five minutes on either side of the failure.
2. Stop the load generator and let Logpond drain. The buffer and flush should complete within a minute.
3. File the regression with the metrics window attached.
4. Do not retry the soak on the same host until the cause is understood. Unbounded growth or correctness bugs will repeat.
