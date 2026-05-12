# FAQ

## What does Logpond replace?

For a single-host or small Dokku fleet, Logpond replaces the slice of a managed log product (Datadog, Sumo, Better Stack, Loki+Grafana) that handles short-window log search and faceted browsing. It is not an APM, not an alerting platform, and not a metrics store. It happily complements those.

## How is this different from Loki, Vector itself, or `grep`-on-files?

- **Vector** ships logs; Logpond stores them. Logpond is a downstream sink for Vector (or any HTTP NDJSON producer).
- **Loki** stores logs but its query story is line-oriented and benefits from labels, tracing-style indexing, and operators familiar with PromQL. Logpond is row-oriented (DuckDB + Parquet), exposes a Datadog-style search bar, and persists fewer dimensions natively. Pick Loki if you're already in a Prometheus stack; pick Logpond if you want a single binary, a built-in UI, and Parquet on disk.
- **`grep` on files** is the baseline. Logpond adds segmented retention, facets, archive/rehydrate, and a UI without giving up the "logs live in flat files you can grep" property - the sealed Parquet files are readable by `duckdb`, `pyarrow`, or `pandas` (PRD §8.5).

## Why DuckDB + Parquet?

PRD §6 lays out the choice in detail. The short version: DuckDB gives a single embedded SQL engine with first-class Parquet reader/writer, zstd compression, and a memory-bounded execution model. Parquet is a portable, tool-agnostic columnar format - segments archived to S3 remain readable by any DuckDB or pyarrow user, with no special service in the loop.

## Do I need S3?

No. The archive backend is pluggable. `archive.backend: none` (default) disables the archive UI and keeps everything on local disk until retention deletes it. `archive.backend: script` shells out to an operator-provided script - the `examples/archive-restic.sh` reference implementation uses restic.

## How do I authenticate users?

You don't, in Logpond. The PRD §3.2 contract is "authentication is the reverse proxy's problem." Run Dokku's letsencrypt + basic-auth plugins, put Logpond behind Cloudflare Access, or any other reverse-proxy auth solution. Logpond itself trusts the connection.

## Can multiple apps share one Logpond instance?

Yes - that's the intended setup. Each Dokku app is identified by `label.com.dokku.app-name`, which Logpond extracts into the `service` core column. The `service` facet then becomes "filter by app".

## What's the resource budget?

PRD §8.2's hot path: 150-200MB resident, ~256MB peak, < 10% of one core at 1,000 events/sec sustained. The reference deployment fits comfortably on a 2GB / 1-vCPU host. Disk grows at ~6GB/day at the target rate before retention runs.

## How do I tune retention?

`retention.max_age` and `retention.max_size` (PRD §7.8). Both can be set; whichever is hit first triggers archival or deletion. `archive_before_delete: true` (default when an archive backend is configured) ensures segments are archived before being purged from local disk.

## What happens during a deploy / restart?

Sealed Parquet segments and the catalog are durable on disk; restart re-reads them. The active segment is replayed from the WAL on startup. Vector's disk buffer covers the period the ingest endpoint is unavailable - typically a few seconds - so no events are lost in a clean restart.

## How big can a single segment get?

The default `segments.window` is 1 hour and `segments.max_size` is 1.8GB (active DuckDB file). At the §8.1 target rate of 1,000 events/sec, that's the natural one-hour cap; bursty workloads may seal early when the size limit hits first.

## Is the search bar a real query language?

It's a subset of the Datadog search syntax (PRD §7.3) - field:value, ranges with `[a TO b]`, `AND`/`OR`/`NOT`, attribute predicates with `@`, free text, wildcards. The parser emits warnings when a corner case is interpreted leniently (see Phase 5 notes); `POST /api/parse-query` will show you the canonical filter tree without executing.

## Can I run multiple Logponds and shard?

Not in v1. PRD §3.2 explicitly defers multi-tenancy. If you need fleet-wide search, run one Logpond per Dokku host and federate at query time externally.

## How do I migrate from a previous setup?

Two paths:

- **NDJSON files:** push them via `curl --data-binary @file.ndjson http://host/ingest/<source>`. Batching to ~50 lines per request keeps the ingest p99 healthy.
- **Existing Parquet:** drop the parquet + manifest + `.ready` triplet into `<data_dir>/import/` (PRD §7.10). The watcher picks it up within 30s and registers it as a rehydrated segment. The watcher path doesn't enforce the magic-byte check that the HTTP endpoint does, so locally-produced Parquet from any tool is accepted.

## How do I roll back a bad config?

Configuration is reloadable via `POST /api/admin/reload`. The PRD lists which keys are hot-reloadable (PRD §7.11.4). For non-reloadable keys, edit `logpond.yaml` or `dokku config:set` and restart.

## Why isn't there a CLI?

PRD §3.2 / "intentionally not in this plan": a CLI is post-v1. The HTTP API is the contract; `curl` covers the immediate need for scripting.

## Where do I find a recent activity audit?

Operator actions hit `jobs` rows in the catalog (PRD §10.2 + §13). The admin UI shows the latest retention, archive, rehydrate, and import jobs. There is no separate audit log surface in v1 - the jobs table doubles as one.

## How do I verify Parquet output is portable?

Run `scripts/verify-parquet.sh <path/to/segment.parquet>`. The script tries the DuckDB CLI, pyarrow, and pandas in turn, and reports per-tool PASS / FAIL / SKIPPED. PRD §19 success criterion 5 requires at least DuckDB and pyarrow to read every sealed file.

## Where's the changelog?

[`RELEASE-NOTES.md`](../RELEASE-NOTES.md) at the repo root tracks each version. The PRD records the long-form rationale; commit messages reference both.
