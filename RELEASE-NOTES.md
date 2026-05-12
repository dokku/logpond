# Release notes

## Unreleased — v1.0.0-rc1

First release candidate. The 16-phase implementation plan has landed in tree; the v1.0.0 tag is gated on the external soak / archive-matrix / UI-usability passes per Phase 16's definition of done.

### What's in

- Single Go binary that ingests NDJSON over HTTP, segments to DuckDB, seals to Parquet + zstd-3, and queries across active and sealed segments with a Datadog-style search bar.
- Server-rendered HTMX + Alpine + Open Props UI under ~60KB gzipped; light/dark theme with OS-preference autodetect.
- Pluggable archive backend (S3 / R2 / MinIO / operator script) with retention → archive → rehydrate end-to-end.
- WebSocket live tail with slow-client + connection-cap handling.
- Catalog, retention, archive jobs durable in SQLite; restart-safe.
- Prometheus `/metrics` and `/healthz`; memory watchdog sheds tail subscribers under pressure (PRD §8.2).
- Cross-tool Parquet output verified against DuckDB, pyarrow, pandas (PRD §8.5).
- Multi-arch container (`linux/amd64`, `linux/arm64`); bats integration suite that boots Dokku + MinIO + a local registry under docker compose.

### Operator tooling new in Phase 16

- `cmd/loadgen` — sustained + burst NDJSON load generator.
- `cmd/querybench` — representative-query latency suite tied to PRD §8.1.
- `scripts/verify-parquet.sh` — cross-tool Parquet read verification.
- `docs/soak-test.md`, `docs/troubleshooting.md`, and the topic guides under `docs/` - operator-facing docs.

### Security hardening (Phase 16 task 7)

- `POST /api/import` validates the PAR1 magic header + footer before computing SHA-256, so non-Parquet uploads fail fast with a clear message.
- The archive script subprocess no longer inherits `AWS_*` or `LOGPOND_*` from the parent. Operator-supplied env via `archive.script.env.*` and system variables (`PATH`, `HOME`, ...) still pass through.
- `archive.script.path` must be absolute and normalized; relative paths and paths containing `..` are rejected at startup.
- Startup config logging passes through `cfg.Redacted()`, blanking S3 access/secret keys and every `archive.script.env.*` value.

### Known follow-ups before v1.0.0

These items remain operator responsibilities per Phase 16's plan and gate the v1.0.0 tag:

1. 7-day soak at 1,000 events/sec on a 2GB host (PRD §19 criterion 1).
2. Archive backend matrix against real AWS S3, Cloudflare R2, and MinIO endpoints (§19 criterion 4).
3. Cross-tool Parquet verification against a fully-populated dataset (§19 criterion 5).
4. UI usability pass with someone unfamiliar with the project (§19 criterion 7).
5. Under-30-minutes new-user-on-Dokku setup pass (§19 criterion 6).

When all five complete, flip `Phase 16` in `docs/internals/IMPLEMENTATION-STATUS.md` and cut the `v1.0.0` tag.
