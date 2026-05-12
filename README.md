# Logpond

A small, single-container log search service for Dokku hosts. Ingests JSON logs over HTTP, keeps recent logs hot for fast search and faceted browsing, archives aged segments to S3 or via a user-provided script, and lets operators rehydrate archives back into the same UI.

- **Spec:** [`docs/PRD.md`](docs/PRD.md)
- **Deploy on Dokku:** [`docs/DEPLOY-DOKKU.md`](docs/DEPLOY-DOKKU.md)
- **Troubleshooting:** [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)
- **FAQ:** [`docs/FAQ.md`](docs/FAQ.md)
- **Soak test playbook:** [`docs/SOAK-TEST.md`](docs/SOAK-TEST.md)
- **Release notes:** [`RELEASE-NOTES.md`](RELEASE-NOTES.md)
- **Progress:** [`docs/IMPLEMENTATION-STATUS.md`](docs/IMPLEMENTATION-STATUS.md)

## Quick start (local)

Requires Go 1.26+ and Bash for the vendor script.

```bash
scripts/vendor.sh            # fetch htmx, Alpine, Open Props
go run ./cmd/logpond -config /tmp/logpond.yaml
```

A minimal `/tmp/logpond.yaml`:

```yaml
data_dir: /tmp/logpond-data
sources:
  - name: default
    extract:
      timestamp: [timestamp]
      level: [level]
      message: [message]
      service: [service]
      host: [host]
```

Send a few events:

```bash
curl -X POST http://localhost:8080/ingest/default \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"timestamp":"2026-05-12T10:00:00Z","service":"api","level":"info","message":"hello"}\n'
```

Open <http://localhost:8080/> in a browser to search, browse facets, and live-tail.

## Operator tooling

Phase 16 ships three helpers for hardening a Logpond deployment:

| Tool                          | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `go run ./cmd/loadgen`        | Sustained + burst NDJSON load generator. PRD §8.1 targets.                   |
| `go run ./cmd/querybench`     | Representative-query latency suite. Verifies §8.1 query targets.             |
| `scripts/verify-parquet.sh`   | Reads a sealed Parquet via DuckDB, pyarrow, and pandas to verify §8.5.       |

See [`docs/SOAK-TEST.md`](docs/SOAK-TEST.md) for a 7-day soak runbook.

## Integration tests

`make setup test` brings up Dokku, a local Docker registry, and MinIO via `docker compose`, builds the Logpond image, deploys it, and runs the bats suite under `tests/`. Use `make setup-native test-native` if Dokku is installed on the host.
