# Documentation

Complete documentation for Logpond, a single-container log search service for Dokku hosts.

## Getting started

- [Getting Started](getting-started.md) - what Logpond is, install, ship your first event, and search it.
- [Dokku Deployment](dokku-deployment.md) - the production walk-through: app create, storage mount, Vector sink, upgrades, backups.

## Reference

- [Configuration](configuration.md) - YAML and environment-variable settings, including which keys hot-reload.
- [API Reference](api-reference.md) - every public `/api/*` endpoint with request and response shapes.
- [Metrics and Health](metrics-and-health.md) - `/healthz`, `/metrics`, and the memory watchdog.

## Guides

- [Sources and Extraction](sources-and-extraction.md) - defining ingest sources and mapping incoming JSON to core fields.
- [Search Syntax](search-syntax.md) - the Datadog-style query language used by the search bar and `POST /api/parse-query`.
- [Facets](facets.md) - the sidebar's faceted browsing, built-in facets, and adding your own.
- [Retention](retention.md) - keeping disk usage bounded with `max_age` and `max_size`.
- [Archival](archival.md) - the S3 backend, the script backend, and how segments leave the local disk.
- [Rehydration and Import](rehydration-and-import.md) - pulling archived segments back into the UI and sideloading Parquet directly.
- [Live Tail](live-tail.md) - the WebSocket stream of matching events as they ingest.

## Operations

- [Troubleshooting](troubleshooting.md) - symptom-indexed responses to the most common operator issues.
- [Soak Test](soak-test.md) - the operator playbook for the 7-day load run that gates v1 releases.

## Internals

Spec and dev-process docs live in [`internals/`](internals/). They are not required reading for running Logpond.
