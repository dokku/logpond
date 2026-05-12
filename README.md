# Logpond

A single-container log search service for Dokku hosts. Ingests JSON logs over HTTP, keeps recent logs hot for fast search and faceted browsing, and archives aged segments to S3 (or any destination you can script).

## Installation

Pull the multi-arch container image:

```bash
docker pull ghcr.io/dokku/logpond:latest
```

Or deploy on Dokku in one step:

```bash
dokku git:from-image logpond ghcr.io/dokku/logpond:latest
```

Pin a specific tag in production (`ghcr.io/dokku/logpond:v1.0.0` once cut) so an upstream release does not change behavior under you. The full Dokku walk-through (storage mount, env vars, Vector sink, feedback-loop avoidance) is in [`docs/dokku-deployment.md`](docs/dokku-deployment.md).

## Usage

Run Logpond locally with a minimal config:

```bash
mkdir -p /tmp/logpond-data
cat >/tmp/logpond.yaml <<'EOF'
data_dir: /tmp/logpond-data
sources:
  - name: default
    extract:
      timestamp: [timestamp]
      level: [level]
      message: [message]
      service: [service]
      host: [host]
EOF

docker run --rm -p 8080:8080 \
  -e LOGPOND_CONFIG=/etc/logpond/config.yaml \
  -v /tmp/logpond.yaml:/etc/logpond/config.yaml:ro \
  -v /tmp/logpond-data:/data \
  ghcr.io/dokku/logpond:latest
```

Send a log event with `curl`:

```bash
curl -X POST http://localhost:8080/ingest/default \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"timestamp":"2026-05-12T10:00:00Z","service":"api","level":"info","message":"hello"}\n'
```

Open <http://localhost:8080/> to search, browse facets, and live-tail.

## Documentation

Full documentation lives in [`docs/`](docs/). Start with [Getting Started](docs/getting-started.md).

## License

[MIT](LICENSE)
