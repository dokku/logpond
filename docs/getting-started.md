# Getting started

Logpond is a single-container log search service. It accepts JSON log events over HTTP, keeps the recent ones in a fast local store, and pushes older data off to S3 (or anywhere else a script can put it). The web UI gives you a Datadog-style search bar, a faceted sidebar of common fields, and a live tail of incoming events.

## Why Logpond

If you run a couple of apps on a Dokku host, the off-the-shelf log stacks are awkward. Elasticsearch and Loki want their own server class. Datadog and Better Stack want a credit card. `grep` on log files works until you want to filter by field or look at last Tuesday.

Logpond targets the gap: under 256 MB of resident memory, a single container, a built-in UI, and Parquet files on disk that any DuckDB or pyarrow user can read directly. It assumes you ship logs with Vector (Dokku does this for you) and that authentication is your reverse proxy's job.

## A 30-second mental model

You will see these terms throughout the docs. They are not standard log-management vocabulary, so a quick tour:

- A **source** is one named HTTP ingest endpoint. `POST /ingest/<source>` is how events arrive. Each source has a field-extraction map that tells Logpond which JSON keys hold the timestamp, level, message, and so on.
- An **event** is one JSON log line. Logpond stores it as a row.
- A **segment** is a time-bucketed file. By default each segment covers one hour. Active segments are open for writes; sealed segments are immutable Parquet files. This is what makes search fast: a query for "last 15 minutes" scans one file, not the whole disk.
- A **facet** is a column whose distinct values appear in the sidebar, with counts. `service`, `level`, `host`, and `source` are built in. You can add custom facets for any attribute you care about.
- To **archive** a segment is to upload its Parquet file to S3 (or hand it to your archive script). To **rehydrate** is to pull an archived segment back to local disk so it shows up in queries again.

## Installation

The image is published multi-arch (`linux/amd64`, `linux/arm64`):

```bash
docker pull dokku/logpond:latest
```

For production, pin a specific tag (for example `dokku/logpond:v1.0.0` once cut) so an upstream release does not change behavior under you. Published images are signed with cosign keyless-OIDC; you can verify with `cosign verify dokku/logpond:v1.0.0 --certificate-identity-regexp 'https://github.com/dokku/logpond/.*' --certificate-oidc-issuer https://token.actions.githubusercontent.com`. For deploying on Dokku itself, see [Dokku Deployment](dokku-deployment.md).

## Your first ingest

Logpond reads its configuration from a YAML file pointed at by `LOGPOND_CONFIG` (defaulting to `/etc/logpond/config.yaml`). Start with the smallest config that defines one source:

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
```

`extract` maps each core field to an ordered list of JSON paths. Logpond tries each candidate in order and picks the first one that resolves to a usable value. The full extraction story (including fallbacks and level normalization) is in [Sources and Extraction](sources-and-extraction.md).

Start the container, mounting the config and a data directory:

```bash
docker run --rm -p 8080:8080 \
  -e LOGPOND_CONFIG=/etc/logpond/config.yaml \
  -v /tmp/logpond.yaml:/etc/logpond/config.yaml:ro \
  -v /tmp/logpond-data:/data \
  dokku/logpond:latest
```

The `/data` mount is where Logpond writes its catalog, the active DuckDB segment, sealed Parquet files, and rehydrated segments. Without a host mount, all of that disappears when the container stops.

In another terminal, send one event:

```bash
curl -X POST http://localhost:8080/ingest/default \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"timestamp":"2026-05-12T10:00:00Z","service":"api","level":"info","message":"hello"}\n'
```

The body is NDJSON: one JSON object per line, terminated with `\n`. The endpoint returns `202 Accepted` with the number of accepted and skipped lines:

```json
{ "accepted": 1, "skipped": 0 }
```

## See it in the UI

Open <http://localhost:8080/> in a browser. The search bar lives at the top, the facet sidebar on the left, the result list in the middle. Set the time range to "Last 1 hour" (the default `5m` will exclude an event timestamped exactly one minute ago less precisely than you might want) and your event appears.

Try clicking the `info` value under the **Level** facet: the search bar fills in `level:info` and the result list re-runs. Or type free text directly:

```
service:api "hello"
```

Both predicates and free-text search are detailed in [Search Syntax](search-syntax.md).

## What to read next

- [Configuration](configuration.md) - every setting, what it does, and which ones hot-reload.
- [Dokku Deployment](dokku-deployment.md) - the production walk-through for a real Dokku host.
- [Search Syntax](search-syntax.md) - the full Datadog-style query language.
- [Archival](archival.md) - sending sealed segments to S3 or an archive script of your choosing.
