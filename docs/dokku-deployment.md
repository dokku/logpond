# Dokku deployment

Logpond ships as a standard Dokku app. Dokku's `logs` plugin has built-in Vector support: setting a `vector-sink` (globally or per-app) starts a Vector container that ships Docker container logs to whatever you point it at. Logpond is just the other end of that pipe.

This page walks through deploying Logpond, wiring Dokku's Vector sink to its ingest endpoint, and avoiding the obvious pitfall of Logpond shipping its own logs back to itself.

## Dokku's Vector source, briefly

When Dokku's Vector sink is active, every container's log line becomes one JSON event with this shape:

| Field                       | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `message`                   | The log line itself                                          |
| `container_name`            | Docker container name (Dokku-generated, like `web.1.<hash>`) |
| `container_id`              | Docker container ID                                          |
| `image`                     | Image name                                                   |
| `host`                      | The Dokku host's hostname                                    |
| `label.com.dokku.app-name`  | The Dokku app name (for example `myapp`)                     |
| `timestamp`                 | Time Docker received the log line                            |

Knowing the field names matters because Logpond extracts the Dokku **app name** out of `label.com.dokku.app-name` and puts it on the row as `service`. That is what makes the sidebar's `service` facet read "filter by Dokku app."

## 1. Deploy Logpond as a Dokku app

### Step 1: create the app and persistent storage

```bash
dokku apps:create logpond
dokku domains:set logpond logpond.example.com

dokku storage:ensure-directory logpond
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond:/data
```

The `/data` bind mount holds the SQLite catalog, the active DuckDB segment, sealed Parquet files, and rehydrated segments. Without it everything disappears on `dokku ps:rebuild`. Pinning the storage path under `/var/lib/dokku/data/storage/logpond` matches Dokku's convention so your host backups already cover it.

### Step 2: configure with environment variables

For a typical S3-backed deployment (Cloudflare R2 shown - swap the endpoint for AWS or MinIO):

```bash
dokku config:set logpond \
  LOGPOND_LOG_LEVEL=info \
  LOGPOND_RETENTION_MAX_AGE=30d \
  LOGPOND_RETENTION_MAX_SIZE=10GB \
  LOGPOND_ARCHIVE_BACKEND=s3 \
  LOGPOND_ARCHIVE_S3_ENDPOINT=https://abc.r2.cloudflarestorage.com \
  LOGPOND_ARCHIVE_S3_BUCKET=logs-archive \
  LOGPOND_ARCHIVE_S3_PREFIX=logpond/ \
  LOGPOND_ARCHIVE_S3_REGION=auto \
  LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID=... \
  LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY=... \
  LOGPOND_SOURCES_JSON='[{"name":"dokku","extract":{"timestamp":["timestamp"],"message":["message"],"service":["label.com.dokku.app-name"],"host":["host"]}}]' \
  LOGPOND_FACETS_JSON='[{"name":"container","field":"attributes.container_name","display_label":"Container","cardinality_cap":50}]'
```

`LOGPOND_SOURCES_JSON` defines one source named `dokku` whose extractor maps `label.com.dokku.app-name` to the `service` field, which is what surfaces app names in the facet sidebar. The `LOGPOND_FACETS_JSON` line adds a custom facet so you can also filter by container.

For a script archive backend instead of S3:

```bash
dokku config:set logpond \
  LOGPOND_ARCHIVE_BACKEND=script \
  LOGPOND_ARCHIVE_SCRIPT_PATH=/etc/logpond/scripts/archive.sh \
  LOGPOND_ARCHIVE_SCRIPT_TIMEOUT=600s \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=sftp:backup.example.com:/srv/logpond \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

See [Archival](archival.md) for the full backend reference.

### Step 3: deploy

The cleanest path is `git:from-image`, which deploys the published image directly without building anything on the Dokku host:

```bash
dokku git:from-image logpond ghcr.io/dokku/logpond:latest
```

Pin a specific tag in production (for example `ghcr.io/dokku/logpond:v1.0.0` once cut) so an upstream release does not surprise you. If you want to extend the image - to bake a custom archive script in, for example - point a tiny Dockerfile-based git deploy at Dokku instead:

```bash
git init logpond-deploy && cd logpond-deploy
cat >Dockerfile <<'EOF'
FROM ghcr.io/dokku/logpond:latest
RUN apt-get update && apt-get install -y --no-install-recommends restic \
 && rm -rf /var/lib/apt/lists/*
EOF
git add . && git commit -m "deploy logpond"
git remote add dokku dokku@dokku.example.com:logpond
git push dokku main
```

### Step 4 (script backend only): mount your archive script

If you set `archive.backend: script`, the script binary needs to be inside the container at `archive.script.path`. Use Dokku's storage mounting for that:

```bash
sudo mkdir -p /var/lib/dokku/data/storage/logpond-scripts
sudo cp ~/my-archive.sh /var/lib/dokku/data/storage/logpond-scripts/archive.sh
sudo chmod +x /var/lib/dokku/data/storage/logpond-scripts/archive.sh
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond-scripts:/etc/logpond/scripts
```

### Step 5: verify

```bash
curl https://logpond.example.com/healthz
open https://logpond.example.com/
```

`/healthz` should return `{"status":"ok",...}`. The UI's Admin view should show the archive backend status (a green dot once Logpond has successfully reached it).

## 2. Wire Dokku to ship logs to Logpond

### Step 1: set the Vector sink

Dokku's `vector-sink` uses a DSN-style URL that encodes the destination plus encoding and batching options. Point it at Logpond's `/ingest/dokku` endpoint:

```bash
dokku logs:set --global vector-sink \
  "http://?uri=http://logpond.example.com/ingest/dokku&encoding[codec]=json&framing[method]=newline_delimited&compression=gzip&batch[max_events]=500&batch[timeout_secs]=1"
```

What each option does:

- `uri=http://logpond.example.com/ingest/dokku` - the ingest endpoint. The `/dokku` suffix matches the source name in `LOGPOND_SOURCES_JSON`.
- `encoding[codec]=json` - emit JSON, one object per event.
- `framing[method]=newline_delimited` - join events with newlines (NDJSON).
- `compression=gzip` - compress each batch. Vector sets the `Content-Encoding` header; Logpond decompresses automatically.
- `batch[max_events]=500` and `batch[timeout_secs]=1` - send a batch every 500 events or every second, whichever comes first. Those numbers keep request rate manageable while staying close to real-time.

Use `--global` to apply to every app on the host, or set per-app to be selective.

### Step 2: start the Vector container

```bash
dokku logs:vector-start
```

This launches the Vector container managed by Dokku. It survives restarts of individual apps.

### Step 3: verify shipping

```bash
dokku logs:vector-logs --tail
curl https://logpond.example.com/metrics | grep logpond_ingest
```

In the Search view, set the time range to "Last 15 minutes" and you should see events with `service` set to your Dokku app names.

## 3. Avoid the Logpond -> Logpond feedback loop

The global `vector-sink` ships logs from **every** Docker container on the host, including Logpond itself. That means Logpond's startup logs would ingest into Logpond, generating more logs, ingesting more logs, and so on. Three ways to stop the loop:

### A. Per-app sinks (recommended)

Skip the global sink and set Logpond's destination only on the apps you actually want shipped:

```bash
dokku logs:set web-app vector-sink "http://?uri=http://logpond..."
dokku logs:set api-app vector-sink "http://?uri=http://logpond..."
# do not set vector-sink on the logpond app itself
```

This is the cleanest of the three. New apps need to opt in, which is a small drawback in exchange for never accidentally shipping host-internal containers.

### B. Blackhole Logpond's logs

If you do use the global sink, override the Logpond app to point at the null destination:

```bash
dokku logs:set logpond vector-sink "blackhole://?"
```

Vector still runs for that app; it just discards everything.

### C. Quiet Logpond's logging

You can also reduce Logpond's own log volume to where ingesting it is harmless:

```bash
dokku config:set logpond LOGPOND_LOG_LEVEL=warn
```

This does not eliminate the loop; it just makes the loop trivial.

The three are not mutually exclusive. A common combination is A plus C: per-app sinks for the apps you actually care about, plus a quieter Logpond default in case someone re-enables the global sink without thinking.

## 4. What the facet sidebar shows

With the source and facet configuration above, the Search view's sidebar shows:

- **Service** - the Dokku app name (`web-app`, `api-app`, and so on).
- **Level** - from the app's structured logs, falling back to `info` when the app emits plain text.
- **Host** - the Dokku host's hostname.
- **Source** - always `dokku` under this setup.
- **Container** - the custom facet, surfacing Dokku's `web.1.<hash>` container names.

A typical investigation query in the search bar:

```
service:api-app level:(error OR warn) @container_name:web*
```

If the underlying app emits structured JSON, every JSON field becomes queryable as `@field` automatically - that is the value of running an app that logs in JSON.

## 5. Upgrading

For a `git:from-image` deploy, point at the new tag and re-deploy:

```bash
dokku git:from-image logpond ghcr.io/dokku/logpond:v1.1.0
```

For a Dockerfile-based deploy, edit the `FROM` line and push:

```bash
echo "FROM ghcr.io/dokku/logpond:v1.1.0" > Dockerfile
git commit -am "upgrade to v1.1.0"
git push dokku main
```

The catalog, sealed Parquet files, and saved queries are forward-compatible between minor versions. The implementation notes call out any migration steps when a release needs them.

## 6. Backups

The `/data` volume contains segments, the catalog, and rehydrated segments. To decide what to back up:

- **Sealed and archived segments** are already off-host if you have an archive backend. No separate backup needed - your S3 bucket (or wherever your archive script writes) is the backup.
- **The active segment** is volatile. Loss is bounded by Vector's disk buffer, which retries on restart. You will lose at most a few seconds of events.
- **The catalog** (`/data/catalog.db`) is the only file that genuinely needs explicit backup. It is small - under 100 MB typically. Back up `/var/lib/dokku/data/storage/logpond/catalog.db` with your regular host-backup tooling.

If the catalog is lost, Logpond rebuilds from disk on startup by scanning `/data/segments/` and reconciling. Archived segments are rediscovered by listing the archive backend. The rebuild is slower than restoring the catalog file, but the data is not lost.
