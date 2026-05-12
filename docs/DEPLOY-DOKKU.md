# Deploying Logpond on Dokku

Logpond runs as a standard Dokku app. Dokku's `logs` plugin includes built-in Vector support: setting a `vector-sink` (globally or per-app) starts a Vector container that ships Docker container logs to the configured sink. We point that sink at Logpond's HTTP ingest endpoint.

The Dokku Vector source emits one event per log line with these fields:

| Field                       | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `message`                   | The log line itself                                          |
| `container_name`            | Docker container name (Dokku-generated, like `web.1.<hash>`) |
| `container_id`              | Docker container ID                                          |
| `image`                     | Image name                                                   |
| `host`                      | The Dokku host's hostname                                    |
| `label.com.dokku.app-name`  | The Dokku app name (e.g., `myapp`)                           |
| `timestamp`                 | Time Docker received the log line                            |

## 1. Deploying Logpond as a Dokku app

### Step 1: create the app and storage

```bash
dokku apps:create logpond
dokku domains:set logpond logpond.example.com

dokku storage:ensure-directory logpond
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond:/data
```

Logpond keeps the catalog, active segments, sealed segments, and rehydrated archives under `/data`. The bind mount above survives container restarts and `dokku ps:rebuild`.

### Step 2: set environment variables

Minimum useful configuration with S3 (Cloudflare R2 shown):

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

`LOGPOND_SOURCES_JSON` defines a source named `dokku` that extracts `service` from `label.com.dokku.app-name`, which is what makes Dokku app names show up as the `service` facet.

For the script archive backend instead of S3:

```bash
dokku config:set logpond \
  LOGPOND_ARCHIVE_BACKEND=script \
  LOGPOND_ARCHIVE_SCRIPT_PATH=/etc/logpond/scripts/archive.sh \
  LOGPOND_ARCHIVE_SCRIPT_TIMEOUT=600s \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=sftp:backup.example.com:/srv/logpond \
  LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

### Step 3: deploy

```bash
git init logpond-deploy && cd logpond-deploy
echo "FROM ghcr.io/dokku/logpond:1.8.0" > Dockerfile
git add . && git commit -m "deploy logpond"
git remote add dokku dokku@dokku.example.com:logpond
git push dokku main
```

Alternatively, with `dokku-image`:

```bash
dokku git:from-image logpond ghcr.io/dokku/logpond:1.8.0
```

### Step 4 (script backend only): mount the script

```bash
sudo mkdir -p /var/lib/dokku/data/storage/logpond-scripts
sudo cp ~/my-archive.sh /var/lib/dokku/data/storage/logpond-scripts/archive.sh
sudo chmod +x /var/lib/dokku/data/storage/logpond-scripts/archive.sh
dokku storage:mount logpond /var/lib/dokku/data/storage/logpond-scripts:/etc/logpond/scripts
```

If the script needs additional tools, extend the base image:

```dockerfile
FROM ghcr.io/dokku/logpond:1.8.0
RUN apk add --no-cache restic
```

### Step 5: verify

```bash
curl https://logpond.example.com/healthz
open https://logpond.example.com/
```

## 2. Configuring Dokku to ship logs to Logpond

### Step 1: set the Vector sink

```bash
dokku logs:set --global vector-sink \
  "http://?uri=http://logpond.example.com/ingest/dokku&encoding[codec]=json&framing[method]=newline_delimited&compression=gzip&batch[max_events]=500&batch[timeout_secs]=1"
```

The DSN options:

- `uri=http://logpond.example.com/ingest/dokku` is the Logpond ingest endpoint. The `dokku` suffix matches the source name in `LOGPOND_SOURCES_JSON`.
- `encoding[codec]=json` emits JSON events, one per line.
- `framing[method]=newline_delimited` uses NDJSON framing.
- `compression=gzip` compresses the batch.
- `batch[max_events]=500&batch[timeout_secs]=1` batches up to 500 events or 1s.

### Step 2: start the Vector container

```bash
dokku logs:vector-start
```

### Step 3: verify shipping

```bash
dokku logs:vector-logs --tail
curl https://logpond.example.com/metrics | grep logpond_ingest
```

Open the Search view, set time range to "Last 15 minutes," and events should appear with `service` set to Dokku app names.

## 3. Avoiding the Logpond -> Logpond feedback loop

The global `vector-sink` ships logs from every Docker container, including Logpond itself. Three ways to break the loop:

### A. Per-app sinks (recommended)

```bash
dokku logs:set web-app vector-sink "http://?uri=http://logpond..."
dokku logs:set api-app vector-sink "http://?uri=http://logpond..."
# do not set vector-sink on the logpond app itself
```

### B. Blackhole Logpond's logs

```bash
dokku logs:set logpond vector-sink "blackhole://?"
```

### C. Quiet Logpond's logging

```bash
dokku config:set logpond LOGPOND_LOG_LEVEL=warn
```

Per-app sinks (A) is the cleanest of the three.

## 4. Filter dimensions in practice

With the configuration above, the Search view's sidebar shows:

- **Service** is the Dokku app name (`web-app`, `api-app`, ...).
- **Level** comes from app structured logs or falls back to `info`.
- **Host** is the Dokku host hostname.
- **Source** is always `dokku` with this config.
- **Container** (the custom facet defined in `LOGPOND_FACETS_JSON`) is the Dokku process container name.

A typical investigation query in the search bar:

```
service:api-app level:(error OR warn) @container_name:web*
```

If the underlying app emits structured JSON, those fields become queryable as `@field` paths.

## 5. Upgrading

```bash
echo "FROM ghcr.io/dokku/logpond:1.9.0" > Dockerfile
git add . && git commit -m "upgrade to 1.9.0"
git push dokku main
```

Catalog, segments, and saved queries are forward-compatible between minor versions.

## 6. Backup considerations

The `/data` volume contains segments, the catalog, and rehydrated segments. To back up:

- Sealed and archived segments are already off-host; no separate backup needed.
- The active segment is volatile; loss is bounded by Vector's disk buffer.
- The catalog (`/data/catalog.db`) is the only file that needs explicit backup. It is small (under 100MB typically). Back up `/var/lib/dokku/data/storage/logpond/catalog.db` with your regular host backup.

Catalog rebuild from scratch is supported: on startup, Logpond scans `/data/segments/` and reconciles. Archived segments are rediscovered via the backend's list mechanism. This is slower than restoring the catalog file.
