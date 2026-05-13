# Configuration

Logpond reads its configuration from two places that you combine freely: a YAML file and environment variables. Environment variables always win on conflict, which is convenient on Dokku where `dokku config:set` is the natural way to change a value without rebuilding an image.

## Where the config file lives

By default Logpond reads `/etc/logpond/config.yaml`. Point it elsewhere with `LOGPOND_CONFIG`:

```bash
docker run --rm -p 8080:8080 \
  -e LOGPOND_CONFIG=/etc/logpond/myconfig.yaml \
  -v /path/to/myconfig.yaml:/etc/logpond/myconfig.yaml:ro \
  ghcr.io/dokku/logpond:latest
```

The file is plain YAML. A complete minimal example:

```yaml
data_dir: /data
listen_address: 0.0.0.0
port: 8080
sources:
  - name: default
    extract:
      timestamp: [timestamp]
      level: [level]
      message: [message]
      service: [service]
      host: [host]
retention:
  max_age: 30d
  max_size: 10GB
archive:
  backend: none
```

## Environment variables

Every config key has a corresponding environment variable named `LOGPOND_<UPPERCASE_DOTTED_PATH>`. Dots become underscores:

```bash
LOGPOND_DATA_DIR=/data
LOGPOND_RETENTION_MAX_AGE=30d
LOGPOND_ARCHIVE_S3_BUCKET=logs-archive
```

Two settings - sources and custom facets - are objects, not scalars. Pass them as JSON strings:

```bash
LOGPOND_SOURCES_JSON='[{"name":"default","extract":{"timestamp":["timestamp"],"message":["message"]}}]'
LOGPOND_FACETS_JSON='[{"name":"user_id","field":"attributes.user.id","cardinality_cap":25}]'
```

Pass-through env for archive scripts uses a double underscore between the prefix and the key name to keep the script's variable name unambiguous:

```bash
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=sftp:backup.example.com:/srv/logpond
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

These become `RESTIC_REPOSITORY` and `RESTIC_PASSWORD` in the script's environment. See [Archival](archival.md#script-backend) for the full script contract.

## Full configuration reference

The **Reloadable** column says whether changing the value at runtime via `POST /api/admin/reload` (or `SIGHUP`) takes effect immediately. Non-reloadable values require restarting the process.

| Config key                              | Environment variable                       | Default              | Reloadable |
| --------------------------------------- | ------------------------------------------ | -------------------- | ---------- |
| `listen_address`                        | `LOGPOND_LISTEN_ADDRESS`                   | `0.0.0.0`            | No         |
| `port`                                  | `LOGPOND_PORT` (or `PORT`)                 | `8080`               | No         |
| `data_dir`                              | `LOGPOND_DATA_DIR`                         | `/data`              | No         |
| `segment_window`                        | `LOGPOND_SEGMENT_WINDOW`                   | `1h`                 | No         |
| `sealing_interval`                      | `LOGPOND_SEALING_INTERVAL`                 | `60s`                | No         |
| `log_level`                             | `LOGPOND_LOG_LEVEL`                        | `info`               | Yes        |
| `memory_limits.duckdb`                  | `LOGPOND_MEMORY_LIMITS_DUCKDB`             | `128MB`              | No         |
| `memory_limits.ring_buffer`             | `LOGPOND_MEMORY_LIMITS_RING_BUFFER`        | `50MB`               | No         |
| `query.max_time_range`                  | `LOGPOND_QUERY_MAX_TIME_RANGE`             | `7d`                 | Yes        |
| `facets.segment_sample_size`            | `LOGPOND_FACETS_SEGMENT_SAMPLE_SIZE`       | `24`                 | Yes        |
| `facets.default_cardinality_cap`        | `LOGPOND_FACETS_DEFAULT_CARDINALITY_CAP`   | `50`                 | Yes        |
| `facets.builtin.service.cap`            | `LOGPOND_FACETS_BUILTIN_SERVICE_CAP`       | `100`                | Yes        |
| `facets.builtin.level.cap`              | `LOGPOND_FACETS_BUILTIN_LEVEL_CAP`         | `10`                 | Yes        |
| `facets.builtin.host.cap`               | `LOGPOND_FACETS_BUILTIN_HOST_CAP`          | `50`                 | Yes        |
| `facets.builtin.source.cap`             | `LOGPOND_FACETS_BUILTIN_SOURCE_CAP`        | `50`                 | Yes        |
| `facets.custom`                         | `LOGPOND_FACETS_JSON`                      | (none)               | Yes        |
| `live_tail.rate_cap`                    | `LOGPOND_LIVE_TAIL_RATE_CAP`               | `100`                | Yes        |
| `live_tail.max_clients`                 | `LOGPOND_LIVE_TAIL_MAX_CLIENTS`            | `20`                 | Yes        |
| `retention.max_age`                     | `LOGPOND_RETENTION_MAX_AGE`                | (unset)              | Yes        |
| `retention.max_size`                    | `LOGPOND_RETENTION_MAX_SIZE`               | (unset)              | Yes        |
| `retention.archive_before_delete`       | `LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE`  | `true`               | Yes        |
| `retention.evaluation_interval`         | `LOGPOND_RETENTION_EVALUATION_INTERVAL`    | `5m`                 | Yes        |
| `rehydration.ttl_days`                  | `LOGPOND_REHYDRATION_TTL_DAYS`             | `7`                  | Yes        |
| `archive.backend`                       | `LOGPOND_ARCHIVE_BACKEND`                  | `none`               | No         |
| `archive.s3.endpoint`                   | `LOGPOND_ARCHIVE_S3_ENDPOINT`              | (unset)              | Yes        |
| `archive.s3.bucket`                     | `LOGPOND_ARCHIVE_S3_BUCKET`                | (unset)              | Yes        |
| `archive.s3.prefix`                     | `LOGPOND_ARCHIVE_S3_PREFIX`                | `logpond/`           | Yes        |
| `archive.s3.region`                     | `LOGPOND_ARCHIVE_S3_REGION`                | `auto`               | Yes        |
| `archive.s3.access_key_id`              | `LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID`         | (unset)              | Yes        |
| `archive.s3.secret_access_key`          | `LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY`     | (unset)              | Yes        |
| `archive.script.path`                   | `LOGPOND_ARCHIVE_SCRIPT_PATH`              | (unset)              | Yes        |
| `archive.script.timeout`                | `LOGPOND_ARCHIVE_SCRIPT_TIMEOUT`           | `600s`               | Yes        |
| `archive.script.env.*`                  | `LOGPOND_ARCHIVE_SCRIPT_ENV__*`            | (none)               | Yes        |
| `sources`                               | `LOGPOND_SOURCES_JSON`                     | one `default` source | Yes        |
| `sources[].ingest_tokens`               | (see `LOGPOND_INGEST_TOKEN__*` below)      | (none)               | Yes        |
| (n/a)                                   | `LOGPOND_INGEST_TOKEN__<source>`           | (none)               | No         |
| `theme.default`                         | `LOGPOND_THEME_DEFAULT`                    | `auto`               | Yes        |

A few of these need elaboration:

- **`data_dir`** is where Logpond writes the SQLite catalog (`catalog.db`), the active DuckDB segment, sealed Parquet files (`segments/sealed/`), rehydrated Parquet files (`rehydrated/`), and the watched import drop directory (`import/`). Use a host volume in production so it survives restarts.
- **`segment_window`** must divide 24 hours evenly. Valid values are `5m`, `10m`, `15m`, `30m`, `1h`, `2h`, `3h`, `4h`, `6h`, `8h`, `12h`, `24h`. Shorter windows mean more files; longer windows mean more in-flight data at any moment. Stick with `1h` unless you have a reason.
- **`memory_limits.duckdb`** sets DuckDB's `memory_limit` PRAGMA. Lower it on small hosts where 128MB is too much, but expect slower facet aggregations.
- **`memory_limits.ring_buffer`** is the in-memory ingest buffer between the HTTP handler and the segment writer. When it fills, the ingest endpoint returns `429 Retry-After: 1` instead of dropping events. Raise it if you can afford the RAM and frequently see 429s.
- **`query.max_time_range`** caps the span of a single query. The default of seven days is a safety net against accidental "show me everything" queries.
- **`retention.max_age` and `retention.max_size`** are independent. If both are set, whichever is hit first triggers archival or deletion. At least one is required for retention to do anything; leave both unset and Logpond will grow until the disk fills. See [Retention](retention.md).
- **`archive.backend`** is one of `none`, `s3`, or `script` and is the only archive-related setting that requires a restart. The rest (credentials, endpoints, script paths) hot-reload, which is convenient for credential rotation.
- **`sources[].ingest_tokens`** opts a source into bearer-token auth on `POST /ingest/<source>`. An empty list (or omitted field) keeps the source unauthenticated, preserving the local-Vector default. See [Sources and Extraction](sources-and-extraction.md#authenticating-ingest-with-bearer-tokens) for the full rotation flow.
- **`LOGPOND_INGEST_TOKEN__<source>`** is a flat env-var sidecar for the bootstrap / single-token case. It accepts a single token or a comma-separated list and appends to whatever the YAML defines (deduped). Set-at-startup-only: rotating an env-set token requires a restart, because a Linux process's environment is fixed at exec time. For hot rotation, put tokens in the YAML file and reload.

## Reloading config without a restart

For any row marked Reloadable, you can change the value and apply it without restarting:

```bash
curl -X POST http://localhost:8080/api/admin/reload
```

The response lists which fields changed and any warnings:

```json
{ "reloaded": true, "changed_fields": ["retention.max_age", "facets.custom"], "warnings": [] }
```

If you change a non-reloadable field and trigger a reload, you get a `422 field_not_reloadable` error and the change does not take effect until the next process start. To roll back a bad config, edit the YAML (or change the env var and recreate the container) and reload again. There is no separate "test config" subcommand; the reload itself validates the new file before applying it.

You can also reload by sending `SIGHUP` to the process, which is what `dokku ps:restart` will skip past - reload is for hot changes, restart is for the rest.

## Where to look for the current effective config

`GET /api/admin/config` returns the merged YAML-plus-env config that Logpond is actually using, with secrets (S3 keys and every `archive.script.env.*` value) redacted:

```bash
curl http://localhost:8080/api/admin/config
```

This is the safest thing to copy into a bug report.
