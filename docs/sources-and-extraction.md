# Sources and extraction

A **source** is one named HTTP ingest endpoint with an associated field-extraction rule. Every event that arrives at `POST /ingest/<source>` is tagged with that source name and run through the source's extractor before being written to a segment.

You need at least one source. Most deployments run one (`default`, or `dokku` if all logs come from Dokku's Vector sink); a multi-tenant host might run several so the `source` facet becomes a useful filter.

## The shape of a source

In YAML:

```yaml
sources:
  - name: default
    extract:
      timestamp: [timestamp, ts, "@timestamp"]
      level: [level, severity]
      message: [message, msg]
      service: [service, app, "label.com.dokku.app-name"]
      host: [host, hostname]
```

In JSON (`LOGPOND_SOURCES_JSON`):

```json
[
  {
    "name": "default",
    "extract": {
      "timestamp": ["timestamp", "ts", "@timestamp"],
      "level":     ["level", "severity"],
      "message":   ["message", "msg"],
      "service":   ["service", "app", "label.com.dokku.app-name"],
      "host":      ["host", "hostname"]
    }
  }
]
```

The two forms are interchangeable; pick whichever your deployment tooling makes easier.

## Source name rules

Source names must match `^[a-z0-9][a-z0-9_-]{0,63}$`: lowercase alphanumeric, underscore, or hyphen, up to 64 characters, must start with an alphanumeric. The rule exists so that the URL `/ingest/<name>` is always safe and the name can be used as-is in filenames and SQL identifiers without escaping.

Sending to an unconfigured source returns `404`:

```bash
$ curl -X POST http://localhost:8080/ingest/typo --data-binary '...'
{"error":{"code":"unknown_source","message":"source 'typo' is not configured"}}
```

## The extraction map

For each event, Logpond walks the candidate list for each core field in order and uses the first value that "resolves." A candidate resolves when it returns a non-null scalar (and, for timestamps, a parseable one). Candidates that return arrays, objects, or unparseable values are skipped, not erroring out, so a single misconfigured log line does not break the source.

The core fields are:

| Field       | What it means                                                                          |
| ----------- | -------------------------------------------------------------------------------------- |
| `timestamp` | When the event happened. Must be RFC 3339 or a Unix-seconds number.                    |
| `level`     | One of the four canonical log levels: `debug`, `info`, `warn`, `error`.                |
| `message`   | The human-readable log line.                                                           |
| `service`   | What emitted the event - app name, process name, whatever you filter on most often.    |
| `host`      | The host that emitted the event.                                                       |

Anything that is not pulled into a core field stays on the row as part of the JSON-encoded `attributes` column. The original full line is preserved verbatim in the `raw` column, so no data is lost even if your extraction map drops a field.

## Field-by-field rules

### `timestamp`

Each candidate that resolves to a string is parsed as RFC 3339; each candidate that resolves to a number is treated as Unix seconds (integer) or Unix seconds with fractional precision (float). The first candidate that produces a valid time wins.

If every candidate fails, Logpond falls back to **ingest time** - the wall-clock instant the HTTP handler parsed the event - and sets `attributes.logpond_timestamp_fallback = true` so you can tell apart "this was really at 10:00" from "the source omitted the timestamp."

### `level`

Strings are matched case-insensitively against this table:

| Input                                                              | Normalized |
| ------------------------------------------------------------------ | ---------- |
| `trace`, `debug`, `dbg`, `0`, `1`                                  | `debug`    |
| `info`, `information`, `notice`, `2`, `3`                          | `info`     |
| `warn`, `warning`, `4`                                             | `warn`     |
| `error`, `err`, `critical`, `crit`, `fatal`, `emerg`, `alert`, `panic`, `5`, `6`, `7` | `error` |
| anything else                                                      | `info` (with `attributes.logpond_level_unrecognized` set to the original) |

Numeric candidates are interpreted as syslog severities (0-7) mapping into the same four buckets.

Whenever normalization changed the value, the original is preserved in `attributes.logpond_level_original` so a query like `@logpond_level_original:fatal` will still find your panics.

If every candidate fails, the level is set to `info` and `attributes.logpond_level_fallback = true` is set.

### `message`

Candidates may return any scalar; non-string scalars are stringified (`true` becomes `"true"`, `42` becomes `"42"`). If every candidate fails, `message` is left null.

### `service` and `host`

Candidates must return strings. Other types are skipped. If every candidate fails, the column is left null. These two columns are the most commonly used facets, so configure them deliberately.

## Attributes versus raw

Two storage columns hold what does not fit into the core five:

- **`attributes`** is a JSON object containing every top-level field of the event that was not picked up by `extract`. If your event had `user_id`, `request_id`, and `duration_ms`, those land here. Fields that were extracted into core columns are removed from `attributes` to avoid duplication.
- **`raw`** is the original NDJSON line verbatim, exactly as received.

That separation has one operator-visible consequence: if you query `attributes.level` you will find nothing, because `level` was extracted into the `level` core column. Query the `level` column directly (the search bar's `level:error` does this for you). The UI's field autocomplete steers you toward the right column.

If you need a field that lives in `attributes` to participate in faceting and one-click filtering, declare it as a custom facet - see [Facets](facets.md).

## A worked example: Dokku Vector

Dokku's Vector sink ships events with this top-level shape:

```json
{
  "message":  "user 42 logged in",
  "timestamp": "2026-05-12T14:32:01.250Z",
  "host":     "dokku.example.com",
  "container_name": "web.1.abc123",
  "container_id":   "f1d2...",
  "image":          "dokku/myapp:latest",
  "label.com.dokku.app-name": "myapp"
}
```

The Dokku app name lives at `label.com.dokku.app-name`, so the source mapping pulls it into `service` and the `service` facet then becomes "filter by Dokku app":

```yaml
sources:
  - name: dokku
    extract:
      timestamp: [timestamp]
      message: [message]
      service: ["label.com.dokku.app-name"]
      host: [host]
```

The `container_name` field is not part of the core five, so it remains in `attributes`. You can still filter on it with `@container_name:web*` in the search bar, and you can promote it to a sidebar facet by declaring it as a custom facet. See [Dokku Deployment](dokku-deployment.md) for the full Vector-plus-Logpond integration.

## Authenticating ingest with bearer tokens

Sources accept unauthenticated POSTs by default - the assumption is that Vector lives on the same host and reaches Logpond over an internal address. If you want to expose `/ingest/<source>` publicly, attach one or more bearer tokens to the source. Sources without tokens stay open.

```yaml
sources:
  - name: public
    ingest_tokens:
      - "lpk_live_4f8a..."
      - "lpk_live_9c3b..."   # rotation overlap - both valid during cutover
    extract: { ... }
```

Requests must present `Authorization: Bearer <token>` whose value matches one of the configured tokens. Mismatches and missing headers get `401 unauthorized` with `WWW-Authenticate: Bearer`. Comparison is constant-time, so the number of configured tokens does not leak through timing.

Generate a token with the bundled subcommand (32 base64-url characters, `lpk_live_` prefix):

```bash
docker run --rm ghcr.io/dokku/logpond:latest gen-token
# -> lpk_live_<random>
```

The prefix is a convention, not a requirement. Operators can use any non-empty string with no internal whitespace. The prefix makes leaked tokens easy to spot in logs and code search.

For Dokku setups, a single token can also be set via env var (the file-based form is the right knob for hot rotation - see below):

```bash
dokku config:set logpond LOGPOND_INGEST_TOKEN__public=lpk_live_4f8a...
dokku config:set logpond LOGPOND_INGEST_TOKEN__public=lpk_live_4f8a,lpk_live_9c3b   # comma-separated
```

**Rotation, hot.** Tokens live in the YAML file's `sources[].ingest_tokens` list. Edit the file (the recommended Dokku setup mounts it via `dokku storage:mount` so the host path is the same as the container path), add a new token alongside the old, then:

```bash
curl -X POST http://localhost:8080/api/admin/reload
```

The new token is accepted immediately. Update your producer to use it, then remove the old token from the file and reload again.

**Rotation, env-only.** Tokens supplied through `LOGPOND_INGEST_TOKEN__<source>` are read at process start and **cannot hot-rotate**. A Linux process's environment is fixed at `execve` time, so `dokku config:set --no-restart` updates the stored value on disk but not the running container's namespace. Rotating an env-set token requires `dokku ps:restart`. Use the file-based form whenever zero-downtime rotation matters.

**Failure logging.** Logpond emits a rate-limited (one per minute per source) warning log on each authentication failure, carrying the source name and the first six characters of the offered token as a fingerprint. Successful auth is silent. The Prometheus counter `logpond_ingest_auth_failures_total{source}` increments on every 401 so you can alert on unexpected probes.

**Redaction.** Configured tokens are blanked as `***` in both startup config logs and `GET /api/admin/config`. They are not safe to read from the running config; treat the YAML file's permissions (`0600`, owned by the runtime user) as the boundary.

## Adding or changing sources

The `sources` config key is reloadable. After editing the YAML or changing `LOGPOND_SOURCES_JSON`, hit `POST /api/admin/reload`:

```bash
curl -X POST http://localhost:8080/api/admin/reload
```

The response confirms which fields changed. New sources begin accepting events on the next request. Existing segments are not rewritten - the source's old field-extraction rules are baked into rows already stored, so changing `extract` only affects events ingested from this moment on.

## Body format and limits

The ingest endpoint accepts:

- `Content-Type: application/x-ndjson`, `application/ndjson`, `application/json`, or an empty Content-Type (Vector's NDJSON sink omits the header in some versions). Anything else returns `415`.
- `Content-Encoding: gzip` is the only supported encoding.
- The decompressed body is capped at 32 MB. Larger requests get `413`. Split your batch.
- Lines are terminated with `\n` or `\r\n`. Empty lines are silently skipped.
- Lines that are not parseable JSON objects are skipped; the rest of the batch is accepted. The response's `skipped` field tells you how many lines were dropped, and `logpond_ingest_skipped_lines_total` increments. This lenient behavior exists so one bad line in a Vector batch does not force the whole batch to retry.

Success returns `202 Accepted`:

```json
{ "accepted": 487, "skipped": 0 }
```
