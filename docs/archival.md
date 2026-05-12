# Archival

Archival is how sealed segments leave your local disk. When [retention](retention.md) decides a segment is too old or that the disk is too full, Logpond uploads the segment's Parquet file (and its manifest) to an archive backend before deleting the local copy. The archived segment stays queryable via [rehydration](rehydration-and-import.md).

You do not need an archive backend. `archive.backend: none` (the default) disables the archive UI and lets retention delete segments outright. Configure a backend if you want long-term storage at object-storage prices, or if compliance requires you retain logs beyond what fits on the host disk.

## Choosing a backend

Logpond ships two backends, set with `archive.backend`:

- **`s3`** uploads Parquet files to any S3-compatible bucket. Works with AWS S3, Cloudflare R2, MinIO, Backblaze B2, anything that speaks the S3 API. Best when you already use object storage.
- **`script`** invokes an executable that you provide. The script is responsible for putting the Parquet somewhere - restic, rclone, a tape robot, scp to a cold server, whatever you can express as a shell command. Best when you have an existing backup setup or need a destination Logpond's S3 client cannot reach.

The two backends are mutually exclusive: one is configured at a time, switching requires a restart.

## S3 backend

### Minimum configuration

```yaml
archive:
  backend: s3
  s3:
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket: my-logs-archive
    prefix: logpond/
    region: us-east-1
    access_key_id: AKIA...
    secret_access_key: ...
```

Or via environment:

```bash
LOGPOND_ARCHIVE_BACKEND=s3
LOGPOND_ARCHIVE_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
LOGPOND_ARCHIVE_S3_BUCKET=my-logs-archive
LOGPOND_ARCHIVE_S3_PREFIX=logpond/
LOGPOND_ARCHIVE_S3_REGION=us-east-1
LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID=AKIA...
LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY=...
```

The IAM policy needs `s3:PutObject`, `s3:GetObject`, `s3:HeadObject`, and `s3:ListBucket` on the bucket and its prefix. Logpond **never deletes from S3** - cleanup of old archives is your job (lifecycle rules on the bucket are the natural way).

### Cloudflare R2

R2 uses `region: auto` and an account-scoped endpoint:

```yaml
archive:
  backend: s3
  s3:
    endpoint: https://<account-id>.r2.cloudflarestorage.com
    bucket: logs-archive
    prefix: logpond/
    region: auto
    access_key_id: ...
    secret_access_key: ...
```

### MinIO (local or on-prem)

```yaml
archive:
  backend: s3
  s3:
    endpoint: http://minio.internal:9000
    bucket: logpond-archive
    prefix: logpond/
    region: us-east-1
    access_key_id: minio-access-key
    secret_access_key: minio-secret-key
```

Plain HTTP is allowed for `endpoint`; in production prefer HTTPS, ideally with a trusted certificate.

### Object key layout

Sealed segments land at predictable keys under the prefix:

```
<prefix>/segments/YYYY/MM/DD/HH/segment-<id>.parquet
<prefix>/segments/YYYY/MM/DD/HH/manifest.json
```

For a segment whose window starts 2026-05-12T14:00 with prefix `logpond/`:

```
logpond/segments/2026/05/12/14/segment-202605121400.parquet
logpond/segments/2026/05/12/14/manifest.json
```

The keys are deterministic, so you can browse the bucket and find a segment by date without a catalog lookup.

### Idempotency

Each Parquet is uploaded with the `x-amz-meta-parquet-sha256` header set to the file's SHA-256. If you re-archive a segment whose hash matches what is already in the bucket, the upload is a no-op. If the hashes disagree, Logpond logs a warning and overwrites.

### Commit order and resumability

Logpond uploads the Parquet, then `HEAD`-verifies it, then writes the manifest. The manifest is the commit marker - the catalog only marks the segment archived after the manifest write returns successfully. So if Logpond crashes mid-upload, the next retention cycle will re-archive the segment cleanly; there is no half-archived state to clean up.

Objects larger than 64 MB use S3 multi-part upload automatically.

### Verifying the archive

`POST /api/admin/archive/verify` walks the bucket and reports any mismatches between the catalog and what is actually in S3:

```bash
curl -X POST http://localhost:8080/api/admin/archive/verify
```

```json
{
  "backend": "s3",
  "scanned": 458,
  "orphaned_parquets": [],
  "dangling_manifests": [],
  "missing_parquets": []
}
```

`orphaned_parquets` are objects in the bucket with no catalog row (likely from an earlier deployment). `dangling_manifests` are manifests without a corresponding Parquet. `missing_parquets` are catalog entries pointing to objects that are no longer in the bucket.

## Script backend

### When to use it

If your archive destination is not S3-compatible, or if you already have a backup pipeline (restic, borgbackup, rclone, scp-to-cold-server) you want to reuse, write a small script and point Logpond at it. A reference implementation is in [`examples/archive-restic.sh`](../examples/archive-restic.sh).

### Configuration

```yaml
archive:
  backend: script
  script:
    path: /etc/logpond/scripts/archive.sh
    timeout: 600s
    env:
      RESTIC_REPOSITORY: sftp:backup.example.com:/srv/logpond
      RESTIC_PASSWORD: ...
```

Or via environment:

```bash
LOGPOND_ARCHIVE_BACKEND=script
LOGPOND_ARCHIVE_SCRIPT_PATH=/etc/logpond/scripts/archive.sh
LOGPOND_ARCHIVE_SCRIPT_TIMEOUT=600s
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY=sftp:backup.example.com:/srv/logpond
LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD=...
```

The double underscore between `ENV` and the variable name is intentional - it lets the variable name contain underscores without ambiguity.

`archive.script.path` must be an **absolute, normalized** path (no `..`, no `//`). Relative paths and paths with traversal segments are rejected at startup with a clear error. This is a hardening measure - the script runs as the same user as Logpond, and accepting unnormalized paths would invite mistakes.

### What the script must do

The script is invoked in one of three **modes**, passed as the first positional argument:

1. **`archive`** - upload the Parquet and manifest somewhere durable.

   ```
   script archive --segment-id <id> --parquet-path <path> --manifest-path <path>
   ```

2. **`verify`** - confirm an archived segment is still retrievable (or `--all` for a bulk check).

   ```
   script verify --segment-id <id>
   script verify --all
   ```

3. **`retrieve`** - pull a previously archived segment back to local files.

   ```
   script retrieve --segment-id <id> --output-parquet <path> --output-manifest <path>
   ```

If your destination is write-only and you cannot implement retrieve, your script should exit `64` when invoked with `retrieve --probe`. Logpond will mark retrieve unavailable and the rehydrate UI will explain to operators that they have to sideload manually (see [Rehydration and Import](rehydration-and-import.md)).

### Probes for capability detection

When the script is configured, Logpond invokes it once per mode with a `--probe` flag and inspects the exit code:

```
script archive --probe
script verify  --probe
script retrieve --probe
```

Exit `0` means "I support this mode," exit `64` means "I do not." The result is cached in memory and shown in the admin UI:

```
Capabilities: archive ✓   verify ✓   retrieve ✗
```

To refresh after editing the script:

```bash
curl -X POST http://localhost:8080/api/admin/archive/probe
```

### Environment variables passed to the script

Every invocation sets these variables:

| Variable                          | Value                                                       |
| --------------------------------- | ----------------------------------------------------------- |
| `LOGPOND_SEGMENT_ID`              | Segment ID, e.g. `202605121400`                             |
| `LOGPOND_SEGMENT_TIME_START`      | RFC 3339 segment window start                               |
| `LOGPOND_SEGMENT_TIME_END`        | RFC 3339 segment window end                                 |
| `LOGPOND_SEGMENT_ROW_COUNT`       | Row count                                                   |
| `LOGPOND_SEGMENT_PARQUET_SHA256`  | SHA-256 of the Parquet file (hex)                           |
| `LOGPOND_MANIFEST_VERSION`        | Manifest schema version (currently `1`)                     |
| `LOGPOND_INVOCATION_ID`           | Unique ID for log correlation                               |
| `LOGPOND_MODE`                    | `archive`, `verify`, or `retrieve`                          |
| every `archive.script.env.*` key  | Passed through verbatim                                     |

The script subprocess does **not** inherit `AWS_*` or any other `LOGPOND_*` variables from Logpond's environment. This avoids leaking S3 credentials (which Logpond may also hold for its own S3 backend) into a script that does not need them. `PATH`, `HOME`, `USER`, `TZ`, and other system variables do pass through. If your script needs a credential, pass it explicitly via `archive.script.env.*`.

The same per-invocation values are also available as command-line flags (`--segment-id`, `--parquet-path`, etc.), which is what most scripts use because flags are easier to parse than env vars.

### Exit codes

Follow `sysexits.h`:

| Code   | Meaning                                                                  |
| ------ | ------------------------------------------------------------------------ |
| `0`    | Success.                                                                 |
| `1-63` | Generic failure. Logpond retries on the next retention cycle.            |
| `64`   | Mode not supported. Logpond marks the mode unavailable.                  |
| `65`   | Permanent failure. Logpond does not retry. You will need to investigate. |
| `75`   | Temporary failure. Logpond retries on the next cycle.                    |

`65` versus `1` is the difference between "stop retrying, I cannot do this" (a missing destination directory, an invalid configuration the script just discovered) and "the network blipped, try again later."

### Stdout, stderr, and the archive ref

The script's stdout is captured as `info`-level service logs and stored in the `jobs` table for the admin UI. stderr is captured as `warn`-level. Both are tagged with the invocation ID so you can correlate.

If the script prints a line of the form `LOGPOND_ARCHIVE_REF=<value>` to stdout, the value is stored on the segment as `archive_ref` and shown in the admin UI. Use it to record a snapshot ID, a tape number, anything the destination needs to find the segment later. The reference implementation prints `LOGPOND_ARCHIVE_REF=restic:<snapshot-id>`.

### Timeout and concurrency

The default timeout is 600 seconds. After it elapses Logpond sends `SIGTERM`; if the process does not exit within 30 seconds, `SIGKILL`. Raise `archive.script.timeout` if your destination is slow or has cold-storage latency.

Only **one** script invocation runs at a time globally. This is intentional - bringing up a Docker image or a remote SSH connection has fixed overhead, and parallelizing archives on a small host competes for the same memory the rest of Logpond is using.

## What gets archived

Each archived segment is a pair of files - the Parquet (the data) and a JSON manifest (the metadata). The manifest version is `1`:

```json
{
  "manifest_version": 1,
  "schema_version": 1,
  "segment_id": "202605121400",
  "time_start": "2026-05-12T14:00:00Z",
  "time_end": "2026-05-12T15:00:00Z",
  "row_count": 3600000,
  "size_bytes": 1843200000,
  "size_compressed": 184320000,
  "compression": "zstd",
  "compression_level": 3,
  "source_names": ["default", "nginx"],
  "parquet_filename": "segment-202605121400.parquet",
  "parquet_sha256": "...",
  "persistent": false
}
```

The Parquet is plain Apache Parquet, zstd-compressed at level 3, with timestamps stored as `TIMESTAMP_MICROS` with `isAdjustedToUTC=true`. Any DuckDB or pyarrow user can read it directly, without Logpond:

```sql
SELECT count(*) FROM read_parquet('segment-202605121400.parquet');
```

```python
import pyarrow.parquet as pq
table = pq.read_table('segment-202605121400.parquet')
```

That portability is the point. Your archived logs are a flat object-storage tree of Parquet files, not a service-specific blob, and they outlive Logpond if you decide to move on.

## Triggering an archive manually

Retention triggers archives automatically, but you can also start one on demand. By segment ID:

```bash
curl -X POST http://localhost:8080/api/archive \
  -H 'Content-Type: application/json' \
  -d '{"segment_id": "202605121400"}'
```

Or for everything older than a given duration:

```bash
curl -X POST http://localhost:8080/api/archive \
  -H 'Content-Type: application/json' \
  -d '{"older_than": "30d"}'
```

The response gives you a job ID and a status URL to poll:

```json
{ "job_id": "arc-7c1f", "segments": ["202605121400"], "status_url": "/api/jobs/arc-7c1f" }
```

If `archive.backend` is `none`, the endpoint returns `503 archive_unavailable`. The admin UI grays out the archive button entirely in that case.
