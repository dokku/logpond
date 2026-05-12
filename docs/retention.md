# Retention

Without retention, Logpond keeps every event you ingest forever, and your disk eventually fills. Retention is the policy that decides which sealed segments get archived (if you have an archive backend configured) and which then get deleted from local disk.

You set retention by **age**, by **total local size**, or both. At least one is required for retention to do anything.

## The two policy axes

```yaml
retention:
  max_age: 30d
  max_size: 20GB
  archive_before_delete: true
  evaluation_interval: 5m
```

- **`max_age`** deletes segments older than the given duration. Common units: `s`, `m`, `h`, `d`. So `30d` keeps a rolling 30 days of logs.
- **`max_size`** deletes the oldest segments until total local sealed-plus-rehydrated size is under the cap. Supports `KB`, `MB`, `GB`. So `20GB` keeps a rolling 20 GB of logs.
- If both are set, whichever is hit first triggers. Most operators set `max_age` for predictability and `max_size` as a safety net on smaller disks.
- **`archive_before_delete`** is `true` by default. When an archive backend is configured (`archive.backend: s3` or `archive.backend: script`), each segment is archived first and only deleted after the archive succeeds. With `archive.backend: none`, the flag has no effect - there is nowhere to archive to, so retention just deletes.
- **`evaluation_interval`** controls how often retention runs. Default `5m`. Lower for fast feedback when testing; you do not normally need to touch this.

Both `max_age` and `max_size` count **sealed plus rehydrated local segments**. The active segment (the one currently being written) and segments that have been archived-only (with no local Parquet remaining) are not counted - active is volatile and capped by the segment window, archived-only is already off-disk.

## A worked example

A modest setup that keeps recent logs hot and pushes everything older than a month to S3:

```yaml
retention:
  max_age: 30d
  max_size: 50GB
archive:
  backend: s3
  s3:
    bucket: my-logs-archive
    region: us-east-1
    access_key_id: ...
    secret_access_key: ...
```

What happens, second by second:

1. Vector ships events into Logpond every second. They land in the active segment.
2. Once an hour, the active segment closes and a new one opens. The just-closed segment seals - that is, gets written out as a sealed Parquet file with zstd compression.
3. Every 5 minutes, retention evaluates. If any sealed segment is older than 30 days, it is uploaded to S3 (because `archive.backend: s3`) and then deleted from local disk.
4. If at any point the local disk usage exceeds 50 GB, retention starts deleting the oldest segments to bring it back down, archiving them first since `archive_before_delete: true`.

## Protected segments

Two classes of segments are never deleted by retention, regardless of policy:

- **Active segments.** The one currently accepting writes.
- **Segments sealed within the last hour.** Even if `max_age` is `5m` and a segment just sealed, retention leaves it alone for an hour. This bounds the worst-case "I just sealed and now I'm deleting it" race and gives a window for any in-flight queries against the segment to complete.

These protections cannot be turned off. If you find yourself wanting to delete a freshly sealed segment, evict it manually from the admin UI instead.

## Running retention manually

Retention runs on its `evaluation_interval` automatically. You can also kick it off manually, which is the right move when you have just changed the policy and want to see what it would do:

```bash
curl -X POST http://localhost:8080/api/admin/retention/run \
  -H 'Content-Type: application/json' \
  -d '{"dry_run": true}'
```

`dry_run: true` evaluates the policy and reports the actions it would take without taking them:

```json
{
  "evaluated": 482,
  "actions": [
    { "segment_id": "202604120100", "action": "archive_then_delete", "reason": "age > 30d" },
    { "segment_id": "202604120200", "action": "archive_then_delete", "reason": "age > 30d" }
  ]
}
```

The same call with `dry_run: false` (the default) takes the actions. You see the same response shape; the actions are executed instead of just planned.

The Admin view's Retention pane has a "Run now" button (real) and a "Dry run" button (predictive) that hit the same endpoint.

## Tuning to a small disk

If you run on a 2 GB Dokku host and only have a few GB of disk to spare, the right pattern is:

- Keep `max_age` modest (`7d` or `14d`).
- Set `max_size` to about 60-70% of your free disk - leave headroom for the active segment, rehydration, and other Dokku apps' data.
- Configure an archive backend so old data is not lost, just moved.
- Set the segment window to `1h` (the default) so individual segments stay around 180 MB sealed.

If you have no archive backend and `archive_before_delete: true` (which is the default), and you also configure `archive.backend: none`, retention will silently behave like `archive_before_delete: false`. There is nowhere to archive to, so segments just get deleted when retention says.

## Reloading retention policy

All four retention keys are hot-reloadable:

```bash
LOGPOND_RETENTION_MAX_AGE=14d dokku config:set logpond LOGPOND_RETENTION_MAX_AGE=14d
curl -X POST http://localhost:8080/api/admin/reload
```

The new policy takes effect on the next evaluation cycle (or sooner if you trigger a manual run).
