# Rehydration and import

Once a segment is archived, its Parquet file is no longer on local disk. A query for that time range will return nothing because Logpond only searches local segments. To get archived data back into the UI, you **rehydrate** it - pull the Parquet down from the archive backend - or **import** (sideload) a Parquet file you already have from somewhere else.

Both paths put the segment into the `rehydrated` state. The catalog tracks it and queries pick it up immediately. There is a TTL (default 7 days), after which the segment is evicted from local disk again. The original archive is untouched throughout - rehydration is a download, not a move.

## Rehydration

### From the UI

In the Search view, if your time range falls outside what is locally resident, a banner appears below the result list:

```
Older than 30d not in local storage.
[ Rehydrate Apr 1–14 (~2.6 GB) ]
```

Click the button. The admin UI's Jobs section will show the rehydrate job progressing. When it finishes, re-run the query and the events appear.

### From the API

```bash
curl -X POST http://localhost:8080/api/rehydrate \
  -H 'Content-Type: application/json' \
  -d '{
    "time_range": { "from": "2026-04-12T00:00:00Z", "to": "2026-04-13T00:00:00Z" },
    "ttl_days": 7,
    "persistent": false
  }'
```

Response (`202 Accepted`):

```json
{
  "job_id": "reh-3a02",
  "segments": ["202604120000", "202604120100", "202604120200"],
  "segment_count": 24,
  "total_bytes": 4423800000,
  "estimated_seconds": 90,
  "status_url": "/api/jobs/reh-3a02"
}
```

Poll the status URL for progress:

```bash
curl http://localhost:8080/api/jobs/reh-3a02
```

```json
{
  "job_id": "reh-3a02",
  "type": "rehydrate",
  "state": "running",
  "progress": { "completed": 14, "total": 24, "bytes_done": 2580000000, "bytes_total": 4423800000 }
}
```

### TTL and persistence

By default, rehydrated segments live on disk for 7 days (`rehydration.ttl_days`) and then get evicted automatically. The original archive is untouched. To keep a rehydrated segment around forever:

- pass `persistent: true` when triggering the rehydrate, or
- toggle "Make persistent" on the segment in the admin UI later.

Persistent rehydrated segments still count toward `retention.max_size`, so do not pin so many that retention starts deleting your fresh sealed segments to make room.

To evict a rehydrated segment immediately:

```bash
curl -X DELETE http://localhost:8080/api/rehydrated/202604120100
```

This deletes the local Parquet copy. The archive is untouched and you can rehydrate again later.

### When the backend cannot retrieve

The S3 backend always supports retrieve. The script backend may or may not - it depends on whether the script implements the `retrieve` mode (see [Archival](archival.md#script-backend)). If your script reported `retrieve: ✗` during capability probing, `POST /api/rehydrate` returns `501 rehydrate_unsupported`:

```json
{ "error": { "code": "rehydrate_unsupported", "message": "archive backend does not support retrieve" } }
```

In that case, retrieve the Parquet manually using whatever tooling your script wraps, then sideload it via import.

## Import (sideload)

Importing lets you put any Parquet file into Logpond as a rehydrated segment, no archive backend required. Two transport paths exist: the HTTP endpoint, and a watched directory.

### Via the HTTP endpoint

```bash
curl -X POST http://localhost:8080/api/import \
  -F 'parquet=@segment-202604120100.parquet' \
  -F 'manifest=@manifest-202604120100.json'
```

The multipart request takes a `parquet` part and a `manifest` part. The manifest must be the JSON shape Logpond produces during sealing (see [Archival](archival.md#what-gets-archived)). Logpond validates the manifest's schema version, checks that the Parquet's SHA-256 matches the manifest's `parquet_sha256`, and verifies the file actually starts with the Parquet magic header (`PAR1`).

Use the `persistent` query parameter to pin the segment indefinitely:

```bash
curl -X POST 'http://localhost:8080/api/import?persistent=true' \
  -F parquet=@segment.parquet -F manifest=@manifest.json
```

Response (`201 Created`):

```json
{
  "segment_id": "202604120100",
  "state": "rehydrated",
  "row_count": 3601001,
  "size_bytes": 1842500000,
  "evict_after": "2026-05-19T12:00:00Z",
  "persistent": false,
  "overlaps": []
}
```

If the segment ID overlaps an existing segment's time window, the response includes the overlapping segment IDs in `overlaps`. Logpond accepts the overlap - rejecting would block legitimate "merge logs from two environments" use cases - and the UI flags it with a yellow indicator so you know.

Body size cap on `/api/import` is 5 GB.

### Via the watched directory

For large or repeated imports, dropping files into `<data_dir>/import/` is friendlier than a multipart upload. Every 30 seconds Logpond scans the directory for **complete triplets** of files:

```
import/segment-202604120100.parquet
import/segment-202604120100.manifest.json
import/segment-202604120100.ready
```

The `.ready` marker tells Logpond the other two files have finished transferring. Write the parquet and manifest first (use a temp filename, then rename in place), and only when both are on disk write the empty `.ready` file. This avoids partially-written imports being picked up mid-transfer.

On successful import:

```
imported/202604120100/segment-202604120100.parquet
imported/202604120100/manifest.json
imported/202604120100/ready
```

The triplet is moved into `imported/<id>/` so the watcher does not pick it up again.

On failure:

```
failed/202604120100/segment-202604120100.parquet
failed/202604120100/manifest.json
failed/202604120100/error.txt
```

`error.txt` contains the rejection reason - typically a SHA-256 mismatch between the manifest's `parquet_sha256` and the actual file. Recompute the hash locally and verify:

```bash
sha256sum segment-202604120100.parquet
```

The watched path is more forgiving than the HTTP endpoint: it does not enforce the multipart upload limits, so locally-produced Parquet from any tool is accepted as long as it parses, the schema matches, and the SHA-256 is right.

### A worked migration

You have NDJSON files from a previous log system and want them in Logpond. The straightforward path is to push them through the ingest endpoint in batches:

```bash
split -l 50 old-logs.ndjson chunk-
for f in chunk-*; do
  curl -X POST http://localhost:8080/ingest/default \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary @"$f"
done
```

50 lines per request keeps the ingest path's p99 latency healthy. The events flow into the active segment as if they had just been generated.

For larger migrations - or when you need to preserve the original event timestamps without re-sealing - convert the NDJSON to Parquet with the right schema and drop it into the watched `import/` directory. The row schema is fixed: `timestamp` (microsecond UTC), `service`, `level`, `message`, `host`, `source` (all `VARCHAR`), `attributes` (JSON-encoded `VARCHAR`), and `raw` (`VARCHAR`). The full spec is in [the storage-layout section of the PRD](internals/PRD.md#72-storage-layout) if you need to verify column types and Parquet metadata.
