# Live tail

Live tail streams matching events to your browser (or any WebSocket client) within about two seconds of ingest. It is the right tool for watching a deploy unfold, debugging a flaky request as it happens, or just confirming events are flowing.

It is **not** a way to back-fill - the stream only shows events ingested after you subscribe. Use the Search view for anything that already happened.

## The endpoint

```
GET /api/query/stream
```

The connection is an HTTP GET upgraded to WebSocket. The Live tail view in the UI handles all the protocol details for you; this page covers it so you can build a programmatic client if you want one.

## Subscribing

Once connected, send a subscription message containing a filter. Three forms are accepted; pick whichever is most convenient:

```json
{ "q": "service:api level:error" }
```

```json
{ "filter": { "op": "and", "children": [
  { "field": "service", "op": "eq", "value": "api" },
  { "field": "level",   "op": "eq", "value": "error" }
] } }
```

```json
{ "filters": [
  { "field": "service", "op": "eq", "value": "api" }
] }
```

At least one leaf predicate or a non-empty free-text `search`/`q` is required - an unfiltered stream would mean every event from every source, which is not what you want and would saturate the rate cap immediately. An empty subscription closes the connection with code `4001 filter_required`.

## Incoming messages

The server sends two kinds of frames:

**Status:**

```json
{
  "type": "status",
  "subscribed": true,
  "rate_capped": false,
  "events_dropped": 0,
  "filter_summary": "service = api AND level = error"
}
```

Status frames arrive on subscribe and whenever the rate-cap or drop-counter state changes.

**Event:**

```json
{
  "type": "event",
  "event": {
    "timestamp": "2026-05-12T14:58:14.221000Z",
    "service": "api",
    "level": "error",
    "message": "db connection refused",
    "host": "app-1",
    "source": "default",
    "attributes": { "user_id": 42, "request_id": "r-882" },
    "raw": "{...}"
  }
}
```

The event payload shape matches what `POST /api/query` returns for a single result row.

## Updating the filter mid-stream

To change what you are tailing without dropping the connection, send another subscription message. The new filter replaces the old one and the stream continues. The UI does this when you edit the search bar - no flicker, no reconnect.

## Rate and connection limits

A live stream of every event from a busy ingest path would overwhelm the browser. Two caps protect both sides:

- **Per-client rate cap:** 100 events per second by default (`live_tail.rate_cap`). Excess events are dropped and `events_dropped` increments in the next status frame. Tighten the filter if you see drops; loosening the cap rarely helps because the bottleneck is usually the browser, not the server.
- **Connection cap:** 20 concurrent clients globally (`live_tail.max_clients`). The 21st client gets a `4004 too_many_clients` close. This is conservative - each live tail holds memory and a goroutine - so raise it cautiously.

Both are hot-reloadable.

## Slow clients

If your end of the connection is slow to read (a browser tab in the background, a flaky network), the server's per-client send queue fills. After 30 seconds of stalled writes, the server closes the connection with `4003 slow_client`. The browser UI handles this transparently by reconnecting; a programmatic client should reconnect with backoff.

## Heartbeats

The server sends a heartbeat every 30 seconds. If the UI does not see one for 60 seconds it marks the connection stale and tries to reconnect. Programmatic clients should do the same - either reconnect on heartbeat timeout or trust the WebSocket's own keepalive (most libraries handle this).

## Close codes

| Code   | Meaning                                                     |
| ------ | ----------------------------------------------------------- |
| `1001` | Going away (normal browser tab close).                      |
| `1011` | Server error.                                               |
| `4001` | `filter_required` - subscription had no leaf predicate.     |
| `4002` | `invalid_filter` - subscription filter failed to parse.     |
| `4003` | `slow_client` - send queue stalled past the grace period.   |
| `4004` | `too_many_clients` - connection cap hit.                    |

`4001` and `4002` are client errors: fix the filter and reconnect. `4003` and `4004` are operational: read faster, or back off and retry.

## A minimal Python client

For one-off debugging, a tiny client is enough:

```python
import asyncio, json, websockets

async def tail():
    async with websockets.connect("ws://localhost:8080/api/query/stream") as ws:
        await ws.send(json.dumps({"q": "service:api level:error"}))
        async for frame in ws:
            msg = json.loads(frame)
            if msg["type"] == "event":
                e = msg["event"]
                print(f'{e["timestamp"]}  {e["level"]:6s}  {e["service"]:12s}  {e["message"]}')

asyncio.run(tail())
```

That same shape works from anything that speaks WebSocket - shell scripts via `websocat`, Go programs, anything.

## Why this is not a query-replay

Live tail forwards events as they cross the ingest path. Events that already landed in a segment do not get replayed when you subscribe - that is what `POST /api/query` is for. If you need "the last 100 errors and then anything new," issue a query first, then start a tail with the same filter and concatenate.
