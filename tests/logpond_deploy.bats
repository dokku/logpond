#!/usr/bin/env bats

load 'test_helper'

@test "logpond /healthz returns 200" {
  run logpond_curl_status /healthz
  [ "$status" -eq 0 ]
  [ "$output" = "200" ]
}

@test "logpond /metrics exposes prometheus metrics" {
  run logpond_curl /metrics
  [ "$status" -eq 0 ]
  echo "$output" | grep -q '^logpond_'
}

@test "logpond accepts a single NDJSON ingest event" {
  local ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  printf '{"timestamp":"%s","level":"info","service":"deploy-smoke","message":"hello logpond"}\n' "$ts" \
    | logpond_post_event default
}

@test "ingested events surface in /api/query" {
  local marker="deploy-smoke-$(date +%s)-${RANDOM}"
  local ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  printf '{"timestamp":"%s","level":"info","service":"deploy-smoke","message":"%s"}\n' \
    "$ts" "$marker" | logpond_post_event default

  wait_for_event "\"$marker\"" 1 30

  local count
  count=$(logpond_query_count "\"$marker\"")
  [ "$count" -ge 1 ]
}

@test "ingest into an unknown source returns 404" {
  local body='{"timestamp":"2026-05-12T00:00:00Z","message":"nope"}'
  local code
  code=$(printf '%s\n' "$body" | curl -s -o /dev/null -w '%{http_code}' \
    -H "Host: ${LOGPOND_DOMAIN}" \
    -H 'Content-Type: application/x-ndjson' \
    -X POST --data-binary @- \
    "${LOGPOND_BASE}/ingest/no-such-source")
  [ "$code" = "404" ]
}
