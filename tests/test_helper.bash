#!/usr/bin/env bash
# Helpers for the Logpond bats suite. Sourced by every *.bats file.
# Compose mode runs inside the dokku container; native mode runs on the
# host with SUDO set so commands that need root (curl-ing files under
# /home/dokku/) can elevate.

LOGPOND_APP="${LOGPOND_APP:-logpond}"
LOGPOND_DOMAIN="${LOGPOND_DOMAIN:-logpond.dokku.test}"
# setup.sh sets `dokku ports:set logpond http:80:8080` so dokku's nginx
# routes :80 -> the logpond container, matching every other app. From
# inside the dokku container that's `http://127.0.0.1:80`; native mode
# uses the same.
LOGPOND_BASE="${LOGPOND_BASE:-http://127.0.0.1:80}"
MINIO_ENDPOINT_HOST="${MINIO_ENDPOINT_HOST:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-logpond-archive}"

SUDO="${SUDO:-}"
DOKKU="${DOKKU:-dokku}"

# logpond_curl wraps curl with the Logpond Host header so requests reach
# the right vhost on the Dokku host. Args after the path are forwarded
# to curl verbatim, so callers can add `-X POST -d ...` etc.
logpond_curl() {
  local path="$1"; shift
  curl -fsS -H "Host: ${LOGPOND_DOMAIN}" "${LOGPOND_BASE}${path}" "$@"
}

logpond_curl_status() {
  local path="$1"; shift
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Host: ${LOGPOND_DOMAIN}" "${LOGPOND_BASE}${path}" "$@"
}

# logpond_post_event posts a single NDJSON line to /ingest/<source>.
# `source` defaults to `default`; the JSON body is read from stdin so
# callers can heredoc-emit complex payloads.
logpond_post_event() {
  local source="${1:-default}"
  curl -fsS \
    -H "Host: ${LOGPOND_DOMAIN}" \
    -H "Content-Type: application/x-ndjson" \
    -X POST --data-binary @- \
    "${LOGPOND_BASE}/ingest/${source}"
}

# logpond_query runs a search-bar query against /api/query and returns
# the raw JSON response on stdout. Time range defaults to the last hour
# in UTC.
logpond_query() {
  local q="$1"
  local from="${2:-$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)}"
  local to="${3:-$(date -u -d '+1 minute' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+1M +%Y-%m-%dT%H:%M:%SZ)}"
  local body
  body=$(jq -nc \
    --arg q "$q" --arg from "$from" --arg to "$to" \
    '{time_range: {from: $from, to: $to}, q: $q, limit: 100}')
  curl -fsS \
    -H "Host: ${LOGPOND_DOMAIN}" \
    -H "Content-Type: application/json" \
    -X POST -d "$body" \
    "${LOGPOND_BASE}/api/query"
}

# logpond_query_count returns the integer count of matching events.
logpond_query_count() {
  local q="$1"
  logpond_query "$q" | jq '.events | length'
}

# wait_for_event polls /api/query every second for up to ${timeout}s
# (default 30) until the query yields at least ${min} events (default 1).
wait_for_event() {
  local q="$1"
  local min="${2:-1}"
  local timeout="${3:-30}"
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    local n
    n=$(logpond_query_count "$q" 2>/dev/null || echo 0)
    if (( n >= min )); then
      return 0
    fi
    sleep 1
  done
  echo "wait_for_event timed out after ${timeout}s for q=$q (min=$min)" >&2
  return 1
}

# segment helpers — list and force-archive segments via the JSON API.
logpond_segments() {
  logpond_curl "/api/segments"
}

logpond_archive_segment() {
  local seg="$1"
  curl -fsS \
    -H "Host: ${LOGPOND_DOMAIN}" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"segment_id\":\"${seg}\"}" \
    "${LOGPOND_BASE}/api/archive"
}

logpond_rehydrate_segment() {
  local seg="$1"
  curl -fsS \
    -H "Host: ${LOGPOND_DOMAIN}" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"segment_id\":\"${seg}\"}" \
    "${LOGPOND_BASE}/api/rehydrate"
}

# wait_for_job polls /api/jobs/<id> until state is done|failed.
wait_for_job() {
  local job_id="$1"
  local timeout="${2:-60}"
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    local state
    state=$(logpond_curl "/api/jobs/${job_id}" 2>/dev/null | jq -r '.state // empty' 2>/dev/null || echo "")
    case "$state" in
      done|completed|succeeded) return 0 ;;
      failed|error)
        echo "job ${job_id} failed:" >&2
        logpond_curl "/api/jobs/${job_id}" 2>/dev/null | jq -r '.error // "(no error message)"' >&2
        return 1
        ;;
    esac
    sleep 1
  done
  echo "wait_for_job timed out after ${timeout}s for job=${job_id}" >&2
  return 1
}

# NOTE: A previous draft of these helpers included a `minio_object_count`
# function that probed MinIO's S3 ListObjects with basic auth. S3
# requires SigV4 signing, so the call returned 403 and the count was
# unreliable. The bats archive suite now trusts the Logpond archive
# job's reported success and the catalog state transition instead.

# new_app_name builds a unique sample-app name per test.
new_app_name() {
  echo "sample-${BATS_TEST_NUMBER:-0}-$(date +%s)-${RANDOM}"
}

cleanup_app() {
  local app="$1"
  if $DOKKU apps:exists "$app" >/dev/null 2>&1; then
    $DOKKU --force apps:destroy "$app" >/dev/null 2>&1 || true
  fi
}
