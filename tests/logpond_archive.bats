#!/usr/bin/env bats

# Archive + rehydrate flow exercised end-to-end against the live MinIO
# bucket. Logpond's minimum configurable `segment_window` is 5 minutes
# (per `config.validSegmentWindows`), so sealing a fresh ingest takes a
# little over 5 minutes worst case. We run the full roundtrip as a
# single test under one long timeout so the wait is paid once.

load 'test_helper'

# wait_for_sealed_segment polls /api/segments until at least one sealed
# segment exists. Returns the id of the oldest sealed segment.
wait_for_sealed_segment() {
  local timeout="${1:-420}"
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    local seg
    seg=$(logpond_curl "/api/segments?state=sealed&limit=1" 2>/dev/null \
      | jq -r '.segments[0].id // empty')
    if [ -n "$seg" ]; then
      echo "$seg"
      return 0
    fi
    sleep 5
  done
  echo "wait_for_sealed_segment timed out" >&2
  return 1
}

@test "ingest, seal, archive, rehydrate roundtrip" {
  local marker
  marker="archive-marker-$(date +%s)-${RANDOM}"

  local ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  printf '{"timestamp":"%s","level":"info","service":"archive-test","message":"%s"}\n' \
    "$ts" "$marker" | logpond_post_event default

  wait_for_event "\"$marker\"" 1 30

  local segment_id
  segment_id="$(wait_for_sealed_segment 420)"
  [ -n "$segment_id" ]
  echo "sealed: $segment_id"

  # Archive
  local resp job_id
  resp=$(logpond_archive_segment "$segment_id")
  job_id=$(echo "$resp" | jq -r '.job_id')
  [ -n "$job_id" ]
  wait_for_job "$job_id" 120

  local state
  state=$(logpond_curl "/api/segments?limit=200" \
    | jq -r --arg id "$segment_id" '.segments[] | select(.id == $id) | .state')
  echo "post-archive state: $state"
  [[ "$state" == archived* ]] || [ "$state" = "sealed_archived" ] || [ "$state" = "deleted" ] || [ "$state" = "sealed" ]

  # Rehydrate. Even when the segment is still locally present
  # (archive_before_delete=false), POST /api/rehydrate should accept the
  # request as a no-op or restage from S3.
  resp=$(logpond_rehydrate_segment "$segment_id")
  job_id=$(echo "$resp" | jq -r '.job_id // empty')
  if [ -n "$job_id" ]; then
    wait_for_job "$job_id" 180
  fi

  wait_for_event "\"$marker\"" 1 30
  local count
  count=$(logpond_query_count "\"$marker\"")
  [ "$count" -ge 1 ]
}
