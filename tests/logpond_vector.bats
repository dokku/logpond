#!/usr/bin/env bats

load 'test_helper'

# Vector's launched in bridge mode by dokku, so it reaches the dokku
# host's published nginx port via the docker bridge gateway. On Linux
# that's typically 172.17.0.1; on Docker Desktop / OrbStack it varies
# (e.g. 192.168.x.1). We discover the value from docker so the test is
# portable across hosts. The Host header below routes through
# dokku-nginx to the logpond app.
discover_gateway() {
  docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null
}

setup_file() {
  local gw="${VECTOR_GATEWAY:-$(discover_gateway)}"
  [ -n "$gw" ] || gw=172.17.0.1
  local target="http://${gw}:80"
  local sink="http://?uri=${target}/ingest/dokku&request[headers][Host]=${LOGPOND_DOMAIN}&encoding[codec]=json&framing[method]=newline_delimited&batch[max_events]=10&batch[timeout_secs]=1"
  $DOKKU logs:set --global vector-sink "$sink"
  $DOKKU logs:vector-start >/dev/null
  # Vector takes a few seconds to come up.
  sleep 5

  # Deploy a single shared sample app for all tests in this file. Each
  # test generates a unique marker URL hit, so they remain independent.
  SAMPLE_APP="sample-vector-$(date +%s)-${RANDOM}"
  export SAMPLE_APP
  if ! $DOKKU apps:exists "$SAMPLE_APP" >/dev/null 2>&1; then
    $DOKKU apps:create "$SAMPLE_APP"
    $DOKKU checks:disable "$SAMPLE_APP" web || true
    $DOKKU domains:set "$SAMPLE_APP" "${SAMPLE_APP}.dokku.test"
    $DOKKU git:from-image "$SAMPLE_APP" nginx:alpine >/dev/null
  fi
}

teardown_file() {
  if [ -n "${SAMPLE_APP:-}" ]; then
    $DOKKU --force apps:destroy "$SAMPLE_APP" >/dev/null 2>&1 || true
  fi
  $DOKKU logs:vector-stop >/dev/null 2>&1 || true
  $DOKKU logs:set --global vector-sink "" >/dev/null 2>&1 || true
}

@test "vector container is running" {
  # `dokku logs:vector-logs` shells out to `docker logs --follow`, which
  # never returns. Inspect the container's state directly instead.
  run docker inspect --format '{{.State.Status}}' vector-vector-1
  [ "$status" -eq 0 ]
  [ "$output" = "running" ]
}

@test "logs from a deployed sample app reach logpond via vector" {
  local marker
  marker="vector-marker-$(date +%s)-${RANDOM}"

  # Hit a unique 404 path on the deployed nginx; the request gets
  # logged with the path in the access log line.
  curl -sS -o /dev/null -H "Host: ${SAMPLE_APP}.dokku.test" "${LOGPOND_BASE}/${marker}" || true

  # Allow up to 60s for vector to batch and ship the line.
  wait_for_event "\"$marker\"" 1 60

  local count
  count=$(logpond_query_count "\"$marker\"")
  [ "$count" -ge 1 ]
}

@test "shipped events are attributed to the dokku source" {
  local marker
  marker="vector-source-$(date +%s)-${RANDOM}"

  curl -sS -o /dev/null -H "Host: ${SAMPLE_APP}.dokku.test" "${LOGPOND_BASE}/${marker}" || true

  wait_for_event "source:dokku \"$marker\"" 1 60

  local count
  count=$(logpond_query_count "source:dokku \"$marker\"")
  [ "$count" -ge 1 ]
}
