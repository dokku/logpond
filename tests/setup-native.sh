#!/usr/bin/env bash
# Native-mode equivalent of tests/setup.sh: assumes Dokku is installed on
# the host (not in a docker compose container). Supporting services
# (registry, minio) still come from `docker compose -f tests/docker-compose.yml`
# without the `compose-mode` profile, so they're already up and listening
# on 127.0.0.1.
#
# The host-side Makefile builds the Logpond image and pushes it to the
# in-stack registry before this script runs.
set -euo pipefail

APP="${LOGPOND_APP:-logpond}"
IMAGE="${LOGPOND_IMAGE:-127.0.0.1:5000/logpond:test}"
LOGPOND_DOMAIN="${LOGPOND_DOMAIN:-logpond.dokku.test}"
LOGPOND_STORAGE="${LOGPOND_STORAGE:-/var/lib/dokku/data/storage/${APP}}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-90}"

SUDO="${SUDO:-sudo}"
DOKKU="${DOKKU:-${SUDO} -u dokku dokku}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://172.17.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-logpond-archive}"

log() { echo "-----> $*"; }

if $DOKKU apps:exists "$APP" >/dev/null 2>&1; then
  log "App $APP already exists; destroying for a clean redeploy"
  $DOKKU --force apps:destroy "$APP" >/dev/null
fi

log "Creating app $APP"
$DOKKU apps:create "$APP"

log "Disabling per-deploy healthchecks"
$DOKKU checks:disable "$APP" web || true

log "Setting domain $LOGPOND_DOMAIN"
$DOKKU domains:set "$APP" "$LOGPOND_DOMAIN"

log "Setting ports map: dokku-nginx 80 -> logpond container 8080"
$DOKKU ports:set "$APP" http:80:8080

log "Ensuring storage at $LOGPOND_STORAGE"
$SUDO mkdir -p "$LOGPOND_STORAGE"
$DOKKU storage:mount "$APP" "${LOGPOND_STORAGE}:/data"

log "Setting logpond env (S3 archive backend pointed at in-stack minio)"
$DOKKU config:set --no-restart "$APP" \
  PORT=8080 \
  LOGPOND_PORT=8080 \
  LOGPOND_LOG_LEVEL=debug \
  LOGPOND_SEGMENT_WINDOW=5m \
  LOGPOND_SEALING_INTERVAL=5s \
  LOGPOND_RETENTION_EVALUATION_INTERVAL=10s \
  LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE=false \
  LOGPOND_ARCHIVE_BACKEND=s3 \
  LOGPOND_ARCHIVE_S3_ENDPOINT="$MINIO_ENDPOINT" \
  LOGPOND_ARCHIVE_S3_BUCKET="$MINIO_BUCKET" \
  LOGPOND_ARCHIVE_S3_PREFIX=logpond/ \
  LOGPOND_ARCHIVE_S3_REGION=us-east-1 \
  LOGPOND_ARCHIVE_S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  LOGPOND_SOURCES_JSON='[{"name":"dokku","extract":{"timestamp":["timestamp"],"message":["message"],"service":["label.com.dokku.app-name"],"host":["host"]}},{"name":"default","extract":{"timestamp":["timestamp","ts"],"message":["message","msg"],"level":["level","severity"],"service":["service","app"],"host":["host","hostname"]}}]'

log "Deploying $APP from image $IMAGE"
$DOKKU git:from-image "$APP" "$IMAGE"

log "Waiting up to ${WAIT_TIMEOUT}s for /healthz"
deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
while (( $(date +%s) < deadline )); do
  if curl -fsS -H "Host: ${LOGPOND_DOMAIN}" "http://127.0.0.1/healthz" >/dev/null 2>&1; then
    log "Logpond is healthy"
    exit 0
  fi
  sleep 2
done

log "Timeout waiting for /healthz"
$DOKKU logs "$APP" --tail 200 || true
exit 1
