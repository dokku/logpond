#!/usr/bin/env bash
# Run inside the dokku test container. Deploys the locally-built Logpond
# image into Dokku and waits for /healthz. The image must already be
# pushed to the in-stack registry (the Makefile does that on the host
# before invoking this script).
set -euo pipefail

APP="${LOGPOND_APP:-logpond}"
IMAGE="${LOGPOND_IMAGE:-127.0.0.1:5000/logpond:test}"
LOGPOND_DOMAIN="${LOGPOND_DOMAIN:-logpond.dokku.test}"
LOGPOND_STORAGE="${LOGPOND_STORAGE:-/var/lib/dokku/data/storage/${APP}}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-90}"

# Reach the host-network minio + registry from inside the Dokku-managed
# app container. 172.17.0.1 is the docker bridge gateway, which is a
# real interface on the host as well.
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://172.17.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-logpond-archive}"

log() { echo "-----> $*"; }

if dokku apps:exists "$APP" >/dev/null 2>&1; then
  log "App $APP already exists; destroying for a clean redeploy"
  dokku --force apps:destroy "$APP" >/dev/null
fi

log "Creating app $APP"
dokku apps:create "$APP"

# Dokku's port-listening healthcheck uses nsenter to peek inside the
# container; in compose-mode the dokku container itself runs under
# docker, so nsenter cannot cross the namespace boundary. Disable
# zero-downtime checks for the test app and rely on the post-deploy
# /healthz poll below.
log "Disabling per-deploy healthchecks (compose-mode constraint)"
dokku checks:disable "$APP" web || true

log "Setting domain $LOGPOND_DOMAIN"
dokku domains:set "$APP" "$LOGPOND_DOMAIN"

log "Setting ports map: dokku-nginx 80 -> logpond container 8080"
dokku ports:set "$APP" http:80:8080

log "Ensuring storage at $LOGPOND_STORAGE"
mkdir -p "$LOGPOND_STORAGE"
dokku storage:mount "$APP" "${LOGPOND_STORAGE}:/data"

log "Setting logpond env (S3 archive backend pointed at in-stack minio)"
dokku config:set --no-restart "$APP" \
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
dokku git:from-image "$APP" "$IMAGE"

log "Waiting up to ${WAIT_TIMEOUT}s for /healthz"
deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
while (( $(date +%s) < deadline )); do
  if curl -fsS -H "Host: ${LOGPOND_DOMAIN}" "http://127.0.0.1:80/healthz" >/dev/null 2>&1; then
    log "Logpond is healthy"
    exit 0
  fi
  sleep 2
done

log "Timeout waiting for /healthz"
dokku logs "$APP" --tail 200 || true
exit 1
