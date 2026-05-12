#!/usr/bin/env bash
# Fetches the pinned frontend dependencies into static/vendor/. Run this
# once before `go build`. The Dockerfile invokes the same script during
# the image build so production containers always have current files.
#
# Versions are pinned here; bump them deliberately and re-test the UI.

set -euo pipefail

HTMX_VERSION="2.0.4"
HTMX_WS_VERSION="2.0.2"
ALPINE_VERSION="3.14.9"
OPEN_PROPS_VERSION="1.7.13"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/internal/ui/static/vendor"

mkdir -p "$DEST"

fetch() {
  local url="$1" out="$2"
  echo "fetching $url -> $out"
  curl --fail --silent --show-error --location --output "$out" "$url"
}

fetch "https://unpkg.com/htmx.org@${HTMX_VERSION}/dist/htmx.min.js" \
  "$DEST/htmx.min.js"
fetch "https://unpkg.com/htmx-ext-ws@${HTMX_WS_VERSION}/ws.js" \
  "$DEST/htmx-ws.js"
fetch "https://cdn.jsdelivr.net/npm/alpinejs@${ALPINE_VERSION}/dist/cdn.min.js" \
  "$DEST/alpine.min.js"
fetch "https://unpkg.com/open-props@${OPEN_PROPS_VERSION}/open-props.min.css" \
  "$DEST/open-props.min.css"

cat > "$DEST/VERSIONS" <<EOF
htmx.org $HTMX_VERSION
htmx-ext-ws $HTMX_WS_VERSION
alpinejs $ALPINE_VERSION
open-props $OPEN_PROPS_VERSION
EOF

echo "wrote $DEST/VERSIONS"
