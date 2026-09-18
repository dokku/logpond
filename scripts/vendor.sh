#!/usr/bin/env bash
# Fetches the pinned frontend dependencies into internal/ui/static/vendor/.
# Run this once before `go build`. The Dockerfile invokes the same script
# during the image build so production containers always have current files.
#
# Versions are NOT pinned here. They come from internal/ui/package.json
# (exact pins) cross-checked against internal/ui/package-lock.json, so
# Dependabot can bump them like any other npm dependency. Nothing is ever
# installed from npm: the lockfile is only the version-of-record, and the
# bytes still come from the CDN.
#
# Requires: bash, curl, jq.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UI="$ROOT/internal/ui"
DEST="$UI/static/vendor"
PKG="$UI/package.json"
LOCK="$UI/package-lock.json"

for bin in curl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "$0: $bin is required (macOS: brew install $bin; Debian: apt-get install -y $bin)" >&2
    exit 2
  fi
done

for f in "$PKG" "$LOCK"; do
  if [[ ! -f "$f" ]]; then
    echo "$0: missing $f (run: npm install --package-lock-only --prefix internal/ui)" >&2
    exit 2
  fi
done

# Prints "<name> <version>" per vendored dependency, in fetch order, after
# cross-checking three sources: the exact pin in package.json, the range the
# lockfile records for the root package, and the version the lockfile
# actually resolved. Any disagreement - a hand-edited package.json, a stale
# lockfile, a range where an exact pin belongs - is a hard error.
resolve_versions() {
  jq -r -n \
    --slurpfile pkg "$PKG" \
    --slurpfile lock "$LOCK" \
    --argjson names '["htmx.org","htmx-ext-ws","alpinejs","open-props"]' \
    '
      def fail($m): ($m + "\n") | halt_error(1);
      $pkg[0]  as $p
    | $lock[0] as $l
    | ( if (($l.lockfileVersion // 0) | tonumber) < 2
        then fail("package-lock.json has lockfileVersion \($l.lockfileVersion // "none"); regenerate it with npm 7 or newer")
        else empty end ),
      ( $names[] as $n
      | ($p.dependencies[$n])                       as $pin
      | ($l.packages[""].dependencies[$n])          as $rootpin
      | ($l.packages["node_modules/" + $n].version) as $ver
      | if $pin == null
          then fail("\($n) is not listed in internal/ui/package.json dependencies")
        elif ($pin | test("^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z.+-]+)?$") | not)
          then fail("\($n): package.json must carry an exact version pin, got \"\($pin)\"")
        elif $ver == null
          then fail("\($n) is missing from package-lock.json; run: npm install --package-lock-only --prefix internal/ui")
        elif $rootpin != $pin
          then fail("\($n): package.json pins \($pin) but the lockfile root records \($rootpin); run: npm install --package-lock-only --prefix internal/ui")
        elif $ver != $pin
          then fail("\($n): package.json pins \($pin) but the lockfile resolved \($ver); run: npm install --package-lock-only --prefix internal/ui")
        else "\($n) \($ver)"
        end )
    '
}

fetch() {
  local url="$1" out="$2"
  echo "fetching $url -> $out"
  # Stage the download so a failed or truncated fetch can never leave a
  # partial asset behind for //go:embed to pick up on the next build.
  curl --fail --silent --show-error --location --output "$out.download" "$url"
  if [[ ! -s "$out.download" ]]; then
    echo "$0: $url returned an empty body" >&2
    exit 1
  fi
  mv "$out.download" "$out"
}

# Plain assignment, deliberately not `local`/`declare`/`export`: the exit
# status of a command consisting only of assignments is the status of its
# last command substitution, so a jq failure aborts under `set -e`. With
# `local versions="$(...)"` the builtin's own zero status would mask the
# failure and we would curl a URL containing an empty or "null" version.
versions="$(resolve_versions)"

mkdir -p "$DEST"
trap 'rm -f "$DEST"/*.download' EXIT

# A here-string, not a pipe: the loop body runs in this shell, so `exit` and
# `set -e` inside fetch() abort the script rather than a subshell.
while read -r name version; do
  case "$name" in
    htmx.org)
      fetch "https://unpkg.com/htmx.org@${version}/dist/htmx.min.js" \
        "$DEST/htmx.min.js"
      ;;
    htmx-ext-ws)
      # The htmx extension packages publish ws.js at the tarball root, not
      # under dist/.
      fetch "https://unpkg.com/htmx-ext-ws@${version}/ws.js" \
        "$DEST/htmx-ws.js"
      ;;
    alpinejs)
      fetch "https://cdn.jsdelivr.net/npm/alpinejs@${version}/dist/cdn.min.js" \
        "$DEST/alpine.min.js"
      ;;
    open-props)
      fetch "https://unpkg.com/open-props@${version}/open-props.min.css" \
        "$DEST/open-props.min.css"
      ;;
    *)
      echo "$0: no CDN mapping for $name" >&2
      exit 1
      ;;
  esac
done <<<"$versions"

printf '%s\n' "$versions" >"$DEST/VERSIONS"
echo "wrote $DEST/VERSIONS"
