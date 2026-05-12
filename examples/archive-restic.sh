#!/usr/bin/env bash
# archive-restic.sh — reference Logpond script-archive backend.
#
# Implements the contract documented in docs/archival.md
# (full spec in docs/internals/PRD.md §7.9.3):
#
#   archive  --segment-id <id> --parquet-path <p> --manifest-path <m>
#   verify   --segment-id <id> | --all
#   retrieve --segment-id <id> --output-parquet <p> --output-manifest <m>
#
# Each mode also supports a `--probe` flag that returns 0 (supported)
# or exits 64 (mode not implemented). This script implements all three.
#
# Configuration via env vars (set them in archive.script.env.* in
# /etc/logpond/config.yaml or pass them through the systemd unit):
#
#   RESTIC_REPOSITORY  - restic repo location (required)
#   RESTIC_PASSWORD    - restic password (or RESTIC_PASSWORD_FILE)
#   LOGPOND_RESTIC_BIN - path to restic binary (default: restic)
#
# Logpond also exports the following per-invocation env vars; the script
# reads them to label snapshots:
#
#   LOGPOND_SEGMENT_ID, LOGPOND_SEGMENT_TIME_START,
#   LOGPOND_SEGMENT_TIME_END, LOGPOND_SEGMENT_PARQUET_SHA256,
#   LOGPOND_INVOCATION_ID, LOGPOND_MODE

set -euo pipefail

restic_bin="${LOGPOND_RESTIC_BIN:-restic}"
mode="${1:-}"
shift || true

# parse_args extracts --flag values into shell variables.
seg_id=""
parquet=""
manifest=""
output_parquet=""
output_manifest=""
probe=0
all=0

while [ $# -gt 0 ]; do
  case "$1" in
    --segment-id) seg_id="$2"; shift 2;;
    --parquet-path) parquet="$2"; shift 2;;
    --manifest-path) manifest="$2"; shift 2;;
    --output-parquet) output_parquet="$2"; shift 2;;
    --output-manifest) output_manifest="$2"; shift 2;;
    --probe) probe=1; shift;;
    --all) all=1; shift;;
    *) echo "unknown flag: $1" >&2; exit 65;;
  esac
done

# Probe responses: all three modes are supported.
if [ "$probe" = "1" ]; then
  case "$mode" in
    archive|verify|retrieve) exit 0;;
    *) exit 64;;
  esac
fi

# Common required env.
if [ -z "${RESTIC_REPOSITORY:-}" ]; then
  echo "RESTIC_REPOSITORY is required" >&2
  exit 65
fi

snapshot_tag="logpond"
segment_tag="segment:${seg_id:-unknown}"

case "$mode" in
  archive)
    if [ -z "$parquet" ] || [ -z "$manifest" ] || [ -z "$seg_id" ]; then
      echo "archive: --segment-id, --parquet-path, --manifest-path required" >&2
      exit 65
    fi
    if [ ! -f "$parquet" ] || [ ! -f "$manifest" ]; then
      echo "archive: missing files (parquet=$parquet manifest=$manifest)" >&2
      exit 65
    fi
    # Backup both files in a single snapshot so the parquet and its
    # manifest never drift apart.
    snapshot_json="$("$restic_bin" backup \
      --tag "$snapshot_tag" \
      --tag "$segment_tag" \
      --tag "sha:${LOGPOND_SEGMENT_PARQUET_SHA256:-unknown}" \
      --json \
      "$parquet" "$manifest" 2>&1 | tail -n 1)"
    snapshot_id="$(printf '%s' "$snapshot_json" | sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
    if [ -z "$snapshot_id" ]; then
      # Fallback: scrape recent snapshots.
      snapshot_id="$("$restic_bin" snapshots --tag "$segment_tag" --json \
        | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | tail -n 1)"
    fi
    if [ -z "$snapshot_id" ]; then
      echo "archive: failed to locate snapshot id" >&2
      exit 75
    fi
    printf 'LOGPOND_ARCHIVE_REF=restic:%s\n' "$snapshot_id"
    exit 0
    ;;

  verify)
    if [ "$all" = "1" ]; then
      "$restic_bin" check --read-data-subset=5%
      exit $?
    fi
    if [ -z "$seg_id" ]; then
      echo "verify: --segment-id or --all required" >&2
      exit 65
    fi
    if "$restic_bin" snapshots --tag "segment:$seg_id" --json | grep -q '"id"'; then
      exit 0
    fi
    echo "verify: no snapshot tagged segment:$seg_id" >&2
    exit 1
    ;;

  retrieve)
    if [ -z "$seg_id" ] || [ -z "$output_parquet" ] || [ -z "$output_manifest" ]; then
      echo "retrieve: --segment-id, --output-parquet, --output-manifest required" >&2
      exit 65
    fi
    snapshot_id="$("$restic_bin" snapshots --tag "segment:$seg_id" --json \
      | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | tail -n 1)"
    if [ -z "$snapshot_id" ]; then
      echo "retrieve: no snapshot tagged segment:$seg_id" >&2
      exit 65
    fi
    workdir="$(mktemp -d)"
    trap 'rm -rf "$workdir"' EXIT
    "$restic_bin" restore "$snapshot_id" --target "$workdir"
    parquet_src="$(find "$workdir" -name '*.parquet' -type f | head -n 1)"
    manifest_src="$(find "$workdir" -name '*.manifest.json' -type f | head -n 1)"
    if [ -z "$parquet_src" ] || [ -z "$manifest_src" ]; then
      echo "retrieve: snapshot $snapshot_id missing files" >&2
      exit 65
    fi
    cp "$parquet_src" "$output_parquet"
    cp "$manifest_src" "$output_manifest"
    exit 0
    ;;

  *)
    echo "unknown mode: $mode" >&2
    exit 64
    ;;
esac
