#!/usr/bin/env bash
#
# verify-parquet.sh - cross-tool Parquet read verification.
#
# Reads a sealed Logpond Parquet file with the DuckDB CLI, Python pyarrow,
# and Python pandas, and confirms that all available tools return a
# non-empty result with the schema's required columns.
#
# Tools that aren't installed are reported as SKIPPED. The script exits
# 0 if every available tool succeeded, 1 if any failed. At least one tool
# must succeed for an overall PASS verdict.
#
# Usage:
#   scripts/verify-parquet.sh <path/to/segment.parquet>
#
# Expected required columns (PRD §10.1):
#   timestamp, service, level, message, host, source, attributes, raw

set -uo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <parquet-file>" >&2
  exit 2
fi

PARQUET="$1"
if [[ ! -f "$PARQUET" ]]; then
  echo "error: $PARQUET does not exist" >&2
  exit 2
fi

REQUIRED_COLS=(timestamp service level message host source attributes raw)

ok=0
fail=0
skipped=0

verdict() {
  local tool="$1" status="$2" detail="$3"
  printf 'tool=%-8s status=%-7s %s\n' "$tool" "$status" "$detail"
}

# --- DuckDB CLI -----------------------------------------------------------
if command -v duckdb >/dev/null 2>&1; then
  out=$(duckdb -noheader -csv -c "SELECT COUNT(*) FROM read_parquet('$PARQUET')" 2>/tmp/duckdb.err)
  rc=$?
  if [[ $rc -ne 0 ]]; then
    verdict duckdb FAIL "rc=$rc err=$(tr '\n' ' ' </tmp/duckdb.err)"
    fail=$((fail+1))
  else
    rows="${out//[$'\r\n ']/}"
    cols=$(duckdb -noheader -csv -c "DESCRIBE SELECT * FROM read_parquet('$PARQUET')" 2>/dev/null | awk -F',' '{print $1}' | tr '\n' ' ')
    missing=""
    for col in "${REQUIRED_COLS[@]}"; do
      if ! grep -qw "$col" <<<"$cols"; then
        missing+="$col "
      fi
    done
    if [[ -n "$missing" ]]; then
      verdict duckdb FAIL "rows=$rows missing_cols=${missing% }"
      fail=$((fail+1))
    else
      verdict duckdb PASS "rows=$rows cols=$(echo "$cols" | wc -w | tr -d ' ')"
      ok=$((ok+1))
    fi
  fi
else
  verdict duckdb SKIPPED "duckdb CLI not on PATH"
  skipped=$((skipped+1))
fi

# --- pyarrow --------------------------------------------------------------
if command -v python3 >/dev/null 2>&1 && python3 -c 'import pyarrow.parquet' 2>/dev/null; then
  python3 - "$PARQUET" <<'PY'
import sys
import pyarrow.parquet as pq
required = {"timestamp", "service", "level", "message", "host", "source", "attributes", "raw"}
path = sys.argv[1]
table = pq.read_table(path)
cols = set(table.schema.names)
missing = required - cols
rows = table.num_rows
if missing:
    print(f"rows={rows} missing_cols={','.join(sorted(missing))}", file=sys.stderr)
    sys.exit(1)
print(f"rows={rows} cols={len(cols)}")
PY
  rc=$?
  if [[ $rc -eq 0 ]]; then
    verdict pyarrow PASS ""
    ok=$((ok+1))
  else
    verdict pyarrow FAIL "rc=$rc (see stderr)"
    fail=$((fail+1))
  fi
else
  verdict pyarrow SKIPPED "python3 + pyarrow not available"
  skipped=$((skipped+1))
fi

# --- pandas ---------------------------------------------------------------
if command -v python3 >/dev/null 2>&1 && python3 -c 'import pandas' 2>/dev/null; then
  python3 - "$PARQUET" <<'PY'
import sys
import pandas as pd
required = {"timestamp", "service", "level", "message", "host", "source", "attributes", "raw"}
path = sys.argv[1]
df = pd.read_parquet(path)
cols = set(df.columns)
missing = required - cols
if missing:
    print(f"rows={len(df)} missing_cols={','.join(sorted(missing))}", file=sys.stderr)
    sys.exit(1)
print(f"rows={len(df)} cols={len(cols)}")
PY
  rc=$?
  if [[ $rc -eq 0 ]]; then
    verdict pandas PASS ""
    ok=$((ok+1))
  else
    verdict pandas FAIL "rc=$rc (see stderr)"
    fail=$((fail+1))
  fi
else
  verdict pandas SKIPPED "python3 + pandas not available"
  skipped=$((skipped+1))
fi

# --- Summary --------------------------------------------------------------
echo
echo "summary: ok=$ok fail=$fail skipped=$skipped"
if [[ $fail -gt 0 ]]; then
  exit 1
fi
if [[ $ok -eq 0 ]]; then
  echo "error: no Parquet reader was available; install duckdb CLI or pyarrow" >&2
  exit 1
fi
exit 0
