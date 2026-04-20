#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/Users/yanzhang/git/Biocircuits-Explorer"
WEBAPP="$REPO_ROOT/webapp"
ATLAS_STORE="$WEBAPP/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$ATLAS_STORE/specs}"
ATLAS_SUMMARIES_DIR="${ATLAS_SUMMARIES_DIR:-$ATLAS_STORE/summaries}"
ATLAS_LOGS_DIR="${ATLAS_LOGS_DIR:-$ATLAS_STORE/logs}"
DOC_DIR="$REPO_ROOT/doc"
JULIA="/Users/yanzhang/.julia/juliaup/julia-1.12.5+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/bin/julia"
LOCKFILE="$ATLAS_LOGS_DIR/extended_assembly_overnight.pid"

mkdir -p "$ATLAS_LOGS_DIR"

cleanup_lock() {
  if [[ "${LOCK_OWNER:-0}" == "1" ]]; then
    rm -f "$LOCKFILE"
  fi
}

acquire_lock() {
  if [[ -f "$LOCKFILE" ]]; then
    local existing_pid
    existing_pid="$(cat "$LOCKFILE" 2>/dev/null || true)"
    if [[ "$existing_pid" =~ ^[0-9]+$ ]] && kill -0 "$existing_pid" 2>/dev/null; then
      echo "INFO: extended assembly overnight runner already active under PID $existing_pid" >&2
      exit 0
    fi
    rm -f "$LOCKFILE"
  fi
  printf '%s\n' "$$" > "$LOCKFILE"
  LOCK_OWNER=1
  trap cleanup_lock EXIT INT TERM
}

acquire_lock

scan_status() {
  local summary_path="$1"
  python3 - "$summary_path" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
if not path.exists():
    print("missing")
    raise SystemExit(0)
try:
    data = json.loads(path.read_text())
except Exception:
    print("invalid")
    raise SystemExit(0)
print(data.get("status", "missing"))
PY
}

run_scan() {
  local spec="$1"
  local summary="$2"
  JULIA_NUM_THREADS=18 "$JULIA" --project="$WEBAPP" \
    "$WEBAPP/scripts/run_atlas_scan_chunked.jl" \
    "$spec" \
    "$summary"
}

run_scan_with_retry() {
  local spec="$1"
  local summary="$2"
  local attempt
  for attempt in 1 2; do
    if run_scan "$spec" "$summary"; then
      return 0
    fi
    echo "WARN: scan failed for $spec on attempt $attempt" >&2
    sleep 10
  done
  echo "ERROR: scan failed twice for $spec" >&2
  return 1
}

for family in complex_growth homomer4plus; do
  for degree in 2 3 4 5 6 7 8; do
    spec="$ATLAS_SPECS_DIR/report_d${degree}_${family}_scan.json"
    summary="$ATLAS_SUMMARIES_DIR/report_d${degree}_${family}_scan.final.summary.json"
    if [[ ! -f "$spec" ]]; then
      python3 "$WEBAPP/scripts/run_extended_assembly_campaign.py" --write-specs-only >/dev/null
    fi
    if [[ "$(scan_status "$summary")" == "completed" ]]; then
      continue
    fi
    run_scan_with_retry "$spec" "$summary"
  done
done

if [[ -f "$DOC_DIR/generate_extended_assembly_campaign_report.py" ]]; then
  python3 "$DOC_DIR/generate_extended_assembly_campaign_report.py"
fi

if [[ -f "$DOC_DIR/discover_extended_assembly_examples.py" ]]; then
  python3 "$DOC_DIR/discover_extended_assembly_examples.py"
fi
