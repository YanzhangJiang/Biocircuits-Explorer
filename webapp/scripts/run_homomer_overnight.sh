#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/Users/yanzhang/git/Biocircuits-Explorer"
WEBAPP="$REPO_ROOT/webapp"
ATLAS_STORE="$WEBAPP/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$ATLAS_STORE/specs}"
ATLAS_SUMMARIES_DIR="${ATLAS_SUMMARIES_DIR:-$ATLAS_STORE/summaries}"
DOC_DIR="$REPO_ROOT/doc"
JULIA="julia"

CURRENT_D6_PID="${1:-}"

wait_for_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  while kill -0 "$pid" >/dev/null 2>&1; do
    sleep 60
  done
}

scan_status() {
  local summary_path="$1"
  python3 - "$summary_path" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
if not path.exists():
    print("missing")
    raise SystemExit(0)
data = json.loads(path.read_text())
print(data.get("status", "missing"))
PY
}

run_scan() {
  local degree="$1"
  local spec="$ATLAS_SPECS_DIR/report_d${degree}_homomer_scan.json"
  local summary="$ATLAS_SUMMARIES_DIR/report_d${degree}_homomer_scan.final.summary.json"
  JULIA_NUM_THREADS=18 "$JULIA" --project="$WEBAPP" \
    "$WEBAPP/scripts/run_atlas_scan_chunked.jl" \
    "$spec" \
    "$summary"
}

wait_for_pid "$CURRENT_D6_PID"

if [[ "$(scan_status "$ATLAS_SUMMARIES_DIR/report_d6_homomer_scan.final.summary.json")" != "completed" ]]; then
  run_scan 6
fi

run_scan 7
run_scan 8

python3 "$DOC_DIR/generate_homomer_campaign_report.py"
python3 "$DOC_DIR/discover_interesting_examples.py"
