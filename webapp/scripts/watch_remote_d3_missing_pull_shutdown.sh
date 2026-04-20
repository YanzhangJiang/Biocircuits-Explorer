#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REMOTE_HOST="${REMOTE_HOST:-8.136.142.73}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_KEY="${REMOTE_KEY:-/Users/yanzhang/key/aliyun-key-1.pem}"
REMOTE_BASE="${REMOTE_BASE:-/root/Biocircuits-Explorer}"
REMOTE_OUT_DIR="${REMOTE_OUT_DIR:-$REMOTE_BASE/webapp/atlas_store/complex_growth_missing_patch_v2}"
REMOTE_RAW_DIR="${REMOTE_RAW_DIR:-/dev/shm/complex_growth_d3_missing_patch_raw}"
LOCAL_OUT_DIR="${LOCAL_OUT_DIR:-$ROOT_DIR/webapp/atlas_store/complex_growth_missing_patch_v2_remote}"
POLL_SECONDS="${POLL_SECONDS:-30}"

mkdir -p "$LOCAL_OUT_DIR"

SUMMARY_RAW="$REMOTE_RAW_DIR/report_d3_complex_growth_missing_patch.summary.json"
REMOTE_V2="$REMOTE_OUT_DIR/report_d3_complex_growth_missing_patch_v2.sqlite"
REMOTE_STATS="$REMOTE_OUT_DIR/report_d3_complex_growth_missing_patch_v2.stats.json"

ssh_base=(ssh -o StrictHostKeyChecking=accept-new -i "$REMOTE_KEY" "$REMOTE_USER@$REMOTE_HOST")
scp_base=(scp -i "$REMOTE_KEY")

echo "[watch] waiting for $REMOTE_V2"
while true; do
  if "${ssh_base[@]}" "test -f '$REMOTE_V2' && test -f '$SUMMARY_RAW'"; then
    break
  fi
  sleep "$POLL_SECONDS"
done

echo "[watch] pulling results"
"${scp_base[@]}" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_V2" "$LOCAL_OUT_DIR/"
"${scp_base[@]}" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_STATS" "$LOCAL_OUT_DIR/"
"${scp_base[@]}" "$REMOTE_USER@$REMOTE_HOST:$SUMMARY_RAW" "$LOCAL_OUT_DIR/"

echo "[watch] shutting down remote host"
"${ssh_base[@]}" 'shutdown -h now' || true

echo "[watch] done"
