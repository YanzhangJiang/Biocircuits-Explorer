#!/usr/bin/env bash
set -u
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REMOTE_HOST="${REMOTE_HOST:-8.136.142.73}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_KEY="${REMOTE_KEY:-/Users/yanzhang/key/aliyun-key-1.pem}"
REMOTE_V2_DIR="${REMOTE_V2_DIR:-/root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_missing_patch_v2}"
REMOTE_RAW_DIR="${REMOTE_RAW_DIR:-/dev/shm/complex_growth_d3_missing_patch_raw}"
LOCAL_OUT_DIR="${LOCAL_OUT_DIR:-$ROOT_DIR/webapp/atlas_store/complex_growth_missing_patch_v2_remote}"
POLL_SECONDS="${POLL_SECONDS:-15}"
mkdir -p "$LOCAL_OUT_DIR"
REMOTE_V2="$REMOTE_V2_DIR/report_d3_complex_growth_missing_patch_v2.sqlite"
REMOTE_SUMMARY="$REMOTE_RAW_DIR/report_d3_complex_growth_missing_patch.summary.json"
while true; do
  if ssh -o StrictHostKeyChecking=accept-new -i "$REMOTE_KEY" "$REMOTE_USER@$REMOTE_HOST" "test -f '$REMOTE_V2' && [ ! -f '$REMOTE_V2-wal' ]" >/dev/null 2>&1; then
    break
  fi
  sleep "$POLL_SECONDS"
done
scp -i "$REMOTE_KEY" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_V2" "$LOCAL_OUT_DIR/"
scp -i "$REMOTE_KEY" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_SUMMARY" "$LOCAL_OUT_DIR/" || true
printf '{"recovered_from_raw":true,"excluded_chunk17":true}\n' > "$LOCAL_OUT_DIR/recover_meta.json"
ssh -o StrictHostKeyChecking=accept-new -i "$REMOTE_KEY" "$REMOTE_USER@$REMOTE_HOST" 'shutdown -h now' >/dev/null 2>&1 || true
