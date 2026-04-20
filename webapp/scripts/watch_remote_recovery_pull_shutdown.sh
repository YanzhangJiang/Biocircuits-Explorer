#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-8.136.142.73}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_KEY="${REMOTE_KEY:-/Users/yanzhang/key/aliyun-key-1.pem}"
REMOTE_ROOT="${REMOTE_ROOT:-/root/Biocircuits-Explorer}"
REMOTE_V2_DIR="${REMOTE_V2_DIR:-/mnt/complex_growth_recovery_v2}"
LOCAL_OUT_DIR="${LOCAL_OUT_DIR:-/Users/yanzhang/git/Biocircuits-Explorer/webapp/atlas_store/complex_growth_recovery_v2_remote}"
POLL_SECONDS="${POLL_SECONDS:-120}"

mkdir -p "${LOCAL_OUT_DIR}"

ssh_base=(
  ssh
  -i "${REMOTE_KEY}"
  -o StrictHostKeyChecking=no
  "${REMOTE_USER}@${REMOTE_HOST}"
)

scp_base=(
  scp
  -i "${REMOTE_KEY}"
  -o StrictHostKeyChecking=no
)

remote_done_cmd="
set -e
cd '${REMOTE_ROOT}'
running=\$(ps -eo cmd | grep 'run_complex_growth_d2_d3_nonpatch_recovery.sh' | grep -v grep || true)
if [ -n \"\$running\" ]; then
  echo running
  exit 0
fi
for f in \
  '${REMOTE_V2_DIR}/report_d2_complex_growth_nonpatch_v2.sqlite' \
  '${REMOTE_V2_DIR}/report_d2_complex_growth_nonpatch_v2.stats.json' \
  '${REMOTE_V2_DIR}/report_d3_complex_growth_nonpatch_v2.sqlite' \
  '${REMOTE_V2_DIR}/report_d3_complex_growth_nonpatch_v2.stats.json'
do
  [ -f \"\$f\" ] || { echo waiting; exit 0; }
done
echo done
"

while true; do
  state="$("${ssh_base[@]}" "${remote_done_cmd}")"
  case "${state}" in
    done)
      break
      ;;
    running|waiting)
      sleep "${POLL_SECONDS}"
      ;;
    *)
      echo "[warn] unexpected remote state: ${state}" >&2
      sleep "${POLL_SECONDS}"
      ;;
  esac
done

"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/report_d2_complex_growth_nonpatch_v2.sqlite" \
  "${LOCAL_OUT_DIR}/"
"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/report_d2_complex_growth_nonpatch_v2.stats.json" \
  "${LOCAL_OUT_DIR}/"
"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/report_d3_complex_growth_nonpatch_v2.sqlite" \
  "${LOCAL_OUT_DIR}/"
"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/report_d3_complex_growth_nonpatch_v2.stats.json" \
  "${LOCAL_OUT_DIR}/"
"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/../complex_growth_recovery_raw/report_d2_complex_growth_nonpatch.summary.json" \
  "${LOCAL_OUT_DIR}/"
"${scp_base[@]}" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_V2_DIR}/../complex_growth_recovery_raw/report_d3_complex_growth_nonpatch.summary.json" \
  "${LOCAL_OUT_DIR}/"

"${ssh_base[@]}" "shutdown -h now" || true
