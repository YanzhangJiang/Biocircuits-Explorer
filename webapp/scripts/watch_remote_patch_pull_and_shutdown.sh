#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${WEBAPP}/.." && pwd)"

REMOTE_HOST="${REMOTE_HOST:-8.136.142.73}"
REMOTE_USER="${REMOTE_USER:-root}"
SSH_KEY="${SSH_KEY:-/Users/yanzhang/key/aliyun-key-1.pem}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null}"

REMOTE_OUTPUT_DIR="${REMOTE_OUTPUT_DIR:-/root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch}"
REMOTE_COMPLETE_SENTINEL="${REMOTE_COMPLETE_SENTINEL:-${REMOTE_OUTPUT_DIR}/report_d4_complex_growth_failed_patch.summary.json}"
LOCAL_OUTPUT_DIR="${LOCAL_OUTPUT_DIR:-${REPO_ROOT}/webapp/atlas_store/complex_growth_failed_patch_remote}"
POLL_SECONDS="${POLL_SECONDS:-120}"

ssh_cmd() {
  ssh -i "${SSH_KEY}" ${SSH_OPTS} "${REMOTE_USER}@${REMOTE_HOST}" "$@"
}

rsync_pull() {
  rsync -az -e "ssh -i ${SSH_KEY} ${SSH_OPTS}" \
    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_OUTPUT_DIR}/" \
    "${LOCAL_OUTPUT_DIR}/"
}

echo "[watch] REMOTE_HOST=${REMOTE_HOST}"
echo "[watch] REMOTE_OUTPUT_DIR=${REMOTE_OUTPUT_DIR}"
echo "[watch] LOCAL_OUTPUT_DIR=${LOCAL_OUTPUT_DIR}"
echo "[watch] POLL_SECONDS=${POLL_SECONDS}"

mkdir -p "${LOCAL_OUTPUT_DIR}"

while true; do
  completed="$(ssh_cmd "if [ -f '${REMOTE_COMPLETE_SENTINEL}' ]; then echo yes; else echo no; fi")"
  if [[ "${completed}" == "yes" ]]; then
    echo "[watch] Remote completion sentinel found: ${REMOTE_COMPLETE_SENTINEL}"
    break
  fi

  echo "[watch] Remote patch still running; sleeping ${POLL_SECONDS}s..."
  sleep "${POLL_SECONDS}"
done

echo "[pull] Copying remote patch outputs..."
rsync_pull
echo "[pull] Copy complete."

echo "[shutdown] Issuing remote shutdown..."
ssh_cmd "shutdown -h now" || true
echo "[shutdown] Remote shutdown command sent."

