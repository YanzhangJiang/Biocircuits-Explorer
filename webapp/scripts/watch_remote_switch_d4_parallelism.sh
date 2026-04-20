#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-8.136.142.73}"
REMOTE_USER="${REMOTE_USER:-root}"
SSH_KEY="${SSH_KEY:-/Users/yanzhang/key/aliyun-key-1.pem}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null}"
REMOTE_REPO="${REMOTE_REPO:-/root/Biocircuits-Explorer}"
POLL_SECONDS="${POLL_SECONDS:-20}"

ssh_cmd() {
  ssh -i "${SSH_KEY}" ${SSH_OPTS} "${REMOTE_USER}@${REMOTE_HOST}" "$@"
}

echo "[watch-d4] REMOTE_HOST=${REMOTE_HOST}"
echo "[watch-d4] REMOTE_REPO=${REMOTE_REPO}"
echo "[watch-d4] POLL_SECONDS=${POLL_SECONDS}"

while true; do
  state="$(ssh_cmd "python3 - <<'PY'
from pathlib import Path
import json
base = Path('${REMOTE_REPO}/webapp/atlas_store/complex_growth_failed_patch')
d3 = base / 'report_d3_complex_growth_failed_patch.summary.json'
d4 = base / 'report_d4_complex_growth_failed_patch.summary.json'
if not d3.exists():
    print('WAIT_D3')
elif json.loads(d3.read_text()).get('status') != 'completed':
    print('WAIT_D3')
elif d4.exists():
    print('D4_ALREADY_STARTED')
else:
    print('SWITCH')
PY")"

  case "${state}" in
    WAIT_D3)
      sleep "${POLL_SECONDS}"
      ;;
    D4_ALREADY_STARTED)
      echo "[watch-d4] d4 already started before switch; exiting without intervention."
      exit 0
      ;;
    SWITCH)
      echo "[watch-d4] d3 completed; switching d4 to JULIA_THREADS=120 / network_parallelism=64"
      ssh_cmd "bash -s" <<'REMOTE'
set -euo pipefail
if tmux has-session -t cg_patch 2>/dev/null; then tmux kill-session -t cg_patch; fi
pkill -f 'run_complex_growth_failed_chunk_patch.sh' 2>/dev/null || true
pkill -f '/root/Biocircuits-Explorer/webapp/scripts/run_atlas_scan_chunked.jl' 2>/dev/null || true
mkdir -p /root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch
cat > /root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch/run_patch_d4.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd /root/Biocircuits-Explorer
source /root/Biocircuits-Explorer/webapp/scripts/xeon_ubuntu_env.local.sh
export JULIA_BIN="/root/.local/julia/julia-1.12.5/bin/julia"
export JULIA_EXTRA_ARGS='--compiled-modules=existing --pkgimages=existing'
export TOTAL_CPUS=128
export JULIA_THREADS=120
export JULIA_NUM_THREADS=120
export NETWORK_PARALLELISM=64
export NETWORK_PARALLELISM_D4=64
export RUN_DEGREES=4
export RUN_TESTS=0
export OUTPUT_DIR="/root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch"
export ATLAS_SQLITE_LIGHTWEIGHT_PERSIST=1
bash /root/Biocircuits-Explorer/webapp/scripts/run_complex_growth_failed_chunk_patch.sh
EOF
chmod +x /root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch/run_patch_d4.sh
tmux new-session -d -s cg_patch "bash -lc '/root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch/run_patch_d4.sh |& tee -a /root/Biocircuits-Explorer/webapp/atlas_store/complex_growth_failed_patch/run.log'"
REMOTE
      echo "[watch-d4] switch complete."
      exit 0
      ;;
    *)
      echo "[watch-d4] unexpected state=${state}"
      sleep "${POLL_SECONDS}"
      ;;
  esac
done
