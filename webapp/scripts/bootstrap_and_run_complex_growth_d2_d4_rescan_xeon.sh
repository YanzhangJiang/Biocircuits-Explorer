#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="$SCRIPT_DIR/setup_xeon_ubuntu_env.sh"
RUN_SCRIPT="$SCRIPT_DIR/run_complex_growth_d2_d4_rescan_xeon.sh"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/xeon_ubuntu_env.local.sh}"

"$SETUP_SCRIPT"

# shellcheck disable=SC1090
source "$ENV_FILE"

exec "$RUN_SCRIPT"
