#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNNER="${REPO_ROOT}/webapp/scripts/run_degree_complete_local_v2.py"

DEGREES=(2 3)
THREADS="${THREADS:-18}"
PARALLELISM="${PARALLELISM:-18}"
CHUNK_SIZE="${CHUNK_SIZE:-128}"

for DEGREE in "${DEGREES[@]}"; do
  python3 "${RUNNER}" \
    --degree "${DEGREE}" \
    --phase run \
    --network-parallelism "${PARALLELISM}" \
    --julia-threads "${THREADS}" \
    --chunk-size "${CHUNK_SIZE}"
done
