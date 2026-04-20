#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$WEBAPP/.." && pwd)"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

if command -v nproc >/dev/null 2>&1; then
  TOTAL_CPUS_DEFAULT="$(nproc)"
else
  TOTAL_CPUS_DEFAULT="64"
fi

TOTAL_CPUS="${TOTAL_CPUS:-$TOTAL_CPUS_DEFAULT}"
WORKER_COUNT="${WORKER_COUNT:-${SHARD_COUNT:-8}}"
THREADS_PER_WORKER="${THREADS_PER_WORKER:-${THREADS_PER_SHARD:-$(( TOTAL_CPUS / WORKER_COUNT ))}}"
if [[ "$THREADS_PER_WORKER" -lt 1 ]]; then
  THREADS_PER_WORKER=1
fi

NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-4}"
CAMPAIGN_TAG="${CAMPAIGN_TAG:-ubuntu_sharded}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$WEBAPP/atlas_store/extended_assembly_sharded_campaign}"
FAMILIES="${FAMILIES:-complex_growth,homomer4plus}"
DEGREES="${DEGREES:-2-8}"
SHARD_MODE="${SHARD_MODE:-stride}"
SCHEDULER_MODE="${SCHEDULER_MODE:-dynamic}"

CMD=(
  python3
  "$SCRIPT_DIR/run_extended_assembly_campaign_sharded.py"
  --julia "$JULIA_BIN"
  --output-root "$OUTPUT_ROOT"
  --campaign-tag "$CAMPAIGN_TAG"
  --families "$FAMILIES"
  --degrees "$DEGREES"
  --scheduler "$SCHEDULER_MODE"
  --shard-count "$WORKER_COUNT"
  --shard-mode "$SHARD_MODE"
  --julia-threads "$THREADS_PER_WORKER"
)

CMD+=(--network-parallelism "$NETWORK_PARALLELISM")

exec "${CMD[@]}" "$@"
