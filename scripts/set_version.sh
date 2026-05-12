#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN=0

usage() {
    cat <<'USAGE'
Usage:
  scripts/set_version.sh [--dry-run] <semver>

Examples:
  scripts/set_version.sh 0.1.1
  scripts/set_version.sh --dry-run 0.2.0-rc.1
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

NEW_VERSION="$1"
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    echo "Version must be semver-like, for example 0.1.1 or 0.2.0-rc.1" >&2
    exit 2
fi

PROJECT_FILES=(
    "$ROOT_DIR/webapp/Project.toml"
    "$ROOT_DIR/packaging/Project.toml"
)

echo "Setting Biocircuits Explorer version to $NEW_VERSION"
if [ "$DRY_RUN" -eq 1 ]; then
    echo "[dry-run] would write $ROOT_DIR/VERSION"
    for project in "${PROJECT_FILES[@]}"; do
        echo "[dry-run] would update $project"
    done
    exit 0
fi

printf '%s\n' "$NEW_VERSION" > "$ROOT_DIR/VERSION"

export NEW_VERSION
for project in "${PROJECT_FILES[@]}"; do
    if [ -f "$project" ]; then
        perl -0pi -e 's/^version = "[^"]+"/version = "$ENV{NEW_VERSION}"/m' "$project"
    fi
done

echo "Version updated."
