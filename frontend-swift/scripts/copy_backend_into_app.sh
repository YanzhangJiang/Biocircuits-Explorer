#!/bin/sh
set -eu

if [ -z "${SRCROOT:-}" ] || [ -z "${TARGET_BUILD_DIR:-}" ] || [ -z "${UNLOCALIZED_RESOURCES_FOLDER_PATH:-}" ]; then
  echo "warning: Skipping backend embedding because required Xcode build variables are missing."
  exit 0
fi

APP_RESOURCES_DIR="${TARGET_BUILD_DIR}/${UNLOCALIZED_RESOURCES_FOLDER_PATH}"
DEFAULT_BACKEND_SOURCE="${SRCROOT}/../dist/BiocircuitsExplorerBackend"
LEGACY_BACKEND_SOURCE="${SRCROOT}/../dist/ROPExplorerBackend"
BACKEND_SOURCE="${BIOCIRCUITS_EXPLORER_BACKEND_BUNDLE_SOURCE:-${ROP_BACKEND_BUNDLE_SOURCE:-$DEFAULT_BACKEND_SOURCE}}"
BACKEND_DESTINATION="${APP_RESOURCES_DIR}/backend"

if [ ! -d "${BACKEND_SOURCE}" ] && [ "${BACKEND_SOURCE}" = "${DEFAULT_BACKEND_SOURCE}" ] && [ -d "${LEGACY_BACKEND_SOURCE}" ]; then
  BACKEND_SOURCE="${LEGACY_BACKEND_SOURCE}"
fi

mkdir -p "${APP_RESOURCES_DIR}"

if [ ! -d "${BACKEND_SOURCE}" ]; then
  echo "warning: frontend-swift backend bundle not found at ${BACKEND_SOURCE}; skipping backend embedding and relying on source fallback at runtime."
  exit 0
fi

rm -rf "${BACKEND_DESTINATION}"
/usr/bin/ditto "${BACKEND_SOURCE}" "${BACKEND_DESTINATION}"

echo "Bundled Biocircuits Explorer backend into app resources: ${BACKEND_DESTINATION}"
