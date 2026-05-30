#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

APP_DISPLAY_NAME="${APP_DISPLAY_NAME:-Biocircuits Explorer}"
APP_PRODUCT_NAME="${APP_PRODUCT_NAME:-BiocircuitsExplorerMac}"
SCHEME="${SCHEME:-BiocircuitsExplorerMac}"
CONFIGURATION="${CONFIGURATION:-Release}"
MIN_MACOS_VERSION="${MIN_MACOS_VERSION:-14.0}"
VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/VERSION")"
JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_CHANNEL="${JULIA_CHANNEL:-1.10}"
BACKEND_MODE="${BACKEND_MODE:-portable}"
LOCAL_DEPOT="${REPO_ROOT}/.julia_packaging_depot"
INCLUDE_COMPILED_DEPOT="${INCLUDE_COMPILED_DEPOT:-0}"
EXTRA_SOURCE_DEPOT="${EXTRA_SOURCE_DEPOT:-${HOME}/.julia}"

PROJECT_PATH="${REPO_ROOT}/frontend-swift/BiocircuitsExplorerMac.xcodeproj"
PACKAGING_SCRIPT="${REPO_ROOT}/packaging/build_backend_app.jl"
DIST_DIR="${REPO_ROOT}/dist"
BACKEND_ROOT="${DIST_DIR}/BiocircuitsExplorerBackend"
BACKEND_RESOURCE_ROOT="${BACKEND_ROOT}/share/biocircuits-explorer"
WEBAPP_DIR="${REPO_ROOT}/webapp"
BUILD_ROOT="${REPO_ROOT}/build/macos-dmg"
DERIVED_DATA="${BUILD_ROOT}/DerivedData"
DMG_ROOT="${BUILD_ROOT}/root"
APP_SOURCE="${DERIVED_DATA}/Build/Products/${CONFIGURATION}/${APP_PRODUCT_NAME}.app"
APP_DEST="${DMG_ROOT}/${APP_DISPLAY_NAME}.app"
DMG_PATH="${DIST_DIR}/${APP_DISPLAY_NAME}-${VERSION}-demo-arm64.dmg"
SKIP_BACKEND="${SKIP_BACKEND:-0}"
SIGN_IDENTITY="${SIGN_IDENTITY:--}"

log() {
  printf '\n==> %s\n' "$*"
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required tool: $1" >&2
    exit 1
  }
}

is_macho() {
  /usr/bin/file "$1" | /usr/bin/grep -q 'Mach-O'
}

sign_macho_files() {
  local root="$1"
  while IFS= read -r -d '' path; do
    if is_macho "$path"; then
      chmod u+w "$path" || true
      /usr/bin/codesign --force --sign "${SIGN_IDENTITY}" "$path" >/dev/null
    fi
  done < <(/usr/bin/find "$root" -type f -print0)
}

julia_command() {
  julia_cmd=("${JULIA_BIN}")
  if [ -n "${JULIA_CHANNEL}" ]; then
    julia_cmd+=("+${JULIA_CHANNEL}")
  fi
}

build_compiled_backend() {
  julia_command
  "${julia_cmd[@]}" --project="${REPO_ROOT}/packaging" "${PACKAGING_SCRIPT}"
}

build_portable_backend() {
  julia_command

  log "Instantiating portable Julia backend environment"
  mkdir -p "${LOCAL_DEPOT}"
  JULIA_DEPOT_PATH="${LOCAL_DEPOT}:" "${julia_cmd[@]}" --project="${WEBAPP_DIR}" -e '
    import Pkg
    repo = ARGS[1]
    Pkg.develop(Pkg.PackageSpec(path=joinpath(repo, "Bnc_julia")))
    Pkg.instantiate()
  ' "${REPO_ROOT}"

  local julia_root
  julia_root="$("${julia_cmd[@]}" -e 'print(dirname(Sys.BINDIR))')"

  log "Assembling portable backend bundle"
  rm -rf "${BACKEND_ROOT}"
  mkdir -p "${BACKEND_ROOT}/bin" "${BACKEND_ROOT}/depot" "${BACKEND_RESOURCE_ROOT}/webapp"

  /usr/bin/ditto "${julia_root}" "${BACKEND_ROOT}/julia"
  /usr/bin/ditto "${WEBAPP_DIR}/src" "${BACKEND_RESOURCE_ROOT}/webapp/src"
  /usr/bin/ditto "${WEBAPP_DIR}/public" "${BACKEND_RESOURCE_ROOT}/public"
  /usr/bin/ditto "${REPO_ROOT}/Bnc_julia" "${BACKEND_RESOURCE_ROOT}/Bnc_julia"

  /bin/cp "${WEBAPP_DIR}/Project.toml" "${BACKEND_RESOURCE_ROOT}/webapp/Project.toml"
  /bin/cp "${WEBAPP_DIR}/Manifest-v1.10.toml" "${BACKEND_RESOURCE_ROOT}/webapp/Manifest-v1.10.toml"
  /bin/cp "${WEBAPP_DIR}/server.jl" "${BACKEND_RESOURCE_ROOT}/webapp/server.jl"

  local escaped_repo
  escaped_repo="$(printf '%s\n' "${REPO_ROOT}" | /usr/bin/sed 's/[\/&]/\\&/g')"
  /usr/bin/sed -i.bak "s|path = \"${escaped_repo}/Bnc_julia\"|path = \"../Bnc_julia\"|" \
    "${BACKEND_RESOURCE_ROOT}/webapp/Manifest-v1.10.toml"
  rm -f "${BACKEND_RESOURCE_ROOT}/webapp/Manifest-v1.10.toml.bak"

  depot_sources=("${LOCAL_DEPOT}")
  if [ -n "${EXTRA_SOURCE_DEPOT}" ] && [ -d "${EXTRA_SOURCE_DEPOT}" ]; then
    depot_sources+=("${EXTRA_SOURCE_DEPOT}")
  fi

  for depot_entry in packages artifacts scratchspaces; do
    for source_depot in "${depot_sources[@]}"; do
      if [ -d "${source_depot}/${depot_entry}" ]; then
        /usr/bin/ditto "${source_depot}/${depot_entry}" "${BACKEND_ROOT}/depot/${depot_entry}"
      fi
    done
  done

  if [ "${INCLUDE_COMPILED_DEPOT}" = "1" ] && [ -d "${LOCAL_DEPOT}/compiled" ]; then
    /usr/bin/ditto "${LOCAL_DEPOT}/compiled" "${BACKEND_ROOT}/depot/compiled"
  fi

  /bin/cat > "${BACKEND_ROOT}/bin/biocircuits-explorer-backend" <<'EOF'
#!/bin/sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
APP_SUPPORT="${HOME}/Library/Application Support/Biocircuits Explorer"
USER_DEPOT="${APP_SUPPORT}/JuliaDepot"
mkdir -p "${USER_DEPOT}"

export JULIA_DEPOT_PATH="${USER_DEPOT}:${ROOT}/depot"
export JULIA_HISTORY="${APP_SUPPORT}/julia_history"
export BNC_NO_PROGRESS=1
export BIOCIRCUITS_EXPLORER_PUBLIC_DIR="${BIOCIRCUITS_EXPLORER_PUBLIC_DIR:-${ROOT}/share/biocircuits-explorer/public}"
export ROP_PUBLIC_DIR="${ROP_PUBLIC_DIR:-${BIOCIRCUITS_EXPLORER_PUBLIC_DIR}}"

exec "${ROOT}/julia/bin/julia" \
  --compiled-modules=no \
  --startup-file=no \
  --project="${ROOT}/share/biocircuits-explorer/webapp" \
  "${ROOT}/share/biocircuits-explorer/webapp/server.jl"
EOF
  chmod +x "${BACKEND_ROOT}/bin/biocircuits-explorer-backend"
}

require_tool "${JULIA_BIN}"
require_tool xcodebuild
require_tool hdiutil
require_tool codesign

log "Preparing build directories"
rm -rf "${BUILD_ROOT}"
mkdir -p "${DIST_DIR}" "${DMG_ROOT}"

if [ "${SKIP_BACKEND}" != "1" ]; then
  case "${BACKEND_MODE}" in
    compiled)
      log "Building PackageCompiler backend"
      build_compiled_backend
      ;;
    portable)
      log "Building portable Julia backend"
      build_portable_backend
      ;;
    *)
      echo "Unsupported BACKEND_MODE=${BACKEND_MODE}; use portable or compiled." >&2
      exit 1
      ;;
  esac
else
  log "Skipping backend build because SKIP_BACKEND=1"
fi

log "Checking bundled backend"
test -x "${BACKEND_ROOT}/bin/biocircuits-explorer-backend"
test -f "${BACKEND_ROOT}/share/biocircuits-explorer/public/index-node.html"

log "Building ${CONFIGURATION} macOS app"
xcodebuild \
  -project "${PROJECT_PATH}" \
  -scheme "${SCHEME}" \
  -configuration "${CONFIGURATION}" \
  -destination 'platform=macOS' \
  -derivedDataPath "${DERIVED_DATA}" \
  MACOSX_DEPLOYMENT_TARGET="${MIN_MACOS_VERSION}" \
  ONLY_ACTIVE_ARCH=YES \
  CODE_SIGNING_ALLOWED=NO \
  BIOCIRCUITS_EXPLORER_REQUIRE_BUNDLED_BACKEND=1 \
  clean build

log "Staging app and Applications shortcut"
test -d "${APP_SOURCE}"
/usr/bin/ditto "${APP_SOURCE}" "${APP_DEST}"
ln -s /Applications "${DMG_ROOT}/Applications"

log "Ad-hoc signing staged app"
sign_macho_files "${APP_DEST}"
/usr/bin/codesign --force --deep --sign "${SIGN_IDENTITY}" "${APP_DEST}" >/dev/null

log "Verifying staged app signature"
/usr/bin/codesign --verify --deep --strict --verbose=2 "${APP_DEST}"

log "Creating compressed DMG"
rm -f "${DMG_PATH}"
hdiutil create \
  -volname "${APP_DISPLAY_NAME}" \
  -srcfolder "${DMG_ROOT}" \
  -ov \
  -format UDZO \
  "${DMG_PATH}"

log "Verifying DMG"
hdiutil verify "${DMG_PATH}"

log "Created ${DMG_PATH}"
