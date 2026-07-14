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
"${REPO_ROOT}/scripts/set_version.sh" --dry-run "${VERSION}"
source "${REPO_ROOT}/packaging/macos_release_metadata.sh"
APPLE_MARKETING_VERSION="$(apple_marketing_version "${VERSION}")"
APPLE_BUILD_NUMBER_OVERRIDE="${APPLE_BUILD_NUMBER:-}"
APPLE_BUILD_NUMBER="$(apple_bundle_build_version "${VERSION}" "${APPLE_BUILD_NUMBER_OVERRIDE}")"
TARGET_ARCH="$(macos_target_arch "${TARGET_ARCH:-}")"
JULIA_BIN="${JULIA_BIN:-julia}"
# Invoke JULIA_BIN directly unless an operator explicitly selects a juliaup
# channel. Either form must resolve to the release-supported Julia 1.12 line.
JULIA_CHANNEL="${JULIA_CHANNEL-}"
BACKEND_MODE="${BACKEND_MODE:-portable}"
LOCAL_DEPOT="${REPO_ROOT}/.julia_packaging_depot"
INCLUDE_COMPILED_DEPOT="${INCLUDE_COMPILED_DEPOT:-0}"
DETECTED_JULIA_VERSION=""

PROJECT_PATH="${REPO_ROOT}/frontend-swift/BiocircuitsExplorerMac.xcodeproj"
PACKAGING_SCRIPT="${REPO_ROOT}/packaging/build_backend_app.jl"
DESIGN_RUNTIME_MANIFEST="${REPO_ROOT}/packaging/design-runtime-files.txt"
DIST_DIR="${REPO_ROOT}/dist"
BACKEND_ROOT="${DIST_DIR}/BiocircuitsExplorerBackend"
BACKEND_RESOURCE_ROOT="${BACKEND_ROOT}/share/biocircuits-explorer"
DESIGN_PYTHON_SOURCE="${DESIGN_PYTHON_SOURCE:-}"
DESIGN_PYTHON_ROOT="${BACKEND_ROOT}/python"
DESIGN_PYTHON_EXECUTABLE="${DESIGN_PYTHON_ROOT}/bin/python3"
DESIGN_PYTHON_METADATA="${BACKEND_ROOT}/design-python-runtime-metadata.txt"
WEBAPP_DIR="${REPO_ROOT}/webapp"
BUILD_ROOT="${REPO_ROOT}/build/macos-dmg"
DERIVED_DATA="${BUILD_ROOT}/DerivedData"
DMG_ROOT="${BUILD_ROOT}/root"
APP_SOURCE="${DERIVED_DATA}/Build/Products/${CONFIGURATION}/${APP_PRODUCT_NAME}.app"
APP_DEST="${DMG_ROOT}/${APP_DISPLAY_NAME}.app"
SKIP_BACKEND="${SKIP_BACKEND:-0}"
RELEASE_MODE="${RELEASE_MODE:-local}"
SIGN_IDENTITY="${SIGN_IDENTITY:--}"
NOTARY_PROFILE="${NOTARY_PROFILE:-}"
PREBUILT_BACKEND_SHA256="${PREBUILT_BACKEND_SHA256:-}"
BACKEND_ENTITLEMENTS="${REPO_ROOT}/packaging/macos-backend.entitlements"
if [ "${RELEASE_MODE}" = "release" ]; then
  DMG_PATH="${DIST_DIR}/${APP_DISPLAY_NAME}-${VERSION}-${TARGET_ARCH}.dmg"
else
  DMG_PATH="${DIST_DIR}/${APP_DISPLAY_NAME}-${VERSION}-local-${TARGET_ARCH}.dmg"
fi

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

is_macho_executable() {
  /usr/bin/file "$1" | /usr/bin/grep -Eq 'Mach-O .*executable'
}

sign_path() {
  local path="$1"
  local include_backend_entitlements="${2:-0}"

  chmod u+w "${path}" || true
  if [ "${RELEASE_MODE}" = "release" ]; then
    local -a sign_args=(--force --options runtime --timestamp --sign "${SIGN_IDENTITY}")
    if [ "${include_backend_entitlements}" = "1" ]; then
      sign_args+=(--entitlements "${BACKEND_ENTITLEMENTS}")
    fi
    /usr/bin/codesign "${sign_args[@]}" "${path}" >/dev/null
  else
    /usr/bin/codesign --force --sign - "${path}" >/dev/null
  fi
}

sign_macho_files() {
  local root="$1"
  while IFS= read -r -d '' path; do
    if is_macho "$path"; then
      if is_macho_executable "$path" \
        && [[ "$path" == */Contents/Helpers/BiocircuitsExplorerBackend/* ]] \
        && [[ "$path" != */Contents/Helpers/BiocircuitsExplorerBackend/python/* ]]; then
        sign_path "$path" 1
      else
        sign_path "$path"
      fi
    fi
  done < <(/usr/bin/find "$root" -depth -type f -print0)

  # Sign nested code containers from the inside out. The outer application is
  # signed separately, without relying on codesign --deep's heuristic rewrite.
  while IFS= read -r -d '' bundle; do
    [ "$bundle" = "$root" ] && continue
    sign_path "$bundle"
  done < <(/usr/bin/find "$root" -depth -type d \( \
    -name '*.framework' -o -name '*.bundle' -o -name '*.app' -o -name '*.xpc' \
  \) -print0)
}

validate_macho_architectures() {
  local root="$1"
  local failed=0
  while IFS= read -r -d '' path; do
    is_macho "$path" || continue
    local architectures
    architectures="$(/usr/bin/lipo -archs "$path")"
    if ! /usr/bin/grep -Eq "(^|[[:space:]])${TARGET_ARCH}([[:space:]]|$)" <<<"${architectures}"; then
      echo "Mach-O file lacks required ${TARGET_ARCH} slice: ${path} (${architectures})" >&2
      failed=1
    fi
  done < <(/usr/bin/find "$root" -type f -print0)
  [ "${failed}" = "0" ]
}

relative_load_path_stays_in_runtime() {
  local base="$1"
  local suffix="$2"
  local runtime_root="$3"
  local target_is_file="${4:-0}"
  local candidate resolved_candidate

  candidate="${base}/${suffix}"
  if [ "${target_is_file}" = "1" ]; then
    candidate="$(dirname "${candidate}")"
  fi
  if ! resolved_candidate="$(cd "${candidate}" 2>/dev/null && pwd -P)"; then
    return 1
  fi
  case "${resolved_candidate}/" in
    "${runtime_root}/"*) return 0 ;;
    *) return 1 ;;
  esac
}

validate_macho_load_paths() {
  local runtime_root failed path dependency rpath suffix
  runtime_root="$(cd "$1" && pwd -P)"
  failed=0

  while IFS= read -r -d '' path; do
    is_macho "${path}" || continue

    while IFS= read -r dependency; do
      [ -n "${dependency}" ] || continue
      case "${dependency}" in
        /System/Library|/System/Library/*|/usr/lib|/usr/lib/*)
          ;;
        @loader_path/*)
          suffix="${dependency#@loader_path/}"
          if ! relative_load_path_stays_in_runtime \
            "$(dirname "${path}")" "${suffix}" "${runtime_root}" 1; then
            echo "Mach-O dependency escapes the Python runtime: ${path} -> ${dependency}" >&2
            failed=1
          fi
          ;;
        @executable_path/*)
          suffix="${dependency#@executable_path/}"
          if ! relative_load_path_stays_in_runtime \
            "${runtime_root}/bin" "${suffix}" "${runtime_root}" 1; then
            echo "Mach-O dependency escapes the Python runtime: ${path} -> ${dependency}" >&2
            failed=1
          fi
          ;;
        @rpath/*)
          suffix="${dependency#@rpath/}"
          case "/${suffix}/" in
            */../*|*/./*|//* )
              echo "Mach-O @rpath dependency contains unsafe traversal: ${path} -> ${dependency}" >&2
              failed=1
              ;;
          esac
          ;;
        *)
          echo "Mach-O dependency is not relocatable or system-owned: ${path} -> ${dependency}" >&2
          failed=1
          ;;
      esac
    done < <(
      /usr/bin/otool -L "${path}" \
        | /usr/bin/sed -E -n '2,$s/^[[:space:]]*(.*) \(compatibility version.*$/\1/p'
    )

    while IFS= read -r rpath; do
      [ -n "${rpath}" ] || continue
      case "${rpath}" in
        /System/Library|/System/Library/*|/usr/lib|/usr/lib/*)
          ;;
        @loader_path)
          ;;
        @loader_path/*)
          suffix="${rpath#@loader_path/}"
          if ! relative_load_path_stays_in_runtime \
            "$(dirname "${path}")" "${suffix}" "${runtime_root}"; then
            echo "Mach-O LC_RPATH escapes the Python runtime: ${path} -> ${rpath}" >&2
            failed=1
          fi
          ;;
        @executable_path)
          ;;
        @executable_path/*)
          suffix="${rpath#@executable_path/}"
          if ! relative_load_path_stays_in_runtime \
            "${runtime_root}/bin" "${suffix}" "${runtime_root}"; then
            echo "Mach-O LC_RPATH escapes the Python runtime: ${path} -> ${rpath}" >&2
            failed=1
          fi
          ;;
        *)
          echo "Mach-O LC_RPATH is not relocatable or system-owned: ${path} -> ${rpath}" >&2
          failed=1
          ;;
      esac
    done < <(
      /usr/bin/otool -l "${path}" | /usr/bin/awk '
        $1 == "cmd" && $2 == "LC_RPATH" { expecting_path = 1; next }
        expecting_path && $1 == "path" { print $2; expecting_path = 0 }
      '
    )
  done < <(/usr/bin/find "${runtime_root}" -type f -print0)

  [ "${failed}" = "0" ]
}

validate_design_python_symlinks() {
  local runtime_root
  runtime_root="$(cd "$1" && pwd -P)"

  while IFS= read -r -d '' link_path; do
    local link_target link_directory target_parent resolved_parent
    link_target="$(/usr/bin/readlink "${link_path}")"
    if [[ "${link_target}" = /* ]]; then
      echo "Bundled Python contains an absolute symlink: ${link_path} -> ${link_target}" >&2
      return 1
    fi

    link_directory="$(cd "$(dirname "${link_path}")" && pwd -P)"
    target_parent="${link_directory}/$(dirname "${link_target}")"
    if ! resolved_parent="$(cd "${target_parent}" 2>/dev/null && pwd -P)"; then
      echo "Bundled Python contains an unresolved symlink: ${link_path} -> ${link_target}" >&2
      return 1
    fi
    case "${resolved_parent}/" in
      "${runtime_root}/"*) ;;
      *)
        echo "Bundled Python symlink escapes its runtime root: ${link_path} -> ${link_target}" >&2
        return 1
        ;;
    esac
    if [ ! -e "${link_path}" ]; then
      echo "Bundled Python contains a broken symlink: ${link_path} -> ${link_target}" >&2
      return 1
    fi
  done < <(/usr/bin/find "${runtime_root}" -type l -print0)
}

probe_design_python_runtime() {
  local runtime_root="$1"
  local backend_root="$2"
  local python_executable="${runtime_root}/bin/python3"
  local staged_backend_root
  local script_directory="${backend_root}/share/biocircuits-explorer/webapp/scripts"
  local schema_path="${backend_root}/share/biocircuits-explorer/schemas/behavior-spec.schema.json"

  staged_backend_root="$(cd "${backend_root}" && pwd -P)"

  if [ ! -x "${python_executable}" ]; then
    echo "DESIGN_PYTHON_SOURCE must provide an executable bin/python3." >&2
    return 1
  fi
  if [ ! -f "${script_directory}/chat_api.py" ] || [ ! -f "${schema_path}" ]; then
    echo "The staged backend is missing the allowlisted Design Chat script or schema." >&2
    return 1
  fi

  log "Probing relocated Design Chat Python runtime"
  (
    cd "${BUILD_ROOT}"
    /usr/bin/env -i \
      HOME="${HOME:-/tmp}" \
      TMPDIR="${TMPDIR:-/tmp}" \
      PATH="/usr/bin:/bin" \
      LC_CTYPE="UTF-8" \
      "${python_executable}" -I -B -X utf8 -c '
import _sqlite3
import _ssl
import os
import ssl
import sqlite3
import sys
import urllib.request
from pathlib import Path

runtime_root = Path(sys.argv[1]).resolve(strict=True)
backend_root = Path(sys.argv[2]).resolve(strict=True)
script_directory = Path(sys.argv[3]).resolve(strict=True)
schema_path = Path(sys.argv[4]).resolve(strict=True)

def require_within(path, root, label, *, must_exist=True):
    candidate = Path(path).resolve(strict=must_exist)
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise AssertionError(f"{label} escapes {root}: {candidate}") from error
    return candidate

assert sys.version_info >= (3, 9), sys.version
require_within(sys.executable, runtime_root, "sys.executable")
require_within(sys.prefix, runtime_root, "sys.prefix")
require_within(sys.base_prefix, runtime_root, "sys.base_prefix")
require_within(script_directory, backend_root, "Design Chat scripts")
require_within(schema_path, backend_root, "Design Chat schema")

sys.path.insert(0, str(script_directory))
for index, entry in enumerate(sys.path):
    if not entry:
        raise AssertionError(f"sys.path[{index}] is empty in isolated mode")
    require_within(entry, backend_root, f"sys.path[{index}]", must_exist=False)

import chat_api

require_within(chat_api.__file__, script_directory, "chat_api.__file__")
for module in (os, ssl, sqlite3, urllib.request, _ssl, _sqlite3):
    module_path = getattr(module, "__file__", None)
    if not module_path:
        raise AssertionError(f"{module.__name__} has no auditable module path")
    require_within(module_path, runtime_root, f"{module.__name__}.__file__")

assert Path(chat_api.agent.L.SCHEMA_PATH).resolve(strict=True) == schema_path
token = "a" * 64
chat_api._validate_runtime_contract(
    "http://127.0.0.1:18088", token, False, "127.0.0.1", token
)
server = chat_api.ThreadingHTTPServer(("127.0.0.1", 0), chat_api.Handler)
server.server_close()
' "${runtime_root}" "${staged_backend_root}" "${script_directory}" "${schema_path}"
  )
}

validate_design_python_runtime() {
  local runtime_root="$1"
  local backend_root="$2"

  validate_design_python_symlinks "${runtime_root}"
  validate_macho_architectures "${runtime_root}"
  validate_macho_load_paths "${runtime_root}"
  probe_design_python_runtime "${runtime_root}" "${backend_root}"
}

stage_design_python_runtime() {
  rm -f "${DESIGN_PYTHON_METADATA}"

  if [ -z "${DESIGN_PYTHON_SOURCE}" ]; then
    rm -rf "${DESIGN_PYTHON_ROOT}"
    echo "warning: DESIGN_PYTHON_SOURCE is not set; this local build omits Design Chat Python. Development launch may still use BNE_PYTHON or PATH." >&2
    return
  fi
  if [ ! -d "${DESIGN_PYTHON_SOURCE}" ]; then
    echo "DESIGN_PYTHON_SOURCE is not a directory: ${DESIGN_PYTHON_SOURCE}" >&2
    exit 1
  fi

  local source_root destination_parent destination_root
  source_root="$(cd "${DESIGN_PYTHON_SOURCE}" && pwd -P)"
  destination_parent="$(cd "$(dirname "${DESIGN_PYTHON_ROOT}")" && pwd -P)"
  destination_root="${destination_parent}/$(basename "${DESIGN_PYTHON_ROOT}")"
  case "${source_root}/" in
    "${destination_root}/"*)
      echo "DESIGN_PYTHON_SOURCE must not be inside the staged destination: ${source_root}" >&2
      exit 1
      ;;
  esac
  case "${destination_root}/" in
    "${source_root}/"*)
      echo "DESIGN_PYTHON_SOURCE must not contain the staged destination: ${source_root}" >&2
      exit 1
      ;;
  esac

  log "Staging relocatable Design Chat Python runtime"
  rm -rf "${DESIGN_PYTHON_ROOT}"
  /usr/bin/ditto "${source_root}" "${DESIGN_PYTHON_ROOT}"
  validate_design_python_runtime "${DESIGN_PYTHON_ROOT}" "${BACKEND_ROOT}"

  local python_version
  python_version="$("${DESIGN_PYTHON_EXECUTABLE}" -I -B -c 'import platform; print(platform.python_version())')"
  /usr/bin/printf 'python_version=%s\narch=%s\nvalidation=isolated-stdlib-chat-startup\n' \
    "${python_version}" "${TARGET_ARCH}" > "${DESIGN_PYTHON_METADATA}"
}

validate_release_configuration() {
  case "${RELEASE_MODE}" in
    local)
      if [ "${SIGN_IDENTITY}" != "-" ]; then
        echo "Local builds are intentionally ad-hoc signed; set RELEASE_MODE=release for Developer ID signing." >&2
        exit 2
      fi
      ;;
    release)
      if [ -z "${SIGN_IDENTITY}" ] || [ "${SIGN_IDENTITY}" = "-" ]; then
        echo "RELEASE_MODE=release requires SIGN_IDENTITY='Developer ID Application: ...'." >&2
        exit 2
      fi
      case "${VERSION}" in
        *-*|*+*)
          if [ -z "${APPLE_BUILD_NUMBER_OVERRIDE}" ]; then
            echo "A formal prerelease/build-metadata VERSION requires a strictly increasing APPLE_BUILD_NUMBER override." >&2
            exit 2
          fi
          ;;
      esac
      if [ -z "${NOTARY_PROFILE}" ]; then
        echo "RELEASE_MODE=release requires a NOTARY_PROFILE stored for notarytool." >&2
        exit 2
      fi
      if [ -z "${DESIGN_PYTHON_SOURCE}" ]; then
        echo "RELEASE_MODE=release requires DESIGN_PYTHON_SOURCE pointing to a relocatable runtime root with bin/python3." >&2
        exit 2
      fi
      if [ "${SKIP_BACKEND}" = "1" ]; then
        if [[ ! "${PREBUILT_BACKEND_SHA256}" =~ ^[0-9a-f]{64}$ ]]; then
          echo "RELEASE_MODE=release with SKIP_BACKEND=1 requires a pinned lowercase PREBUILT_BACKEND_SHA256." >&2
          exit 2
        fi
      fi
      test -f "${BACKEND_ENTITLEMENTS}"
      require_tool xcrun
      require_tool spctl
      ;;
    *)
      echo "Unsupported RELEASE_MODE=${RELEASE_MODE}; use local or release." >&2
      exit 2
      ;;
  esac
}

julia_command() {
  julia_cmd=("${JULIA_BIN}")
  if [ -n "${JULIA_CHANNEL}" ]; then
    julia_cmd+=("+${JULIA_CHANNEL}")
  fi
  julia_cmd+=(--startup-file=no)
}

validate_julia_1_12() {
  julia_command

  local detected_version
  if ! detected_version="$("${julia_cmd[@]}" -e 'print(VERSION)')"; then
    echo "Unable to run the configured Julia command: ${julia_cmd[*]}" >&2
    return 1
  fi
  case "${detected_version}" in
    1.12|1.12.*) ;;
    *)
      echo "macOS packaging requires Julia 1.12; ${julia_cmd[*]} reports ${detected_version}." >&2
      return 1
      ;;
  esac
  DETECTED_JULIA_VERSION="${detected_version}"
}

build_compiled_backend() {
  julia_command
  "${julia_cmd[@]}" --project="${REPO_ROOT}/packaging" "${PACKAGING_SCRIPT}"
}

build_portable_backend() {
  julia_command

  log "Instantiating portable Julia backend environment"
  mkdir -p "${LOCAL_DEPOT}"
  JULIA_DEPOT_PATH="${LOCAL_DEPOT}" "${julia_cmd[@]}" --project="${WEBAPP_DIR}" -e '
    import Pkg
    webapp = ARGS[1]
    cd(webapp) do
      Pkg.instantiate()
    end
  ' "${WEBAPP_DIR}"

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
  /bin/cp "${WEBAPP_DIR}/Manifest.toml" "${BACKEND_RESOURCE_ROOT}/webapp/Manifest.toml"
  /bin/cp "${WEBAPP_DIR}/server.jl" "${BACKEND_RESOURCE_ROOT}/webapp/server.jl"
  /bin/cp "${REPO_ROOT}/VERSION" "${BACKEND_RESOURCE_ROOT}/VERSION"

  if [ ! -f "${DESIGN_RUNTIME_MANIFEST}" ]; then
    echo "Missing Design Agent runtime manifest: ${DESIGN_RUNTIME_MANIFEST}" >&2
    exit 1
  fi
  while IFS= read -r raw_path; do
    relative_path="${raw_path%%#*}"
    relative_path="$(printf '%s' "${relative_path}" | /usr/bin/sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [ -n "${relative_path}" ] || continue
    case "/${relative_path}/" in
      /*/../*|/*/./*|//* )
        echo "Unsafe path in Design Agent runtime manifest: ${relative_path}" >&2
        exit 1
        ;;
    esac
    source_path="${REPO_ROOT}/${relative_path}"
    destination_path="${BACKEND_RESOURCE_ROOT}/${relative_path}"
    if [ ! -e "${source_path}" ]; then
      echo "Missing Design Agent runtime path: ${source_path}" >&2
      exit 1
    fi
    mkdir -p "$(dirname "${destination_path}")"
    /usr/bin/ditto "${source_path}" "${destination_path}"
  done < "${DESIGN_RUNTIME_MANIFEST}"
  /usr/bin/find "${BACKEND_RESOURCE_ROOT}" -type d -name __pycache__ -prune -exec rm -rf {} +

  # The release depot is instantiated in isolation above. Never copy a
  # developer's ~/.julia scratchspaces or unrelated package history into an app.
  for depot_entry in packages artifacts; do
    if [ -d "${LOCAL_DEPOT}/${depot_entry}" ]; then
      /usr/bin/ditto "${LOCAL_DEPOT}/${depot_entry}" "${BACKEND_ROOT}/depot/${depot_entry}"
    fi
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
RUNTIME_ROOT="${APP_SUPPORT}/Runtime"
mkdir -p "${USER_DEPOT}" "${RUNTIME_ROOT}/Jobs" "${RUNTIME_ROOT}/Atlas"

export JULIA_DEPOT_PATH="${USER_DEPOT}:${ROOT}/depot"
export JULIA_HISTORY="${APP_SUPPORT}/julia_history"
export BNC_NO_PROGRESS=1
export BIOCIRCUITS_EXPLORER_JOB_STORE="${BIOCIRCUITS_EXPLORER_JOB_STORE:-${RUNTIME_ROOT}/Jobs}"
export BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT="${BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT:-${RUNTIME_ROOT}/Atlas}"
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

write_backend_metadata() {
  local payload_sha256

  case "${DETECTED_JULIA_VERSION}" in
    1.12|1.12.*) ;;
    *)
      echo "Cannot write backend provenance without a verified Julia 1.12 version." >&2
      return 1
      ;;
  esac
  payload_sha256="$(backend_payload_sha256 "${BACKEND_ROOT}")"
  /usr/bin/printf 'version=%s\narch=%s\nmode=%s\njulia_version=%s\nbackend_payload_sha256=%s\n' \
    "${VERSION}" "${TARGET_ARCH}" "${BACKEND_MODE}" \
    "${DETECTED_JULIA_VERSION}" "${payload_sha256}" \
    > "${BACKEND_ROOT}/macos-release-metadata.txt"
}

validate_backend_metadata() {
  local metadata="${BACKEND_ROOT}/macos-release-metadata.txt"
  local line_count julia_version recorded_sha256 actual_sha256

  test -f "${metadata}"
  line_count="$(/usr/bin/wc -l < "${metadata}" | /usr/bin/tr -d '[:space:]')"
  if [ "${line_count}" != "5" ]; then
    echo "Backend provenance must contain exactly five canonical fields: ${metadata}" >&2
    return 1
  fi
  /usr/bin/grep -Fxq "version=${VERSION}" "${metadata}"
  /usr/bin/grep -Fxq "arch=${TARGET_ARCH}" "${metadata}"
  /usr/bin/grep -Fxq "mode=${BACKEND_MODE}" "${metadata}"
  julia_version="$(/usr/bin/sed -n 's/^julia_version=//p' "${metadata}")"
  case "${julia_version}" in
    1.12|1.12.*) ;;
    *)
      echo "Prebuilt backend provenance does not declare Julia 1.12: ${julia_version:-missing}" >&2
      return 1
      ;;
  esac
  recorded_sha256="$(/usr/bin/sed -n 's/^backend_payload_sha256=//p' "${metadata}")"
  if [[ ! "${recorded_sha256}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "Prebuilt backend provenance has an invalid payload hash: ${recorded_sha256:-missing}" >&2
    return 1
  fi
  actual_sha256="$(backend_payload_sha256 "${BACKEND_ROOT}")"
  if [ "${recorded_sha256}" != "${actual_sha256}" ]; then
    echo "Prebuilt backend payload hash mismatch: recorded ${recorded_sha256}, actual ${actual_sha256}" >&2
    return 1
  fi
  if [ -n "${PREBUILT_BACKEND_SHA256}" ] \
    && [ "${recorded_sha256}" != "${PREBUILT_BACKEND_SHA256}" ]; then
    echo "Prebuilt backend payload does not match pinned PREBUILT_BACKEND_SHA256." >&2
    return 1
  fi
}

require_tool xcodebuild
require_tool hdiutil
require_tool codesign
require_tool lipo
require_tool otool
require_tool shasum
validate_release_configuration

log "Preparing build directories"
rm -rf "${BUILD_ROOT}"
mkdir -p "${DIST_DIR}" "${DMG_ROOT}"

if [ "${SKIP_BACKEND}" != "1" ]; then
  require_tool "${JULIA_BIN}"
  validate_julia_1_12
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
  write_backend_metadata
else
  log "Skipping backend build because SKIP_BACKEND=1"
fi

log "Checking bundled backend"
test -x "${BACKEND_ROOT}/bin/biocircuits-explorer-backend"
test -f "${BACKEND_ROOT}/share/biocircuits-explorer/public/index-node.html"
validate_backend_metadata
stage_design_python_runtime

log "Building ${CONFIGURATION} macOS app"
xcodebuild \
  -project "${PROJECT_PATH}" \
  -scheme "${SCHEME}" \
  -configuration "${CONFIGURATION}" \
  -destination 'platform=macOS' \
  -derivedDataPath "${DERIVED_DATA}" \
  MACOSX_DEPLOYMENT_TARGET="${MIN_MACOS_VERSION}" \
  MARKETING_VERSION="${APPLE_MARKETING_VERSION}" \
  CURRENT_PROJECT_VERSION="${APPLE_BUILD_NUMBER}" \
  ARCHS="${TARGET_ARCH}" \
  ONLY_ACTIVE_ARCH=YES \
  CODE_SIGNING_ALLOWED=NO \
  BIOCIRCUITS_EXPLORER_REQUIRE_BUNDLED_BACKEND=1 \
  clean build

log "Staging app and Applications shortcut"
test -d "${APP_SOURCE}"
/usr/bin/ditto "${APP_SOURCE}" "${APP_DEST}"
ln -s /Applications "${DMG_ROOT}/Applications"
if [ -n "${DESIGN_PYTHON_SOURCE}" ]; then
  test -x "${APP_DEST}/Contents/Helpers/BiocircuitsExplorerBackend/python/bin/python3"
  test -f "${APP_DEST}/Contents/Helpers/BiocircuitsExplorerBackend/design-python-runtime-metadata.txt"
fi
validate_macho_architectures "${APP_DEST}"

log "Signing staged app (${RELEASE_MODE})"
sign_macho_files "${APP_DEST}"
sign_path "${APP_DEST}"

log "Verifying staged app signature"
/usr/bin/codesign --verify --deep --strict --verbose=2 "${APP_DEST}"
if [ -n "${DESIGN_PYTHON_SOURCE}" ]; then
  log "Probing signed Design Chat Python runtime"
  probe_design_python_runtime \
    "${APP_DEST}/Contents/Helpers/BiocircuitsExplorerBackend/python" \
    "${APP_DEST}/Contents/Helpers/BiocircuitsExplorerBackend"
  /usr/bin/codesign --verify --deep --strict --verbose=2 "${APP_DEST}"
fi

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

if [ "${RELEASE_MODE}" = "release" ]; then
  log "Signing and notarizing DMG"
  /usr/bin/codesign --force --timestamp --sign "${SIGN_IDENTITY}" "${DMG_PATH}"
  xcrun notarytool submit "${DMG_PATH}" --keychain-profile "${NOTARY_PROFILE}" --wait
  xcrun stapler staple "${DMG_PATH}"
  xcrun stapler validate "${DMG_PATH}"
  /usr/sbin/spctl --assess --type execute --verbose=4 "${APP_DEST}"
  /usr/sbin/spctl --assess --type open --context context:primary-signature --verbose=4 "${DMG_PATH}"
fi

log "Created ${DMG_PATH}"
