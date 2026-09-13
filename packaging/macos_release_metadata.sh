#!/usr/bin/env bash

apple_marketing_version() {
  local full_version="$1"
  local numeric_core="${full_version%%[-+]*}"

  if [[ ! "${numeric_core}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "VERSION must have a three-part numeric SemVer core for Apple bundles: ${full_version}" >&2
    return 2
  fi

  printf '%s\n' "${numeric_core}"
}

apple_build_number() {
  local full_version="$1"
  local numeric_core="${full_version%%[-+]*}"

  # CFBundleVersion allows a positive one-to-four-digit first component and
  # optional second and third components of at most two digits each. Existing
  # Xcode builds used 1000, so map the supported SemVer domain x.y.z to
  # (x + 1001).y.z. This keeps every generated build newer than that baseline
  # while preserving ordinary SemVer ordering.
  if [[ ! "${numeric_core}" =~ ^(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)$ ]]; then
    echo "VERSION build mapping requires canonical SemVer components major 0...8998 and minor/patch 0...99: ${full_version}" >&2
    return 2
  fi

  local major="${BASH_REMATCH[1]}"
  local minor="${BASH_REMATCH[2]}"
  local patch="${BASH_REMATCH[3]}"
  if (( major > 8998 )); then
    echo "VERSION major must be <= 8998 so the mapped Apple build major remains four digits: ${numeric_core}" >&2
    return 2
  fi

  printf '%d.%d.%d\n' "$((major + 1001))" "${minor}" "${patch}"
}

apple_build_version_is_strictly_greater() {
  local candidate="$1"
  local baseline="$2"
  local candidate_major candidate_minor candidate_patch
  local baseline_major baseline_minor baseline_patch

  IFS=. read -r candidate_major candidate_minor candidate_patch <<<"${candidate}"
  IFS=. read -r baseline_major baseline_minor baseline_patch <<<"${baseline}"
  candidate_minor="${candidate_minor:-0}"
  candidate_patch="${candidate_patch:-0}"
  baseline_minor="${baseline_minor:-0}"
  baseline_patch="${baseline_patch:-0}"

  (( candidate_major > baseline_major )) && return 0
  (( candidate_major < baseline_major )) && return 1
  (( candidate_minor > baseline_minor )) && return 0
  (( candidate_minor < baseline_minor )) && return 1
  (( candidate_patch > baseline_patch ))
}

apple_bundle_build_version() {
  local full_version="$1"
  local requested_build_version="${2:-}"
  local default_build_version

  default_build_version="$(apple_build_number "${full_version}")" || return

  if [ -z "${requested_build_version}" ]; then
    printf '%s\n' "${default_build_version}"
    return
  fi

  # Accept only the canonical subset of Apple's format: a positive one-to-four
  # digit first integer and up to two additional zero-to-99 integers. Keeping
  # leading zeros out prevents distinct strings from naming the same build.
  if [[ ! "${requested_build_version}" =~ ^[1-9][0-9]{0,3}(\.(0|[1-9][0-9]?)){0,2}$ ]]; then
    echo "APPLE_BUILD_NUMBER must be 1...9999 with up to two additional canonical 0...99 components: ${requested_build_version}" >&2
    return 2
  fi

  if ! apple_build_version_is_strictly_greater \
    "${requested_build_version}" "${default_build_version}"; then
    echo "APPLE_BUILD_NUMBER must be strictly greater than the derived build ${default_build_version}: ${requested_build_version}" >&2
    return 2
  fi

  printf '%s\n' "${requested_build_version}"
}

backend_payload_sha256() {
  # Preserve the existing tree identity in one process; stream file contents.
  python3 - "$1" <<'PYTHON'
import hashlib
import os
from pathlib import Path
import sys

root = Path(sys.argv[1])
if not root.is_dir():
    sys.exit(f"Backend payload root is not a directory: {root}")
excluded = {"macos-release-metadata.txt", "design-python-runtime-metadata.txt", "python"}
paths = []
def walk_error(error):
    raise error

for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
    if Path(directory) == root:
        dirs[:] = [name for name in dirs if name not in excluded]
        files = [name for name in files if name not in excluded]
    paths.extend(Path(directory) / name for name in dirs + files)

tree = hashlib.sha256()
for path in sorted(paths, key=lambda path: os.fsencode(path.relative_to(root))):
    relative = str(path.relative_to(root))
    if "\n" in relative or "\t" in relative:
        sys.exit(f"Backend payload path contains an unsupported tab or newline: {relative}")
    if path.is_symlink():
        target = os.readlink(path)
        if "\n" in target or "\t" in target:
            sys.exit(f"Backend payload symlink contains an unsupported tab or newline: {relative}")
        record = f"link\t{relative}\t{target}\n"
    elif path.is_file():
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        record = f"file\t{relative}\t{int(os.access(path, os.X_OK))}\t{digest.hexdigest()}\n"
    elif path.is_dir():
        record = f"directory\t{relative}\t{int(os.access(path, os.X_OK))}\n"
    else:
        sys.exit(f"Backend payload contains an unsupported filesystem entry: {relative}")
    tree.update(os.fsencode(record))
print(tree.hexdigest())
PYTHON
}

macos_target_arch() {
  local host_arch="${2:-$(/usr/bin/uname -m)}"
  local requested="${1:-${host_arch}}"

  case "${host_arch}" in
    arm64|x86_64) ;;
    *)
      echo "Unsupported macOS build host architecture: ${host_arch}" >&2
      return 2
      ;;
  esac
  if [ "${requested}" != "${host_arch}" ]; then
    echo "Cross-architecture DMGs are unsupported: requested ${requested}, host Julia is ${host_arch}" >&2
    return 2
  fi

  printf '%s\n' "${requested}"
}
