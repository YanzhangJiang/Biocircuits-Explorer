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
  local root="$1"

  if [ ! -d "${root}" ]; then
    echo "Backend payload root is not a directory: ${root}" >&2
    return 2
  fi

  (
    cd "${root}"
    while IFS= read -r -d '' path; do
      local relative executable digest target
      relative="${path#./}"
      case "${relative}" in
        .|macos-release-metadata.txt|design-python-runtime-metadata.txt|python|python/*)
          continue
          ;;
      esac
      case "${relative}" in
        *$'\n'*|*$'\t'*)
          echo "Backend payload path contains an unsupported tab or newline: ${relative}" >&2
          return 2
          ;;
      esac

      if [ -L "${path}" ]; then
        target="$(/usr/bin/readlink "${path}")"
        case "${target}" in
          *$'\n'*|*$'\t'*)
            echo "Backend payload symlink contains an unsupported tab or newline: ${relative}" >&2
            return 2
            ;;
        esac
        printf 'link\t%s\t%s\n' "${relative}" "${target}"
      elif [ -f "${path}" ]; then
        executable=0
        [ -x "${path}" ] && executable=1
        digest="$(/usr/bin/shasum -a 256 "${path}" | /usr/bin/awk '{print $1}')"
        printf 'file\t%s\t%s\t%s\n' "${relative}" "${executable}" "${digest}"
      elif [ -d "${path}" ]; then
        executable=0
        [ -x "${path}" ] && executable=1
        printf 'directory\t%s\t%s\n' "${relative}" "${executable}"
      else
        echo "Backend payload contains an unsupported filesystem entry: ${relative}" >&2
        return 2
      fi
    done < <(/usr/bin/find . -print0 | LC_ALL=C /usr/bin/sort -z)
  ) | /usr/bin/shasum -a 256 | /usr/bin/awk '{print $1}'
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
