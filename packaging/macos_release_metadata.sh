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
