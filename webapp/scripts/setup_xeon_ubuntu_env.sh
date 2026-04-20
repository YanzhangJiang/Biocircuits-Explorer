#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$WEBAPP/.." && pwd)"

JULIA_VERSION="${JULIA_VERSION:-1.12.5}"
JULIA_SHA256="${JULIA_SHA256:-41b84d727e4e96fbf3ed9e92fa195d773d247b9097f73fad688f8b699758bae7}"
JULIA_ARCHIVE_BASENAME="julia-${JULIA_VERSION}-linux-x86_64.tar.gz"
JULIA_URL="${JULIA_URL:-https://julialang-s3.julialang.org/bin/linux/x64/1.12/${JULIA_ARCHIVE_BASENAME}}"

INSTALL_SYSTEM_PACKAGES="${INSTALL_SYSTEM_PACKAGES:-1}"
JULIA_INSTALL_ROOT="${JULIA_INSTALL_ROOT:-$HOME/.local/julia}"
JULIA_INSTALL_DIR="${JULIA_INSTALL_DIR:-$JULIA_INSTALL_ROOT/julia-${JULIA_VERSION}}"
JULIA_BIN_PATH="${JULIA_INSTALL_DIR}/bin/julia"
ENV_FILE="${ENV_FILE:-$WEBAPP/scripts/xeon_ubuntu_env.local.sh}"

DEFAULT_JULIA_THREADS="${DEFAULT_JULIA_THREADS:-96}"
DEFAULT_NETWORK_PARALLELISM="${DEFAULT_NETWORK_PARALLELISM:-24}"

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

run_root() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  elif have_cmd sudo; then
    sudo "$@"
  else
    echo "Need root privileges for: $*" >&2
    return 1
  fi
}

ensure_system_packages() {
  if [[ "$INSTALL_SYSTEM_PACKAGES" != "1" ]]; then
    echo "Skipping apt system package installation."
    return
  fi
  if ! have_cmd apt-get; then
    echo "apt-get not found; skipping system package installation."
    return
  fi

  echo "== Installing Ubuntu system packages =="
  run_root apt-get update
  run_root apt-get install -y \
    ca-certificates \
    curl \
    git \
    python3 \
    sqlite3 \
    tmux \
    tar \
    xz-utils
}

download_file() {
  local url="$1"
  local out="$2"
  if have_cmd curl; then
    curl -fL --retry 5 --retry-delay 2 "$url" -o "$out"
  elif have_cmd wget; then
    wget -O "$out" "$url"
  else
    python3 - "$url" "$out" <<'PY'
import sys, urllib.request
url, out = sys.argv[1:]
with urllib.request.urlopen(url) as r, open(out, "wb") as fh:
    fh.write(r.read())
PY
  fi
}

verify_sha256() {
  local path="$1"
  local expected="$2"
  local actual=""
  if have_cmd sha256sum; then
    actual="$(sha256sum "$path" | awk '{print $1}')"
  elif have_cmd shasum; then
    actual="$(shasum -a 256 "$path" | awk '{print $1}')"
  else
    actual="$(python3 - "$path" <<'PY'
import hashlib, sys
with open(sys.argv[1], "rb") as fh:
    print(hashlib.sha256(fh.read()).hexdigest())
PY
)"
  fi
  if [[ "$actual" != "$expected" ]]; then
    echo "SHA256 mismatch for $path" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    return 1
  fi
}

install_julia() {
  if [[ -x "$JULIA_BIN_PATH" ]]; then
    echo "Reusing Julia at $JULIA_BIN_PATH"
    return
  fi

  echo "== Installing Julia ${JULIA_VERSION} =="
  mkdir -p "$JULIA_INSTALL_ROOT"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT

  local archive_path="${tmp_dir}/${JULIA_ARCHIVE_BASENAME}"
  download_file "$JULIA_URL" "$archive_path"
  verify_sha256 "$archive_path" "$JULIA_SHA256"

  tar -xzf "$archive_path" -C "$tmp_dir"
  local extracted_dir="${tmp_dir}/julia-${JULIA_VERSION}"
  if [[ ! -d "$extracted_dir" ]]; then
    echo "Expected extracted Julia dir not found: $extracted_dir" >&2
    exit 1
  fi

  rm -rf "$JULIA_INSTALL_DIR"
  mkdir -p "$(dirname "$JULIA_INSTALL_DIR")"
  mv "$extracted_dir" "$JULIA_INSTALL_DIR"

  trap - EXIT
  rm -rf "$tmp_dir"
}

write_env_file() {
  mkdir -p "$(dirname "$ENV_FILE")"
  cat >"$ENV_FILE" <<EOF
#!/usr/bin/env bash
export JULIA_BIN="${JULIA_BIN_PATH}"
export JULIA="${JULIA_BIN_PATH}"
export JULIA_THREADS="\${JULIA_THREADS:-${DEFAULT_JULIA_THREADS}}"
export JULIA_NUM_THREADS="\${JULIA_NUM_THREADS:-\${JULIA_THREADS}}"
export NETWORK_PARALLELISM="\${NETWORK_PARALLELISM:-${DEFAULT_NETWORK_PARALLELISM}}"
export OPENBLAS_NUM_THREADS="\${OPENBLAS_NUM_THREADS:-1}"
export OMP_NUM_THREADS="\${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="\${MKL_NUM_THREADS:-1}"
EOF
  chmod +x "$ENV_FILE"
}

instantiate_repo() {
  echo "== Instantiating Julia environments =="
  "$JULIA_BIN_PATH" --project="$WEBAPP" -e 'using Pkg; Pkg.develop(path="Bnc_julia"); Pkg.instantiate(); Pkg.precompile()'
}

echo "== Xeon Ubuntu setup =="
echo "REPO_ROOT=$REPO_ROOT"
echo "WEBAPP=$WEBAPP"
echo "JULIA_VERSION=$JULIA_VERSION"
echo "JULIA_URL=$JULIA_URL"
echo "JULIA_INSTALL_DIR=$JULIA_INSTALL_DIR"
echo "ENV_FILE=$ENV_FILE"
echo

ensure_system_packages
install_julia
write_env_file
instantiate_repo

echo
echo "Setup completed."
echo "Julia: $JULIA_BIN_PATH"
echo "Environment file: $ENV_FILE"
echo "Next step:"
echo "  source \"$ENV_FILE\""
echo "  \"$WEBAPP/scripts/run_complex_growth_d2_d4_rescan_xeon.sh\""
