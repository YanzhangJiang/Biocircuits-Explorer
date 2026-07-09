#!/usr/bin/env bash
# Biocircuits Explorer EC2 deployment entry point (Ubuntu 22.04+ x86_64).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/image_reference.sh
source "$SCRIPT_DIR/image_reference.sh"

export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

REPO_URL="${BIOCIRCUITS_EXPLORER_REPO_URL:-https://github.com/YanzhangJiang/Biocircuits-Explorer.git}"
DEFAULT_INSTALL_DIR="/opt/Biocircuits-Explorer"
LEGACY_INSTALL_DIR="/opt/ROP-Explorer"
INSTALL_DIR="${BIOCIRCUITS_EXPLORER_INSTALL_DIR:-${ROP_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}}"
if [ -z "${BIOCIRCUITS_EXPLORER_INSTALL_DIR:-}" ] && \
   [ -z "${ROP_INSTALL_DIR:-}" ] && \
   [ ! -d "$INSTALL_DIR" ] && [ -d "$LEGACY_INSTALL_DIR" ]; then
    INSTALL_DIR="$LEGACY_INSTALL_DIR"
fi

# Load configuration before any host mutation. An explicitly requested file is
# a contract; silently ignoring a typo can deploy the wrong domain or image.
ENV_FILE_EXPLICIT=0
if [ -n "${BIOCIRCUITS_EXPLORER_ENV_FILE:-}" ]; then
    ENV_FILE="$BIOCIRCUITS_EXPLORER_ENV_FILE"
    ENV_FILE_EXPLICIT=1
else
    ENV_FILE="$SCRIPT_DIR/aws-runtime.env"
fi
if [ "$ENV_FILE_EXPLICIT" -eq 1 ] && [ ! -f "$ENV_FILE" ]; then
    echo "Configured deployment environment file does not exist: $ENV_FILE" >&2
    exit 1
fi
if [ -f "$ENV_FILE" ]; then
    echo "Loading deployment environment from $ENV_FILE"
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

BIOCIRCUITS_EXPLORER_SECRETS_DIR="${BIOCIRCUITS_EXPLORER_SECRETS_DIR:-/opt/rop-explorer-secrets}"
BIOCIRCUITS_EXPLORER_SERVER_NAME="${BIOCIRCUITS_EXPLORER_SERVER_NAME:-biocircuits-explorer.com www.biocircuits-explorer.com}"
export BIOCIRCUITS_EXPLORER_SECRETS_DIR BIOCIRCUITS_EXPLORER_SERVER_NAME
PRIMARY_SERVER_NAME="${BIOCIRCUITS_EXPLORER_SERVER_NAME%% *}"
if ! [[ "$PRIMARY_SERVER_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
    echo "Invalid primary server name: $PRIMARY_SERVER_NAME" >&2
    exit 2
fi
for server_name in $BIOCIRCUITS_EXPLORER_SERVER_NAME; do
    if ! [[ "$server_name" =~ ^(\*\.)?[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
        echo "Invalid Nginx server name: $server_name" >&2
        exit 2
    fi
done

if [ -n "${BIOCIRCUITS_EXPLORER_IMAGE:-}" ]; then
    require_release_image_reference "$BIOCIRCUITS_EXPLORER_IMAGE" "BIOCIRCUITS_EXPLORER_IMAGE"
fi

CERT_DIR="$BIOCIRCUITS_EXPLORER_SECRETS_DIR/certs"
for cert_file in origin.crt origin.key; do
    if [ ! -r "$CERT_DIR/$cert_file" ]; then
        echo "Missing readable TLS file: $CERT_DIR/$cert_file" >&2
        echo "Provision origin.crt and origin.key before deployment; Nginx TLS is mandatory." >&2
        exit 1
    fi
done

echo "=== Biocircuits Explorer Deployment ==="
echo "Install directory: $INSTALL_DIR"
echo "Server name:      $PRIMARY_SERVER_NAME"
echo "Image source:     ${BIOCIRCUITS_EXPLORER_IMAGE:-local versioned build}"

# Install explicit prerequisites only. A product deployment must not perform an
# unrelated full operating-system upgrade.
echo "[1/5] Ensuring host prerequisites..."
sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates curl git gnupg awscli openssl

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    echo "Installing Docker Engine and the Compose plugin..."
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
        sudo gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo DEBIAN_FRONTEND=noninteractive apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
        docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
if ! sudo docker compose version >/dev/null 2>&1; then
    echo "Docker Compose plugin is unavailable after installation." >&2
    exit 1
fi
COMPOSE_UP_HELP="$(sudo docker compose up --help)"
if ! grep -q -- '--wait' <<< "$COMPOSE_UP_HELP"; then
    echo "Docker Compose is too old: this deployment requires 'compose up --wait'." >&2
    exit 1
fi

echo "[2/5] Validating TLS identity..."
if ! openssl x509 -in "$CERT_DIR/origin.crt" -noout -checkend 86400 >/dev/null; then
    echo "TLS certificate is invalid, expired, or expires within 24 hours: $CERT_DIR/origin.crt" >&2
    exit 1
fi
for server_name in $BIOCIRCUITS_EXPLORER_SERVER_NAME; do
    certificate_probe_name="$server_name"
    if [[ "$server_name" == \*.* ]]; then
        certificate_probe_name="biocircuits-wildcard-check.${server_name#*.}"
    fi
    if ! openssl x509 -in "$CERT_DIR/origin.crt" -noout \
        -checkhost "$certificate_probe_name" >/dev/null; then
        echo "TLS certificate does not cover configured server name $server_name" >&2
        exit 1
    fi
done
CERT_PUBLIC_KEY="$(openssl x509 -in "$CERT_DIR/origin.crt" -pubkey -noout | \
    openssl pkey -pubin -outform DER 2>/dev/null | openssl dgst -sha256 || true)"
KEY_PUBLIC_KEY="$(openssl pkey -in "$CERT_DIR/origin.key" -passin pass: -pubout -outform DER 2>/dev/null | \
    openssl dgst -sha256 || true)"
if [ -z "$CERT_PUBLIC_KEY" ] || [ "$CERT_PUBLIC_KEY" != "$KEY_PUBLIC_KEY" ]; then
    echo "TLS certificate and private key do not match." >&2
    exit 1
fi

# Snapshot the running image plus the fully rendered Compose/Nginx contract
# before source update. A rollout can then recover from configuration changes,
# not only from bad image bytes. External secrets and cloud resources remain
# outside this rollback boundary.
ROLLBACK_IMAGE=""
ROLLBACK_CONFIG=""
TARGET_IMAGE_WAS_SET=0
TARGET_IMAGE=""
if [ -n "${BIOCIRCUITS_EXPLORER_IMAGE:-}" ]; then
    TARGET_IMAGE_WAS_SET=1
    TARGET_IMAGE="$BIOCIRCUITS_EXPLORER_IMAGE"
fi
PREVIOUS_IMAGE_ID="$(sudo docker inspect --format '{{.Image}}' biocircuits-explorer-julia 2>/dev/null || true)"
if [ -n "$PREVIOUS_IMAGE_ID" ] && [ -f "$INSTALL_DIR/deploy/docker-compose.yml" ]; then
    ROLLBACK_STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
    ROLLBACK_IMAGE="biocircuits-explorer:rollback-$ROLLBACK_STAMP"
    ROLLBACK_DIR="${BIOCIRCUITS_EXPLORER_ROLLBACK_DIR:-/opt/biocircuits-explorer-rollbacks}/$ROLLBACK_STAMP"
    sudo install -d -m 700 "$ROLLBACK_DIR"
    sudo docker image tag "$PREVIOUS_IMAGE_ID" "$ROLLBACK_IMAGE"
    sudo cp "$INSTALL_DIR/deploy/nginx.conf" "$ROLLBACK_DIR/nginx.conf"

    # The pre-P5 stack served browser assets from the mutable source checkout.
    # Snapshot that bind before git pull so a failed first migration cannot
    # combine the previous backend image with newly checked-out frontend bytes.
    LEGACY_STATIC_SOURCE=""
    LEGACY_STATIC_SNAPSHOT=""
    if grep -q 'webapp/public.*:/usr/share/nginx/html/public' \
        "$INSTALL_DIR/deploy/docker-compose.yml"; then
        LEGACY_STATIC_SOURCE="$(sudo docker inspect \
            --format '{{range .Mounts}}{{if eq .Destination "/usr/share/nginx/html/public"}}{{.Source}}{{end}}{{end}}' \
            biocircuits-explorer-nginx 2>/dev/null || true)"
        LEGACY_STATIC_SOURCE="${LEGACY_STATIC_SOURCE:-$INSTALL_DIR/webapp/public}"
        if [ ! -d "$LEGACY_STATIC_SOURCE" ]; then
            echo "Cannot snapshot legacy frontend bind for rollback: $LEGACY_STATIC_SOURCE" >&2
            exit 1
        fi
        LEGACY_STATIC_SNAPSHOT="$ROLLBACK_DIR/public"
        sudo cp -a "$LEGACY_STATIC_SOURCE" "$LEGACY_STATIC_SNAPSHOT"
    fi

    BIOCIRCUITS_EXPLORER_IMAGE="$ROLLBACK_IMAGE"
    export BIOCIRCUITS_EXPLORER_IMAGE
    sudo -E docker compose \
        -f "$INSTALL_DIR/deploy/docker-compose.yml" \
        config | sudo tee "$ROLLBACK_DIR/docker-compose.yml" >/dev/null
    if [ "$TARGET_IMAGE_WAS_SET" -eq 1 ]; then
        BIOCIRCUITS_EXPLORER_IMAGE="$TARGET_IMAGE"
        export BIOCIRCUITS_EXPLORER_IMAGE
    else
        unset BIOCIRCUITS_EXPLORER_IMAGE
    fi

    ROLLBACK_REWRITES=(
        --replace "$INSTALL_DIR/deploy/nginx.conf" "$ROLLBACK_DIR/nginx.conf"
    )
    if [ -n "$LEGACY_STATIC_SOURCE" ]; then
        ROLLBACK_REWRITES+=(
            --replace "$LEGACY_STATIC_SOURCE" "$LEGACY_STATIC_SNAPSHOT"
        )
    fi
    sudo python3 "$SCRIPT_DIR/rewrite_rollback_config.py" \
        "$ROLLBACK_DIR/docker-compose.yml" "${ROLLBACK_REWRITES[@]}"
    ROLLBACK_CONFIG="$ROLLBACK_DIR/docker-compose.yml"
    echo "Preserved previous image and rendered deployment contract in $ROLLBACK_DIR"
fi

echo "[3/5] Synchronizing source checkout..."
if [ -d "$INSTALL_DIR/.git" ]; then
    cd "$INSTALL_DIR"
    if [ -n "$(sudo git status --porcelain --untracked-files=normal)" ]; then
        echo "Refusing to deploy from a dirty checkout: $INSTALL_DIR" >&2
        exit 1
    fi
    sudo git pull --ff-only
elif [ -e "$INSTALL_DIR" ]; then
    echo "Install path exists but is not a Git checkout: $INSTALL_DIR" >&2
    exit 1
else
    sudo git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

if [ -n "$(sudo git status --porcelain --untracked-files=normal)" ]; then
    echo "Refusing to deploy from a dirty checkout after update: $INSTALL_DIR" >&2
    exit 1
fi
SOURCE_VERSION="$(< "$INSTALL_DIR/VERSION")"
"$INSTALL_DIR/scripts/set_version.sh" --dry-run "$SOURCE_VERSION" >/dev/null
SOURCE_REVISION="$(sudo git -C "$INSTALL_DIR" rev-parse HEAD)"
SOURCE_REVISION_TAG="$(sudo git -C "$INSTALL_DIR" rev-parse --short=12 HEAD)"

echo "[4/5] Building or pulling the release image..."
cd "$INSTALL_DIR/deploy"

if [ -n "${BIOCIRCUITS_EXPLORER_IMAGE:-}" ]; then
    echo "Using prebuilt image: $BIOCIRCUITS_EXPLORER_IMAGE"
    REGISTRY="${BIOCIRCUITS_EXPLORER_IMAGE%%/*}"
    if [[ "$REGISTRY" =~ ^[0-9]+\.dkr\.ecr\.[A-Za-z0-9-]+\.amazonaws\.com$ ]]; then
        ECR_REGION="$(printf '%s' "$REGISTRY" | sed -E 's/^[0-9]+\.dkr\.ecr\.([A-Za-z0-9-]+)\.amazonaws\.com$/\1/')"
        AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$ECR_REGION}}"
        export AWS_REGION AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
        echo "Logging Docker into ECR registry $REGISTRY in $AWS_REGION"
        aws ecr get-login-password --region "$AWS_REGION" | \
            sudo docker login --username AWS --password-stdin "$REGISTRY"
    fi
    sudo -E docker compose pull julia-app
else
    VERSION_TAG="${SOURCE_VERSION//+/_}"
    BIOCIRCUITS_EXPLORER_IMAGE="biocircuits-explorer:${VERSION_TAG}-${SOURCE_REVISION_TAG}"
    BIOCIRCUITS_EXPLORER_VERSION="$SOURCE_VERSION"
    BIOCIRCUITS_EXPLORER_REVISION="$SOURCE_REVISION"
    BIOCIRCUITS_EXPLORER_CREATED="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    export BIOCIRCUITS_EXPLORER_IMAGE BIOCIRCUITS_EXPLORER_VERSION \
        BIOCIRCUITS_EXPLORER_REVISION BIOCIRCUITS_EXPLORER_CREATED
    echo "Building source image: $BIOCIRCUITS_EXPLORER_IMAGE"
    sudo -E docker compose build julia-app
fi

echo "[5/5] Starting services and waiting for readiness..."
if ! sudo -E docker compose up -d --wait --wait-timeout 900; then
    sudo -E docker compose ps >&2 || true
    sudo -E docker compose logs --tail=200 >&2 || true
    echo "Deployment failed its container health/readiness gate." >&2

    if [ -n "$ROLLBACK_CONFIG" ]; then
        echo "Attempting rollback from $ROLLBACK_CONFIG..." >&2
        if sudo -E docker compose -f "$ROLLBACK_CONFIG" \
            up -d --wait --wait-timeout 900; then
            echo "Rollback succeeded; the previous image and rendered configuration are serving again." >&2
        else
            echo "Rollback also failed; inspect 'docker compose ps' and logs immediately." >&2
        fi
    fi
    exit 1
fi

PUBLIC_URL="${BIOCIRCUITS_EXPLORER_PUBLIC_URL:-https://$PRIMARY_SERVER_NAME}"
echo ""
echo "=== Deployment Complete ==="
echo "Access the application at: $PUBLIC_URL"
echo "Release image: $BIOCIRCUITS_EXPLORER_IMAGE"
echo ""
echo "Useful commands:"
echo "  cd $INSTALL_DIR/deploy"
echo "  sudo docker compose logs -f"
echo "  sudo docker compose restart"
echo "  sudo docker compose down"
echo "NOTE: Docker Compose reported both backend readiness and Nginx health before this message."
