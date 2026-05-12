#!/bin/bash
###
 # @Author: Yanzhang resicojyz@gmail.com
 # @Date: 2026-03-04 05:04:49
 # @LastEditors: Yanzhang resicojyz@gmail.com
 # @LastEditTime: 2026-03-05 16:43:40
 # @FilePath: /Biocircuits-Explorer/deploy/deploy.sh
 # @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
### 
# Biocircuits Explorer EC2 Deployment Script
# Tested on: Ubuntu 22.04+ (x86_64)

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

REPO_URL="https://github.com/YanzhangJiang/Biocircuits-Explorer.git"
DEFAULT_INSTALL_DIR="/opt/Biocircuits-Explorer"
LEGACY_INSTALL_DIR="/opt/ROP-Explorer"
INSTALL_DIR="${BIOCIRCUITS_EXPLORER_INSTALL_DIR:-${ROP_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}}"
if [ -z "${BIOCIRCUITS_EXPLORER_INSTALL_DIR:-}" ] && [ -z "${ROP_INSTALL_DIR:-}" ] && [ ! -d "$INSTALL_DIR" ] && [ -d "$LEGACY_INSTALL_DIR" ]; then
    INSTALL_DIR="$LEGACY_INSTALL_DIR"
fi

echo "=== Biocircuits Explorer Deployment ==="

# 1. Update system
echo "[1/4] Updating system packages..."
sudo DEBIAN_FRONTEND=noninteractive apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y

# 2. Install Docker
if ! command -v docker &> /dev/null; then
    echo "[2/4] Installing Docker..."
    sudo apt-get install -y ca-certificates curl gnupg awscli
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo usermod -aG docker "$USER"
    echo "Docker installed. You may need to re-login for group changes."
else
    echo "[2/4] Docker already installed, skipping."
    if ! command -v aws &> /dev/null; then
        echo "Installing AWS CLI for optional ECR login..."
        sudo apt-get install -y awscli
    fi
fi

# 3. Clone or update repository
if [ -d "$INSTALL_DIR" ]; then
    echo "[3/4] Updating existing repository..."
    cd "$INSTALL_DIR"
    sudo git pull
else
    echo "[3/4] Cloning repository..."
    sudo git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

# 4. Build and start services
# NOTE: Firewall is managed by AWS Security Group, no need for UFW
echo "[4/4] Building and starting Docker services..."
cd "$INSTALL_DIR/deploy"

ENV_FILE="${BIOCIRCUITS_EXPLORER_ENV_FILE:-./aws-runtime.env}"
if [ -f "$ENV_FILE" ]; then
    echo "Loading deployment environment from $ENV_FILE"
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

if [ -n "${BIOCIRCUITS_EXPLORER_IMAGE:-}" ]; then
    echo "Using prebuilt image: $BIOCIRCUITS_EXPLORER_IMAGE"
    REGISTRY="${BIOCIRCUITS_EXPLORER_IMAGE%%/*}"
    if [[ "$REGISTRY" =~ ^[0-9]+\.dkr\.ecr\.[A-Za-z0-9-]+\.amazonaws\.com$ ]]; then
        ECR_REGION="$(printf '%s' "$REGISTRY" | sed -E 's/^[0-9]+\.dkr\.ecr\.([A-Za-z0-9-]+)\.amazonaws\.com$/\1/')"
        AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$ECR_REGION}}"
        export AWS_REGION AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
        echo "Logging Docker into ECR registry $REGISTRY in $AWS_REGION"
        aws ecr get-login-password --region "$AWS_REGION" | sudo docker login --username AWS --password-stdin "$REGISTRY"
    fi
    sudo -E docker compose pull julia-app
else
    sudo -E docker compose build
fi

sudo -E docker compose up -d

# Output
PUBLIC_IP=$(curl -s http://checkip.amazonaws.com || echo "<your-server-ip>")
echo ""
echo "=== Deployment Complete ==="
echo "Access the application at: http://$PUBLIC_IP"
echo ""
echo "Useful commands:"
echo "  cd $INSTALL_DIR/deploy"
echo "  sudo docker compose logs -f        # View logs"
echo "  sudo docker compose restart         # Restart services"
echo "  sudo docker compose down            # Stop services"
echo ""
echo "To enable HTTPS with Let's Encrypt:"
echo "  1. Point your domain to $PUBLIC_IP"
echo "  2. sudo apt install certbot python3-certbot-nginx"
echo "  3. sudo certbot --nginx -d yourdomain.com"
echo "  4. Uncomment the SSL volume in docker-compose.yml"
echo ""
echo "NOTE: First request may take 30-60s due to Julia JIT compilation."
echo "NOTE: Ensure AWS Security Group allows inbound TCP 22, 80, 443."
