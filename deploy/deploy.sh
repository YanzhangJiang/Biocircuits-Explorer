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
INSTALL_DIR="/opt/Biocircuits-Explorer"

echo "=== Biocircuits Explorer Deployment ==="

# 1. Update system
echo "[1/4] Updating system packages..."
sudo DEBIAN_FRONTEND=noninteractive apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y

# 2. Install Docker
if ! command -v docker &> /dev/null; then
    echo "[2/4] Installing Docker..."
    sudo apt-get install -y ca-certificates curl gnupg
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
sudo docker compose build
sudo docker compose up -d

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
