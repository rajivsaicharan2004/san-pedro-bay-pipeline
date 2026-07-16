#!/usr/bin/env bash
# Run this by hand over SSH on a fresh instance (infra/oci/compute.tf,
# Ubuntu 24.04 A1.Flex) to install Docker/Java/Python and clone the repo.
#
# Deliberately NOT cloud-init: AISSTREAM_API_KEY has to end up in .env
# somehow, and cloud-init user_data is readable by anything on the box via
# the instance metadata service (and ends up in Terraform-adjacent
# tooling) -- running this by hand and filling in .env afterward keeps
# that secret out of both.
set -euo pipefail

REPO_URL="https://github.com/rajivsaicharan2004/san-pedro-bay-pipeline.git"
REPO_DIR="$HOME/san-pedro-bay-pipeline"

sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg openjdk-21-jdk-headless python3.12 python3.12-venv python3-pip git

# Docker's own apt repo, not Ubuntu's docker.io -- the latter trails
# upstream and has shipped without a working compose plugin before.
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker "$USER"

if [ ! -d "$REPO_DIR" ]; then
  git clone "$REPO_URL" "$REPO_DIR"
fi
cd "$REPO_DIR"

python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

if [ ! -f .env ]; then
  printf 'AISSTREAM_API_KEY=\n' > .env
  echo "Wrote empty .env -- fill in AISSTREAM_API_KEY (chmod 600 .env) before starting the producer."
fi

sudo cp infra/oci/systemd/spb-positions-silver.service infra/oci/systemd/spb-vessel-state.service /etc/systemd/system/
sudo systemctl daemon-reload

echo "Done. Remaining manual steps (in order, after filling in .env):"
echo "  1. Log out and back in (or run 'newgrp docker') for docker group membership to take effect."
echo "  2. cd $REPO_DIR && docker compose -f docker-compose.prod.yml up -d"
echo "  3. sudo systemctl enable --now spb-positions-silver spb-vessel-state"
