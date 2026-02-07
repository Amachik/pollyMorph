#!/bin/bash
# PollyMorph VPS Bootstrap Script
# Run this ONCE on a fresh Ubuntu VPS (Vultr, DigitalOcean, etc.)
# Usage: ssh root@your-vps-ip 'bash -s' < scripts/vps_bootstrap.sh

set -euo pipefail

echo "=== PollyMorph VPS Bootstrap ==="
echo "OS: $(uname -srm)"
echo ""

# Detect user (Vultr uses root, Oracle/DO may use ubuntu)
BOT_USER=${SUDO_USER:-$(whoami)}
BOT_HOME=$(eval echo ~$BOT_USER)
echo "Bot user: $BOT_USER (home: $BOT_HOME)"

# Create swap if < 2GB RAM (needed for Rust compilation on 1GB VPS)
TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
if [ "$TOTAL_MEM_KB" -lt 2097152 ]; then
    echo "--- Creating 2GB swap (needed for Rust compilation) ---"
    if [ ! -f /swapfile ]; then
        sudo fallocate -l 2G /swapfile
        sudo chmod 600 /swapfile
        sudo mkswap /swapfile
        sudo swapon /swapfile
        echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
        echo "Swap created and enabled"
    else
        echo "Swap already exists"
    fi
else
    echo "--- Sufficient RAM ($(($TOTAL_MEM_KB / 1024))MB), skipping swap ---"
fi
echo ""

# Update system
echo "--- Updating system packages ---"
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

# Install build dependencies
echo "--- Installing build dependencies ---"
sudo apt-get install -y -qq \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    curl \
    wget \
    htop \
    unzip

# Install Rust
echo "--- Installing Rust ---"
if command -v rustup &> /dev/null; then
    echo "Rust already installed, updating..."
    rustup update
else
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi
echo "Rust version: $(rustc --version)"

# Detect architecture for downloads
MACH=$(uname -m)
if [ "$MACH" = "x86_64" ]; then
    PROM_ARCH="linux-amd64"
    GRAFANA_DEB_ARCH="amd64"
elif [ "$MACH" = "aarch64" ]; then
    PROM_ARCH="linux-arm64"
    GRAFANA_DEB_ARCH="arm64"
else
    echo "ERROR: Unsupported architecture: $MACH" && exit 1
fi
echo "Detected arch: $MACH -> $PROM_ARCH"

# Install Prometheus
echo "--- Installing Prometheus ---"
PROM_VERSION="2.51.0"
if [ ! -f "/usr/local/bin/prometheus" ]; then
    cd /tmp
    wget -q "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.${PROM_ARCH}.tar.gz"
    tar xzf "prometheus-${PROM_VERSION}.${PROM_ARCH}.tar.gz"
    sudo mv "prometheus-${PROM_VERSION}.${PROM_ARCH}/prometheus" /usr/local/bin/
    sudo mv "prometheus-${PROM_VERSION}.${PROM_ARCH}/promtool" /usr/local/bin/
    rm -rf "prometheus-${PROM_VERSION}.${PROM_ARCH}"*
    echo "Prometheus installed: $(prometheus --version 2>&1 | head -1)"
else
    echo "Prometheus already installed"
fi

# Install Grafana
echo "--- Installing Grafana ---"
if ! command -v grafana-server &> /dev/null; then
    sudo apt-get install -y -qq adduser libfontconfig1 musl
    GRAFANA_VERSION="10.4.0"
    cd /tmp
    wget -q "https://dl.grafana.com/oss/release/grafana_${GRAFANA_VERSION}_${GRAFANA_DEB_ARCH}.deb"
    sudo dpkg -i "grafana_${GRAFANA_VERSION}_${GRAFANA_DEB_ARCH}.deb"
    rm -f "grafana_${GRAFANA_VERSION}_${GRAFANA_DEB_ARCH}.deb"
    echo "Grafana installed"
else
    echo "Grafana already installed"
fi

# Create directories
echo "--- Setting up directories ---"
mkdir -p $BOT_HOME/PollyMorph
mkdir -p $BOT_HOME/prometheus-data

# Create Prometheus systemd service
echo "--- Creating Prometheus service ---"
sudo tee /etc/systemd/system/prometheus.service > /dev/null <<SVCEOF
[Unit]
Description=Prometheus Monitoring
After=network.target

[Service]
Type=simple
User=$BOT_USER
ExecStart=/usr/local/bin/prometheus \\
    --config.file=$BOT_HOME/PollyMorph/config/prometheus.yml \\
    --storage.tsdb.path=$BOT_HOME/prometheus-data \\
    --web.listen-address=:9091 \\
    --storage.tsdb.retention.time=30d
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF

# Create PollyMorph systemd service
echo "--- Creating PollyMorph service ---"
sudo tee /etc/systemd/system/polymorph.service > /dev/null <<SVCEOF
[Unit]
Description=PollyMorph HFT Bot
After=network.target prometheus.service

[Service]
Type=simple
User=$BOT_USER
WorkingDirectory=$BOT_HOME/PollyMorph
ExecStart=$BOT_HOME/PollyMorph/target/release/polymorph-hft
Restart=always
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
SVCEOF

# Enable services
sudo systemctl daemon-reload
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
sudo systemctl enable prometheus

echo ""
echo "=== Bootstrap Complete ==="
echo ""
echo "Next steps:"
echo "  1. Deploy code:  From your local machine, run: scripts/deploy.ps1"
echo "  2. Create .env:  ssh $BOT_USER@VPS 'nano $BOT_HOME/PollyMorph/.env'"
echo "  3. Build:        ssh $BOT_USER@VPS 'cd $BOT_HOME/PollyMorph && cargo build --release'"
echo "  4. Start bot:    sudo systemctl start polymorph"
echo "  5. Start prom:   sudo systemctl start prometheus"
echo ""
echo "Grafana:    http://YOUR_VPS_IP:3000  (admin/admin)"
echo "Prometheus: http://YOUR_VPS_IP:9091"
echo "Bot logs:   journalctl -u polymorph -f"
echo ""
