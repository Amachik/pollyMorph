# PollyMorph Live Trading Setup

## 1. Get Your Polymarket API Keys

### Step 1: Create API Keys on Polymarket
1. Go to [polymarket.com](https://polymarket.com) and log in
2. Go to **Settings → API Keys**
3. Click **Create API Key**
4. You'll receive three values:
   - `api_key` — your API key identifier
   - `api_secret` — base64-encoded HMAC secret
   - `api_passphrase` — passphrase from L1 key derivation

### Step 2: Get Your Wallet Private Key
Your wallet private key is the hex-encoded private key of the wallet you use on Polymarket.

**If you use MetaMask:**
1. Open MetaMask → click the three dots → Account Details → Export Private Key
2. Enter your password
3. Copy the hex string (WITHOUT the `0x` prefix)

**⚠️ SECURITY WARNING:** Never share your private key. Never commit it to git.

## 2. Configure the Bot

### Create `.env` file
```bash
cp .env.example .env
```

Edit `.env` and fill in your keys:
```
POLYMARKET_API_KEY=your-api-key
POLYMARKET_API_SECRET=your-api-secret
POLYMARKET_API_PASSPHRASE=your-passphrase
POLYMARKET_PRIVATE_KEY=your-private-key-hex-no-0x
```

The bot loads config from `config/local.toml` (trading parameters) and `.env` (secrets).

## 3. Install Prometheus & Grafana (No Docker)

### Windows (Local Development)

#### Prometheus
1. Download from https://prometheus.io/download/ (Windows amd64)
2. Extract to `C:\prometheus\`
3. Copy our config:
   ```powershell
   copy config\prometheus.yml C:\prometheus\prometheus.yml
   ```
4. Start Prometheus:
   ```powershell
   C:\prometheus\prometheus.exe --config.file=C:\prometheus\prometheus.yml --storage.tsdb.path=C:\prometheus\data --web.listen-address=:9091
   ```
5. Verify: open http://localhost:9091 in browser

#### Grafana
1. Download from https://grafana.com/grafana/download?platform=windows (OSS edition, standalone)
2. Extract to `C:\grafana\`
3. Start Grafana:
   ```powershell
   C:\grafana\bin\grafana-server.exe
   ```
4. Open http://localhost:3000 (default login: admin/admin)
5. Add Prometheus data source:
   - Go to **Connections → Data Sources → Add data source → Prometheus**
   - URL: `http://localhost:9091`
   - Click **Save & Test**
6. Import dashboard:
   - Go to **Dashboards → Import**
   - Click **Upload JSON file** → select `docker/grafana_dashboard.json`
   - Select your Prometheus data source
   - Click **Import**

### Linux (AWS VPS)

#### Prometheus
```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.linux-amd64.tar.gz
tar xzf prometheus-2.51.0.linux-amd64.tar.gz
cd prometheus-2.51.0.linux-amd64
./prometheus --config.file=/home/ubuntu/PollyMorph/config/prometheus.yml --storage.tsdb.path=./data --web.listen-address=:9091 &
```

#### Grafana
```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_10.4.0_amd64.deb
sudo dpkg -i grafana_10.4.0_amd64.deb
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
```
Then add Prometheus data source and import dashboard (same as Windows steps 5-6).

## 4. VPS Setup (Oracle Cloud — FREE)

### Why Oracle Cloud?
- **Free forever** — 4 ARM cores + 24GB RAM on the Always Free tier
- **Ashburn, VA datacenter** — same region as Polymarket's API (<2ms latency)
- Beats AWS ($62/mo+) for a $22 wallet

### Create Oracle Cloud Account
1. Go to [cloud.oracle.com/free](https://cloud.oracle.com) and sign up
2. Select **Home Region: US East (Ashburn)** ← critical, cannot change later!
3. You'll need a credit card for verification (won't be charged)

### Launch Free ARM Instance
1. Go to **Compute → Instances → Create Instance**
2. Settings:
   - **Name:** PollyMorph-Bot
   - **Image:** Ubuntu 24.04 (aarch64)
   - **Shape:** VM.Standard.A1.Flex → **4 OCPUs, 24GB RAM** (max free)
   - **Networking:** Create new VCN, assign public IP
   - **SSH key:** Upload your public key or generate one
   - **Boot volume:** 50GB (free up to 200GB)
3. Click **Create**
4. **Security List:** Add ingress rules for:
   - SSH (22) from your IP
   - Grafana (3000) from your IP
   - Prometheus (9091) from your IP

### Bootstrap VPS (one-time setup)
Run this from your local machine — it installs Rust, Prometheus, and Grafana:
```powershell
# Replace with your VPS IP and key path
ssh -i C:\path\to\your-key ubuntu@YOUR_VPS_IP 'bash -s' < scripts\vps_bootstrap.sh
```

### Deploy Bot
```powershell
# Set your VPS connection details
$env:VPS_HOST = "YOUR_VPS_IP"
$env:VPS_KEY = "C:\path\to\your-key"

# Deploy (syncs code, builds on VPS, restarts service)
powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1
```

### First time: Create .env on VPS
```bash
ssh -i C:\path\to\your-key ubuntu@YOUR_VPS_IP
cd ~/PollyMorph
cp .env.example .env
nano .env   # paste your API keys
```

### Manual commands
```bash
# Check bot status
sudo systemctl status polymorph

# View live logs
journalctl -u polymorph -f

# Restart bot
sudo systemctl restart polymorph

# Stop bot
sudo systemctl stop polymorph
```

### Fallback: Cheap Paid VPS Alternatives
If Oracle Cloud isn't available (capacity limits):

| Provider | Specs | Location | Cost |
|----------|-------|----------|------|
| Racknerd | 1 vCPU, 1.5GB, 30GB | New York | ~$11/year |
| Hetzner | 2 ARM, 4GB, 40GB | Ashburn, VA | $4.5/mo |
| Vultr | 1 vCPU, 1GB, 25GB | New Jersey | $6/mo |

### Run as systemd Service (auto-restart)
The bootstrap script already creates this, but for reference:
```bash
sudo tee /etc/systemd/system/polymorph.service << 'EOF'
[Unit]
Description=PollyMorph HFT Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/PollyMorph
ExecStart=/home/ubuntu/PollyMorph/target/release/polymorph-hft
Restart=always
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable polymorph
sudo systemctl start polymorph

# Check logs
journalctl -u polymorph -f
```

## 5. Start Trading

### Pre-flight Checklist
- [ ] `.env` file created with all 4 keys
- [ ] `config/local.toml` has correct wallet size ($22)
- [ ] Prometheus running and scraping bot metrics
- [ ] Grafana dashboard imported and showing data
- [ ] Bot starts without errors (`cargo run` or `./target/release/polymorph-hft`)
- [ ] Grafana shows WebSocket connections as UP
- [ ] Kill switch shows ARMED (not TRIGGERED)

### First Run (Test Locally First)
```powershell
# From project root
cargo run --release
```

Watch the logs for:
- `Configuration loaded` — config OK
- `WebSocket polymarket connected` — CLOB feed is live
- `Maker order submitted` — orders being placed
- `Order filled` — fills coming in

### Monitor in Grafana
Open http://localhost:3000 and watch:
- **Wallet Balance** — should stay near $22
- **Active Orders** — should be 2-4 at a time
- **Kill Switch** — must stay ARMED
- **Daily P&L** — track profitability
