# Weather Bot — VPS Setup Guide

## Requirements
- Ubuntu 22.04+ (or any Linux with systemd)
- Python 3.10+ (`python3 --version`)
- 512MB RAM minimum, 1GB recommended
- ~500MB disk for data logs

---

## 1. Upload the project

```bash
# From your Mac:
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='data/' \
  /Users/hiyori/Documents/pollyMorph/ ubuntu@YOUR_VPS_IP:~/pollyMorph/
```

---

## 2. Install Python dependencies

```bash
ssh ubuntu@YOUR_VPS_IP

cd ~/pollyMorph

# Install Python venv if needed
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv python3-dev

# CRITICAL on Ubuntu: install tzdata for zoneinfo (Python 3.9+ stdlib)
sudo apt-get install -y tzdata

# Create virtualenv
python3 -m venv venv
source venv/bin/activate

# Install all dependencies
pip install -r tools/requirements.txt
```

---

## 3. Create the .env file

```bash
cat > ~/pollyMorph/.env << 'EOF'
POLYMARKET_PRIVATE_KEY=your_private_key_here
POLYMARKET_API_KEY=your_api_key_here
POLYMARKET_API_SECRET=your_api_secret_here
POLYMARKET_API_PASSPHRASE=your_passphrase_here
POLYMARKET_PROXY_ADDRESS=0x9b1c285cC6aA3C40844D48789807cE239D8f0e82
EOF

# Lock down permissions — only owner can read
chmod 600 ~/pollyMorph/.env
```

---

## 4. Create log directory

```bash
mkdir -p ~/pollyMorph/logs
```

---

## 5. Test the bot (dry run)

```bash
cd ~/pollyMorph
source venv/bin/activate

# Single dry-run scan — should complete without errors
python3 -m tools.weather.executor --dry-run --bankroll 164

# Check today's schedule
python3 -m tools.weather.executor --schedule
```

---

## 6. Install systemd service

```bash
# Copy service file (edit User= and paths if your username isn't 'ubuntu')
sudo cp ~/pollyMorph/weather-bot.service /etc/systemd/system/

# If your username is NOT ubuntu, edit the service file first:
# sudo nano /etc/systemd/system/weather-bot.service
# Change: User=ubuntu → User=YOUR_USERNAME
# Change: /home/ubuntu/ → /home/YOUR_USERNAME/

sudo systemctl daemon-reload
sudo systemctl enable weather-bot    # start on boot
sudo systemctl start weather-bot
```

---

## 7. Verify it's running

```bash
# Check status
sudo systemctl status weather-bot

# Watch live logs
tail -f ~/pollyMorph/logs/weather-bot.log

# Check next scan time
grep "Next scan" ~/pollyMorph/logs/weather-bot.log | tail -5
```

---

## 8. Log rotation (prevent disk fill)

```bash
sudo tee /etc/logrotate.d/weather-bot << 'EOF'
/home/ubuntu/pollyMorph/logs/weather-bot.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    copytruncate
}
EOF
```

---

## Common commands

```bash
# Stop the bot
sudo systemctl stop weather-bot

# Restart after code update
sudo systemctl restart weather-bot

# View last 100 log lines
tail -100 ~/pollyMorph/logs/weather-bot.log

# Force a MOS rebuild on next start
sudo systemctl stop weather-bot
# Then start with rebuild flag (one-off):
cd ~/pollyMorph && source venv/bin/activate
python3 -m tools.weather.executor --dry-run --rebuild-mos
sudo systemctl start weather-bot

# Check positions
cat ~/pollyMorph/weather_positions.json | python3 -m json.tool | grep -E "status|city_key|bucket"
```

---

## Updating the code

```bash
# From your Mac, sync changes (excludes data/ to avoid overwriting logs):
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='data/' \
  --exclude='weather_positions.json' --exclude='weather_mos.json' \
  --exclude='weather_orders.json' --exclude='weather_calibration.json' \
  --exclude='logs/' \
  /Users/hiyori/Documents/pollyMorph/ ubuntu@YOUR_VPS_IP:~/pollyMorph/

# Then restart:
ssh ubuntu@YOUR_VPS_IP 'sudo systemctl restart weather-bot'
```

---

## Troubleshooting

**Bot won't start — "No module named X"**
```bash
source ~/pollyMorph/venv/bin/activate
pip install -r ~/pollyMorph/tools/requirements.txt
```

**zoneinfo errors on Ubuntu 20.04**
```bash
sudo apt-get install -y tzdata
pip install tzdata  # Python fallback package
```

**"not enough balance" on live orders**
- The CLOB balance is separate from your on-chain wallet
- Deposit USDC at polymarket.com → your account → Deposit

**Positions file corrupted**
```bash
# The bot uses atomic writes (tmp+rename) so corruption is rare
# If it happens, the .tmp file has the last good state:
cp ~/pollyMorph/weather_positions.tmp ~/pollyMorph/weather_positions.json
```
