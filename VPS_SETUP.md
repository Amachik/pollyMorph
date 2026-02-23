# Weather Bot — VPS Setup Guide

## Requirements
- Ubuntu 22.04+ (or any Linux with systemd)
- Python 3.10+ (`python3 --version`)
- 512MB RAM minimum, 1GB recommended
- ~500MB disk for data logs

---

## 1. Upload the project

```bash
# From your Mac (delete stale test data first):
rm /Users/hiyori/Documents/pollyMorph/weather_orders.json
rm /Users/hiyori/Documents/pollyMorph/weather_positions.json

# Then sync to VPS:
rsync -av --exclude='.git' --exclude='__pycache__' \
  /Users/hiyori/Documents/pollyMorph/ root@YOUR_VPS_IP:~/PollyMorph/
```

---

## 2. Install Python dependencies

```bash
ssh root@YOUR_VPS_IP

cd ~/PollyMorph

# Install system dependencies
apt-get update
apt-get install -y python3-pip python3-dev

# CRITICAL: install tzdata for zoneinfo (Python 3.9+ stdlib)
apt-get install -y tzdata

# Install all Python dependencies (system-wide as root)
pip install -r tools/requirements.txt
```

---

## 3. Create the .env file

```bash
cat > ~/PollyMorph/.env << 'EOF'
POLYMARKET_PRIVATE_KEY=your_private_key_here
POLYMARKET_API_KEY=your_api_key_here
POLYMARKET_API_SECRET=your_api_secret_here
POLYMARKET_API_PASSPHRASE=your_passphrase_here
POLYMARKET_PROXY_ADDRESS=0x9b1c285cC6aA3C40844D48789807cE239D8f0e82
EOF

# Lock down permissions — only owner can read
chmod 600 ~/PollyMorph/.env
```

---

## 4. Create log directory

```bash
mkdir -p ~/PollyMorph/logs
```

---

## 5. Test the bot (dry run)

```bash
cd /root/PollyMorph

# Check today's scheduled scan times
python3 -m tools.weather.executor --schedule

# Single dry-run scan — should complete without errors
python3 -m tools.weather.executor --dry-run --bankroll 164

# Verbose dry-run — shows all markets including ones without edge
python3 -m tools.weather.executor --dry-run --bankroll 164 --verbose
```

---

## 6. Install systemd service

```bash
# Copy service file (already configured with --probe for first 2 weeks)
cp /root/PollyMorph/weather-bot.service /etc/systemd/system/

systemctl daemon-reload
systemctl enable weather-bot    # start on boot
systemctl start weather-bot
```

---

## 7. Verify it's running

```bash
# Check status
systemctl status weather-bot

# Watch live logs
tail -f ~/PollyMorph/logs/weather-bot.log

# Check next scan time
grep "Next scan" ~/PollyMorph/logs/weather-bot.log | tail -5

# Check fill rate (after 1+ week of live orders)
cd ~/PollyMorph && python3 -m tools.weather.executor --fill-report

# Check open positions
cat ~/PollyMorph/weather_positions.json | python3 -m json.tool | grep -E "status|city_key|bucket_label"
```

---

## 8. Log rotation (prevent disk fill)

```bash
tee /etc/logrotate.d/weather-bot << 'EOF'
/root/PollyMorph/logs/weather-bot.log {
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
systemctl stop weather-bot

# Restart after code update
systemctl restart weather-bot

# View last 100 log lines
tail -100 ~/PollyMorph/logs/weather-bot.log

# Print today's scan schedule
cd ~/PollyMorph && python3 -m tools.weather.executor --schedule

# Check fill rates
cd ~/PollyMorph && python3 -m tools.weather.executor --fill-report

# Force a MOS rebuild (one-off dry run, then restart service)
systemctl stop weather-bot
cd ~/PollyMorph && python3 -m tools.weather.executor --dry-run --rebuild-mos
systemctl start weather-bot

# After 2 weeks: switch from probe to full Kelly sizing
# Edit service file, remove --probe from ExecStart:
nano /etc/systemd/system/weather-bot.service
systemctl daemon-reload && systemctl restart weather-bot

# Check positions
cat ~/PollyMorph/weather_positions.json | python3 -m json.tool | grep -E "status|city_key|bucket_label"
```

---

## Updating the code

```bash
# From your Mac — excludes data files that should not be overwritten on VPS:
rsync -av --exclude='.git' --exclude='__pycache__' \
  --exclude='weather_positions.json' --exclude='weather_mos.json' \
  --exclude='weather_orders.json' --exclude='weather_calibration.json' \
  --exclude='data/' --exclude='logs/' \
  /Users/hiyori/Documents/pollyMorph/ root@YOUR_VPS_IP:~/PollyMorph/

# Then restart:
ssh root@YOUR_VPS_IP 'systemctl restart weather-bot'
```

---

## Troubleshooting

**Bot won't start — "No module named X"**
```bash
pip install -r ~/PollyMorph/tools/requirements.txt
```

**zoneinfo errors**
```bash
apt-get install -y tzdata
pip install tzdata
```

**"not enough balance" on live orders**
- Deposit USDC at polymarket.com → your account → Deposit (Polygon network)
- The CLOB balance is separate from your on-chain wallet

**Check fill rate after going live**
```bash
cd ~/PollyMorph && python3 -m tools.weather.executor --fill-report
# If fill rate < 50%: orders priced too far from ask — investigate place_order() offset
# If fill rate > 90%: pricing is working correctly
```

**Positions file corrupted**
```bash
# The bot uses atomic writes (tmp+rename) so corruption is rare
# If it happens, the .tmp file has the last good state:
cp ~/PollyMorph/weather_positions.tmp ~/PollyMorph/weather_positions.json
```
