#!/usr/bin/env bash
# measure_latency.sh — Measure RTT from this host to Polymarket infrastructure
# Run this on the VPS: bash scripts/measure_latency.sh

set -euo pipefail

CLOB_REST="clob.polymarket.com"
CLOB_WS="ws-subscriptions-clob.polymarket.com"
GAMMA_API="gamma-api.polymarket.com"
POLYGON_RPC="polygon-rpc.com"
VIRGINIA_REF="ec2.us-east-1.amazonaws.com"  # reference: where Polymarket likely runs

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BOLD}=== PollyMorph Latency Benchmark ===${NC}"
echo "Host: $(hostname) | $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

# ── 1. ICMP ping (raw network RTT) ──────────────────────────────────────────
ping_host() {
    local host=$1
    local label=$2
    local result
    result=$(ping -c 10 -q "$host" 2>/dev/null | tail -1 | awk -F'/' '{print $5}')
    if [ -z "$result" ]; then
        echo -e "  ${RED}FAILED${NC}  $label ($host)"
    else
        local ms
        ms=$(echo "$result" | cut -d'.' -f1)
        if   [ "$ms" -lt 20 ];  then color=$GREEN
        elif [ "$ms" -lt 80 ];  then color=$YELLOW
        else                         color=$RED
        fi
        echo -e "  ${color}${result}ms avg${NC}  $label ($host)"
    fi
}

echo -e "${BOLD}── ICMP Ping (10 packets) ──────────────────────────────────────${NC}"
ping_host "$CLOB_REST"    "Polymarket CLOB REST"
ping_host "$CLOB_WS"      "Polymarket CLOB WebSocket"
ping_host "$GAMMA_API"    "Polymarket Gamma API"
ping_host "$POLYGON_RPC"  "Polygon RPC"
ping_host "$VIRGINIA_REF" "AWS us-east-1 (reference)"
echo ""

# ── 2. HTTP/S time-to-first-byte (TTFB) ─────────────────────────────────────
ttfb() {
    local url=$1
    local label=$2
    local result
    result=$(curl -o /dev/null -s -w "%{time_connect},%{time_starttransfer},%{time_total}" \
        --max-time 5 "$url" 2>/dev/null)
    if [ -z "$result" ]; then
        echo -e "  ${RED}FAILED${NC}  $label"
        return
    fi
    local connect ttfb total
    connect=$(echo "$result" | cut -d',' -f1 | awk '{printf "%.0f", $1*1000}')
    ttfb=$(echo "$result"    | cut -d',' -f2 | awk '{printf "%.0f", $1*1000}')
    total=$(echo "$result"   | cut -d',' -f3 | awk '{printf "%.0f", $1*1000}')
    if   [ "$ttfb" -lt 50 ];  then color=$GREEN
    elif [ "$ttfb" -lt 150 ]; then color=$YELLOW
    else                           color=$RED
    fi
    echo -e "  ${color}connect=${connect}ms  TTFB=${ttfb}ms  total=${total}ms${NC}  $label"
}

echo -e "${BOLD}── HTTPS TTFB (single request) ─────────────────────────────────${NC}"
ttfb "https://clob.polymarket.com/markets?limit=1"       "CLOB REST /markets"
ttfb "https://gamma-api.polymarket.com/markets?limit=1"  "Gamma API /markets"
ttfb "https://clob.polymarket.com/book?token_id=test"    "CLOB REST /book"
echo ""

# ── 3. WebSocket handshake time ──────────────────────────────────────────────
echo -e "${BOLD}── WebSocket Handshake Time ────────────────────────────────────${NC}"
if command -v wscat &>/dev/null; then
    for i in 1 2 3; do
        start=$(date +%s%3N)
        timeout 3 wscat -c "wss://ws-subscriptions-clob.polymarket.com/ws/market" \
            --no-color -x '{"assets_ids":[],"type":"market"}' 2>/dev/null | head -1 >/dev/null || true
        end=$(date +%s%3N)
        echo "  Run $i: $((end - start))ms to first WS message"
    done
else
    # Fallback: measure TCP+TLS handshake via curl
    result=$(curl -o /dev/null -s -w "%{time_connect},%{time_appconnect}" \
        --max-time 5 "https://ws-subscriptions-clob.polymarket.com" 2>/dev/null || echo "0,0")
    tcp=$(echo "$result"  | cut -d',' -f1 | awk '{printf "%.0f", $1*1000}')
    tls=$(echo "$result"  | cut -d',' -f2 | awk '{printf "%.0f", $1*1000}')
    echo "  TCP handshake: ${tcp}ms | TLS handshake: ${tls}ms"
    echo "  (install wscat for full WS timing: npm install -g wscat)"
fi
echo ""

# ── 4. Order submission round-trip estimate ──────────────────────────────────
echo -e "${BOLD}── Order Submission RTT Estimate ───────────────────────────────${NC}"
echo "  (POST /order — unauthenticated, measures network RTT only)"
result=$(curl -o /dev/null -s -w "%{time_connect},%{time_starttransfer}" \
    -X POST "https://clob.polymarket.com/order" \
    -H "Content-Type: application/json" \
    -d '{}' --max-time 5 2>/dev/null || echo "0,0")
connect=$(echo "$result" | cut -d',' -f1 | awk '{printf "%.0f", $1*1000}')
ttfb=$(echo "$result"    | cut -d',' -f2 | awk '{printf "%.0f", $1*1000}')
echo "  TCP connect: ${connect}ms | Server response: ${ttfb}ms"
echo ""

# ── 5. Summary ───────────────────────────────────────────────────────────────
echo -e "${BOLD}── Interpretation ──────────────────────────────────────────────${NC}"
echo "  < 20ms ping  → co-located in us-east-1 (ideal)"
echo "  20-50ms ping → same region, different AZ"
echo "  50-100ms     → US but different region"
echo "  > 100ms      → Europe/Asia (significant disadvantage)"
echo ""
echo "  RetamzZ likely sees: ~5ms ping, ~15ms order RTT"
echo "  Your order submission RTT = ping × 2 + server processing (~5ms)"
echo ""
echo -e "${BOLD}=== Done ===${NC}"
