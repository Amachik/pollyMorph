#!/bin/bash
# PollyMorph - Latency Test to Polymarket CLOB API (Linux)
# Usage: bash scripts/test_latency.sh

echo ""
echo "=== PollyMorph Latency Test ==="
echo "Testing from: $(hostname) ($(hostname -I | awk '{print $1}'))"
echo ""

REST_HOST="clob.polymarket.com"
WS_HOST="ws-subscriptions-clob.polymarket.com"

# DNS Resolution
echo "--- DNS Resolution ---"
echo "REST API: $(dig +short $REST_HOST | head -2 | tr '\n' ', ')"
echo "WS API:   $(dig +short $WS_HOST | head -2 | tr '\n' ', ')"
echo ""

# TCP Connect latency
echo "--- TCP Connect Latency (10 samples) ---"
TCP_TIMES=()
for i in $(seq 1 10); do
    START=$(date +%s%N)
    timeout 5 bash -c "echo > /dev/tcp/$REST_HOST/443" 2>/dev/null
    END=$(date +%s%N)
    MS=$(echo "scale=2; ($END - $START) / 1000000" | bc)
    TCP_TIMES+=($MS)
    echo "  [$i/10] TCP connect: ${MS}ms"
    sleep 0.1
done

# TCP stats
if [ ${#TCP_TIMES[@]} -gt 0 ]; then
    AVG=$(echo "${TCP_TIMES[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {printf "%.2f", sum/NR}')
    MIN=$(echo "${TCP_TIMES[@]}" | tr ' ' '\n' | sort -n | head -1)
    MAX=$(echo "${TCP_TIMES[@]}" | tr ' ' '\n' | sort -n | tail -1)
    echo "  TCP Summary: avg=${AVG}ms  min=${MIN}ms  max=${MAX}ms"
fi
echo ""

# HTTPS round-trip (curl)
echo "--- HTTPS Round-Trip (GET /time, 10 samples) ---"
HTTP_TIMES=()
for i in $(seq 1 10); do
    RESULT=$(curl -o /dev/null -s -w "%{time_total}" --connect-timeout 5 "https://clob.polymarket.com/time")
    MS=$(echo "scale=2; $RESULT * 1000" | bc)
    HTTP_TIMES+=($MS)
    echo "  [$i/10] HTTPS GET /time: ${MS}ms"
    sleep 0.2
done

if [ ${#HTTP_TIMES[@]} -gt 0 ]; then
    AVG=$(echo "${HTTP_TIMES[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {printf "%.2f", sum/NR}')
    MIN=$(echo "${HTTP_TIMES[@]}" | tr ' ' '\n' | sort -n | head -1)
    MAX=$(echo "${HTTP_TIMES[@]}" | tr ' ' '\n' | sort -n | tail -1)
    echo "  HTTPS Summary: avg=${AVG}ms  min=${MIN}ms  max=${MAX}ms"
fi
echo ""

# Ping
echo "--- ICMP Ping ---"
ping -c 5 $REST_HOST 2>/dev/null | tail -1 || echo "  Ping blocked (normal for Cloudflare)"
echo ""

echo "=== Latency Guidelines ==="
echo "  < 5ms   = Excellent (co-located VPS in us-east-1)"
echo "  5-20ms  = Great (US East coast)"
echo "  20-50ms = Good (US other regions)"
echo "  50-100ms = OK (US West / Canada)"
echo "  > 100ms = Poor (consider a closer VPS)"
echo ""
