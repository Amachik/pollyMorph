#!/usr/bin/env python3
"""
Polymarket vs Binance Price Delay Detector

Compares real-time Binance BTC/USDT price against Polymarket BTC Up/Down 1hr
market odds to detect if Polymarket is delayed.

Usage:
    pip install websockets aiohttp python-dotenv
    python tools/delay_detector.py
"""

import asyncio
import json
import time
import os
import sys
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv

# ─── Config ───────────────────────────────────────────────────────────────────

POLYMARKET_REST = "https://clob.polymarket.com"
POLYMARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
SERIES_SLUG = "btc-up-or-down-hourly"

# How often to print the comparison table (seconds)
PRINT_INTERVAL = 5.0

# ─── Data structures ─────────────────────────────────────────────────────────

@dataclass
class MarketState:
    """Tracks a single BTC Up/Down hourly market."""
    event_slug: str
    title: str
    up_token_id: str
    down_token_id: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    open_price: Optional[float] = None  # BTC price at market open
    # Best bid/ask from Polymarket orderbook
    up_best_bid: float = 0.0
    up_best_ask: float = 0.0
    down_best_bid: float = 0.0
    down_best_ask: float = 0.0
    up_last_update: float = 0.0
    down_last_update: float = 0.0

@dataclass
class State:
    """Global state shared across tasks."""
    binance_price: float = 0.0
    binance_last_update: float = 0.0
    binance_price_at_market_open: dict = field(default_factory=dict)  # event_slug -> price
    markets: dict = field(default_factory=dict)  # event_slug -> MarketState
    poly_ws_connected: bool = False
    binance_ws_connected: bool = False

state = State()

# ─── Polymarket REST: discover current BTC 1hr markets ───────────────────────

async def discover_markets(session: aiohttp.ClientSession):
    """Find active BTC Up/Down hourly markets via Gamma API (same as Rust bot)."""
    print("\n🔍 Discovering BTC Up/Down hourly markets...")

    # Step 1: GET series stubs from Gamma API
    series_url = f"https://gamma-api.polymarket.com/series?slug={SERIES_SLUG}&active=true"
    print(f"  Fetching: {series_url}")
    try:
        async with session.get(series_url) as resp:
            if resp.status != 200:
                print(f"  ⚠ Series API returned {resp.status}")
                return
            series_list = await resp.json()
    except Exception as e:
        print(f"  ⚠ Series API error: {e}")
        return

    if not series_list:
        print("  ⚠ No series found")
        return

    series = series_list[0] if isinstance(series_list, list) else series_list
    events = series.get("events", [])
    print(f"  Found {len(events)} event stubs")

    # Step 2: Filter to non-expired events
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()
    candidates = []

    for event in events:
        end_str = event.get("endDate", "")
        if end_str:
            end_dt = parse_time(end_str)
            if end_dt:
                end_utc = end_dt if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)
                if end_utc.timestamp() < now_ts - 300:
                    continue  # Ended > 5 min ago
                candidates.append((event, end_utc.timestamp()))
            else:
                candidates.append((event, float('inf')))
        else:
            candidates.append((event, float('inf')))

    # Sort by end time (soonest first = currently active)
    candidates.sort(key=lambda x: x[1])
    # Take first 10 (we only need the active + upcoming ones)
    candidates = candidates[:10]
    print(f"  {len(candidates)} candidate events after filtering")

    # Step 3: Fetch full event details for each candidate
    for event_stub, _ in candidates:
        event_id = event_stub.get("id", "")
        if not event_id:
            continue

        detail_url = f"https://gamma-api.polymarket.com/events/{event_id}"
        try:
            async with session.get(detail_url) as resp:
                if resp.status != 200:
                    print(f"  ⚠ Event {event_id}: HTTP {resp.status}")
                    continue
                event = await resp.json()
        except Exception as e:
            print(f"  ⚠ Event {event_id} fetch error: {e}")
            continue

        await parse_gamma_event(session, event)


async def parse_event(session: aiohttp.ClientSession, event: dict):
    """Parse a CLOB event into a MarketState."""
    slug = event.get("slug", event.get("event_slug", ""))
    title = event.get("title", slug)
    markets = event.get("markets", [])

    up_token = None
    down_token = None

    for m in markets:
        outcome = m.get("outcome", "").lower()
        tokens = m.get("tokens", [])
        if not tokens:
            token_id = m.get("token_id", m.get("condition_id", ""))
            if "up" in outcome:
                up_token = token_id
            elif "down" in outcome:
                down_token = token_id
        else:
            for t in tokens:
                t_outcome = t.get("outcome", "").lower()
                if "up" in t_outcome or "yes" in t_outcome:
                    up_token = t.get("token_id", "")
                elif "down" in t_outcome or "no" in t_outcome:
                    down_token = t.get("token_id", "")

    if up_token and down_token:
        # Parse times
        start = event.get("start_date") or event.get("game_start_time")
        end = event.get("end_date") or event.get("expiration_time")
        start_dt = parse_time(start) if start else None
        end_dt = parse_time(end) if end else None

        ms = MarketState(
            event_slug=slug,
            title=title,
            up_token_id=up_token,
            down_token_id=down_token,
            start_time=start_dt,
            end_time=end_dt,
        )
        state.markets[slug] = ms
        print(f"  ✅ {title}")
        print(f"     Up:   {up_token[:20]}...")
        print(f"     Down: {down_token[:20]}...")
        if start_dt:
            print(f"     Window: {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M') if end_dt else '?'} UTC")


async def parse_gamma_event(session: aiohttp.ClientSession, event: dict):
    """Parse a Gamma API event (matches Rust bot's parse_arb_market logic)."""
    slug = event.get("slug", "")
    title = event.get("title", slug)
    markets = event.get("markets", [])

    if not markets:
        print(f"  ⚠ {title}: no markets array")
        return

    # Each event has one market with two outcomes (Up/Down)
    # The Gamma API returns clobTokenIds and outcomes as JSON *strings*
    for m in markets:
        # Parse clobTokenIds — JSON string like '["id1", "id2"]'
        raw_tokens = m.get("clobTokenIds", "")
        if isinstance(raw_tokens, str):
            try:
                tokens = json.loads(raw_tokens)
            except:
                tokens = []
        elif isinstance(raw_tokens, list):
            tokens = raw_tokens
        else:
            tokens = []

        # Parse outcomes — JSON string like '["Up", "Down"]'
        raw_outcomes = m.get("outcomes", "")
        if isinstance(raw_outcomes, str):
            try:
                outcomes = json.loads(raw_outcomes)
            except:
                outcomes = []
        elif isinstance(raw_outcomes, list):
            outcomes = raw_outcomes
        else:
            outcomes = []

        if len(tokens) < 2:
            print(f"  ⚠ {title}: only {len(tokens)} tokens (need 2)")
            continue

        # Determine Up/Down token mapping
        up_token = None
        down_token = None
        if len(outcomes) >= 2:
            if outcomes[0] == "Up":
                up_token, down_token = tokens[0], tokens[1]
            else:
                up_token, down_token = tokens[1], tokens[0]
        else:
            # Default: first is Up
            up_token, down_token = tokens[0], tokens[1]

        # Parse times — market-level eventStartTime and endDate
        start_str = m.get("eventStartTime") or event.get("startTime") or event.get("startDate") or ""
        end_str = m.get("endDate") or event.get("endDate") or ""

        start_dt = parse_time(start_str)
        end_dt = parse_time(end_str)

        # Filter: only 1hr windows (2700-4500s duration)
        if start_dt and end_dt:
            s_utc = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
            e_utc = end_dt if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)
            duration = (e_utc - s_utc).total_seconds()
            if duration < 2700 or duration > 4500:
                continue  # Not a 1hr window

        ms = MarketState(
            event_slug=slug,
            title=title,
            up_token_id=up_token,
            down_token_id=down_token,
            start_time=start_dt,
            end_time=end_dt,
        )
        state.markets[slug] = ms
        time_str = ""
        if start_dt and end_dt:
            s = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
            e = end_dt if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)
            time_str = f"  Window: {s.strftime('%H:%M')} → {e.strftime('%H:%M')} UTC"
        print(f"  ✅ {title}")
        print(f"     Up:   {up_token[:30]}...")
        print(f"     Down: {down_token[:30]}...")
        if time_str:
            print(f"    {time_str}")
        break  # One market per event is enough


def parse_time(s: str) -> Optional[datetime]:
    """Parse ISO time string."""
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except:
        return None


# ─── Polymarket WebSocket: real-time orderbook ────────────────────────────────

async def polymarket_ws_task():
    """Connect to Polymarket WS and stream orderbook updates."""
    while True:
        try:
            # Collect all token IDs to subscribe
            asset_ids = []
            token_to_market = {}
            for slug, m in state.markets.items():
                asset_ids.append(m.up_token_id)
                asset_ids.append(m.down_token_id)
                token_to_market[m.up_token_id] = (slug, "up")
                token_to_market[m.down_token_id] = (slug, "down")

            if not asset_ids:
                print("⏳ No markets discovered yet, retrying in 5s...")
                await asyncio.sleep(5)
                continue

            print(f"\n📡 Connecting to Polymarket WS ({len(asset_ids)} assets)...")
            async with websockets.connect(POLYMARKET_WS, ping_interval=10) as ws:
                state.poly_ws_connected = True
                print("📡 Polymarket WS connected!")

                # Subscribe
                sub_msg = json.dumps({
                    "assets_ids": asset_ids,
                    "type": "market"
                })
                await ws.send(sub_msg)

                async for raw in ws:
                    if raw == "PONG" or not raw:
                        continue

                    try:
                        data = json.loads(raw)
                    except:
                        continue

                    msgs = data if isinstance(data, list) else [data]
                    for msg in msgs:
                        process_poly_message(msg, token_to_market)

        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            state.poly_ws_connected = False
            print(f"📡 Polymarket WS disconnected: {e}, reconnecting in 3s...")
            await asyncio.sleep(3)


def process_poly_message(msg: dict, token_to_market: dict):
    """Process a single Polymarket WS message and update best bid/ask."""
    event_type = msg.get("event_type", "")
    market_id = msg.get("asset_id", "")

    if market_id not in token_to_market:
        return

    slug, side = token_to_market[market_id]
    if slug not in state.markets:
        return

    m = state.markets[slug]
    now = time.time()

    # Extract best bid/ask from book snapshot or price update
    if event_type in ("book", "last_trade_price", "price_change", "tick_size_change"):
        pass  # handled below

    # Book snapshot
    bids = msg.get("bids", [])
    asks = msg.get("asks", [])

    best_bid = 0.0
    best_ask = 1.0

    if bids:
        # bids are [{price, size}] — highest price is best
        try:
            best_bid = max(float(b.get("price", b) if isinstance(b, dict) else b) for b in bids)
        except:
            pass

    if asks:
        try:
            best_ask = min(float(a.get("price", a) if isinstance(a, dict) else a) for a in asks)
        except:
            pass

    # Also check for "price" field (price change events)
    if "price" in msg:
        try:
            p = float(msg["price"])
            if side == "up":
                m.up_best_bid = p
                m.up_best_ask = p
                m.up_last_update = now
            else:
                m.down_best_bid = p
                m.down_best_ask = p
                m.down_last_update = now
            return
        except:
            pass

    if best_bid > 0 or best_ask < 1.0:
        if side == "up":
            if best_bid > 0:
                m.up_best_bid = best_bid
            if best_ask < 1.0:
                m.up_best_ask = best_ask
            m.up_last_update = now
        else:
            if best_bid > 0:
                m.down_best_bid = best_bid
            if best_ask < 1.0:
                m.down_best_ask = best_ask
            m.down_last_update = now


# ─── Binance WebSocket: real-time BTC/USDT price ─────────────────────────────

async def binance_ws_task():
    """Connect to Binance WS and stream BTC/USDT trades."""
    while True:
        try:
            print("\n💹 Connecting to Binance WS (BTC/USDT trades)...")
            async with websockets.connect(BINANCE_WS) as ws:
                state.binance_ws_connected = True
                print("💹 Binance WS connected!")

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        price = float(data.get("p", 0))
                        if price > 0:
                            state.binance_price = price
                            state.binance_last_update = time.time()
                    except:
                        continue

        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            state.binance_ws_connected = False
            print(f"💹 Binance WS disconnected: {e}, reconnecting in 3s...")
            await asyncio.sleep(3)


# ─── Display: compare prices and detect delay ────────────────────────────────

async def display_task():
    """Periodically print comparison table."""
    await asyncio.sleep(3)  # Let connections establish

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        now = time.time()
        now_dt = datetime.now(timezone.utc)

        # Header
        print("\n" + "=" * 100)
        print(f"  📊 POLYMARKET vs BINANCE — {now_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  💹 Binance BTC/USDT: ${state.binance_price:,.2f}"
              f"  (updated {now - state.binance_last_update:.1f}s ago)" if state.binance_last_update else "  💹 Binance: waiting...")
        print(f"  📡 Poly WS: {'✅' if state.poly_ws_connected else '❌'}  |  Binance WS: {'✅' if state.binance_ws_connected else '❌'}")
        print("=" * 100)

        if not state.markets:
            print("  No markets discovered yet...")
            continue

        if state.binance_price == 0:
            print("  Waiting for Binance price...")
            continue

        print(f"  {'Market':<45} {'Up Bid':>8} {'Up Ask':>8} {'Dn Bid':>8} {'Dn Ask':>8} {'Impl%':>7} {'Signal':>12} {'Delay':>8}")
        print("  " + "-" * 96)

        for slug, m in sorted(state.markets.items()):
            # Only show markets that are currently active or starting soon
            if m.end_time and m.end_time.replace(tzinfo=timezone.utc) < now_dt:
                continue  # expired

            # Calculate implied probability from Polymarket
            up_mid = (m.up_best_bid + m.up_best_ask) / 2 if m.up_best_bid > 0 else 0
            down_mid = (m.down_best_bid + m.down_best_ask) / 2 if m.down_best_bid > 0 else 0

            if up_mid == 0 and down_mid == 0:
                continue  # no data yet

            # Implied "Up" probability from Polymarket
            poly_up_prob = up_mid if up_mid > 0 else (1 - down_mid if down_mid > 0 else 0.5)

            # What Binance says: is BTC up or down from market open?
            # We need the open price — track it when market starts
            if m.start_time:
                start_utc = m.start_time if m.start_time.tzinfo else m.start_time.replace(tzinfo=timezone.utc)
                # If market just started and we don't have open price, record current
                if slug not in state.binance_price_at_market_open:
                    if start_utc <= now_dt:
                        state.binance_price_at_market_open[slug] = state.binance_price

            open_price = state.binance_price_at_market_open.get(slug, 0)
            if open_price > 0:
                btc_change_pct = ((state.binance_price - open_price) / open_price) * 100
                # Binance implied probability: simple model
                # If BTC is up 0.5%, "Up" should be ~70-80% likely
                # Scale: ±1% → ~90% certainty
                if btc_change_pct > 0:
                    binance_up_prob = min(0.95, 0.5 + btc_change_pct * 0.3)
                else:
                    binance_up_prob = max(0.05, 0.5 + btc_change_pct * 0.3)

                # Time remaining factor — closer to expiry = more certain
                if m.end_time:
                    end_utc = m.end_time if m.end_time.tzinfo else m.end_time.replace(tzinfo=timezone.utc)
                    remaining = (end_utc - now_dt).total_seconds()
                    total = (end_utc - start_utc).total_seconds() if m.start_time else 3600
                    time_factor = max(0.1, remaining / total) if total > 0 else 1.0
                    # Less time remaining → more confident in current direction
                    confidence_boost = (1 - time_factor) * 0.15
                    if btc_change_pct > 0:
                        binance_up_prob = min(0.98, binance_up_prob + confidence_boost)
                    else:
                        binance_up_prob = max(0.02, binance_up_prob - confidence_boost)
                    remaining_str = f"{int(remaining//60)}m{int(remaining%60):02d}s"
                else:
                    remaining_str = "?"

                # Delay = difference between what Binance implies and what Polymarket shows
                delay = binance_up_prob - poly_up_prob

                # Signal
                if abs(delay) > 0.10:
                    if delay > 0:
                        signal = f"🟢 BUY UP"
                    else:
                        signal = f"🔴 BUY DN"
                    delay_str = f"{delay:+.1%}"
                elif abs(delay) > 0.05:
                    signal = f"⚡ WATCH"
                    delay_str = f"{delay:+.1%}"
                else:
                    signal = "—"
                    delay_str = f"{delay:+.1%}"

                # Short title
                short_title = m.title[:43] if len(m.title) > 43 else m.title

                print(f"  {short_title:<45} {m.up_best_bid:>8.3f} {m.up_best_ask:>8.3f} "
                      f"{m.down_best_bid:>8.3f} {m.down_best_ask:>8.3f} "
                      f"{btc_change_pct:>+6.2f}% "
                      f"{signal:>12} {delay_str:>8}")
            else:
                short_title = m.title[:43] if len(m.title) > 43 else m.title
                print(f"  {short_title:<45} {m.up_best_bid:>8.3f} {m.up_best_ask:>8.3f} "
                      f"{m.down_best_bid:>8.3f} {m.down_best_ask:>8.3f} "
                      f"{'?':>7} {'waiting':>12} {'—':>8}")

        # Summary
        print("  " + "-" * 96)
        print(f"  Legend: Impl% = BTC change from market open | Signal = Polymarket lags Binance by >10%")
        print(f"  🟢 BUY UP = Polymarket underprices 'Up' | 🔴 BUY DN = Polymarket underprices 'Down'")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    load_dotenv()

    print("=" * 60)
    print("  🔬 Polymarket vs Binance Delay Detector")
    print("  Target: BTC Up/Down 1hr markets")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        await discover_markets(session)

    if not state.markets:
        print("\n❌ No active BTC Up/Down hourly markets found!")
        print("   This could mean all current markets are closed.")
        print("   Try again when a new hourly window opens.")
        sys.exit(1)

    print(f"\n✅ Tracking {len(state.markets)} markets")

    # Run all tasks concurrently
    await asyncio.gather(
        polymarket_ws_task(),
        binance_ws_task(),
        display_task(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Stopped.")
