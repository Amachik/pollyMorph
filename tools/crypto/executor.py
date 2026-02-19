#!/usr/bin/env python3
"""
BTC 5-minute Up/Down mean-reversion executor.

Runs in a tight loop (every ~15s), scans active markets, applies the
mean-reversion strategy, and paper-trades (dry-run) or places real orders.

Dry-run mode is 1:1 with live: uses real Polymarket order books, real prices,
real market data. Only difference is no actual order submission.

Usage:
    python -m tools.crypto.executor --dry-run --bankroll 100 --loop 15
    python -m tools.crypto.executor --live --bankroll 50 --loop 15
"""

import asyncio
import argparse
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

from .scanner import fetch_active_markets, BtcMarket, seconds_to_resolution, seconds_into_window, is_window_active
from .strategy import evaluate_market, TradeSignal

# ─── Configuration ────────────────────────────────────────────────────────────

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
SCAN_INTERVAL_SECONDS = 15
POSITIONS_FILE = Path(__file__).parent.parent.parent / "crypto_positions.json"
ORDERS_LOG_FILE = Path(__file__).parent.parent.parent / "crypto_orders.json"

# Fee constants (from market data)
TAKER_FEE_BPS = 1000   # 100bps = 1%
MAKER_FEE_BPS = 0      # 0% effective (100% rebate on maker)


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class CryptoPosition:
    event_slug: str
    question: str
    side: str               # "Up" or "Down"
    token_id: str
    entry_price: float
    size_usdc: float        # USDC spent
    tokens: float           # tokens received
    order_id: str
    placed_at: str
    end_time: str           # ISO — when market resolves
    status: str = "open"    # open, won, lost, expired
    pnl: float = 0.0


@dataclass
class OrderRecord:
    order_id: str
    event_slug: str
    question: str
    side: str
    token_id: str
    entry_price: float
    size_usdc: float
    tokens: float
    edge: float
    signal_strength: float
    deviation: float
    secs_elapsed: float
    secs_remaining: float
    timestamp: str
    status: str = "placed"
    dry_run: bool = True
    pnl: float = 0.0


# ─── Persistence ─────────────────────────────────────────────────────────────

def load_positions() -> Dict[str, CryptoPosition]:
    if not POSITIONS_FILE.exists():
        return {}
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
        positions = {}
        now = datetime.now(timezone.utc)
        for k, v in data.items():
            pos = CryptoPosition(**v)
            # Auto-expire resolved markets
            if pos.status == "open":
                end = datetime.fromisoformat(pos.end_time)
                if end <= now:
                    pos.status = "expired"
            positions[k] = pos
        return positions
    except Exception as e:
        print(f"  ⚠️  Error loading positions: {e}")
        return {}


def save_positions(positions: Dict[str, CryptoPosition]):
    try:
        tmp = POSITIONS_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump({k: asdict(v) for k, v in positions.items()}, f, indent=2)
        tmp.replace(POSITIONS_FILE)
    except Exception as e:
        print(f"  ⚠️  Error saving positions: {e}")


def log_order(record: OrderRecord):
    try:
        orders = []
        if ORDERS_LOG_FILE.exists():
            with open(ORDERS_LOG_FILE) as f:
                orders = json.load(f)
        orders.append(asdict(record))
        with open(ORDERS_LOG_FILE, "w") as f:
            json.dump(orders, f, indent=2)
    except Exception as e:
        print(f"  ⚠️  Error logging order: {e}")


def deployed_capital(positions: Dict[str, CryptoPosition]) -> float:
    return sum(p.size_usdc for p in positions.values() if p.status == "open")


# ─── CLOB Client ─────────────────────────────────────────────────────────────

def create_clob_client():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        load_dotenv()
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        if not private_key:
            return None
        api_key = os.getenv("POLYMARKET_API_KEY")
        api_secret = os.getenv("POLYMARKET_API_SECRET")
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE")
        funder = os.getenv("POLYMARKET_PROXY_ADDRESS")
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase) \
            if all([api_key, api_secret, api_passphrase]) else None
        return ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            creds=creds,
            signature_type=1 if funder else None,
            funder=funder,
        )
    except Exception as e:
        print(f"  ❌ CLOB client error: {e}")
        return None


# ─── Order Placement ─────────────────────────────────────────────────────────

def place_order(
    client,
    signal: TradeSignal,
    market: BtcMarket,
    dry_run: bool,
) -> Optional[OrderRecord]:
    """Place a maker limit order (or simulate in dry-run)."""
    tokens = round(signal.bet_size / signal.entry_price, 2)
    if tokens < 1.0:
        print(f"    ⚠️  Too few tokens ({tokens:.2f}) — skipping")
        return None

    record = OrderRecord(
        order_id="",
        event_slug=market.event_slug,
        question=market.question,
        side=signal.side,
        token_id=signal.token_id,
        entry_price=signal.entry_price,
        size_usdc=signal.bet_size,
        tokens=tokens,
        edge=signal.edge,
        signal_strength=signal.signal_strength,
        deviation=signal.deviation,
        secs_elapsed=signal.secs_elapsed,
        secs_remaining=signal.secs_remaining,
        timestamp=datetime.now(timezone.utc).isoformat(),
        dry_run=dry_run,
    )

    if dry_run:
        record.order_id = f"DRY-{int(time.time())}-{signal.token_id[:8]}"
        record.status = "dry_run"
        print(f"    🏷️  [DRY RUN] BUY {signal.side} {tokens:.1f} tokens @ "
              f"${signal.entry_price:.2f} (${signal.bet_size:.1f} USDC) "
              f"edge={signal.edge:.3f}")
        return record

    # Live order — post GTC limit (maker, 0% fee)
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        order_args = OrderArgs(
            token_id=signal.token_id,
            price=signal.entry_price,
            size=tokens,
            side="BUY",
            fee_rate_bps=MAKER_FEE_BPS,
        )
        signed = client.create_order(order_args)
        response = client.post_order(signed, orderType=OrderType.GTC)
        if response and isinstance(response, dict):
            record.order_id = response.get("orderID", response.get("id", "unknown"))
            record.status = "placed"
            print(f"    ✅ ORDER PLACED: {record.order_id}")
            print(f"       BUY {signal.side} {tokens:.1f} tokens @ "
                  f"${signal.entry_price:.2f} (${signal.bet_size:.1f} USDC)")
        else:
            record.order_id = str(response) if response else "error"
            record.status = "failed"
            print(f"    ⚠️  Unexpected response: {response}")
    except Exception as e:
        record.status = "failed"
        record.order_id = f"FAILED-{int(time.time())}"
        print(f"    ❌ Order FAILED: {e}")

    return record


# ─── P&L Tracking ────────────────────────────────────────────────────────────

async def check_resolved_positions(
    session: aiohttp.ClientSession,
    positions: Dict[str, CryptoPosition],
) -> float:
    """
    Check expired positions against resolved market outcomes.
    Returns total realized P&L from newly resolved positions.
    """
    realized_pnl = 0.0
    for key, pos in positions.items():
        if pos.status != "expired":
            continue

        # Fetch resolved market to get outcome
        url = f"https://gamma-api.polymarket.com/events?slug={pos.event_slug}"
        try:
            async with session.get(url) as resp:
                events = await resp.json()
            if not events:
                continue
            mkt_list = events[0].get("markets", [])
            if not mkt_list:
                continue
            mkt = mkt_list[0]

            # Check if resolved
            outcome_prices = json.loads(mkt.get("outcomePrices", "[0.5,0.5]"))
            outcomes = json.loads(mkt.get("outcomes", '["Up","Down"]'))

            # Find our side's resolution price (1.0 = won, 0.0 = lost)
            if pos.side in outcomes:
                idx = outcomes.index(pos.side)
                resolution = float(outcome_prices[idx])
                if resolution >= 0.99:
                    pos.status = "won"
                    pos.pnl = round(pos.tokens * 1.0 - pos.size_usdc, 4)
                elif resolution <= 0.01:
                    pos.status = "lost"
                    pos.pnl = -pos.size_usdc
                else:
                    continue  # not yet resolved

                realized_pnl += pos.pnl
                icon = "✅" if pos.status == "won" else "❌"
                print(f"  {icon} RESOLVED: {pos.question[:50]}...")
                print(f"     Side={pos.side} | P&L=${pos.pnl:+.2f}")
        except Exception as e:
            pass  # silently skip — market may not be resolved yet

    return realized_pnl


# ─── Main Scan Loop ───────────────────────────────────────────────────────────

async def run_scan(
    bankroll: float,
    dry_run: bool,
    positions: Dict[str, CryptoPosition],
    client,
    session: aiohttp.ClientSession,
    verbose: bool = False,
) -> Dict[str, CryptoPosition]:
    """Single scan: fetch markets, evaluate signals, place orders."""
    mode = "🏷️  DRY RUN" if dry_run else "💰 LIVE"
    print(f"\n{'─'*70}")
    print(f"  ₿  BTC 5M MEAN REVERSION — {mode}")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Check for resolved positions
    realized = await check_resolved_positions(session, positions)
    if realized != 0.0:
        print(f"  💵 Realized P&L this scan: ${realized:+.2f}")
        save_positions(positions)

    # Stats
    open_pos = {k: p for k, p in positions.items() if p.status == "open"}
    total_pnl = sum(p.pnl for p in positions.values() if p.status in ("won", "lost"))
    deployed = deployed_capital(positions)
    available = bankroll - deployed

    print(f"  Open positions: {len(open_pos)} (${deployed:.1f} deployed)")
    print(f"  Realized P&L: ${total_pnl:+.2f} | Available: ${available:.1f}")

    if available < 5.0:
        print(f"  ⚠️  Insufficient bankroll (${available:.1f} < $5 min)")
        return positions

    # Fetch active markets (current window + 3 upcoming)
    markets = await fetch_active_markets(session, limit=4)
    if not markets:
        print("  📊 No active markets found")
        return positions

    if verbose:
        active_now = [m for m in markets if is_window_active(m)]
        print(f"\n  Found {len(markets)} markets ({len(active_now)} active now):")
        for m in markets:
            secs = seconds_to_resolution(m)
            elapsed = seconds_into_window(m)
            dev = abs(m.mid_up - 0.50)
            status = "▶ LIVE" if is_window_active(m) else "  next"
            print(f"    {status} {m.question[-40:]:<40} "
                  f"mid={m.mid_up:.3f} dev={dev:.3f} "
                  f"elapsed={elapsed:.0f}s rem={secs:.0f}s")

    # Evaluate signals
    signals_found = 0
    orders_placed = 0

    for market in markets:
        # Skip if already have open position in this market
        if market.event_slug in positions and positions[market.event_slug].status == "open":
            if verbose:
                print(f"  ⏭️  Already positioned: {market.event_slug}")
            continue

        signal = evaluate_market(market, available)
        if signal is None:
            continue

        signals_found += 1
        print(f"\n  📍 SIGNAL: {market.question[-55:]}")
        print(f"     {signal.reason}")
        print(f"     Bet: ${signal.bet_size:.1f} USDC | "
              f"Signal={signal.signal_strength:.3f} | "
              f"Liq=${market.liquidity:.0f}")

        record = place_order(client, signal, market, dry_run)
        if record is None:
            continue

        # Track position (dry-run counts too for bankroll management)
        if record.status in ("placed", "dry_run"):
            positions[market.event_slug] = CryptoPosition(
                event_slug=market.event_slug,
                question=market.question,
                side=signal.side,
                token_id=signal.token_id,
                entry_price=signal.entry_price,
                size_usdc=signal.bet_size,
                tokens=record.tokens,
                order_id=record.order_id,
                placed_at=record.timestamp,
                end_time=market.end_time.isoformat(),
                status="open",
            )
            available -= signal.bet_size
            orders_placed += 1
            log_order(record)

        if available < 5.0:
            break

    if signals_found == 0:
        print("  📊 No signals this scan (no qualifying deviations)")

    save_positions(positions)
    return positions


# ─── Entry Point ─────────────────────────────────────────────────────────────

async def run_loop(
    bankroll: float,
    dry_run: bool,
    interval: int,
    verbose: bool,
):
    mode = "DRY RUN" if dry_run else "LIVE TRADING"
    print(f"\n🔄 BTC 5m Mean Reversion Bot — {mode}")
    print(f"   Bankroll: ${bankroll:.2f} | Scan every {interval}s")
    print(f"   Press Ctrl+C to stop.\n")

    client = None
    if not dry_run:
        client = create_clob_client()
        if not client:
            print("❌ Cannot create CLOB client. Check .env")
            return

    positions = load_positions()

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                positions = await run_scan(
                    bankroll=bankroll,
                    dry_run=dry_run,
                    positions=positions,
                    client=client,
                    session=session,
                    verbose=verbose,
                )
            except Exception as e:
                print(f"\n❌ Scan error: {e}")
                import traceback
                traceback.print_exc()

            print(f"\n⏳ Next scan in {interval}s...")
            await asyncio.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="BTC 5m Mean Reversion Bot")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--bankroll", type=float, default=100.0)
    parser.add_argument("--loop", type=int, default=SCAN_INTERVAL_SECONDS)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    dry_run = not args.live

    if not dry_run:
        print("\n⚠️  LIVE TRADING MODE — Real orders will be placed!")
        print("    Press Ctrl+C within 5 seconds to cancel...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n❌ Cancelled.")
            return

    try:
        asyncio.run(run_loop(
            bankroll=args.bankroll,
            dry_run=dry_run,
            interval=args.loop,
            verbose=args.verbose,
        ))
    except KeyboardInterrupt:
        print("\n\n👋 Bot stopped.")


if __name__ == "__main__":
    main()
