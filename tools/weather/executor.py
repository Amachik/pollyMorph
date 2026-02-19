#!/usr/bin/env python3
"""
Weather Bot Execution Module — Places orders on Polymarket CLOB.

Integrates with the weather scanner to automatically place bets on
mispriced weather outcomes. Supports dry-run mode for testing.

Usage:
    python -m tools.weather.executor [--dry-run] [--bankroll 164] [--loop 30]
"""

import asyncio
import json
import os
import sys
import argparse
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    OrderArgs,
    OrderType,
    AssetType,
    BalanceAllowanceParams,
)

from .scanner import run_scanner, calculate_kelly_bet, expected_value
from .config import CITIES, MIN_EDGE, MAX_EDGE, KELLY_FRACTION, MAX_BET_USDC
from .markets import WeatherMarket, WeatherOutcome
from .mos import load_mos_cache, build_all_mos, save_mos_cache, MOS_FRESHNESS_HOURS

import aiohttp

# ─── Configuration ────────────────────────────────────────────────────────────

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

# Order placement settings
ORDER_TYPE = OrderType.GTC        # Good-Till-Cancelled
MIN_ORDER_SIZE = 1.0              # Minimum order size in USDC
MAX_ORDER_SIZE = MAX_BET_USDC     # Maximum order size from config
FEE_RATE_BPS = 200                # Conservative taker fee estimate (2%)
MAX_PRICE_CAP = 0.65              # Never pay more than 65¢ (preserves edge on tail bets)

# Position tracking
POSITIONS_FILE = Path(__file__).parent.parent.parent / "weather_positions.json"
ORDERS_LOG_FILE = Path(__file__).parent.parent.parent / "weather_orders.json"

# Timing
SCAN_INTERVAL_MINUTES = 30        # How often to scan for new opportunities
ORDER_COOLDOWN_SECONDS = 5        # Delay between placing orders


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class WeatherPosition:
    """Tracks a position in a weather market outcome."""
    city_key: str
    target_date: str           # ISO date
    bucket_label: str
    token_id: str
    market_id: str
    avg_price: float           # Average entry price
    size: float                # Total size in USDC
    tokens: float              # Number of Yes tokens held
    order_ids: List[str] = field(default_factory=list)
    created_at: str = ""
    status: str = "open"       # open, won, lost, expired

@dataclass
class OrderRecord:
    """Record of a placed order."""
    order_id: str
    city_key: str
    target_date: str
    bucket_label: str
    token_id: str
    side: str                  # BUY
    price: float
    size: float                # USDC
    edge: float
    forecast_prob: float
    market_prob: float
    timestamp: str
    status: str = "placed"     # placed, filled, cancelled, failed
    dry_run: bool = False


# ─── CLOB Client ─────────────────────────────────────────────────────────────

def create_clob_client() -> Optional[ClobClient]:
    """Create an authenticated CLOB client from environment variables."""
    load_dotenv()

    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        print("❌ POLYMARKET_PRIVATE_KEY not set in .env")
        return None

    api_key = os.getenv("POLYMARKET_API_KEY")
    api_secret = os.getenv("POLYMARKET_API_SECRET")
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE")

    if not all([api_key, api_secret, api_passphrase]):
        print("⚠️  API credentials incomplete — using Level 1 auth only")
        return ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
        )

    creds = ApiCreds(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )

    # Polymarket proxy wallet address (Magic.link account)
    funder = os.getenv("POLYMARKET_PROXY_ADDRESS")

    return ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=private_key,
        creds=creds,
        signature_type=1 if funder else None,  # POLY_PROXY=1
        funder=funder,
    )


# ─── Position Management ─────────────────────────────────────────────────────

def load_positions() -> Dict[str, WeatherPosition]:
    """Load tracked positions from disk, auto-expire old ones."""
    if not POSITIONS_FILE.exists():
        return {}
    try:
        with open(POSITIONS_FILE, "r") as f:
            data = json.load(f)
        positions = {k: WeatherPosition(**v) for k, v in data.items()}
    except Exception as e:
        print(f"⚠️  Error loading positions: {e}")
        return {}

    # Auto-expire positions for resolved markets (target_date < today)
    today = date.today()
    changed = False
    for key, pos in positions.items():
        if pos.status == "open":
            try:
                td = date.fromisoformat(pos.target_date)
                if td < today:
                    pos.status = "expired"
                    changed = True
            except ValueError:
                pass
    if changed:
        save_positions(positions)

    return positions


def deployed_capital(positions: Dict[str, WeatherPosition]) -> float:
    """Sum of capital in open positions."""
    return sum(p.size for p in positions.values() if p.status == "open")


def save_positions(positions: Dict[str, WeatherPosition]):
    """Save positions to disk."""
    try:
        data = {k: asdict(v) for k, v in positions.items()}
        with open(POSITIONS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error saving positions: {e}")


def log_order(record: OrderRecord):
    """Append an order record to the orders log."""
    try:
        orders = []
        if ORDERS_LOG_FILE.exists():
            with open(ORDERS_LOG_FILE, "r") as f:
                orders = json.load(f)
        orders.append(asdict(record))
        with open(ORDERS_LOG_FILE, "w") as f:
            json.dump(orders, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error logging order: {e}")


def position_key(city_key: str, target_date: str, bucket_label: str) -> str:
    """Generate a unique key for a position."""
    return f"{city_key}:{target_date}:{bucket_label}"


# ─── Order Execution ─────────────────────────────────────────────────────────

def get_best_ask(client: ClobClient, token_id: str) -> Optional[float]:
    """Query the order book and return the best (lowest) ask price."""
    try:
        book = client.get_order_book(token_id)
        if book and book.asks:
            # asks are sorted by price ascending; first = best ask
            return float(book.asks[0].price)
    except Exception as e:
        print(f"    ⚠️  Could not fetch order book: {e}")
    return None


def place_order(
    client: ClobClient,
    outcome: WeatherOutcome,
    bet_size: float,
    forecast_prob: float,
    edge: float,
    dry_run: bool = True,
) -> Optional[OrderRecord]:
    """
    Place a buy order for a weather outcome.

    Strategy: Query the real order book, place at or above the best ask
    to guarantee immediate fill. Cap price to preserve edge.
    """
    token_id = outcome.token_id
    market_price = outcome.market_prob

    # Query real order book for best ask (only in live mode)
    best_ask = None
    if client and not dry_run:
        best_ask = get_best_ask(client, token_id)
        if best_ask:
            print(f"    📊 Order book: best ask = ${best_ask:.2f} (market midpoint = ${market_price:.2f})")

    # Determine order price:
    # 1. If we have the real ask, place 1¢ above it to guarantee fill
    # 2. Cap at forecast_prob * 0.85 (keep at least 15% of our edge)
    # 3. Never exceed MAX_PRICE_CAP
    max_willing = min(round(forecast_prob * 0.85, 2), MAX_PRICE_CAP)
    max_willing = max(max_willing, round(market_price + 0.01, 2))  # at least 1¢ above market

    if best_ask:
        # Place 1¢ above best ask to cross the spread
        cross_price = round(best_ask + 0.01, 2)
        order_price = min(cross_price, max_willing)
        # If best ask is already above our max willing price, place at max willing
        # (this becomes a resting limit order that may fill if price drops)
        if best_ask > max_willing:
            order_price = max_willing
            print(f"    ⚠️  Best ask ${best_ask:.2f} > max willing ${max_willing:.2f}, placing limit")
    else:
        # Fallback: aggressive offset above market midpoint
        if market_price < 0.10:
            offset = 0.06  # 6¢ for very cheap markets (wide spreads)
        elif market_price < 0.20:
            offset = 0.05  # 5¢ for cheap markets
        elif market_price < 0.40:
            offset = 0.04  # 4¢ for mid-range
        else:
            offset = 0.03  # 3¢ for normal markets
        order_price = min(round(market_price + offset, 2), max_willing)

    order_price = min(order_price, 0.99)
    order_price = max(order_price, 0.01)

    # Size: number of tokens = USDC / price
    tokens = bet_size / order_price
    if tokens < 1.0:
        return None

    # Round to Polymarket's precision
    order_price = round(order_price, 2)
    tokens = round(tokens, 2)

    record = OrderRecord(
        order_id="",
        city_key="",  # Filled by caller
        target_date="",
        bucket_label=outcome.bucket.label,
        token_id=token_id,
        side="BUY",
        price=order_price,
        size=bet_size,
        edge=edge,
        forecast_prob=forecast_prob,
        market_prob=market_price,
        timestamp=datetime.now(timezone.utc).isoformat(),
        dry_run=dry_run,
    )

    if dry_run:
        record.order_id = f"DRY-{int(time.time())}-{token_id[:8]}"
        record.status = "dry_run"
        ask_str = f" (ask=${best_ask:.2f})" if best_ask else ""
        print(f"    🏷️  [DRY RUN] BUY {tokens:.1f} tokens @ ${order_price:.2f}{ask_str} "
              f"(${bet_size:.1f} USDC)")
        return record

    # LIVE ORDER — try GTC limit order (works with neg-risk matching engine)
    # Note: create_market_order only SIGNS locally — post_order actually submits.
    # For weather markets (neg-risk), GTC limits work best: the matching engine
    # converts YES buys into NO sells on complementary outcomes automatically.
    try:
        order_args = OrderArgs(
            token_id=token_id,
            price=order_price,
            size=tokens,
            side="BUY",
            fee_rate_bps=FEE_RATE_BPS,
        )

        signed = client.create_order(order_args)
        response = client.post_order(signed, orderType=OrderType.GTC)

        if response and isinstance(response, dict):
            record.order_id = response.get("orderID", response.get("id", "unknown"))
            record.status = "placed"
            print(f"    ✅ ORDER PLACED: {record.order_id}")
            print(f"       BUY {tokens:.1f} tokens @ ${order_price:.2f} (${bet_size:.1f} USDC)")
        else:
            record.order_id = str(response) if response else "error"
            record.status = "failed"
            print(f"    ⚠️  Unexpected response: {response}")

        return record

    except Exception as e:
        record.status = "failed"
        record.order_id = f"FAILED-{int(time.time())}"
        print(f"    ❌ Order FAILED: {e}")
        return record


# ─── Order Cleanup ────────────────────────────────────────────────────────────

def cancel_stale_orders(client: ClobClient):
    """Cancel open GTC orders for resolved/expired markets only.

    With FOK market orders, most orders fill instantly or not at all.
    This only matters for GTC fallback orders that may be resting.
    We cancel ALL open orders since weather markets resolve daily —
    any order from a previous scan is likely for a stale price.
    """
    try:
        resp = client.cancel_all()
        cancelled = resp.get("canceled", []) if isinstance(resp, dict) else resp
        if cancelled:
            n = len(cancelled) if isinstance(cancelled, list) else cancelled
            print(f"  🧹 Cancelled {n} stale GTC orders")
        else:
            print(f"  🧹 No stale orders to cancel")
    except Exception as e:
        print(f"  ⚠️  Could not cancel stale orders: {e}")


# ─── Main Executor ────────────────────────────────────────────────────────────

async def execute_weather_bets(
    bankroll: float = 164.0,
    dry_run: bool = True,
    verbose: bool = False,
    max_orders_per_scan: int = 10,
):
    """
    Run the full weather execution pipeline:
    1. Scan for opportunities
    2. Filter already-held positions
    3. Place orders for new opportunities
    4. Track positions
    """
    mode_str = "🏷️  DRY RUN" if dry_run else "💰 LIVE TRADING"
    print("\n" + "=" * 90)
    print(f"  🌤️  WEATHER BOT EXECUTOR — {mode_str}")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Bankroll: ${bankroll:.2f} | Max orders/scan: {max_orders_per_scan}")
    print("=" * 90)

    # Initialize CLOB client (only for live mode)
    client = None
    if not dry_run:
        client = create_clob_client()
        if not client:
            print("❌ Cannot create CLOB client. Check .env credentials.")
            return

        # Set allowance and check balance
        try:
            collateral_params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            client.update_balance_allowance(collateral_params)
            bal = client.get_balance_allowance(collateral_params)
            if bal:
                print(f"  💰 CLOB Balance/Allowance: {bal}")
        except Exception as e:
            print(f"  ⚠️  Could not set allowance / check balance: {e}")

        # Cancel any stale unfilled orders from previous scans
        # This frees up locked capital so we can place fresh orders
        cancel_stale_orders(client)

    # Ensure MOS is fresh
    mos_cache = load_mos_cache()
    mos_fresh = False
    if mos_cache:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MOS_FRESHNESS_HOURS)
        sample = next(iter(mos_cache.values()))
        try:
            last = datetime.fromisoformat(sample.last_updated)
            mos_fresh = last > cutoff
        except Exception:
            pass

    if not mos_fresh:
        print("\n🔄 MOS data stale — rebuilding...")
        async with aiohttp.ClientSession() as session:
            mos_cache = await build_all_mos(session)
            if mos_cache:
                save_mos_cache(mos_cache)
                print(f"   ✅ MOS rebuilt for {len(mos_cache)} stations")
    else:
        print(f"\n✅ MOS cache fresh ({len(mos_cache)} stations)")

    # Load existing positions
    positions = load_positions()
    open_positions = {k: p for k, p in positions.items() if p.status == "open"}
    expired = {k: p for k, p in positions.items() if p.status == "expired"}
    if open_positions:
        print(f"\n📂 {len(open_positions)} open positions (${deployed_capital(positions):.1f} deployed):")
        for key, pos in open_positions.items():
            print(f"   • {pos.city_key} {pos.target_date} \"{pos.bucket_label}\" "
                  f"${pos.size:.1f} @ {pos.avg_price:.2f}")
    if expired:
        print(f"   📋 {len(expired)} expired positions (awaiting resolution)")

    # Adjust bankroll for deployed capital
    available = bankroll - deployed_capital(positions)
    if available < 5.0:
        print(f"\n⚠️  Insufficient bankroll: ${bankroll:.1f} - ${deployed_capital(positions):.1f} deployed = ${available:.1f} available")
        return
    print(f"\n💰 Available bankroll: ${available:.1f} (${bankroll:.1f} - ${deployed_capital(positions):.1f} deployed)")

    # Run the scanner with available bankroll
    print("\n🔍 Running weather edge scanner...")
    opportunities = await run_scanner(bankroll=available, verbose=verbose)

    if not opportunities:
        print("\n📊 No actionable opportunities found this scan.")
        return

    # Filter: skip outcomes where we already have a position or high is locked
    new_opportunities = []
    today_date = datetime.now().date()

    for market, outcome, fp, edge, bet, ev in opportunities:
        # Timezone-aware same-day filter: skip if local time past 5pm (high likely locked)
        if str(market.target_date) == today_date.isoformat():
            city = CITIES.get(market.city_key)
            if city:
                try:
                    import zoneinfo
                    local_tz = zoneinfo.ZoneInfo(city.tz)
                    local_now = datetime.now(local_tz)
                    if local_now.hour >= 17:  # 5pm+ local → high is locked
                        if verbose:
                            print(f"\n⏭️  Skipping {city.name} {market.target_date} — {local_now.strftime('%H:%M')} local (high locked)")
                        continue
                except Exception:
                    pass  # If timezone fails, allow the bet
        elif str(market.target_date) < today_date.isoformat():
            continue  # Skip past dates entirely

        key = position_key(market.city_key, str(market.target_date), outcome.bucket.label)
        if key in positions and positions[key].status == "open":
            city = CITIES.get(market.city_key)
            cn = city.name if city else market.city_key
            print(f"\n⏭️  Already positioned: {cn} {market.target_date} \"{outcome.bucket.label}\"")
            continue
        new_opportunities.append((market, outcome, fp, edge, bet, ev))

    if not new_opportunities:
        print("\n📊 No NEW opportunities (already positioned in all edges).")
        return

    # Sort by edge (highest first) and limit
    new_opportunities.sort(key=lambda x: -x[3])
    to_execute = new_opportunities[:max_orders_per_scan]

    print(f"\n🎯 Executing {len(to_execute)} orders:")
    print("-" * 80)

    orders_placed = 0
    total_invested = 0.0

    for market, outcome, fp, edge, bet, ev in to_execute:
        city = CITIES.get(market.city_key)
        cn = city.name if city else market.city_key

        print(f"\n  📍 {cn} {market.target_date} — \"{outcome.bucket.label}\"")
        print(f"     Forecast: {fp:.1%} | Market: {outcome.market_prob:.1%} | "
              f"Edge: {edge:+.1%} | Kelly bet: ${bet:.1f} | EV: ${ev:+.2f}")

        # Place order
        record = place_order(
            client=client,
            outcome=outcome,
            bet_size=bet,
            forecast_prob=fp,
            edge=edge,
            dry_run=dry_run,
        )

        if record:
            record.city_key = market.city_key
            record.target_date = str(market.target_date)
            log_order(record)

            if record.status == "placed":
                # Track position
                key = position_key(market.city_key, str(market.target_date), outcome.bucket.label)
                if key in positions:
                    # Add to existing position
                    pos = positions[key]
                    old_total = pos.size
                    pos.size += bet
                    pos.tokens += bet / record.price
                    pos.avg_price = pos.size / pos.tokens if pos.tokens > 0 else record.price
                    pos.order_ids.append(record.order_id)
                else:
                    positions[key] = WeatherPosition(
                        city_key=market.city_key,
                        target_date=str(market.target_date),
                        bucket_label=outcome.bucket.label,
                        token_id=outcome.token_id,
                        market_id=outcome.market_id,
                        avg_price=record.price,
                        size=bet,
                        tokens=bet / record.price,
                        order_ids=[record.order_id],
                        created_at=record.timestamp,
                        status="open",
                    )

                orders_placed += 1
                total_invested += bet

        # Rate limit between orders
        await asyncio.sleep(ORDER_COOLDOWN_SECONDS)

    # Save positions
    save_positions(positions)

    # Summary
    print("\n" + "=" * 90)
    print(f"  📊 EXECUTION SUMMARY")
    print(f"  Orders placed: {orders_placed}")
    print(f"  Total invested: ${total_invested:.2f}")
    print(f"  Active positions: {sum(1 for p in positions.values() if p.status == 'open')}")
    if dry_run:
        print(f"  ⚠️  DRY RUN — No real orders placed")
    print("=" * 90)

    return positions


async def run_loop(
    bankroll: float = 164.0,
    dry_run: bool = True,
    verbose: bool = False,
    interval_minutes: int = SCAN_INTERVAL_MINUTES,
    max_orders_per_scan: int = 10,
):
    """Run the executor in a continuous loop."""
    print(f"\n🔄 Starting weather bot loop (every {interval_minutes} min)")
    print(f"   Press Ctrl+C to stop.\n")

    while True:
        try:
            await execute_weather_bets(
                bankroll=bankroll,
                dry_run=dry_run,
                verbose=verbose,
                max_orders_per_scan=max_orders_per_scan,
            )
        except Exception as e:
            print(f"\n❌ Error in execution cycle: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n⏳ Next scan in {interval_minutes} minutes...")
        await asyncio.sleep(interval_minutes * 60)


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Weather Bot Executor for Polymarket")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Simulate orders without placing them (default: True)")
    parser.add_argument("--live", action="store_true",
                        help="Place real orders (overrides --dry-run)")
    parser.add_argument("--bankroll", type=float, default=164.0,
                        help="Available bankroll in USDC (default: 164)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show all markets including ones without edges")
    parser.add_argument("--loop", type=int, default=0,
                        help="Run in loop mode, scan every N minutes (0 = single run)")
    parser.add_argument("--max-orders", type=int, default=10,
                        help="Maximum orders per scan (default: 10)")
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
        if args.loop > 0:
            asyncio.run(run_loop(
                bankroll=args.bankroll,
                dry_run=dry_run,
                verbose=args.verbose,
                interval_minutes=args.loop,
                max_orders_per_scan=args.max_orders,
            ))
        else:
            asyncio.run(execute_weather_bets(
                bankroll=args.bankroll,
                dry_run=dry_run,
                verbose=args.verbose,
                max_orders_per_scan=args.max_orders,
            ))
    except KeyboardInterrupt:
        print("\n\n👋 Weather bot stopped.")


if __name__ == "__main__":
    main()
