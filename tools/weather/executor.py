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
import zoneinfo
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, date, timedelta
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
FEE_RATE_BPS = 0                  # Weather markets: maker fee = 0% (100% rebate on makerBaseFee)
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
    forecast_mean: Optional[float] = None   # Our predicted daily high (native units)
    forecast_std: Optional[float] = None    # Our forecast uncertainty
    actual_temp: Optional[int] = None       # WU actual high after resolution
    temp_error: Optional[float] = None      # actual_temp - forecast_mean (+ = we predicted too cold)

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
    today = datetime.now(timezone.utc).date()
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
    """Save positions to disk atomically (tmp+rename prevents corruption on crash)."""
    try:
        data = {k: asdict(v) for k, v in positions.items()}
        tmp = POSITIONS_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        tmp.replace(POSITIONS_FILE)
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


async def resolve_expired_positions(
    positions: Dict[str, WeatherPosition],
    session,
) -> Dict[str, WeatherPosition]:
    """
    Query Polymarket for expired positions and mark them won/lost.
    Prints a P&L summary of resolved positions.
    Returns updated positions dict.
    """
    from .config import GAMMA_API_BASE, MONTH_NAMES
    import json as _json

    expired = {k: p for k, p in positions.items() if p.status == "expired"}
    if not expired:
        return positions

    print(f"\n📋 Resolving {len(expired)} expired positions...")

    resolved_count = 0
    total_invested = 0.0
    total_returned = 0.0

    for key, pos in expired.items():
        try:
            td = date.fromisoformat(pos.target_date)
            city = CITIES.get(pos.city_key)
            if not city:
                continue

            month_name = MONTH_NAMES.get(td.month, "")
            if not month_name:
                continue

            slug = f"highest-temperature-in-{city.slug_name}-on-{month_name}-{td.day}-{td.year}"
            url = f"{GAMMA_API_BASE}/events?slug={slug}"

            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()

            events = data if isinstance(data, list) else [data]
            if not events:
                continue
            event = events[0]
            if not event.get("closed", False):
                continue  # Not resolved yet

            # Find the winning bucket
            winning_label = None
            for m in event.get("markets", []):
                prices_raw = m.get("outcomePrices", "")
                try:
                    prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                except Exception:
                    prices = []
                if isinstance(prices, list) and len(prices) >= 1:
                    if float(prices[0]) >= 0.99:
                        # Extract bucket label from question
                        q = m.get("question", "")
                        # "Will the highest temperature in X be Y?" → extract Y
                        label_part = q.split(" be ")[-1].rstrip("?")
                        # Strip date suffix like " on February 17"
                        import re
                        label_part = re.sub(r"\s+on\s+\w+\s+\d+.*$", "", label_part).strip()
                        winning_label = label_part
                        break

            if winning_label is None:
                continue

            # Normalize our bucket label for comparison
            our_label = pos.bucket_label.strip()
            won = our_label == winning_label

            # Fallback: check if our label is contained in winner or vice versa
            if not won:
                won = (our_label in winning_label) or (winning_label in our_label)

            pos.status = "won" if won else "lost"
            resolved_count += 1

            payout = pos.size / pos.avg_price if won else 0.0
            profit = payout - pos.size
            total_invested += pos.size
            total_returned += payout

            # Fetch actual WU temperature for directional error tracking
            try:
                from .wunderground import fetch_wu_daily
                wu_actual = await fetch_wu_daily(session, pos.city_key, td)
                if wu_actual and wu_actual.is_complete:
                    pos.actual_temp = wu_actual.high_temp
                    if pos.forecast_mean is not None:
                        pos.temp_error = wu_actual.high_temp - pos.forecast_mean
            except Exception:
                pass

            icon = "✅" if won else "❌"
            temp_str = ""
            if pos.actual_temp is not None and pos.forecast_mean is not None:
                temp_str = f" | actual={pos.actual_temp}° pred={pos.forecast_mean:.1f}° err={pos.temp_error:+.1f}°"
            print(f"  {icon} {pos.city_key:12s} {pos.target_date} \"{pos.bucket_label}\" "
                  f"→ winner=\"{winning_label}\" | "
                  f"${pos.size:.1f} @ {pos.avg_price:.2f} → ${profit:+.1f}{temp_str}")

        except Exception as e:
            print(f"  ⚠️  Resolution error for {key}: {e}")

    if resolved_count > 0:
        profit = total_returned - total_invested
        roi = profit / total_invested * 100 if total_invested > 0 else 0
        wins = sum(1 for p in positions.values() if p.status == "won")
        losses = sum(1 for p in positions.values() if p.status == "lost")
        print(f"\n  📊 Resolved {resolved_count} positions: "
              f"${total_invested:.1f} invested → ${profit:+.1f} ({roi:+.1f}% ROI)")
        print(f"  All-time: {wins}W / {losses}L = {wins/(wins+losses)*100:.0f}% win rate"
              if (wins + losses) > 0 else "")
        save_positions(positions)
    else:
        print(f"  No new resolutions found.")

    return positions


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

def cancel_stale_orders(client: ClobClient, max_age_minutes: int = SCAN_INTERVAL_MINUTES):
    """Cancel GTC orders older than max_age_minutes.

    Only cancels orders that have been resting longer than one scan interval —
    fresh orders placed this scan are left alone to fill.
    """
    try:
        open_orders = client.get_orders() or []
        cutoff_ts = time.time() - (max_age_minutes * 60)
        stale_ids = []
        for o in open_orders:
            created = o.get("createdAt") or o.get("created_at") or 0
            try:
                created_ts = float(created) / 1000 if float(created) > 1e10 else float(created)
            except (TypeError, ValueError):
                continue
            if created_ts < cutoff_ts:
                stale_ids.append(o.get("id") or o.get("orderID", ""))

        if stale_ids:
            for oid in stale_ids:
                try:
                    client.cancel(oid)
                except Exception:
                    pass
            print(f"  🧹 Cancelled {len(stale_ids)} stale GTC orders (>{max_age_minutes}min old)")
        else:
            print(f"  🧹 No stale orders to cancel")
    except Exception as e:
        print(f"  ⚠️  Could not cancel stale orders: {e}")


# ─── Main Executor ────────────────────────────────────────────────────────────

PROBE_BET_SIZE = 1.0   # $1 per bet in probe mode


async def execute_weather_bets(
    bankroll: float = 164.0,
    dry_run: bool = True,
    verbose: bool = False,
    max_orders_per_scan: int = 10,
    force_rebuild_mos: bool = False,
    probe_mode: bool = False,
):
    """
    Run the full weather execution pipeline:
    1. Scan for opportunities
    2. Filter already-held positions
    3. Place orders for new opportunities
    4. Track positions

    probe_mode: cap all bets at $1 regardless of Kelly sizing.
    Use for the first 2 weeks to validate fill rates and edge
    before committing real capital.
    """
    if probe_mode and not dry_run:
        mode_str = "🔬 PROBE MODE ($1/bet)"
    elif dry_run:
        mode_str = "🏷️  DRY RUN"
    else:
        mode_str = "💰 LIVE TRADING"
    print("\n" + "=" * 90)
    print(f"  🌤️  WEATHER BOT EXECUTOR — {mode_str}")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Bankroll: ${bankroll:.2f} | Max orders/scan: {max_orders_per_scan}")
    if probe_mode and not dry_run:
        print(f"  ⚠️  PROBE MODE: all bets capped at ${PROBE_BET_SIZE:.2f} — validating fills & edge")
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
    if mos_cache and not force_rebuild_mos:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MOS_FRESHNESS_HOURS)
        sample = next(iter(mos_cache.values()))
        try:
            last = datetime.fromisoformat(sample.last_updated)
            mos_fresh = last > cutoff
        except Exception:
            pass

    if force_rebuild_mos:
        print("\n🔄 Forcing MOS rebuild (--rebuild-mos flag)...")
    if not mos_fresh:
        print("\n🔄 MOS data stale — rebuilding...")
        async with aiohttp.ClientSession() as session:
            mos_cache = await build_all_mos(session)
            if mos_cache:
                save_mos_cache(mos_cache)
                print(f"   ✅ MOS rebuilt for {len(mos_cache)} stations")
    else:
        print(f"\n✅ MOS cache fresh ({len(mos_cache)} stations)")

    # Load existing positions and resolve any expired ones
    positions = load_positions()

    # Resolve expired positions against Polymarket outcomes
    async with aiohttp.ClientSession() as _res_session:
        positions = await resolve_expired_positions(positions, _res_session)

    # Print per-city bias report so directional errors are visible every scan
    from .bias_report import compute_city_bias, print_bias_report
    _positions_dict = {k: vars(p) if not isinstance(p, dict) else p
                       for k, p in positions.items()}
    city_bias = compute_city_bias(_positions_dict)
    print_bias_report(city_bias)

    open_positions = {k: p for k, p in positions.items() if p.status == "open"}
    expired = {k: p for k, p in positions.items() if p.status == "expired"}
    won_pos = {k: p for k, p in positions.items() if p.status == "won"}
    lost_pos = {k: p for k, p in positions.items() if p.status == "lost"}
    if open_positions:
        print(f"\n📂 {len(open_positions)} open positions (${deployed_capital(positions):.1f} deployed):")
        for key, pos in open_positions.items():
            print(f"   • {pos.city_key} {pos.target_date} \"{pos.bucket_label}\" "
                  f"${pos.size:.1f} @ {pos.avg_price:.2f}")
    if expired:
        print(f"   📋 {len(expired)} expired positions (awaiting resolution)")
    if won_pos or lost_pos:
        total_w = len(won_pos)
        total_l = len(lost_pos)
        total_n = total_w + total_l
        wr = total_w / total_n * 100 if total_n > 0 else 0
        print(f"   📊 All-time: {total_w}W / {total_l}L ({wr:.0f}% win rate)")

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

    for market, outcome, fp, edge, bet, ev, fm, fs in opportunities:
        # Timezone-aware same-day filter: skip if local time past 5pm (high likely locked)
        if str(market.target_date) == today_date.isoformat():
            city = CITIES.get(market.city_key)
            if city:
                try:
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
        new_opportunities.append((market, outcome, fp, edge, bet, ev, fm, fs))

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

    for market, outcome, fp, edge, bet, ev, fm, fs in to_execute:
        city = CITIES.get(market.city_key)
        cn = city.name if city else market.city_key

        # In probe mode, cap bet at $1 regardless of Kelly
        actual_bet = PROBE_BET_SIZE if (probe_mode and not dry_run) else bet

        print(f"\n  📍 {cn} {market.target_date} — \"{outcome.bucket.label}\"")
        print(f"     Forecast: {fp:.1%} | Market: {outcome.market_prob:.1%} | "
              f"Edge: {edge:+.1%} | Kelly bet: ${bet:.1f}"
              + (f" → PROBE ${actual_bet:.2f}" if probe_mode and not dry_run else "")
              + f" | EV: ${ev:+.2f}")

        # Place order
        record = place_order(
            client=client,
            outcome=outcome,
            bet_size=actual_bet,
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
                        forecast_mean=fm,
                        forecast_std=fs,
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


def _next_scan_times_utc() -> List[datetime]:
    """
    Return today's scheduled scan times in UTC, sorted ascending.

    Strategy: scan shortly after each major model update lands on Open-Meteo.
    GFS runs: 00Z (+3.5h lag) → 03:30, 06Z → 09:30, 12Z → 15:30, 18Z → 21:30
    ECMWF runs: 00Z (+6h lag) → 06:00, 12Z → 18:00

    We add a 30-min buffer so Open-Meteo's own cache has time to refresh.
    Result: 6 scans/day at 04:00, 06:30, 10:00, 16:00, 18:30, 22:00 UTC.

    Also adds last-chance scans 45 min before each active city hits 5pm local
    (the cutoff after which same-day highs are locked).
    """
    now = datetime.now(timezone.utc)
    today = now.date()

    # Fixed model-update-aligned scan times (UTC)
    model_scan_hours = [4, 6, 10, 16, 18, 22]          # hours UTC
    model_scan_mins  = [0, 30,  0,  0, 30,  0]          # minutes UTC

    times: List[datetime] = []
    for h, m in zip(model_scan_hours, model_scan_mins):
        t = datetime(today.year, today.month, today.day, h, m, tzinfo=timezone.utc)
        times.append(t)

    # Last-chance scans: 30 min before the SAME_DAY_MIN_HOURS cutoff.
    # SAME_DAY_MIN_HOURS=4.0 means we stop betting same-day when < 4h to 5pm,
    # i.e. the cutoff is 1pm local. We scan at 12:30 local (30 min before cutoff)
    # so we catch the last window where same-day bets are still allowed.
    from .config import SAME_DAY_MIN_HOURS
    cutoff_hour = 17 - SAME_DAY_MIN_HOURS          # 13.0 = 1pm local
    cutoff_h = int(cutoff_hour)
    cutoff_m = int((cutoff_hour - cutoff_h) * 60)  # 0 min
    for city in CITIES.values():
        try:
            tz = zoneinfo.ZoneInfo(city.tz)
            local_cutoff = datetime(today.year, today.month, today.day,
                                    cutoff_h, cutoff_m, tzinfo=tz)
            last_chance = local_cutoff.astimezone(timezone.utc) - timedelta(minutes=30)
            # Only add if it's a distinct time (>15 min from any existing scan)
            if all(abs((last_chance - t).total_seconds()) > 900 for t in times):
                times.append(last_chance)
        except Exception:
            pass

    return sorted(times)


def _seconds_until_next_scan(scan_times: List[datetime]) -> Tuple[float, datetime]:
    """Return (seconds_to_wait, next_scan_datetime) for the next upcoming scan."""
    now = datetime.now(timezone.utc)
    future = [t for t in scan_times if t > now]
    if not future:
        # All today's scans done — next scan is first slot tomorrow
        tomorrow = now.date().toordinal() + 1
        d = date.fromordinal(tomorrow)
        first = scan_times[0]
        next_t = datetime(d.year, d.month, d.day,
                          first.hour, first.minute, tzinfo=timezone.utc)
    else:
        next_t = future[0]
    return max(0.0, (next_t - now).total_seconds()), next_t


async def run_loop(
    bankroll: float = 164.0,
    dry_run: bool = True,
    verbose: bool = False,
    interval_minutes: int = 0,
    max_orders_per_scan: int = 10,
    force_rebuild_mos: bool = False,
    probe_mode: bool = False,
):
    """
    Run the executor on a smart schedule aligned to model update times.

    If interval_minutes > 0, falls back to the legacy fixed-interval mode.
    Default (interval_minutes=0): scans at 04:00, 06:30, 10:00, 16:00, 18:30,
    22:00 UTC plus last-chance scans 45 min before each city's 5pm local cutoff.

    This reduces API usage from ~1,150 WU calls/day (30-min loop) to ~150/day
    while still capturing every meaningful model update.
    """
    if interval_minutes > 0:
        print(f"\n🔄 Starting weather bot loop (fixed interval: every {interval_minutes} min)")
    else:
        scan_times = _next_scan_times_utc()
        print(f"\n🔄 Starting weather bot (model-update-aligned schedule)")
        print(f"   Today's scan times (UTC): "
              + ", ".join(t.strftime("%H:%M") for t in scan_times))
    print(f"   Press Ctrl+C to stop.\n")

    first_run = True
    while True:
        cycle_start = time.time()
        try:
            await execute_weather_bets(
                bankroll=bankroll,
                dry_run=dry_run,
                verbose=verbose,
                max_orders_per_scan=max_orders_per_scan,
                force_rebuild_mos=force_rebuild_mos and first_run,
                probe_mode=probe_mode,
            )
            first_run = False
        except Exception as e:
            print(f"\n❌ Error in execution cycle: {e}")
            import traceback
            traceback.print_exc()

        elapsed = time.time() - cycle_start

        if interval_minutes > 0:
            # Legacy fixed-interval mode
            wait = max(0, interval_minutes * 60 - elapsed)
            next_str = f"in {wait/60:.1f} min"
        else:
            # Smart schedule: sleep until next model-update-aligned slot
            # Recompute scan times daily (handles midnight rollover)
            scan_times = _next_scan_times_utc()
            wait, next_dt = _seconds_until_next_scan(scan_times)
            next_str = f"at {next_dt.strftime('%H:%M')} UTC ({wait/60:.0f} min)"

        print(f"\n⏳ Next scan {next_str} (cycle took {elapsed:.0f}s)...")
        await asyncio.sleep(wait)


# ─── Fill Rate Report ─────────────────────────────────────────────────────────

def print_fill_report():
    """
    Read the orders log and report fill rates.

    For each placed order, checks:
    - dry_run orders: skipped
    - 'placed' orders: cross-checks CLOB for fill status
    - 'filled'/'cancelled'/'failed': uses logged status directly

    Prints summary by city and by edge bucket.
    """
    if not ORDERS_LOG_FILE.exists():
        print("No orders log found. Run the bot live first.")
        return

    try:
        with open(ORDERS_LOG_FILE, "r") as f:
            orders = json.load(f)
    except Exception as e:
        print(f"⚠️  Could not read orders log: {e}")
        return

    live_orders = [o for o in orders if not o.get("dry_run", True)]
    if not live_orders:
        print("No live orders in log yet (all are dry-run).")
        return

    # Build a set of order_ids that actually filled, from CLOB trade history
    filled_order_ids: set = set()
    partial_order_ids: set = set()
    try:
        client = create_clob_client()
        if client:
            trades = client.get_trades() or []
            for t in trades:
                oid = t.get("taker_order_id", "")
                status_t = t.get("status", "").upper()
                if status_t == "CONFIRMED" and oid:
                    filled_order_ids.add(oid)
                elif oid:
                    partial_order_ids.add(oid)
    except Exception:
        pass  # No CLOB access — use logged statuses only

    filled = cancelled = failed = unknown = 0
    by_city: Dict[str, Dict[str, int]] = {}
    by_edge: Dict[str, Dict[str, int]] = {}  # edge bucket → {filled, total}

    for o in live_orders:
        status = o.get("status", "unknown")
        city = o.get("city_key", "unknown")
        edge = o.get("edge", 0.0)
        order_id = o.get("order_id", "")

        # Override status using live trade data if available
        if order_id and order_id in filled_order_ids:
            status = "filled"
        elif order_id and order_id in partial_order_ids:
            status = "partial"

        # Tally
        edge_bucket = f"{int(edge*100//5)*5}-{int(edge*100//5)*5+5}%"
        if city not in by_city:
            by_city[city] = {"filled": 0, "total": 0}
        if edge_bucket not in by_edge:
            by_edge[edge_bucket] = {"filled": 0, "total": 0}

        by_city[city]["total"] += 1
        by_edge[edge_bucket]["total"] += 1

        if status in ("filled",):
            filled += 1
            by_city[city]["filled"] += 1
            by_edge[edge_bucket]["filled"] += 1
        elif status == "partial":
            filled += 1  # count partial as filled for now
            by_city[city]["filled"] += 1
            by_edge[edge_bucket]["filled"] += 1
        elif status == "cancelled":
            cancelled += 1
        elif status == "failed":
            failed += 1
        else:
            unknown += 1

    total = len(live_orders)
    fill_rate = filled / total * 100 if total > 0 else 0

    print("\n" + "=" * 70)
    print("  📊 FILL RATE REPORT")
    print("=" * 70)
    print(f"  Total live orders: {total}")
    print(f"  Filled:    {filled:3d}  ({fill_rate:.0f}%)")
    print(f"  Cancelled: {cancelled:3d}  (stale GTC orders cleaned up)")
    print(f"  Failed:    {failed:3d}  (order rejected by CLOB)")
    print(f"  Unknown:   {unknown:3d}  (status not yet resolved)")
    print()

    if by_city:
        print("  By city:")
        for city, s in sorted(by_city.items(), key=lambda x: -x[1]["total"]):
            fr = s["filled"] / s["total"] * 100 if s["total"] > 0 else 0
            bar = "█" * int(fr / 10) + "░" * (10 - int(fr / 10))
            print(f"    {city:15s}  {s['filled']:2d}/{s['total']:2d}  [{bar}] {fr:.0f}%")
    print()

    if by_edge:
        print("  By edge bucket:")
        for bucket, s in sorted(by_edge.items()):
            fr = s["filled"] / s["total"] * 100 if s["total"] > 0 else 0
            print(f"    edge {bucket:8s}  {s['filled']:2d}/{s['total']:2d}  {fr:.0f}% fill rate")
    print("=" * 70)

    if fill_rate < 50 and total >= 5:
        print("\n  ⚠️  Fill rate below 50% — orders may be priced too far from ask.")
        print("     Consider increasing the offset in place_order().")
    elif fill_rate > 90 and total >= 5:
        print("\n  ✅ Fill rate >90% — pricing is aggressive enough.")


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
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously on model-update-aligned schedule (recommended)")
    parser.add_argument("--loop-fixed", type=int, default=0, metavar="MINUTES",
                        help="Run continuously on a fixed interval (minutes). Overrides --loop.")
    parser.add_argument("--schedule", action="store_true",
                        help="Print today's planned scan times and exit")
    parser.add_argument("--max-orders", type=int, default=10,
                        help="Maximum orders per scan (default: 10)")
    parser.add_argument("--rebuild-mos", action="store_true",
                        help="Force MOS rebuild regardless of cache freshness")
    parser.add_argument("--probe", action="store_true",
                        help="Probe mode: cap all bets at $1 to validate fills & edge before scaling up")
    parser.add_argument("--fill-report", action="store_true",
                        help="Print fill rate report from order log and exit")
    args = parser.parse_args()

    if args.schedule:
        scan_times = _next_scan_times_utc()
        print("Today's scheduled scan times (UTC):")
        for t in scan_times:
            print(f"  {t.strftime('%H:%M')} UTC")
        return

    if args.fill_report:
        print_fill_report()
        return

    dry_run = not args.live

    if not dry_run:
        print("\n⚠️  LIVE TRADING MODE — Real orders will be placed!")
        print("    Press Ctrl+C within 5 seconds to cancel...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n❌ Cancelled.")
            return

    import signal

    def _handle_shutdown(signum, frame):
        print(f"\n\n👋 Weather bot received signal {signum} — shutting down cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        if args.loop_fixed > 0:
            asyncio.run(run_loop(
                bankroll=args.bankroll,
                dry_run=dry_run,
                verbose=args.verbose,
                interval_minutes=args.loop_fixed,
                max_orders_per_scan=args.max_orders,
                force_rebuild_mos=args.rebuild_mos,
                probe_mode=args.probe,
            ))
        elif args.loop:
            asyncio.run(run_loop(
                bankroll=args.bankroll,
                dry_run=dry_run,
                verbose=args.verbose,
                interval_minutes=0,  # 0 = smart schedule
                max_orders_per_scan=args.max_orders,
                force_rebuild_mos=args.rebuild_mos,
                probe_mode=args.probe,
            ))
        else:
            asyncio.run(execute_weather_bets(
                bankroll=args.bankroll,
                dry_run=dry_run,
                verbose=args.verbose,
                max_orders_per_scan=args.max_orders,
                force_rebuild_mos=args.rebuild_mos,
                probe_mode=args.probe,
            ))
    except (KeyboardInterrupt, SystemExit):
        print("\n\n👋 Weather bot stopped.")


if __name__ == "__main__":
    main()
