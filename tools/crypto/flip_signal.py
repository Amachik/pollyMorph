#!/usr/bin/env python3
"""
BTC 5m Flip Signal Detector — Real-Time.

Monitors the current BTC 5m window and fires a signal when conditions
indicate the bot is likely to flip the market price at the next :20 mark.

SIGNAL CONDITIONS (from 30-day backtested analysis):
  UP-zone flip signal (buy Down token):
    - At 200s into window (60s before mark3 at 260s fires)
    - Market price in [0.65, 0.75) — just entered up zone
    - First time in up zone this window (consec=1)
    - Window started near 0.5 (p0_before in [0.40, 0.60])
    - BTC move from priceToBeat <= +0.05% (BTC not strongly confirming Up)
    => P(flip) = 54-67%  (n=9-11, 30-day backtest)

  DOWN-zone flip signal (buy Up token):
    - Same but mirrored for down zone
    - P(flip) = ~13% — weaker signal, use with caution

USAGE:
    python -m tools.crypto.flip_signal                    # monitor current window
    python -m tools.crypto.flip_signal --loop 5           # loop every 5s
    python -m tools.crypto.flip_signal --dry-run          # print signals, no orders
    python -m tools.crypto.flip_signal --verbose          # extra debug output
"""

import asyncio
import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List

import aiohttp

GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3"
BTC_SLUG_PREFIX = "btc-updown-5m-"

# ── Signal parameters (from backtest) ────────────────────────────────────────
FLIP_THRESHOLD     = 0.15   # zone boundary: up >= 0.65, down <= 0.35
NEAR_BOUNDARY_LO   = 0.65   # lower bound of near-boundary up zone
NEAR_BOUNDARY_HI   = 0.75   # upper bound of near-boundary up zone
P0_NEUTRAL_RANGE   = 0.10   # window started neutral if |p0 - 0.5| < this
BTC_MOVE_MAX_PCT   = 0.05   # BTC move <= this % for signal to fire (up zone)
BOT_MARKS_SECS     = [80, 140, 200, 260]
MARK_WINDOW_SECS   = 30     # ±30s around each mark

# Signal confidence levels
# SIGNAL_E: mark>=2, p[0.65,0.70), btc<=+0.02%  → P=53%, n=15
# SIGNAL_D: mark=3,  p[0.65,0.75), btc<=+0.05%  → P=55%, n=11
# SIGNAL_B: mark=3,  p[0.65,0.70), btc<=+0.05%  → P=56%, n=9
# SIGNAL_C: mark=3,  p[0.65,0.75), btc<=+0.02%  → P=71%, n=7


@dataclass
class FlipSignal:
    window_slug: str
    window_start_ts: int
    elapsed_secs: int
    next_mark_secs: int        # which mark is about to fire (80/140/200/260)
    secs_until_mark: int       # seconds until the mark fires

    # Market state
    market_price: float        # current Up token price
    market_zone: str           # "up" / "down" / "neutral"
    p0_price: float            # price at window start (mark0)
    consec_in_zone: int        # consecutive marks in current zone
    prior_flip: bool

    # BTC state
    price_to_beat: float       # BTC reference price at window start
    btc_current: float         # current BTC spot price
    btc_move_pct: float        # (btc_current - price_to_beat) / price_to_beat * 100

    # Signal
    signal_name: str           # "SIGNAL_B", "SIGNAL_C", etc.
    flip_direction: str        # "up_to_down" (buy Down) or "down_to_up" (buy Up)
    p_flip: float              # estimated P(flip)
    confidence: str            # "HIGH" / "MEDIUM" / "LOW"
    action: str                # "BUY_DOWN" / "BUY_UP"
    entry_price: float         # current price of the token to buy


@dataclass
class PricePoint:
    ts: int
    price: float


# ── Fetching ─────────────────────────────────────────────────────────────────

async def fetch_current_market(session: aiohttp.ClientSession) -> Optional[dict]:
    now_ts = int(time.time())
    window_start = (now_ts // 300) * 300
    for offset in [0, -300, 300]:
        slug = f"{BTC_SLUG_PREFIX}{window_start + offset}"
        try:
            async with session.get(f"{GAMMA_API}/events", params={"slug": slug}) as r:
                r.raise_for_status()
                data = await r.json()
            if data:
                ev = data[0]
                mkts = ev.get("markets", [])
                if mkts:
                    m = mkts[0]
                    m["_event_metadata"] = ev.get("eventMetadata", {})
                    return m
        except Exception:
            pass
    return None


async def fetch_price_history(
    session: aiohttp.ClientSession,
    token_id: str,
    start_ts: int,
    end_ts: int,
) -> List[PricePoint]:
    params = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 1}
    try:
        async with session.get(f"{CLOB_API}/prices-history", params=params) as r:
            r.raise_for_status()
            data = await r.json()
        return sorted(
            [PricePoint(ts=int(h["t"]), price=float(h["p"])) for h in data.get("history", [])],
            key=lambda x: x.ts,
        )
    except Exception:
        return []


async def fetch_btc_price(session: aiohttp.ClientSession) -> Optional[float]:
    try:
        async with session.get(f"{BINANCE_API}/ticker/price", params={"symbol": "BTCUSDT"}) as r:
            r.raise_for_status()
            data = await r.json()
        return float(data["price"])
    except Exception:
        return None


async def fetch_btc_at_timestamp(session: aiohttp.ClientSession, ts: int) -> Optional[float]:
    """Fetch BTC/USDT close price at a specific timestamp from Binance 1m klines."""
    try:
        async with session.get(f"{BINANCE_API}/klines", params={
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": ts * 1000,
            "limit": 1,
        }) as r:
            r.raise_for_status()
            data = await r.json()
        if data:
            return float(data[0][4])  # close price
        return None
    except Exception:
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def zone(price: float) -> str:
    if price >= 0.5 + FLIP_THRESHOLD:
        return "up"
    elif price <= 0.5 - FLIP_THRESHOLD:
        return "down"
    return "neutral"


def price_at_time(history: List[PricePoint], target_ts: int, max_dist: int = 90) -> Optional[float]:
    best = None
    best_dist = max_dist
    for pt in history:
        dist = abs(pt.ts - target_ts)
        if dist < best_dist:
            best_dist = dist
            best = pt.price
    return best


def classify_signal(
    mark_index: int,
    price_before: float,
    btc_move_pct: float,
) -> tuple[str, float, str]:
    """Returns (signal_name, p_flip, confidence)."""
    z = zone(price_before)
    if z == "up":
        in_narrow = 0.65 <= price_before < 0.70
        in_wide   = 0.65 <= price_before < 0.75
        btc_ok_tight = btc_move_pct <= 0.02
        btc_ok_loose = btc_move_pct <= 0.05

        if mark_index == 3 and in_narrow and btc_ok_tight:
            return "SIGNAL_A", 0.667, "HIGH"
        if mark_index == 3 and in_wide and btc_ok_tight:
            return "SIGNAL_C", 0.714, "HIGH"
        if mark_index == 3 and in_narrow and btc_ok_loose:
            return "SIGNAL_B", 0.556, "MEDIUM"
        if mark_index == 3 and in_wide and btc_ok_loose:
            return "SIGNAL_D", 0.545, "MEDIUM"
        if mark_index >= 2 and in_narrow and btc_ok_tight:
            return "SIGNAL_E", 0.533, "MEDIUM"
        if mark_index >= 2 and in_wide and btc_ok_loose:
            return "SIGNAL_F", 0.323, "LOW"

    elif z == "down":
        in_narrow = 0.30 < price_before <= 0.35
        in_wide   = 0.25 < price_before <= 0.35
        btc_contradicts = btc_move_pct >= 0.0

        if mark_index == 3 and in_narrow and btc_contradicts:
            return "SIGNAL_G", 0.125, "LOW"
        if mark_index >= 2 and in_wide and btc_contradicts:
            return "SIGNAL_H", 0.088, "LOW"

    return "", 0.0, ""


# ── Core Signal Detection ─────────────────────────────────────────────────────

async def check_window(session: aiohttp.ClientSession, verbose: bool = False) -> Optional[FlipSignal]:
    now_ts = int(time.time())

    mkt = await fetch_current_market(session)
    if not mkt:
        if verbose:
            print("  No active market found")
        return None

    # Parse market
    meta = mkt.get("_event_metadata") or {}
    price_to_beat = float(meta.get("priceToBeat") or 0)
    # For live markets, priceToBeat is not in metadata — fetch from Binance at window start
    if price_to_beat <= 0:
        start_str_tmp = mkt.get("eventStartTime") or mkt.get("startDate", "")
        try:
            start_dt_tmp = datetime.fromisoformat(start_str_tmp.replace("Z", "+00:00"))
            wts_tmp = int(start_dt_tmp.timestamp())
            price_to_beat = await fetch_btc_at_timestamp(session, wts_tmp) or 0.0
        except Exception:
            pass
    if price_to_beat <= 0:
        if verbose:
            print("  Could not determine priceToBeat")
        return None

    token_ids_raw = mkt.get("clobTokenIds", "[]")
    try:
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
    except Exception:
        return None

    outcomes_raw = mkt.get("outcomes", '["Up","Down"]')
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    except Exception:
        outcomes = ["Up", "Down"]

    up_idx      = outcomes.index("Up") if "Up" in outcomes else 0
    dn_idx      = 1 - up_idx
    up_token_id = token_ids[up_idx] if up_idx < len(token_ids) else token_ids[0]
    dn_token_id = token_ids[dn_idx] if dn_idx < len(token_ids) else token_ids[1]

    start_str = mkt.get("eventStartTime") or mkt.get("startDate", "")
    try:
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    except Exception:
        return None
    window_start_ts = int(start_dt.timestamp())
    elapsed = now_ts - window_start_ts

    if elapsed < 0 or elapsed > 300:
        if verbose:
            print(f"  Window not active (elapsed={elapsed}s)")
        return None

    # Determine which mark we're approaching
    next_mark_idx  = None
    next_mark_secs = None
    secs_until     = None
    for i, mark_secs in enumerate(BOT_MARKS_SECS):
        remaining = mark_secs - elapsed
        if -MARK_WINDOW_SECS <= remaining <= MARK_WINDOW_SECS + 30:
            next_mark_idx  = i
            next_mark_secs = mark_secs
            secs_until     = remaining
            break
        if remaining > MARK_WINDOW_SECS + 30:
            next_mark_idx  = i
            next_mark_secs = mark_secs
            secs_until     = remaining
            break

    if next_mark_idx is None:
        if verbose:
            print(f"  No upcoming mark (elapsed={elapsed}s)")
        return None

    # Fetch price history and BTC price in parallel
    price_hist, btc_price = await asyncio.gather(
        fetch_price_history(session, up_token_id,
                            start_ts=window_start_ts - 90,
                            end_ts=now_ts + 30),
        fetch_btc_price(session),
    )

    if not price_hist or btc_price is None:
        if verbose:
            print("  Failed to fetch price history or BTC price")
        return None

    # p0: price at window start
    p0 = price_at_time(price_hist, window_start_ts + 20, max_dist=90)  # mark0 is at +80s, before is +20s
    if p0 is None:
        p0 = price_hist[0].price if price_hist else 0.5

    # price_before: price ~60s before the next mark
    before_ts    = window_start_ts + next_mark_secs - 60
    price_before = price_at_time(price_hist, before_ts)
    if price_before is None:
        price_before = price_hist[-1].price if price_hist else 0.5

    # Current price (most recent)
    current_price = price_hist[-1].price if price_hist else 0.5

    # Compute consec_in_zone
    z = zone(price_before)
    consec = 0
    for mark_secs in BOT_MARKS_SECS[:next_mark_idx + 1]:
        pt_ts = window_start_ts + mark_secs - 60
        pt_p  = price_at_time(price_hist, pt_ts)
        if pt_p is not None and zone(pt_p) == z:
            consec += 1
        else:
            consec = 0  # reset on zone change

    # Prior flip: did price cross zones earlier in this window?
    prior_flip = False
    prev_zone  = None
    for mark_secs in BOT_MARKS_SECS[:next_mark_idx]:
        pt_ts = window_start_ts + mark_secs
        pt_p  = price_at_time(price_hist, pt_ts)
        if pt_p is None:
            continue
        pt_z = zone(pt_p)
        if prev_zone is not None and pt_z != "neutral" and prev_zone != "neutral" and pt_z != prev_zone:
            prior_flip = True
            break
        if pt_z != "neutral":
            prev_zone = pt_z

    # BTC move
    btc_move_pct = (btc_price - price_to_beat) / price_to_beat * 100

    # p0 neutral check
    p0_neutral = abs(p0 - 0.5) < P0_NEUTRAL_RANGE

    if verbose:
        print(f"  elapsed={elapsed}s  next_mark={next_mark_secs}s (+{secs_until}s)")
        print(f"  price_before={price_before:.3f}  zone={z}  consec={consec}  prior_flip={prior_flip}")
        print(f"  p0={p0:.3f}  p0_neutral={p0_neutral}")
        print(f"  btc_price={btc_price:.2f}  ptb={price_to_beat:.2f}  btc_move={btc_move_pct:+.4f}%")

    # Check signal conditions
    if not p0_neutral or prior_flip:
        if verbose:
            print(f"  No signal: p0_neutral={p0_neutral}  prior_flip={prior_flip}")
        return None

    signal_name, p_flip, confidence = classify_signal(next_mark_idx, price_before, btc_move_pct)
    if not signal_name:
        if verbose:
            print(f"  No signal for zone={z}  mark_idx={next_mark_idx}  p={price_before:.3f}  btc={btc_move_pct:+.4f}%")
        return None

    # Determine action
    if z == "up":
        flip_direction = "up_to_down"
        action         = "BUY_DOWN"
        entry_price    = float(mkt.get("bestAsk") or 1.0) - float(mkt.get("bestAsk") or 1.0) + (1.0 - current_price)
        # Down token price ≈ 1 - Up price
        entry_price = round(1.0 - current_price, 2)
    else:
        flip_direction = "down_to_up"
        action         = "BUY_UP"
        entry_price    = current_price

    return FlipSignal(
        window_slug=mkt.get("slug", ""),
        window_start_ts=window_start_ts,
        elapsed_secs=elapsed,
        next_mark_secs=next_mark_secs,
        secs_until_mark=secs_until,
        market_price=round(current_price, 3),
        market_zone=z,
        p0_price=round(p0, 3),
        consec_in_zone=consec,
        prior_flip=prior_flip,
        price_to_beat=round(price_to_beat, 2),
        btc_current=round(btc_price, 2),
        btc_move_pct=round(btc_move_pct, 4),
        signal_name=signal_name,
        flip_direction=flip_direction,
        p_flip=p_flip,
        confidence=confidence,
        action=action,
        entry_price=entry_price,
    )


def print_signal(sig: FlipSignal):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    conf_color = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}.get(sig.confidence, "")
    print(f"\n{'='*65}")
    print(f"  {conf_color} FLIP SIGNAL DETECTED  [{now} UTC]")
    print(f"{'='*65}")
    print(f"  Signal    : {sig.signal_name}  ({sig.confidence})  P(flip)={sig.p_flip:.1%}")
    print(f"  Window    : {sig.window_slug}")
    print(f"  Elapsed   : {sig.elapsed_secs}s / 300s")
    print(f"  Next mark : {sig.next_mark_secs}s  (fires in ~{sig.secs_until_mark}s)")
    print(f"  Market    : Up={sig.market_price:.3f}  zone={sig.market_zone}  consec={sig.consec_in_zone}")
    print(f"  Window p0 : {sig.p0_price:.3f}  (started neutral: {abs(sig.p0_price-0.5)<0.10})")
    print(f"  BTC       : {sig.btc_current:.2f}  ptb={sig.price_to_beat:.2f}  move={sig.btc_move_pct:+.4f}%")
    print(f"  Action    : {sig.action}  entry≈{sig.entry_price:.2f}")
    print(f"  Direction : {sig.flip_direction}")
    ev = sig.p_flip * (1.0 / sig.entry_price) - 1.0 if sig.entry_price > 0 else 0
    print(f"  EV        : {ev:+.1%}  (at P={sig.p_flip:.1%}, entry={sig.entry_price:.2f})")
    print(f"{'='*65}")


# ── Main Loop ─────────────────────────────────────────────────────────────────

async def run(loop_secs: int, dry_run: bool, verbose: bool, min_confidence: str):
    conf_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    min_rank  = conf_rank.get(min_confidence, 1)

    print(f"\n{'='*65}")
    print(f"  BTC 5m Flip Signal Detector")
    print(f"  Min confidence: {min_confidence}  Loop: {loop_secs}s  Dry-run: {dry_run}")
    print(f"{'='*65}\n")

    fired_slugs: set = set()

    async with aiohttp.ClientSession() as session:
        while True:
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            if verbose:
                print(f"[{now_str}] Checking...")

            try:
                sig = await check_window(session, verbose=verbose)
            except Exception as e:
                print(f"[{now_str}] Error: {e}")
                sig = None

            if sig is not None:
                rank = conf_rank.get(sig.confidence, 0)
                if rank >= min_rank:
                    # Deduplicate: don't fire same signal for same window+mark twice
                    key = f"{sig.window_slug}:{sig.next_mark_secs}"
                    if key not in fired_slugs:
                        fired_slugs.add(key)
                        print_signal(sig)
                        if not dry_run:
                            print(f"  [LIVE] Would place order: {sig.action} {sig.entry_price:.2f}")
                            # TODO: integrate with executor.py order placement
                    elif verbose:
                        print(f"[{now_str}] Signal already fired for {key}, skipping")
                elif verbose:
                    print(f"[{now_str}] Signal {sig.signal_name} below min confidence ({sig.confidence} < {min_confidence})")
            elif verbose:
                print(f"[{now_str}] No signal")

            # Clean up old fired slugs (keep last 20)
            if len(fired_slugs) > 20:
                fired_slugs = set(list(fired_slugs)[-20:])

            if loop_secs <= 0:
                break
            await asyncio.sleep(loop_secs)


def main():
    parser = argparse.ArgumentParser(description="BTC 5m Flip Signal Detector")
    parser.add_argument("--loop",           type=int,   default=10,
                        help="Loop interval in seconds (0 = run once)")
    parser.add_argument("--dry-run",        action="store_true", default=True,
                        help="Print signals only, no orders (default: True)")
    parser.add_argument("--live",           action="store_true",
                        help="Enable live order placement")
    parser.add_argument("--verbose",        action="store_true",
                        help="Print debug info every loop")
    parser.add_argument("--min-confidence", type=str,   default="MEDIUM",
                        choices=["LOW", "MEDIUM", "HIGH"],
                        help="Minimum signal confidence to act on")
    args = parser.parse_args()

    dry_run = not args.live
    asyncio.run(run(args.loop, dry_run, args.verbose, args.min_confidence))


if __name__ == "__main__":
    main()
