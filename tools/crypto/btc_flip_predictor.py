#!/usr/bin/env python3
"""
BTC 5m Flip Predictor — BTC Price vs priceToBeat Analysis.

The core hypothesis: the bot flips the Polymarket price when the BTC spot price
has moved far enough from the window's reference price (priceToBeat) to make
the current market price clearly wrong.

For each historical window:
  1. Get priceToBeat (BTC reference price at window start) from Gamma eventMetadata
  2. Fetch BTC/USDT 1m klines from Binance for the window duration
  3. At each :20-mark (80s, 140s, 200s, 260s), compute:
     - btc_move_pct = (btc_price_at_mark - priceToBeat) / priceToBeat * 100
     - market_price_at_mark (from CLOB price history)
     - flip_happened (zone changed between before and at mark)
  4. Compute P(flip | btc_move_pct, market_price_discrepancy)

Usage:
    python -m tools.crypto.btc_flip_predictor --days 7
    python -m tools.crypto.btc_flip_predictor --days 7 --csv /tmp/btc_pred.csv
"""

import asyncio
import argparse
import csv
import json
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import aiohttp

GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3"
BTC_SLUG_PREFIX = "btc-updown-5m-"

FLIP_THRESHOLD = 0.15
BOT_MARKS_SECS = [80, 140, 200, 260]


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class PricePoint:
    ts: int
    price: float


@dataclass
class MarkObs:
    window_slug: str
    window_start_ts: int
    mark_index: int
    mark_secs: int
    price_to_beat: float
    btc_at_mark: float
    btc_move_pct: float
    btc_move_abs: float
    btc_direction: str
    market_price_before: float
    market_price_at: float
    market_zone_before: str
    market_zone_at: str
    implied_fair: float
    discrepancy: float
    prior_flip: bool
    flip_happened: bool
    flip_direction: str
    flip_magnitude: float


# ─── Fetching ─────────────────────────────────────────────────────────────────

async def fetch_one_event(session: aiohttp.ClientSession, slug: str) -> Optional[dict]:
    try:
        async with session.get(f"{GAMMA_API}/events", params={"slug": slug}) as resp:
            resp.raise_for_status()
            data = await resp.json()
        if not data:
            return None
        ev = data[0]
        mkts = ev.get("markets", [])
        if not mkts:
            return None
        m = mkts[0]
        m["_event_slug"] = ev.get("slug", slug)
        m["_event_metadata"] = ev.get("eventMetadata", {})
        return m
    except Exception:
        return None


async def fetch_market_price_history(
    session: aiohttp.ClientSession,
    token_id: str,
    start_ts: int,
    end_ts: int,
) -> List[PricePoint]:
    params = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 1}
    try:
        async with session.get(f"{CLOB_API}/prices-history", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        history = data.get("history", [])
        return sorted([PricePoint(ts=int(h["t"]), price=float(h["p"])) for h in history],
                      key=lambda x: x.ts)
    except Exception:
        return []


async def fetch_btc_klines(
    session: aiohttp.ClientSession,
    start_ts: int,
    end_ts: int,
) -> List[PricePoint]:
    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "startTime": start_ts * 1000,
        "endTime": end_ts * 1000,
        "limit": 20,
    }
    try:
        async with session.get(f"{BINANCE_API}/klines", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return sorted([PricePoint(ts=int(k[0]) // 1000, price=float(k[4])) for k in data],
                      key=lambda x: x.ts)
    except Exception:
        return []


async def fetch_historical_markets(
    session: aiohttp.ClientSession,
    days_back: int,
) -> List[dict]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    current_window_start = (now_ts // 300) * 300
    total_windows = (days_back * 24 * 60) // 5

    slugs = [
        f"{BTC_SLUG_PREFIX}{current_window_start - i * 300}"
        for i in range(1, total_windows + 1)
    ]

    print(f"  Enumerating {len(slugs)} windows ({days_back} days)...")

    markets = []
    batch_size = 20
    found = 0
    consecutive_misses = 0

    for batch_start in range(0, len(slugs), batch_size):
        batch = slugs[batch_start:batch_start + batch_size]
        results = await asyncio.gather(*[fetch_one_event(session, s) for s in batch])
        for mkt in results:
            if mkt is not None:
                markets.append(mkt)
                found += 1
                consecutive_misses = 0
            else:
                consecutive_misses += 1

        if batch_start % 200 == 0 and batch_start > 0:
            print(f"    {batch_start}/{len(slugs)} checked, {found} found...")

        if consecutive_misses > 100 and batch_start > 500:
            print(f"    Stopping: {consecutive_misses} consecutive misses")
            break

        await asyncio.sleep(0.1)

    print(f"  Found {len(markets)} markets")
    return markets


# ─── Helpers ─────────────────────────────────────────────────────────────────

def price_at_time(history: List[PricePoint], target_ts: int, max_dist: int = 90) -> Optional[float]:
    best = None
    best_dist = max_dist
    for pt in history:
        dist = abs(pt.ts - target_ts)
        if dist < best_dist:
            best_dist = dist
            best = pt.price
    return best


def zone(price: float, threshold: float = FLIP_THRESHOLD) -> str:
    if price >= 0.5 + threshold:
        return "up"
    elif price <= 0.5 - threshold:
        return "down"
    return "neutral"


def label_flip(price_before: float, price_at: float) -> Tuple[bool, str, float]:
    zb = zone(price_before)
    za = zone(price_at)
    if zb == "neutral" or za == "neutral" or zb == za:
        return False, "none", 0.0
    return True, f"{zb}_to_{za}", abs(price_at - price_before)


# ─── Build Observations ───────────────────────────────────────────────────────

def build_observations(
    slug: str,
    window_start_ts: int,
    price_to_beat: float,
    market_history: List[PricePoint],
    btc_history: List[PricePoint],
) -> List[MarkObs]:
    observations = []
    prior_flip = False

    for i, mark_secs in enumerate(BOT_MARKS_SECS):
        mark_ts   = window_start_ts + mark_secs
        before_ts = mark_ts - 60

        market_before = price_at_time(market_history, before_ts)
        market_at     = price_at_time(market_history, mark_ts)
        btc_at        = price_at_time(btc_history, mark_ts, max_dist=120)

        if market_before is None or market_at is None or btc_at is None:
            continue
        if price_to_beat <= 0:
            continue

        btc_move_pct = (btc_at - price_to_beat) / price_to_beat * 100
        btc_move_abs = abs(btc_move_pct)
        btc_dir      = "up" if btc_move_pct > 0 else "down"

        implied_fair = 0.5 + btc_move_pct * 0.5
        implied_fair = max(0.01, min(0.99, implied_fair))
        discrepancy  = market_at - implied_fair

        zb = zone(market_before)
        za = zone(market_at)

        flip_happened, flip_dir, flip_mag = label_flip(market_before, market_at)

        obs = MarkObs(
            window_slug=slug,
            window_start_ts=window_start_ts,
            mark_index=i,
            mark_secs=mark_secs,
            price_to_beat=round(price_to_beat, 2),
            btc_at_mark=round(btc_at, 2),
            btc_move_pct=round(btc_move_pct, 4),
            btc_move_abs=round(btc_move_abs, 4),
            btc_direction=btc_dir,
            market_price_before=round(market_before, 4),
            market_price_at=round(market_at, 4),
            market_zone_before=zb,
            market_zone_at=za,
            implied_fair=round(implied_fair, 4),
            discrepancy=round(discrepancy, 4),
            prior_flip=prior_flip,
            flip_happened=flip_happened,
            flip_direction=flip_dir,
            flip_magnitude=round(flip_mag, 4),
        )
        observations.append(obs)

        if flip_happened:
            prior_flip = True

    return observations


# ─── Analysis ─────────────────────────────────────────────────────────────────

def pct_bar(rate: float, width: int = 40) -> str:
    return "▓" * int(rate * width)


def analyze(obs_list: List[MarkObs]):
    n = len(obs_list)
    if n == 0:
        print("  No observations to analyze.")
        return

    n_flip    = sum(1 for o in obs_list if o.flip_happened)
    base_rate = n_flip / n

    print(f"\n{'='*70}")
    print(f"  BTC PRICE PREDICTOR ANALYSIS")
    print(f"  {n} mark-observations | {n_flip} flips | base rate: {base_rate:.2%}")
    print(f"{'='*70}")

    # ── 1. P(flip) by |BTC move| ──────────────────────────────────────────────
    print(f"\n  P(flip) by |BTC move %| from priceToBeat at mark time:")
    print(f"  {'BTC move range':<22} {'n':>5}  {'P(flip)':>8}  {'Lift':>5}  bar")
    print(f"  {'-'*65}")
    btc_buckets = [
        (0.00, 0.05, "0.00-0.05%"),
        (0.05, 0.10, "0.05-0.10%"),
        (0.10, 0.15, "0.10-0.15%"),
        (0.15, 0.20, "0.15-0.20%"),
        (0.20, 0.30, "0.20-0.30%"),
        (0.30, 0.50, "0.30-0.50%"),
        (0.50, 1.00, "0.50-1.00%"),
        (1.00, 99.0, ">1.00%"),
    ]
    for lo, hi, label in btc_buckets:
        sub = [o for o in obs_list if lo <= o.btc_move_abs < hi]
        if not sub:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"  {label:<22} {len(sub):>5}  {rate:>7.1%}  {lift:>4.1f}x  {pct_bar(rate)}")

    # ── 2. BTC direction vs market zone alignment ─────────────────────────────
    print(f"\n  P(flip) by BTC direction vs market zone (aligned/misaligned):")
    print(f"  {'Condition':<42} {'n':>5}  {'P(flip)':>8}  {'Lift':>5}  bar")
    print(f"  {'-'*70}")
    combos = [
        ("BTC up,   market=up   (aligned)",
         lambda o: o.btc_direction == "up"   and o.market_zone_at == "up"),
        ("BTC up,   market=down (MISALIGNED)",
         lambda o: o.btc_direction == "up"   and o.market_zone_at == "down"),
        ("BTC up,   market=neutral",
         lambda o: o.btc_direction == "up"   and o.market_zone_at == "neutral"),
        ("BTC down, market=down (aligned)",
         lambda o: o.btc_direction == "down" and o.market_zone_at == "down"),
        ("BTC down, market=up   (MISALIGNED)",
         lambda o: o.btc_direction == "down" and o.market_zone_at == "up"),
        ("BTC down, market=neutral",
         lambda o: o.btc_direction == "down" and o.market_zone_at == "neutral"),
    ]
    for label, fn in combos:
        sub = [o for o in obs_list if fn(o)]
        if not sub:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"  {label:<42} {len(sub):>5}  {rate:>7.1%}  {lift:>4.1f}x  {pct_bar(rate)}")

    # ── 3. P(flip) by discrepancy ─────────────────────────────────────────────
    print(f"\n  P(flip) by discrepancy (market_price - implied_fair):")
    print(f"  Positive = market too HIGH vs BTC move | Negative = market too LOW")
    print(f"  {'Discrepancy range':<28} {'n':>5}  {'P(flip)':>8}  {'Lift':>5}  bar")
    print(f"  {'-'*70}")
    disc_buckets = [
        (-2.0, -0.40, "< -0.40 (mkt way too low)"),
        (-0.40, -0.25, "-0.40 to -0.25"),
        (-0.25, -0.10, "-0.25 to -0.10"),
        (-0.10,  0.10, "-0.10 to +0.10 (aligned)"),
        ( 0.10,  0.25, "+0.10 to +0.25"),
        ( 0.25,  0.40, "+0.25 to +0.40"),
        ( 0.40,  2.0,  "> +0.40 (mkt way too high)"),
    ]
    for lo, hi, label in disc_buckets:
        sub = [o for o in obs_list if lo <= o.discrepancy < hi]
        if not sub:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"  {label:<28} {len(sub):>5}  {rate:>7.1%}  {lift:>4.1f}x  {pct_bar(rate)}")

    # ── 4. Combined: BTC move + discrepancy ───────────────────────────────────
    print(f"\n  P(flip) — BTC move + discrepancy combined (strongest signals):")
    print(f"  {'Condition':<52} {'n':>5}  {'P(flip)':>8}  {'Lift':>5}")
    print(f"  {'-'*72}")
    full_combos = [
        ("btc_move>0.10%, market misaligned",
         lambda o: o.btc_move_abs > 0.10 and (
             (o.btc_direction == "up"   and o.market_zone_at == "down") or
             (o.btc_direction == "down" and o.market_zone_at == "up"))),
        ("btc_move>0.20%, market misaligned",
         lambda o: o.btc_move_abs > 0.20 and (
             (o.btc_direction == "up"   and o.market_zone_at == "down") or
             (o.btc_direction == "down" and o.market_zone_at == "up"))),
        ("btc_move>0.30%, market misaligned",
         lambda o: o.btc_move_abs > 0.30 and (
             (o.btc_direction == "up"   and o.market_zone_at == "down") or
             (o.btc_direction == "down" and o.market_zone_at == "up"))),
        ("btc_move>0.50%, market misaligned",
         lambda o: o.btc_move_abs > 0.50 and (
             (o.btc_direction == "up"   and o.market_zone_at == "down") or
             (o.btc_direction == "down" and o.market_zone_at == "up"))),
        ("btc_move>0.20%, market aligned",
         lambda o: o.btc_move_abs > 0.20 and (
             (o.btc_direction == "up"   and o.market_zone_at == "up") or
             (o.btc_direction == "down" and o.market_zone_at == "down"))),
        ("discrepancy > +0.30 (mkt too high)",
         lambda o: o.discrepancy > 0.30),
        ("discrepancy < -0.30 (mkt too low)",
         lambda o: o.discrepancy < -0.30),
        ("discrepancy > +0.40",
         lambda o: o.discrepancy > 0.40),
        ("discrepancy < -0.40",
         lambda o: o.discrepancy < -0.40),
        ("btc>0.15% AND disc>+0.25",
         lambda o: o.btc_move_abs > 0.15 and o.discrepancy > 0.25),
        ("btc>0.15% AND disc<-0.25",
         lambda o: o.btc_move_abs > 0.15 and o.discrepancy < -0.25),
        ("btc>0.20% AND disc>+0.30",
         lambda o: o.btc_move_abs > 0.20 and o.discrepancy > 0.30),
        ("btc>0.20% AND disc<-0.30",
         lambda o: o.btc_move_abs > 0.20 and o.discrepancy < -0.30),
        ("btc>0.30% AND disc>+0.30",
         lambda o: o.btc_move_abs > 0.30 and o.discrepancy > 0.30),
        ("btc>0.30% AND disc<-0.30",
         lambda o: o.btc_move_abs > 0.30 and o.discrepancy < -0.30),
        ("btc>0.50% AND disc>+0.30",
         lambda o: o.btc_move_abs > 0.50 and o.discrepancy > 0.30),
        ("btc>0.50% AND disc<-0.30",
         lambda o: o.btc_move_abs > 0.50 and o.discrepancy < -0.30),
    ]
    for label, fn in full_combos:
        sub = [o for o in obs_list if fn(o)]
        if not sub:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        lift = rate / base_rate if base_rate > 0 else 0
        bar  = pct_bar(rate)
        print(f"  {label:<52} {len(sub):>5}  {rate:>7.1%}  {lift:>4.1f}x  {bar}")

    # ── 5. Threshold sweep on misaligned observations ─────────────────────────
    print(f"\n  THRESHOLD SWEEP — misaligned only (market shows wrong direction vs BTC):")
    print(f"  {'BTC move threshold':<25} {'n':>5}  {'P(flip)':>8}  {'Lift':>5}  bar")
    print(f"  {'-'*65}")
    misaligned = [o for o in obs_list if
                  (o.btc_direction == "up"   and o.market_zone_at == "down") or
                  (o.btc_direction == "down" and o.market_zone_at == "up")]
    for thr in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.75, 1.00]:
        sub = [o for o in misaligned if o.btc_move_abs >= thr]
        if not sub:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"  btc_move >= {thr:.2f}%          {len(sub):>5}  {rate:>7.1%}  {lift:>4.1f}x  {pct_bar(rate)}")

    # ── 6. Calibration check ──────────────────────────────────────────────────
    print(f"\n  CALIBRATION: implied_fair scale (0.5 + btc_move_pct * 0.5)")
    disc_vals = [o.discrepancy for o in obs_list]
    avg_disc  = sum(disc_vals) / len(disc_vals)
    std_disc  = math.sqrt(sum((d - avg_disc) ** 2 for d in disc_vals) / len(disc_vals))
    aligned   = sum(1 for d in disc_vals if abs(d) < 0.10)
    print(f"  Mean discrepancy : {avg_disc:+.4f}  (0 = perfectly calibrated)")
    print(f"  Std  discrepancy : {std_disc:.4f}")
    print(f"  Within ±0.10     : {aligned}/{n} ({100*aligned/n:.0f}%)")

    # ── 7. Summary ────────────────────────────────────────────────────────────
    print(f"\n  {'='*70}")
    print(f"  SUMMARY — top conditions sorted by P(flip):")
    print(f"  {'='*70}")
    scored = []
    for label, fn in full_combos:
        sub = [o for o in obs_list if fn(o)]
        if len(sub) < 10:
            continue
        rate = sum(1 for o in sub if o.flip_happened) / len(sub)
        scored.append((rate, len(sub), label))
    scored.sort(reverse=True)
    for rate, n_sub, label in scored[:15]:
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"  P={rate:.1%}  n={n_sub:>5}  lift={lift:.1f}x  [{label}]")

    if scored and scored[0][0] >= 0.50:
        print(f"\n  ✅ FOUND condition with P(flip) >= 50%: {scored[0][2]}")
        print(f"     P={scored[0][0]:.1%}  n={scored[0][1]}")
    else:
        best = scored[0] if scored else (0, 0, "none")
        print(f"\n  ⚠️  Best condition: P={best[0]:.1%}  n={best[1]}  [{best[2]}]")
        print(f"     The implied_fair scale (0.5 per 1% BTC move) may need recalibration.")
        print(f"     Try re-running with --scale to sweep the scale parameter.")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run(days: int, csv_path: Optional[str], verbose: bool, scale: float):
    print(f"\n{'='*70}")
    print(f"  BTC 5m Flip Predictor — BTC Price vs priceToBeat")
    print(f"  Days: {days} | Marks: {BOT_MARKS_SECS}s | Flip threshold: ±{FLIP_THRESHOLD}")
    print(f"  implied_fair scale: 0.5 + btc_move_pct * {scale}")
    print(f"{'='*70}\n")

    async with aiohttp.ClientSession() as session:
        raw_markets = await fetch_historical_markets(session, days)

        if not raw_markets:
            print("  No markets found.")
            return

        all_obs: List[MarkObs] = []
        skipped = 0
        no_ptb  = 0

        print(f"\n  Processing {len(raw_markets)} windows...")

        for mkt in raw_markets:
            meta = mkt.get("_event_metadata") or {}
            price_to_beat = float(meta.get("priceToBeat") or 0)
            if price_to_beat <= 0:
                no_ptb += 1
                continue

            token_ids_raw = mkt.get("clobTokenIds", "[]")
            try:
                token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            except Exception:
                skipped += 1
                continue
            if not token_ids:
                skipped += 1
                continue

            outcomes_raw = mkt.get("outcomes", '["Up","Down"]')
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            except Exception:
                outcomes = ["Up", "Down"]
            up_idx      = outcomes.index("Up") if "Up" in outcomes else 0
            up_token_id = token_ids[up_idx] if up_idx < len(token_ids) else token_ids[0]

            start_str = mkt.get("eventStartTime") or mkt.get("startDate", "")
            end_str   = mkt.get("endDate", "")
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                skipped += 1
                continue

            window_start_ts = int(start_dt.timestamp())
            if (end_dt - start_dt).total_seconds() > 600:
                end_dt = start_dt + timedelta(seconds=300)
            window_end_ts = int(end_dt.timestamp())

            slug = mkt.get("_event_slug") or mkt.get("slug", f"btc-updown-5m-{window_start_ts}")

            market_hist, btc_hist = await asyncio.gather(
                fetch_market_price_history(session, up_token_id,
                                           start_ts=window_start_ts - 90,
                                           end_ts=window_end_ts + 30),
                fetch_btc_klines(session,
                                 start_ts=window_start_ts - 60,
                                 end_ts=window_end_ts + 60),
            )

            if len(market_hist) < 3 or len(btc_hist) < 2:
                skipped += 1
                continue

            # Apply custom scale to implied_fair calculation
            obs_raw = build_observations(slug, window_start_ts, price_to_beat,
                                         market_hist, btc_hist)
            # Recompute implied_fair with custom scale
            for o in obs_raw:
                o.implied_fair = round(max(0.01, min(0.99, 0.5 + o.btc_move_pct * scale)), 4)
                o.discrepancy  = round(o.market_price_at - o.implied_fair, 4)
            all_obs.extend(obs_raw)

            if verbose and obs_raw:
                flips_here = sum(1 for o in obs_raw if o.flip_happened)
                btc_range  = (f"{min(o.btc_move_pct for o in obs_raw):+.3f}% to "
                              f"{max(o.btc_move_pct for o in obs_raw):+.3f}%")
                print(f"    {slug}: {len(obs_raw)} marks, {flips_here} flips, BTC: {btc_range}")

            await asyncio.sleep(0.05)

        print(f"\n  Done. {len(all_obs)} observations "
              f"({no_ptb} skipped: no priceToBeat, {skipped} skipped: bad data)")

        analyze(all_obs)

        if csv_path and all_obs:
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(asdict(all_obs[0]).keys()))
                writer.writeheader()
                for o in all_obs:
                    writer.writerow(asdict(o))
            print(f"\n  ✅ Observations saved to: {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="BTC Flip Predictor")
    parser.add_argument("--days",    type=int,   default=7,    help="Days of history")
    parser.add_argument("--csv",     type=str,   default=None, help="Save CSV to path")
    parser.add_argument("--verbose", action="store_true",      help="Per-window output")
    parser.add_argument("--scale",   type=float, default=0.5,
                        help="Scale for implied_fair: 0.5 + btc_move_pct * scale (default 0.5)")
    args = parser.parse_args()
    asyncio.run(run(args.days, args.csv, args.verbose, args.scale))


if __name__ == "__main__":
    main()
