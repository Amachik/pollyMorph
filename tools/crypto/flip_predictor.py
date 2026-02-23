#!/usr/bin/env python3
"""
Polymarket BTC 5m Flip Predictor.

For every historical window, reconstructs the price state at each of the 4
candidate bot-fire marks (80s, 140s, 200s, 260s into the window) and labels
whether a flip actually happened at that mark.

Then computes conditional probabilities:
  P(flip | price_deviation, momentum, which_mark, prior_flip_in_window)

This tells us: given what we observe RIGHT BEFORE a candidate mark,
how confident can we be that a flip is about to happen?

Usage:
    python -m tools.crypto.flip_predictor --days 7
    python -m tools.crypto.flip_predictor --days 14 --csv /tmp/marks.csv
"""

import asyncio
import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple

import aiohttp

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
BTC_SLUG_PREFIX = "btc-updown-5m-"

FLIP_THRESHOLD = 0.15   # price must cross 0.5 by this much on both sides to count as flip
BOT_MARKS_SECS = [80, 140, 200, 260]   # candidate flip times (seconds into window)
MARK_WINDOW    = 30     # ±30s around each mark to detect a flip
LOOKBACK_SECS  = 70     # how many seconds before the mark to measure state


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class PricePoint:
    ts: int
    price: float


@dataclass
class MarkObservation:
    """State at one candidate bot-fire mark, with label of whether flip happened."""
    window_slug: str
    window_start_ts: int
    mark_index: int         # 0=80s, 1=140s, 2=200s, 3=260s
    mark_secs: int          # nominal seconds into window (80/140/200/260)

    # Features measured BEFORE the mark (state we'd know in real-time)
    price_before: float     # Up price ~60s before mark
    price_at_mark: float    # Up price at the mark
    price_after: float      # Up price ~60s after mark (for label validation)
    deviation_before: float # abs(price_before - 0.5)
    deviation_at_mark: float
    momentum: float         # price_at_mark - price_before (positive = moving up)
    zone_before: str        # committed zone 60s before mark
    committed_zone: str     # "up" / "down" / "neutral" at mark time
    prior_flip: bool        # did a flip already happen earlier in this window?
    mark_price_history_len: int  # how many price points we have (data quality)

    # Label: flip = committed zone changed between price_before and price_at_mark
    # i.e. the bot fired AT this mark and moved price from one side to the other
    flip_happened: bool     # zone_before != committed_zone (both committed, not neutral)
    flip_direction: str     # "up_to_down" / "down_to_up" / "none"
    flip_magnitude: float   # abs(price_at_mark - price_before)


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
        return m
    except Exception:
        return None


async def fetch_price_history(
    session: aiohttp.ClientSession,
    token_id: str,
    start_ts: int,
    end_ts: int,
) -> List[PricePoint]:
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": 1,
    }
    try:
        async with session.get(f"{CLOB_API}/prices-history", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        history = data.get("history", [])
        points = [PricePoint(ts=int(h["t"]), price=float(h["p"])) for h in history]
        return sorted(points, key=lambda x: x.ts)
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


# ─── Mark Analysis ────────────────────────────────────────────────────────────

def price_at_time(history: List[PricePoint], target_ts: int) -> Optional[float]:
    """Return the price closest to target_ts (within 90s), or None."""
    best = None
    best_dist = 90
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


def label_flip_at_mark(
    price_before: float,
    price_at_mark: float,
    threshold: float = FLIP_THRESHOLD,
) -> Tuple[bool, str, float]:
    """
    A flip AT this mark = the committed zone changed between price_before and price_at_mark.
    Both must be in committed zones (not neutral) and they must be opposite zones.
    Returns (flip_happened, direction, magnitude).
    """
    zb = zone(price_before, threshold)
    za = zone(price_at_mark, threshold)
    if zb == "neutral" or za == "neutral":
        return False, "none", 0.0
    if zb == za:
        return False, "none", 0.0
    direction = f"{zb}_to_{za}"
    magnitude = abs(price_at_mark - price_before)
    return True, direction, magnitude


def build_mark_observations(
    slug: str,
    window_start_ts: int,
    history: List[PricePoint],
) -> List[MarkObservation]:
    """
    For each of the 4 candidate marks, build a MarkObservation with
    pre-mark features and a flip label.
    """
    observations = []
    prior_flip = False

    for i, mark_secs in enumerate(BOT_MARKS_SECS):
        mark_ts = window_start_ts + mark_secs

        # Price ~60s before the mark (what we'd know in real-time before it fires)
        before_ts = mark_ts - 60
        # Price at the mark
        # Price ~60s after the mark (for validation / next-mark features)
        after_ts  = mark_ts + 60

        price_before = price_at_time(history, before_ts)
        price_at     = price_at_time(history, mark_ts)
        price_after  = price_at_time(history, after_ts)  # may be None for last mark

        if price_before is None or price_at is None:
            continue

        dev_before = abs(price_before - 0.5)
        dev_at     = abs(price_at - 0.5)
        momentum   = price_at - price_before
        zb         = zone(price_before)
        za         = zone(price_at)

        # Flip label: committed zone changed between before and at_mark
        flip_happened, flip_dir, flip_mag = label_flip_at_mark(price_before, price_at)

        obs = MarkObservation(
            window_slug=slug,
            window_start_ts=window_start_ts,
            mark_index=i,
            mark_secs=mark_secs,
            price_before=round(price_before, 4),
            price_at_mark=round(price_at, 4),
            price_after=round(price_after, 4) if price_after is not None else -1.0,
            deviation_before=round(dev_before, 4),
            deviation_at_mark=round(dev_at, 4),
            momentum=round(momentum, 4),
            zone_before=zb,
            committed_zone=za,
            prior_flip=prior_flip,
            mark_price_history_len=len(history),
            flip_happened=flip_happened,
            flip_direction=flip_dir,
            flip_magnitude=round(flip_mag, 4),
        )
        observations.append(obs)

        if flip_happened:
            prior_flip = True

    return observations


# ─── Conditional Probability Analysis ────────────────────────────────────────

def analyze_observations(obs_list: List[MarkObservation]):
    n = len(obs_list)
    n_flip = sum(1 for o in obs_list if o.flip_happened)
    base_rate = n_flip / n if n > 0 else 0

    print(f"\n{'='*70}")
    print(f"  PREDICTIVE ANALYSIS — {n} mark-observations, {n_flip} flips")
    print(f"  Base flip rate per mark: {base_rate:.1%}")
    print(f"{'='*70}")

    # ── 1. By mark index ──────────────────────────────────────────────────────
    print(f"\n  P(flip) by which mark (80s / 140s / 200s / 260s):")
    for idx, secs in enumerate(BOT_MARKS_SECS):
        subset = [o for o in obs_list if o.mark_index == idx]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        print(f"    Mark {secs:3d}s (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 2. By committed zone at mark ──────────────────────────────────────────
    print(f"\n  P(flip) by price zone at mark time:")
    for z in ["up", "down", "neutral"]:
        subset = [o for o in obs_list if o.committed_zone == z]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        print(f"    zone={z:<8} (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 3. By deviation bucket ────────────────────────────────────────────────
    print(f"\n  P(flip) by deviation from 0.5 at mark time:")
    dev_buckets = [(0.0, 0.05), (0.05, 0.10), (0.10, 0.15),
                   (0.15, 0.20), (0.20, 0.30), (0.30, 0.50)]
    for lo, hi in dev_buckets:
        subset = [o for o in obs_list if lo <= o.deviation_at_mark < hi]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        print(f"    dev [{lo:.2f},{hi:.2f}) (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 4. By prior flip in window ────────────────────────────────────────────
    print(f"\n  P(flip) by whether a flip already happened earlier in this window:")
    for pf in [False, True]:
        subset = [o for o in obs_list if o.prior_flip == pf]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        label = "prior flip=YES" if pf else "prior flip=NO "
        print(f"    {label} (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 5. By momentum direction ──────────────────────────────────────────────
    print(f"\n  P(flip) by momentum (price change in 60s before mark):")
    mom_buckets = [
        ("strong_down", lambda o: o.momentum < -0.10),
        ("mild_down",   lambda o: -0.10 <= o.momentum < -0.03),
        ("flat",        lambda o: -0.03 <= o.momentum <= 0.03),
        ("mild_up",     lambda o: 0.03 < o.momentum <= 0.10),
        ("strong_up",   lambda o: o.momentum > 0.10),
    ]
    for label, fn in mom_buckets:
        subset = [o for o in obs_list if fn(o)]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        print(f"    {label:<12} (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 6. Combined: zone + deviation (most actionable) ───────────────────────
    print(f"\n  P(flip) by zone AND deviation (most actionable combos):")
    combos = [
        ("up   + dev>0.20", lambda o: o.committed_zone == "up"   and o.deviation_at_mark > 0.20),
        ("up   + dev>0.30", lambda o: o.committed_zone == "up"   and o.deviation_at_mark > 0.30),
        ("down + dev>0.20", lambda o: o.committed_zone == "down" and o.deviation_at_mark > 0.20),
        ("down + dev>0.30", lambda o: o.committed_zone == "down" and o.deviation_at_mark > 0.30),
        ("neutral",         lambda o: o.committed_zone == "neutral"),
    ]
    for label, fn in combos:
        subset = [o for o in obs_list if fn(o)]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        print(f"    {label:<22} (n={len(subset):4d}): {rate:.1%}  {'▓'*int(rate*40)}")

    # ── 7. The FULL conditional: zone + deviation + prior_flip ────────────────
    print(f"\n  P(flip) — full conditional (zone + dev + prior_flip):")
    print(f"  {'Condition':<45} {'n':>5}  {'P(flip)':>8}  {'Lift':>6}")
    print(f"  {'-'*70}")
    full_combos = [
        ("up,   dev>0.20, no prior flip",
         lambda o: o.committed_zone=="up"   and o.deviation_at_mark>0.20 and not o.prior_flip),
        ("up,   dev>0.30, no prior flip",
         lambda o: o.committed_zone=="up"   and o.deviation_at_mark>0.30 and not o.prior_flip),
        ("down, dev>0.20, no prior flip",
         lambda o: o.committed_zone=="down" and o.deviation_at_mark>0.20 and not o.prior_flip),
        ("down, dev>0.30, no prior flip",
         lambda o: o.committed_zone=="down" and o.deviation_at_mark>0.30 and not o.prior_flip),
        ("up,   dev>0.20, had prior flip",
         lambda o: o.committed_zone=="up"   and o.deviation_at_mark>0.20 and o.prior_flip),
        ("down, dev>0.20, had prior flip",
         lambda o: o.committed_zone=="down" and o.deviation_at_mark>0.20 and o.prior_flip),
        ("neutral, no prior flip",
         lambda o: o.committed_zone=="neutral" and not o.prior_flip),
        ("neutral, had prior flip",
         lambda o: o.committed_zone=="neutral" and o.prior_flip),
    ]
    for label, fn in full_combos:
        subset = [o for o in obs_list if fn(o)]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        lift = rate / base_rate if base_rate > 0 else 0
        bar = "▓" * int(rate * 40)
        print(f"  {label:<45} {len(subset):>5}  {rate:>7.1%}  {lift:>5.1f}x  {bar}")

    # ── 8. Momentum + zone combo ──────────────────────────────────────────────
    print(f"\n  P(flip) — momentum INTO the mark (does price moving toward 0.5 predict flip?):")
    print(f"  (momentum = price_at_mark - price_60s_before)")
    print(f"  {'Condition':<50} {'n':>5}  {'P(flip)':>8}")
    print(f"  {'-'*65}")
    mom_zone_combos = [
        # Price is in 'up' zone and momentum is downward (moving toward 0.5) — flip likely
        ("up zone, momentum DOWN (toward 0.5)",
         lambda o: o.committed_zone=="up" and o.momentum < -0.05),
        ("up zone, momentum FLAT",
         lambda o: o.committed_zone=="up" and -0.05 <= o.momentum <= 0.05),
        ("up zone, momentum UP (away from 0.5)",
         lambda o: o.committed_zone=="up" and o.momentum > 0.05),
        # Price is in 'down' zone
        ("down zone, momentum UP (toward 0.5)",
         lambda o: o.committed_zone=="down" and o.momentum > 0.05),
        ("down zone, momentum FLAT",
         lambda o: o.committed_zone=="down" and -0.05 <= o.momentum <= 0.05),
        ("down zone, momentum DOWN (away from 0.5)",
         lambda o: o.committed_zone=="down" and o.momentum < -0.05),
    ]
    for label, fn in mom_zone_combos:
        subset = [o for o in obs_list if fn(o)]
        if not subset:
            continue
        rate = sum(1 for o in subset if o.flip_happened) / len(subset)
        lift = rate / base_rate if base_rate > 0 else 0
        bar = "▓" * int(rate * 40)
        print(f"  {label:<50} {len(subset):>5}  {rate:>7.1%}  {lift:>5.1f}x  {bar}")

    # ── 9. Best single predictor summary ─────────────────────────────────────
    print(f"\n  {'='*70}")
    print(f"  SUMMARY: Best predictive conditions (sorted by P(flip))")
    print(f"  {'='*70}")

    # Enumerate all combinations and find the best ones with n >= 20
    all_conditions = []
    for z_val in ["up", "down", "neutral"]:
        for dev_lo in [0.15, 0.20, 0.25, 0.30]:
            for pf in [False, True]:
                for mom_dir in ["toward", "away", "flat", "any"]:
                    def make_fn(zv, dl, pfv, md):
                        def fn(o):
                            if o.committed_zone != zv:
                                return False
                            if o.deviation_at_mark < dl:
                                return False
                            if o.prior_flip != pfv:
                                return False
                            if md == "toward":
                                # momentum toward 0.5
                                if zv == "up" and o.momentum >= 0:
                                    return False
                                if zv == "down" and o.momentum <= 0:
                                    return False
                            elif md == "away":
                                if zv == "up" and o.momentum <= 0:
                                    return False
                                if zv == "down" and o.momentum >= 0:
                                    return False
                            elif md == "flat":
                                if abs(o.momentum) > 0.05:
                                    return False
                            return True
                        return fn
                    fn = make_fn(z_val, dev_lo, pf, mom_dir)
                    subset = [o for o in obs_list if fn(o)]
                    if len(subset) < 15:
                        continue
                    rate = sum(1 for o in subset if o.flip_happened) / len(subset)
                    label = (f"zone={z_val}, dev>={dev_lo:.2f}, "
                             f"prior={'Y' if pf else 'N'}, mom={mom_dir}")
                    all_conditions.append((rate, len(subset), label))

    all_conditions.sort(reverse=True)
    print(f"\n  Top conditions with P(flip) >= 50% and n >= 15:")
    shown = 0
    for rate, n_sub, label in all_conditions:
        if rate < 0.50:
            break
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"    P={rate:.1%}  n={n_sub:4d}  lift={lift:.1f}x  [{label}]")
        shown += 1
        if shown >= 20:
            break

    if shown == 0:
        print("    (none found with P >= 50%)")

    # ── 10. What does NOT predict a flip ─────────────────────────────────────
    print(f"\n  Conditions with LOWEST P(flip) (safe to skip):")
    shown = 0
    for rate, n_sub, label in reversed(all_conditions):
        if rate > 0.15:
            break
        lift = rate / base_rate if base_rate > 0 else 0
        print(f"    P={rate:.1%}  n={n_sub:4d}  lift={lift:.1f}x  [{label}]")
        shown += 1
        if shown >= 10:
            break


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run(days: int, csv_path: Optional[str], verbose: bool):
    print(f"\n{'='*70}")
    print(f"  BTC 5m Flip Predictor — Conditional Probability Analysis")
    print(f"  Days: {days} | Bot marks: {BOT_MARKS_SECS}s | Flip threshold: ±{FLIP_THRESHOLD}")
    print(f"{'='*70}\n")

    async with aiohttp.ClientSession() as session:
        raw_markets = await fetch_historical_markets(session, days)

        if not raw_markets:
            print("  No markets found.")
            return

        all_obs: List[MarkObservation] = []
        total = len(raw_markets)
        skipped = 0

        print(f"\n  Building mark observations for {total} windows...")

        for mkt in raw_markets:
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

            up_idx = outcomes.index("Up") if "Up" in outcomes else 0
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
            raw_dur = (end_dt - start_dt).total_seconds()
            if raw_dur > 600:
                end_dt = start_dt + timedelta(seconds=300)
            window_end_ts = int(end_dt.timestamp())

            slug = mkt.get("_event_slug") or mkt.get("slug", f"btc-updown-5m-{window_start_ts}")

            # Fetch full window price history with generous buffer
            history = await fetch_price_history(
                session, up_token_id,
                start_ts=window_start_ts - 90,
                end_ts=window_end_ts + 30,
            )

            if len(history) < 3:
                skipped += 1
                continue

            obs = build_mark_observations(slug, window_start_ts, history)
            all_obs.extend(obs)

            if verbose and obs:
                flips_here = sum(1 for o in obs if o.flip_happened)
                print(f"    {slug}: {len(history)} pts, {len(obs)} marks, {flips_here} flips")

            await asyncio.sleep(0.05)

        print(f"  Done. {len(all_obs)} mark-observations from {total - skipped} windows "
              f"({skipped} skipped)")

        analyze_observations(all_obs)

        if csv_path and all_obs:
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(asdict(all_obs[0]).keys()))
                writer.writeheader()
                for o in all_obs:
                    writer.writerow(asdict(o))
            print(f"\n  ✅ Observations saved to: {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="BTC 5m Flip Predictor")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--csv", type=str, default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(days=args.days, csv_path=args.csv, verbose=args.verbose))


if __name__ == "__main__":
    main()
