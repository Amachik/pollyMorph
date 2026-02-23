#!/usr/bin/env python3
"""
Polymarket BTC 5m Flip Analyzer.

Fetches historical BTC 5-minute markets and their price histories,
detects "flip" events (price crossing 0.5 with significant magnitude),
and analyzes timing patterns to detect bot behavior.

A "flip" is defined as: within a single 5-min window, the Up price moves
from one side of 0.5 to the other by at least FLIP_THRESHOLD (e.g. 0.15),
i.e. goes from <0.35 to >0.65 or vice versa.

Usage:
    python -m tools.crypto.flip_analyzer
    python -m tools.crypto.flip_analyzer --days 14 --threshold 0.15
    python -m tools.crypto.flip_analyzer --days 7 --csv flips.csv
"""

import asyncio
import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple

import aiohttp

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BTC_SLUG_PREFIX = "btc-updown-5m-"
SERIES_SLUG = "btc-up-or-down-5m"

# ─── Flip Detection Parameters ────────────────────────────────────────────────

FLIP_THRESHOLD = 0.15       # price must cross 0.5 by at least this much on both sides
FLIP_MIN_SPEED = 60         # flip must complete within this many seconds (1 price-history bucket = 1min)
PRICE_HISTORY_FIDELITY = 1  # minutes — 1-minute resolution from CLOB

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class PricePoint:
    ts: int         # unix timestamp
    price: float    # Up token price


@dataclass
class FlipEvent:
    window_slug: str
    window_start_ts: int
    window_end_ts: int
    flip_start_ts: int          # when price was last on the "from" side
    flip_end_ts: int            # when price reached the "to" side
    flip_start_price: float
    flip_end_price: float
    direction: str              # "up_to_down" or "down_to_up"
    secs_into_window: float     # seconds from window start to flip_start
    flip_duration_secs: float   # how long the flip took
    pre_flip_price: float       # price just before flip started
    post_flip_price: float      # price just after flip completed
    magnitude: float            # abs(post_flip_price - pre_flip_price)
    window_date: str            # YYYY-MM-DD HH:MM UTC of window start


# ─── Gamma API: Fetch Historical Markets ─────────────────────────────────────

async def fetch_one_event(session: aiohttp.ClientSession, slug: str) -> Optional[dict]:
    """Fetch a single BTC 5m event by slug. Returns the first market dict or None."""
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
        # Inject the event slug into the market dict for reference
        m["_event_slug"] = ev.get("slug", slug)
        return m
    except Exception:
        return None


async def fetch_historical_markets(
    session: aiohttp.ClientSession,
    days_back: int = 7,
) -> List[dict]:
    """
    Fetch resolved BTC 5m markets from the past N days.
    Enumerates slugs by 5-min timestamp boundaries (same method as scanner.py).
    BTC 5m windows align to 300s UTC boundaries.
    Slug format: btc-updown-5m-{window_start_unix_ts}
    """
    now_ts = int(datetime.now(timezone.utc).timestamp())
    # Align to 5-min boundary
    current_window_start = (now_ts // 300) * 300
    # How many 5-min windows in days_back days
    total_windows = (days_back * 24 * 60) // 5  # e.g. 7 days = 2016 windows

    # Generate all slugs from oldest to newest (skip current/future)
    slugs = [
        f"{BTC_SLUG_PREFIX}{current_window_start - i * 300}"
        for i in range(1, total_windows + 1)  # skip i=0 (current, not resolved)
    ]

    print(f"  Fetching historical BTC 5m markets (past {days_back} days = {len(slugs)} windows)...")
    print(f"  Fetching in batches of 20 concurrent requests...")

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
            print(f"    Checked {batch_start}/{len(slugs)} windows, found {found} markets...")

        # If we've had many consecutive misses deep into history, the series may not go back that far
        if consecutive_misses > 100 and batch_start > 500:
            print(f"    Stopping early: {consecutive_misses} consecutive misses at window {batch_start}")
            break

        await asyncio.sleep(0.1)

    print(f"  Total markets fetched: {len(markets)}")
    return markets


# ─── CLOB API: Fetch Price History ───────────────────────────────────────────

async def fetch_price_history(
    session: aiohttp.ClientSession,
    token_id: str,
    start_ts: int,
    end_ts: int,
) -> List[PricePoint]:
    """
    Fetch 1-minute price history for a token from CLOB API.
    Returns list of PricePoint sorted by timestamp ascending.
    """
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": PRICE_HISTORY_FIDELITY,
    }
    try:
        async with session.get(f"{CLOB_API}/prices-history", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        history = data.get("history", [])
        points = [PricePoint(ts=int(h["t"]), price=float(h["p"])) for h in history]
        return sorted(points, key=lambda x: x.ts)
    except Exception as e:
        return []


# ─── Flip Detection ───────────────────────────────────────────────────────────

def detect_flips(
    slug: str,
    window_start_ts: int,
    window_end_ts: int,
    history: List[PricePoint],
    threshold: float = FLIP_THRESHOLD,
) -> List[FlipEvent]:
    """
    Detect flip events in a price history series.

    A flip is: price was on one side of 0.5 (by >= threshold), then moves
    to the other side (by >= threshold) within FLIP_MIN_SPEED seconds.

    We scan for the "committed" state: price >= 0.5+threshold (Up dominant)
    or <= 0.5-threshold (Down dominant), then look for a crossing.
    """
    if len(history) < 2:
        return []

    flips = []
    window_date = datetime.fromtimestamp(window_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    # State machine: track which "zone" we're in
    # zone: "up" (price > 0.5+threshold), "down" (price < 0.5-threshold), "neutral"
    def zone(p: float) -> str:
        if p >= 0.5 + threshold:
            return "up"
        elif p <= 0.5 - threshold:
            return "down"
        return "neutral"

    prev_committed_zone = None
    prev_committed_price = None
    prev_committed_ts = None

    for i, pt in enumerate(history):
        z = zone(pt.price)

        if z == "neutral":
            continue

        if prev_committed_zone is None:
            prev_committed_zone = z
            prev_committed_price = pt.price
            prev_committed_ts = pt.ts
            continue

        if z != prev_committed_zone:
            # Zone changed — this is a flip!
            flip_duration = pt.ts - prev_committed_ts
            secs_into = prev_committed_ts - window_start_ts

            flips.append(FlipEvent(
                window_slug=slug,
                window_start_ts=window_start_ts,
                window_end_ts=window_end_ts,
                flip_start_ts=prev_committed_ts,
                flip_end_ts=pt.ts,
                flip_start_price=prev_committed_price,
                flip_end_price=pt.price,
                direction="up_to_down" if prev_committed_zone == "up" else "down_to_up",
                secs_into_window=max(0.0, secs_into),
                flip_duration_secs=flip_duration,
                pre_flip_price=prev_committed_price,
                post_flip_price=pt.price,
                magnitude=abs(pt.price - prev_committed_price),
                window_date=window_date,
            ))

        # Update committed state (always track latest committed zone)
        prev_committed_zone = z
        prev_committed_price = pt.price
        prev_committed_ts = pt.ts

    return flips


# ─── Analysis ─────────────────────────────────────────────────────────────────

def analyze_flips(flips: List[FlipEvent]):
    """Print statistical analysis of flip timing patterns."""
    if not flips:
        print("\n  No flips detected.")
        return

    n = len(flips)
    print(f"\n{'='*70}")
    print(f"  FLIP ANALYSIS — {n} flips detected")
    print(f"{'='*70}")

    # Basic stats
    timings = [f.secs_into_window for f in flips]
    durations = [f.flip_duration_secs for f in flips]
    magnitudes = [f.magnitude for f in flips]

    def stats(vals, label):
        if not vals:
            return
        mn = min(vals)
        mx = max(vals)
        avg = sum(vals) / len(vals)
        sorted_v = sorted(vals)
        med = sorted_v[len(sorted_v) // 2]
        variance = sum((v - avg) ** 2 for v in vals) / len(vals)
        std = math.sqrt(variance)
        print(f"  {label}:")
        print(f"    min={mn:.1f}  max={mx:.1f}  mean={avg:.1f}  median={med:.1f}  std={std:.1f}")

    stats(timings, "Seconds into window when flip starts")
    stats(durations, "Flip duration (seconds)")
    stats(magnitudes, "Flip magnitude (price change)")

    # Direction breakdown
    up_to_down = [f for f in flips if f.direction == "up_to_down"]
    down_to_up = [f for f in flips if f.direction == "down_to_up"]
    print(f"\n  Direction: Up→Down={len(up_to_down)} ({100*len(up_to_down)/n:.0f}%)  "
          f"Down→Up={len(down_to_up)} ({100*len(down_to_up)/n:.0f}%)")

    # Timing histogram: 30-second buckets across the 300s window
    print(f"\n  Timing histogram (30s buckets, 0-300s window):")
    buckets = defaultdict(int)
    bucket_size = 30
    for f in flips:
        b = int(f.secs_into_window // bucket_size) * bucket_size
        b = min(b, 270)  # cap at last bucket
        buckets[b] += 1
    for b in range(0, 300, bucket_size):
        count = buckets[b]
        bar = "█" * count
        pct = 100 * count / n
        print(f"    {b:3d}-{b+bucket_size:3d}s: {bar:<20} {count:3d} ({pct:.0f}%)")

    # Look for periodicity: do flips cluster at specific seconds?
    print(f"\n  Top 10 most common flip-start seconds (rounded to nearest 10s):")
    rounded = defaultdict(int)
    for f in flips:
        r = round(f.secs_into_window / 10) * 10
        rounded[r] += 1
    top = sorted(rounded.items(), key=lambda x: -x[1])[:10]
    for sec, cnt in top:
        print(f"    {sec:3d}s: {cnt} flips ({100*cnt/n:.0f}%)")

    # Inter-flip interval analysis: time between consecutive flips across windows
    # Sort all flips by absolute timestamp
    sorted_flips = sorted(flips, key=lambda f: f.flip_start_ts)
    if len(sorted_flips) >= 2:
        intervals = []
        for i in range(1, len(sorted_flips)):
            gap = sorted_flips[i].flip_start_ts - sorted_flips[i-1].flip_start_ts
            if gap < 3600:  # only gaps < 1 hour (same trading session)
                intervals.append(gap)
        if intervals:
            print(f"\n  Inter-flip intervals (consecutive flips, gaps < 1h):")
            stats(intervals, "Gap between flips (seconds)")

            # Check for periodicity using autocorrelation-like bucketing
            print(f"\n  Inter-flip interval histogram (60s buckets):")
            ibuckets = defaultdict(int)
            for gap in intervals:
                b = int(gap // 60) * 60
                ibuckets[b] += 1
            top_gaps = sorted(ibuckets.items(), key=lambda x: -x[1])[:8]
            for gap_b, cnt in top_gaps:
                bar = "█" * cnt
                print(f"    {gap_b//60:3d}-{gap_b//60+1:3d}min: {bar:<20} {cnt}")

    # Flips per window
    windows_with_flips = len(set(f.window_slug for f in flips))
    print(f"\n  Windows with ≥1 flip: {windows_with_flips}")
    flip_counts = defaultdict(int)
    for f in flips:
        flip_counts[f.window_slug] += 1
    multi = {k: v for k, v in flip_counts.items() if v > 1}
    print(f"  Windows with ≥2 flips: {len(multi)}")
    if multi:
        max_flips = max(multi.values())
        print(f"  Max flips in one window: {max_flips}")

    # Time-of-day pattern: do flips cluster at certain UTC hours?
    print(f"\n  Flips by UTC hour:")
    hour_counts = defaultdict(int)
    for f in flips:
        dt = datetime.fromtimestamp(f.flip_start_ts, tz=timezone.utc)
        hour_counts[dt.hour] += 1
    for h in range(24):
        cnt = hour_counts[h]
        if cnt > 0:
            bar = "█" * cnt
            print(f"    {h:02d}:00: {bar:<30} {cnt}")

    # Day-of-week pattern
    print(f"\n  Flips by day of week (UTC):")
    dow_counts = defaultdict(int)
    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for f in flips:
        dt = datetime.fromtimestamp(f.flip_start_ts, tz=timezone.utc)
        dow_counts[dt.weekday()] += 1
    for d in range(7):
        cnt = dow_counts[d]
        bar = "█" * cnt
        print(f"    {dow_names[d]}: {bar:<30} {cnt}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run(days: int, threshold: float, csv_path: Optional[str], verbose: bool):
    print(f"\n{'='*70}")
    print(f"  Polymarket BTC 5m Flip Analyzer")
    print(f"  Days back: {days} | Flip threshold: ±{threshold:.2f} from 0.5")
    print(f"{'='*70}\n")

    async with aiohttp.ClientSession() as session:
        # Step 1: Fetch historical markets
        raw_markets = await fetch_historical_markets(session, days_back=days)

        if not raw_markets:
            print("  ❌ No historical markets found. Check API or series slug.")
            return

        # Step 2: For each market, fetch price history and detect flips
        all_flips: List[FlipEvent] = []
        total = len(raw_markets)
        processed = 0
        skipped = 0

        print(f"\n  Processing {total} markets for price history...")

        for mkt in raw_markets:
            # Extract token IDs
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

            # Parse window times
            # eventStartTime = actual 5-min window start
            # endDate = actual 5-min window end (eventStartTime + 300s)
            start_str = mkt.get("eventStartTime") or mkt.get("startDate", "")
            end_str = mkt.get("endDate", "")
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                skipped += 1
                continue

            window_start_ts = int(start_dt.timestamp())
            # If endDate is more than 10 minutes after start, use start+300 as window end
            # (endDate can sometimes be the series end, not the window end)
            raw_duration = (end_dt - start_dt).total_seconds()
            if raw_duration > 600:
                end_dt = start_dt + timedelta(seconds=300)
            window_end_ts = int(end_dt.timestamp())

            slug = mkt.get("_event_slug") or mkt.get("slug", f"btc-updown-5m-{window_start_ts}")

            # Fetch price history (add 60s buffer on each side)
            history = await fetch_price_history(
                session,
                up_token_id,
                start_ts=window_start_ts - 60,
                end_ts=window_end_ts + 60,
            )

            if verbose and history:
                prices = [p.price for p in history]
                mn, mx = min(prices), max(prices)
                print(f"    {slug}: {len(history)} points, price range [{mn:.3f}, {mx:.3f}]")

            # Detect flips
            flips = detect_flips(slug, window_start_ts, window_end_ts, history, threshold)
            all_flips.extend(flips)

            processed += 1
            if processed % 50 == 0:
                print(f"    Processed {processed}/{total} markets, {len(all_flips)} flips so far...")

            await asyncio.sleep(0.05)  # gentle rate limiting

        print(f"\n  Done. Processed={processed}, Skipped={skipped}, Flips={len(all_flips)}")

        # Step 3: Analyze
        analyze_flips(all_flips)

        # Step 4: Save CSV if requested
        if csv_path and all_flips:
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(asdict(all_flips[0]).keys()))
                writer.writeheader()
                for flip in sorted(all_flips, key=lambda x: x.flip_start_ts):
                    writer.writerow(asdict(flip))
            print(f"\n  ✅ Flips saved to: {csv_path}")

        # Step 5: Print sample flips
        if all_flips:
            print(f"\n  Sample flips (first 10):")
            print(f"  {'Date/Time':<20} {'Direction':<14} {'Into Window':>12} {'Duration':>10} {'Magnitude':>10}")
            print(f"  {'-'*70}")
            for f in sorted(all_flips, key=lambda x: x.flip_start_ts)[:10]:
                dt = datetime.fromtimestamp(f.flip_start_ts, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
                print(f"  {dt:<20} {f.direction:<14} {f.secs_into_window:>10.0f}s "
                      f"{f.flip_duration_secs:>8.0f}s  {f.magnitude:>9.3f}")


def main():
    parser = argparse.ArgumentParser(description="Polymarket BTC 5m Flip Analyzer")
    parser.add_argument("--days", type=int, default=7, help="Days of history to analyze (default: 7)")
    parser.add_argument("--threshold", type=float, default=0.15,
                        help="Flip threshold: price must cross 0.5 by this much on both sides (default: 0.15)")
    parser.add_argument("--csv", type=str, default=None, help="Save flip events to CSV file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose per-market output")
    args = parser.parse_args()

    asyncio.run(run(
        days=args.days,
        threshold=args.threshold,
        csv_path=args.csv,
        verbose=args.verbose,
    ))


if __name__ == "__main__":
    main()
