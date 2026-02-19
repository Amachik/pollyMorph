#!/usr/bin/env python3
"""
Historical Backtester for Weather Markets

Fetches past resolved weather markets from Polymarket, determines outcomes,
runs our forecast model retroactively, and calculates comprehensive P&L
and accuracy metrics across a large historical dataset.

Usage:
    python -m tools.weather.backtest [--days 7] [--verbose]
"""

import asyncio
import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

import aiohttp
import numpy as np

from .config import (
    CITIES, City, GAMMA_API_BASE, MONTH_NAMES,
    MIN_EDGE, MAX_EDGE, MIN_FORECAST_PROB, TOP_N_BUCKETS,
    SPREAD_EDGE_BOOST, KELLY_FRACTION, MAX_BET_USDC,
    MAX_BETS_PER_MARKET, MARKET_DISAGREE_CAP,
)
from .markets import (
    WeatherMarket, WeatherOutcome, TempBucket,
    parse_bucket, _parse_market_outcome,
)
from .forecast import get_forecast, calculate_bucket_probabilities
from .calibration import get_or_compute_calibration, CityCalibration
from .wunderground import fetch_wu_daily
from .datalog import log_market, log_forecast


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class ResolvedMarket:
    """A fully resolved weather market with known outcome."""
    city_key: str
    city_name: str
    target_date: date
    winning_label: str          # Bucket label that won (from Polymarket resolution)
    wu_actual: Optional[int]    # WU observed high (whole degrees), if available
    # Per-bucket market prices at close
    market_probs: Dict[str, float]
    buckets: List[TempBucket]
    # Our forecast (if we can generate one)
    forecast_probs: Dict[str, float] = field(default_factory=dict)
    forecast_mean: float = 0.0
    forecast_std: float = 0.0
    # Scores
    market_brier: float = 0.0
    forecast_brier: float = 0.0


# ─── Fetch Resolved Markets ──────────────────────────────────────────────────

async def _fetch_resolved_event(
    session: aiohttp.ClientSession,
    slug: str,
    city_key: str,
    target_date: date,
) -> Optional[ResolvedMarket]:
    """Fetch a resolved weather event and extract the winning bucket."""
    url = f"{GAMMA_API_BASE}/events?slug={slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    events = data if isinstance(data, list) else [data]
    if not events:
        return None

    event = events[0]
    if not event.get("closed", False):
        return None  # Not resolved yet

    city = CITIES.get(city_key)
    if not city:
        return None

    raw_markets = event.get("markets", [])
    if not raw_markets:
        return None

    # Parse outcomes and find the winner
    # Use oneDayPriceChange to reconstruct pre-resolution market prices
    winning_label = None
    market_probs = {}      # Pre-resolution prices (24h before)
    buckets = []

    for m in raw_markets:
        # Parse bucket
        outcome = _parse_market_outcome(m, city.unit)
        if not outcome:
            continue

        label = outcome.bucket.label
        buckets.append(outcome.bucket)

        # Check if this bucket won: outcomePrices=['1','0'] means Yes won
        prices_raw = m.get("outcomePrices", "")
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        except (json.JSONDecodeError, TypeError):
            prices = []

        if isinstance(prices, list) and len(prices) >= 1:
            yes_price = float(prices[0])
            if yes_price >= 0.99:
                winning_label = label

        # Reconstruct pre-resolution price using oneDayPriceChange
        # price_24h_ago ≈ lastTradePrice - oneDayPriceChange
        last_trade = float(m.get("lastTradePrice", 0) or 0)
        one_day_chg = float(m.get("oneDayPriceChange", 0) or 0)
        pre_price = max(0.001, min(0.999, last_trade - one_day_chg))
        market_probs[label] = pre_price

    if not winning_label:
        return None

    buckets.sort(key=lambda b: b.low)

    return ResolvedMarket(
        city_key=city_key,
        city_name=city.name,
        target_date=target_date,
        winning_label=winning_label,
        wu_actual=None,  # Fill in separately
        market_probs=market_probs,
        buckets=buckets,
    )


async def fetch_all_resolved(
    session: aiohttp.ClientSession,
    days_back: int = 7,
) -> List[ResolvedMarket]:
    """Fetch all resolved weather markets for the past N days."""
    today = datetime.now(timezone.utc).date()
    resolved = []

    for days_ago in range(1, days_back + 1):
        d = today - timedelta(days=days_ago)
        month_name = MONTH_NAMES.get(d.month, "")
        if not month_name:
            continue

        tasks = []
        task_meta = []
        for city_key, city in CITIES.items():
            slug = f"highest-temperature-in-{city.slug_name}-on-{month_name}-{d.day}-{d.year}"
            tasks.append(_fetch_resolved_event(session, slug, city_key, d))
            task_meta.append((city_key, d))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result, (ck, dt) in zip(results, task_meta):
            if isinstance(result, ResolvedMarket):
                resolved.append(result)
                # Log resolved market data to persistent archive
                try:
                    log_market(
                        result.city_key, result.target_date,
                        [{"label": lbl, "market_prob": prob}
                         for lbl, prob in result.market_probs.items()],
                        resolved=True, winning_label=result.winning_label,
                    )
                except Exception:
                    pass

        await asyncio.sleep(0.3)

    return resolved


# ─── Brier Score ──────────────────────────────────────────────────────────────

def brier_score(probs: Dict[str, float], winner: str) -> float:
    """Calculate Brier score. Lower = better."""
    score = 0.0
    for label, prob in probs.items():
        actual = 1.0 if label == winner else 0.0
        score += (prob - actual) ** 2
    return score / max(len(probs), 1)


# ─── Main Backtest ────────────────────────────────────────────────────────────

async def backtest(days_back: int = 7, verbose: bool = False):
    """Run full historical backtest."""
    print("=" * 95)
    print(f"  🔬 HISTORICAL BACKTEST — Past {days_back} days of resolved weather markets")
    print("=" * 95)

    async with aiohttp.ClientSession() as session:
        # 1. Fetch all resolved markets
        print(f"\n📡 Fetching resolved markets...")
        resolved = await fetch_all_resolved(session, days_back)
        print(f"   Found {len(resolved)} resolved markets")

        if not resolved:
            print("   No resolved markets found. Exiting.")
            return

        # 2. Verify winners with WU actuals (parallel)
        print(f"\n🌡️  Verifying with Weather Underground (resolution source)...")
        wu_matches = 0
        wu_mismatches = 0
        wu_unavailable = 0

        _bt_sem = asyncio.Semaphore(8)

        async def _verify_wu(rm_idx: int):
            async with _bt_sem:
                rm = resolved[rm_idx]
                return rm_idx, await fetch_wu_daily(session, rm.city_key, rm.target_date)

        wu_verify_results = await asyncio.gather(
            *[_verify_wu(i) for i in range(len(resolved))], return_exceptions=True
        )
        for vr in wu_verify_results:
            if isinstance(vr, Exception):
                wu_unavailable += 1
                continue
            rm_idx, wu = vr
            rm = resolved[rm_idx]
            if wu and wu.is_complete:
                rm.wu_actual = wu.high_temp
                city = CITIES[rm.city_key]
                for b in rm.buckets:
                    low = b.low if b.low != float('-inf') else -9999
                    high = b.high if b.high != float('inf') else 9999
                    if low <= wu.high_temp <= high:
                        if b.label == rm.winning_label:
                            wu_matches += 1
                        else:
                            wu_mismatches += 1
                            if verbose:
                                print(f"   ⚠️  {rm.city_name} {rm.target_date}: "
                                      f"WU={wu.high_temp}° → \"{b.label}\" "
                                      f"but market resolved \"{rm.winning_label}\"")
                        break
            else:
                wu_unavailable += 1

        print(f"   WU verification: {wu_matches} match, {wu_mismatches} mismatch, "
              f"{wu_unavailable} unavailable")

        # 3. Load calibrations from cache (skip expensive recompute — MOS is primary path)
        print(f"\n📐 Loading calibrations...")
        from .calibration import load_calibration
        cached_cals = load_calibration()
        calibrations: Dict[str, CityCalibration] = {}
        city_keys = sorted(set(rm.city_key for rm in resolved))
        for ck in city_keys:
            if ck in cached_cals:
                calibrations[ck] = cached_cals[ck]
        if calibrations:
            print(f"   Loaded {len(calibrations)} cached calibrations")
        else:
            # Only recompute if no cache exists at all
            print(f"   No cache — computing calibrations in parallel...")
            async def _get_cal(ck):
                cal = await get_or_compute_calibration(session, ck)
                return ck, cal
            cal_results = await asyncio.gather(
                *[_get_cal(ck) for ck in city_keys], return_exceptions=True
            )
            for cr in cal_results:
                if isinstance(cr, Exception):
                    continue
                ck, cal = cr
                if cal:
                    calibrations[ck] = cal

        # 4. Generate forecasts for each resolved market (parallel, bounded)
        print(f"\n🔮 Generating retroactive forecasts for {len(resolved)} markets...")

        # Quick API health check — abort early if daily limit is hit
        import time as _time
        _api_ok = True
        try:
            _test_params = {
                "latitude": 40.71, "longitude": -74.01,
                "hourly": "temperature_2m",
                "start_date": resolved[0].target_date.isoformat(),
                "end_date": resolved[0].target_date.isoformat(),
                "models": "ecmwf_ifs025",
                "temperature_unit": "fahrenheit",
            }
            from .config import OPEN_METEO_ENSEMBLE_URL
            async with session.get(OPEN_METEO_ENSEMBLE_URL, params=_test_params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as _r:
                _d = await _r.json()
                if _d.get("error"):
                    print(f"   ⛔ Open-Meteo API unavailable: {_d.get('reason', 'daily limit')}")
                    print(f"   Skipping forecast generation — backtest will show market-only stats.")
                    _api_ok = False
        except Exception:
            pass  # Network error — try anyway

        _fc_sem = asyncio.Semaphore(10)
        _fc_done = 0
        _t0 = _time.time()

        async def _gen_forecast(rm_idx: int):
            nonlocal _fc_done
            rm = resolved[rm_idx]
            cal = calibrations.get(rm.city_key)
            async with _fc_sem:
                try:
                    ref_date = rm.target_date - timedelta(days=1)
                    forecast = await get_forecast(
                        session, rm.city_key, rm.target_date,
                        calibration=cal,
                        reference_date=ref_date,
                    )
                    if forecast and forecast.n_members > 0:
                        bucket_probs = calculate_bucket_probabilities(forecast, rm.buckets)
                        rm.forecast_probs = bucket_probs
                        rm.forecast_mean = forecast.mean
                        rm.forecast_std = forecast.std
                except Exception as e:
                    if verbose:
                        print(f"   ❌ {rm.city_name} {rm.target_date}: {e}")

            # Compute Brier scores
            if rm.forecast_probs:
                rm.forecast_brier = brier_score(rm.forecast_probs, rm.winning_label)
            rm.market_brier = brier_score(rm.market_probs, rm.winning_label)

            _fc_done += 1
            if _fc_done % 10 == 0:
                elapsed = _time.time() - _t0
                print(f"   ... {_fc_done}/{len(resolved)} markets ({elapsed:.0f}s)")

        if _api_ok:
            await asyncio.gather(
                *[_gen_forecast(i) for i in range(len(resolved))],
                return_exceptions=True,
            )
        forecast_count = sum(1 for rm in resolved if rm.forecast_probs)
        elapsed = _time.time() - _t0
        print(f"   Generated forecasts for {forecast_count}/{len(resolved)} markets ({elapsed:.0f}s)")

        # 5. Print detailed results
        scored = [rm for rm in resolved if rm.forecast_probs]

        if not scored:
            print(f"\n  ⚠️  No forecasts generated — cannot compute accuracy or P&L.")
            print(f"      (Open-Meteo API may be rate-limited. Try again later.)")
            print(f"\n{'=' * 95}")
            return

        if verbose:
            for rm in sorted(scored, key=lambda r: (r.target_date, r.city_name)):
                city = CITIES[rm.city_key]
                unit = city.unit
                wu_str = f" (WU: {rm.wu_actual}°{unit})" if rm.wu_actual is not None else ""
                better = "📊 US" if rm.forecast_brier < rm.market_brier else "🏪 Mkt"

                # Our top-1 pick
                our_top = max(rm.forecast_probs.items(), key=lambda x: x[1])
                mkt_top = max(rm.market_probs.items(), key=lambda x: x[1])

                print(f"\n  {rm.city_name:15s} {rm.target_date} "
                      f"→ \"{rm.winning_label}\"{wu_str}")
                print(f"    Our #{1}: \"{our_top[0]}\" {our_top[1]:.0%} | "
                      f"Mkt #{1}: \"{mkt_top[0]}\" {mkt_top[1]:.0%} | "
                      f"Brier: {rm.forecast_brier:.4f} vs {rm.market_brier:.4f} {better}")

        # 6. Aggregate statistics
        print(f"\n{'=' * 95}")
        print(f"  📊 BACKTEST RESULTS — {len(scored)} markets with forecasts")
        print(f"{'=' * 95}")

        # Group by date
        by_date = {}
        for rm in scored:
            by_date.setdefault(str(rm.target_date), []).append(rm)

        for dt_str in sorted(by_date.keys()):
            subset = by_date[dt_str]
            avg_fb = np.mean([r.forecast_brier for r in subset])
            avg_mb = np.mean([r.market_brier for r in subset])
            f_wins = sum(1 for r in subset if r.forecast_brier < r.market_brier)
            m_wins = sum(1 for r in subset if r.market_brier < r.forecast_brier)
            print(f"\n  {dt_str} ({len(subset)} markets):")
            print(f"    Brier: Forecast={avg_fb:.4f}  Market={avg_mb:.4f}  "
                  f"{'📊 US' if avg_fb < avg_mb else '🏪 Mkt'}")
            print(f"    Head-to-head: Us={f_wins} Market={m_wins}")

        # Overall Brier (exclude markets where forecast failed → brier=0 with no probs)
        scored_with_forecast = [r for r in scored if r.forecast_probs]
        all_fb = np.mean([r.forecast_brier for r in scored_with_forecast]) if scored_with_forecast else float('nan')
        all_mb = np.mean([r.market_brier for r in scored_with_forecast]) if scored_with_forecast else float('nan')
        improvement = (all_mb - all_fb) / all_mb * 100 if all_mb > 0 and not np.isnan(all_fb) else 0

        print(f"\n  {'─' * 85}")
        print(f"  OVERALL ({len(scored)} markets):")
        print(f"    Brier: Forecast={all_fb:.4f}  Market={all_mb:.4f}")
        if all_fb < all_mb:
            print(f"    ✅ OUR FORECAST IS {improvement:.1f}% MORE ACCURATE")
        else:
            print(f"    ❌ MARKET IS {-improvement:.1f}% MORE ACCURATE")

        f_top1 = sum(1 for r in scored
                     if max(r.forecast_probs, key=r.forecast_probs.get) == r.winning_label)
        m_top1 = sum(1 for r in scored
                     if max(r.market_probs, key=r.market_probs.get) == r.winning_label)
        print(f"    Top-1 correct: Forecast={f_top1}/{len(scored)} ({f_top1/len(scored):.0%}) "
              f"Market={m_top1}/{len(scored)} ({m_top1/len(scored):.0%})")

        # 7. Simulated P&L with smart filter
        print(f"\n  {'─' * 85}")
        print(f"  📈 SIMULATED P&L (smart filter: top-{TOP_N_BUCKETS}, "
              f"min prob {MIN_FORECAST_PROB:.0%}, Kelly sized)")
        print(f"  {'─' * 85}")

        bankroll = 164.0
        total_invested = 0.0
        total_return = 0.0
        bets_won = 0
        bets_lost = 0
        bet_details = []

        for rm in scored:
            ranked = sorted(rm.forecast_probs.items(), key=lambda x: -x[1])
            top_n = {label for label, _ in ranked[:TOP_N_BUCKETS]}

            # Spread penalty
            eff_min_edge = MIN_EDGE
            if len(ranked) >= 2:
                spread = ranked[0][1] - ranked[1][1]
                if spread < 0.05:
                    eff_min_edge = MIN_EDGE + SPREAD_EDGE_BOOST

            market_bets = []  # Track bets for this market (for MAX_BETS_PER_MARKET)
            for bucket_label, fp in rm.forecast_probs.items():
                if bucket_label not in top_n:
                    continue
                if fp < MIN_FORECAST_PROB:
                    continue
                mp = rm.market_probs.get(bucket_label, 0.0)
                edge = fp - mp
                # Market disagree cap: skip when market prices very low
                if mp < MARKET_DISAGREE_CAP:
                    continue
                if edge > eff_min_edge and edge < MAX_EDGE and mp > 0.01:
                    b = (1.0 / mp) - 1.0
                    kelly_f = ((b * fp) - (1.0 - fp)) / b if b > 0 else 0.0
                    bet = min(max(kelly_f * KELLY_FRACTION * bankroll, 0.0), MAX_BET_USDC)
                    if bet < 1.0:
                        continue
                    market_bets.append((bucket_label, fp, mp, edge, bet))

            # Limit bets per market
            market_bets.sort(key=lambda x: x[3], reverse=True)  # Sort by edge
            for bucket_label, fp, mp, edge, bet in market_bets[:MAX_BETS_PER_MARKET]:
                    total_invested += bet
                    won = bucket_label == rm.winning_label
                    if won:
                        payout = bet / mp
                        total_return += payout
                        bets_won += 1
                    else:
                        bets_lost += 1
                    bet_details.append((
                        rm.city_name, rm.target_date, bucket_label,
                        fp, mp, edge, bet, won,
                    ))

        if total_invested > 0:
            profit = total_return - total_invested
            roi = profit / total_invested * 100
            win_rate = bets_won / (bets_won + bets_lost) * 100

            print(f"\n  Total bets: {bets_won + bets_lost} "
                  f"({bets_won} won, {bets_lost} lost, {win_rate:.0f}% win rate)")
            print(f"  Invested: ${total_invested:.2f}")
            print(f"  Returned: ${total_return:.2f}")
            print(f"  Profit:   ${profit:+.2f} ({roi:+.1f}% ROI)")
            print(f"  Avg bet:  ${total_invested / (bets_won + bets_lost):.1f}")

            if verbose:
                print(f"\n  Bet details:")
                for city, dt, bl, fp, mp, edge, bet, won in sorted(
                    bet_details, key=lambda x: (x[1], x[0])
                ):
                    icon = "✅" if won else "❌"
                    payout = bet / mp if won else 0
                    print(f"    {icon} {city:15s} {dt} \"{bl}\" "
                          f"bet ${bet:.1f} @ {mp:.0%} (our: {fp:.0%}, edge: {edge:+.0%})"
                          f"{f' → ${payout:.1f}' if won else ''}")
        else:
            print(f"\n  No bets would have been placed")

        print(f"\n{'=' * 95}")


def main():
    parser = argparse.ArgumentParser(description="Weather market historical backtester")
    parser.add_argument("--days", type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed results")
    args = parser.parse_args()
    asyncio.run(backtest(args.days, args.verbose))


if __name__ == "__main__":
    main()
