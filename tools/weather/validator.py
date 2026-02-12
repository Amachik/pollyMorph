#!/usr/bin/env python3
"""
Weather Forecast Validator

Fetches actual observed temperatures and compares them against our forecast
probabilities and Polymarket odds. Calculates Brier scores to determine
who is more accurate: us or the market.

Usage:
    python -m tools.weather.validator [--days 3]
"""

import asyncio
import argparse
import csv
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from .config import (
    CITIES, City, OPEN_METEO_FORECAST_URL, MONTH_NAMES,
    MIN_EDGE, MAX_EDGE, MIN_FORECAST_PROB, TOP_N_BUCKETS,
    SPREAD_EDGE_BOOST, SAME_DAY_MIN_HOURS, KELLY_FRACTION, MAX_BET_USDC,
)
from .markets import (
    WeatherMarket, WeatherOutcome, TempBucket,
    discover_weather_markets, parse_bucket,
)
from .forecast import (
    get_forecast, calculate_bucket_probabilities,
    get_current_weather, sanity_check_probabilities,
    MAX_RETRIES, RETRY_DELAY,
)
from .calibration import get_or_compute_calibration, CityCalibration
from .wunderground import fetch_wu_daily, WUDailyResult

import pytz

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class ActualResult:
    """Observed actual temperature for a city/date."""
    city_key: str
    target_date: date
    actual_high: float          # Observed daily high temperature
    winning_bucket: str         # Which bucket it falls in
    source: str = "Open-Meteo"
    is_final: bool = False      # True if the day is fully over in local time


@dataclass
class ValidationRow:
    """One row of validation: a single market with forecast, market, and actual."""
    city_key: str
    city_name: str
    target_date: date
    actual_high: float
    winning_bucket: str
    is_final: bool
    # Per-bucket data
    buckets: List[str]
    forecast_probs: Dict[str, float]
    market_probs: Dict[str, float]
    # Scores
    forecast_brier: float       # Brier score for our forecast (lower = better)
    market_brier: float         # Brier score for market prices
    forecast_winner_prob: float # Our probability for the winning bucket
    market_winner_prob: float   # Market's probability for the winning bucket
    forecast_rank: int          # Our rank for the winning bucket (1 = we said most likely)
    market_rank: int            # Market's rank for the winning bucket


# ─── Fetch Actual Temperatures ────────────────────────────────────────────────

async def fetch_actual_high(
    session: aiohttp.ClientSession,
    city_key: str,
    target_date: date,
) -> Optional[ActualResult]:
    """
    Fetch the actual observed high temperature for a city on a given date.

    Primary source: Weather Underground (Polymarket's resolution source).
    Fallback: Open-Meteo historical data.

    WU reports whole-degree integers which exactly match Polymarket resolution
    precision. This eliminates the 1°C/°F rounding mismatch between Open-Meteo
    grid-point data and WU station observations.
    """
    city = CITIES.get(city_key)
    if not city:
        return None

    today = datetime.now(timezone.utc).date()
    days_ago = (today - target_date).days

    if days_ago < 0:
        return None  # Future date, no actual yet

    # ── Try Weather Underground first (resolution source) ──
    wu_result = await fetch_wu_daily(session, city_key, target_date)
    if wu_result and wu_result.n_observations > 0:
        return ActualResult(
            city_key=city_key,
            target_date=target_date,
            actual_high=float(wu_result.high_temp),  # Already whole degrees
            winning_bucket="",  # Filled in later
            source=f"WU:{wu_result.station_id}",
            is_final=wu_result.is_complete and target_date < today,
        )

    # ── Fallback: Open-Meteo ──
    if days_ago > 7:
        return None

    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "hourly": "temperature_2m",
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "timezone": city.tz,
    }
    if city.unit == "F":
        params["temperature_unit"] = "fahrenheit"

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                OPEN_METEO_FORECAST_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return None

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])

        if not temps:
            return None

        now_utc = datetime.now(timezone.utc)
        is_final = target_date < today

        if target_date == today:
            valid_temps = [(t, v) for t, v in zip(times, temps) if v is not None]
            if valid_temps:
                last_time = valid_temps[-1][0]
                try:
                    last_hour = int(last_time.split("T")[1].split(":")[0])
                    is_final = last_hour >= 18
                except (IndexError, ValueError):
                    pass

        valid = [v for v in temps if v is not None]
        if not valid:
            return None

        actual_high = max(valid)

        return ActualResult(
            city_key=city_key,
            target_date=target_date,
            actual_high=round(actual_high, 1),
            winning_bucket="",  # Filled in later
            source="Open-Meteo (fallback)",
            is_final=is_final,
        )

    return None


def find_winning_bucket(actual_high: float, buckets: List[TempBucket]) -> Optional[str]:
    """Determine which bucket the actual temperature falls into."""
    # Round to nearest integer (Weather Underground reports integers)
    rounded = round(actual_high)

    for bucket in buckets:
        low = bucket.low if bucket.low != float('-inf') else -9999
        high = bucket.high if bucket.high != float('inf') else 9999
        if low <= rounded <= high:
            return bucket.label

    # If no exact match, find closest
    best = None
    best_dist = float('inf')
    for bucket in buckets:
        mid = (bucket.low + bucket.high) / 2 if bucket.low != float('-inf') and bucket.high != float('inf') else (
            bucket.high if bucket.low == float('-inf') else bucket.low
        )
        dist = abs(rounded - mid)
        if dist < best_dist:
            best_dist = dist
            best = bucket.label
    return best


# ─── Brier Score ──────────────────────────────────────────────────────────────

def brier_score(probs: Dict[str, float], winning_bucket: str) -> float:
    """
    Calculate Brier score for a set of probability predictions.
    Brier = (1/N) * Σ (prob_i - outcome_i)² where outcome_i is 1 for winner, 0 otherwise.
    Lower is better. Perfect = 0, worst = 2.
    """
    score = 0.0
    n = len(probs)
    for label, prob in probs.items():
        outcome = 1.0 if label == winning_bucket else 0.0
        score += (prob - outcome) ** 2
    return score / n if n > 0 else 1.0


def rank_of_winner(probs: Dict[str, float], winning_bucket: str) -> int:
    """Get the rank (1-based) of the winning bucket in our probability ranking."""
    sorted_buckets = sorted(probs.items(), key=lambda x: -x[1])
    for i, (label, _) in enumerate(sorted_buckets):
        if label == winning_bucket:
            return i + 1
    return len(probs)


# ─── Main Validation ─────────────────────────────────────────────────────────

async def validate(days_back: int = 1, verbose: bool = False):
    """
    Run validation on recent weather markets.

    For each market that has actual temperature data available:
    1. Fetch our ensemble forecast
    2. Get market prices from Polymarket
    3. Fetch actual observed high temperature
    4. Compare accuracy (Brier score, rank of winner, probability of winner)
    """
    now = datetime.now(timezone.utc)
    today = now.date()

    print("=" * 95)
    print(f"  🔬 WEATHER FORECAST VALIDATOR — {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Checking markets for: {today} and {days_back} day(s) back")
    print("=" * 95)

    async with aiohttp.ClientSession() as session:
        # Discover markets
        print("\n📡 Discovering weather markets...")
        markets = await discover_weather_markets(session)

        # Filter to today and recent days
        target_markets = [
            m for m in markets
            if m.target_date <= today and (today - m.target_date).days <= days_back
        ]
        target_markets.sort(key=lambda m: (m.target_date, m.city_key))
        print(f"   Found {len(target_markets)} markets to validate")

        results: List[ValidationRow] = []

        # Compute calibrations for all cities
        city_keys = list(set(m.city_key for m in target_markets))
        calibrations: Dict[str, CityCalibration] = {}
        print(f"\n📐 Calibrating models for {len(city_keys)} cities...")
        for ck in sorted(city_keys):
            cal = await get_or_compute_calibration(session, ck)
            if cal:
                calibrations[ck] = cal
                city = CITIES.get(ck)
                model_info = ", ".join(
                    f"{m}: b={mc.mean_bias:+.1f} w={mc.weight:.0%}"
                    for m, mc in cal.models.items()
                )
                print(f"   ✅ {city.name if city else ck}: {model_info}")

        for market in target_markets:
            city = CITIES.get(market.city_key)
            if not city:
                continue

            # Fetch actual temperature
            actual = await fetch_actual_high(session, market.city_key, market.target_date)
            await asyncio.sleep(0.1)

            if not actual:
                if verbose:
                    print(f"\n   ⏭️  {city.name} {market.target_date} — no actual data yet")
                continue

            # Find winning bucket
            buckets = [o.bucket for o in market.outcomes]
            winner = find_winning_bucket(actual.actual_high, buckets)
            if not winner:
                continue
            actual.winning_bucket = winner

            # For today: get current weather for Bayesian conditioning
            current_high_val = None
            hours_remaining_val = None
            if market.target_date == today:
                current = await get_current_weather(session, market.city_key)
                if current:
                    current_high_val = current.daily_high_so_far
                    try:
                        local_tz = pytz.timezone(city.tz)
                        local_now = datetime.now(timezone.utc).astimezone(local_tz)
                        hours_remaining_val = max(0.0, 17 - local_now.hour - local_now.minute / 60.0)
                    except Exception:
                        hours_remaining_val = 4.0
                    await asyncio.sleep(0.1)

            # Fetch calibrated forecast
            cal = calibrations.get(market.city_key)
            forecast = await get_forecast(
                session, market.city_key, market.target_date,
                calibration=cal,
                current_high=current_high_val,
                hours_remaining=hours_remaining_val,
            )
            if not forecast:
                if verbose:
                    print(f"\n   ⏭️  {city.name} {market.target_date} — forecast unavailable")
                continue

            # Calculate our bucket probabilities
            bucket_probs = calculate_bucket_probabilities(forecast, buckets)

            # Get market probabilities
            market_probs = {o.bucket.label: o.market_prob for o in market.outcomes}

            # Calculate scores
            f_brier = brier_score(bucket_probs, winner)
            m_brier = brier_score(market_probs, winner)
            f_winner_prob = bucket_probs.get(winner, 0.0)
            m_winner_prob = market_probs.get(winner, 0.0)
            f_rank = rank_of_winner(bucket_probs, winner)
            m_rank = rank_of_winner(market_probs, winner)

            row = ValidationRow(
                city_key=market.city_key,
                city_name=city.name,
                target_date=market.target_date,
                actual_high=actual.actual_high,
                winning_bucket=winner,
                is_final=actual.is_final,
                buckets=[o.bucket.label for o in market.outcomes],
                forecast_probs=bucket_probs,
                market_probs=market_probs,
                forecast_brier=f_brier,
                market_brier=m_brier,
                forecast_winner_prob=f_winner_prob,
                market_winner_prob=m_winner_prob,
                forecast_rank=f_rank,
                market_rank=m_rank,
            )
            results.append(row)

            # Display individual result
            status = "✅ FINAL" if actual.is_final else "⏳ PARTIAL"
            unit = city.unit

            print(f"\n{'=' * 95}")
            print(f"  🌡️  {city.name} — {market.target_date} [{status}]")
            print(f"  Actual high: {actual.actual_high:.0f}°{unit} → Winner: \"{winner}\"")
            print(f"  Forecast: {forecast.mean:.1f}°{unit} ± {forecast.std:.1f}°{unit} ({forecast.n_members} members)")
            print(f"{'=' * 95}")

            # Show per-bucket comparison
            print(f"  {'Bucket':<22} {'Forecast':>9} {'Market':>9} {'Winner':>8}")
            print(f"  {'-' * 52}")

            for outcome in market.outcomes:
                label = outcome.bucket.label
                fp = bucket_probs.get(label, 0.0)
                mp = outcome.market_prob
                is_winner = "  ⭐" if label == winner else ""
                print(f"  {label:<22} {fp:>8.1%} {mp:>8.1%} {is_winner}")

            print(f"  {'-' * 52}")

            # Score comparison
            brier_diff = f_brier - m_brier
            who_better = "📊 US" if brier_diff < 0 else "🏪 MARKET"
            print(f"  Brier score: Forecast={f_brier:.4f}  Market={m_brier:.4f}  → {who_better} is better")
            print(f"  Winner prob: Forecast={f_winner_prob:.1%}  Market={m_winner_prob:.1%}")
            print(f"  Winner rank: Forecast=#{f_rank}  Market=#{m_rank}")

            # Did we have an edge signal?
            edge = f_winner_prob - m_winner_prob
            if edge > 0.08:
                print(f"  🟢 We had a {edge:+.1%} edge on the winning bucket — WOULD HAVE PROFITED")
            elif edge < -0.08:
                print(f"  🔴 Market was better by {-edge:.1%} on the winning bucket")
            else:
                print(f"  ↔️  Similar accuracy (edge: {edge:+.1%})")

        # ─── Summary Statistics ───────────────────────────────────────────────
        if not results:
            print("\n❌ No markets with actual data available for validation.")
            print("   Try again later when today's markets have more data,")
            print("   or tomorrow when today's markets are fully resolved.")
            return

        print(f"\n{'=' * 95}")
        print(f"  📊 VALIDATION SUMMARY — {len(results)} markets scored")
        print(f"{'=' * 95}")

        final_results = [r for r in results if r.is_final]
        partial_results = [r for r in results if not r.is_final]

        for label, subset in [("FINAL", final_results), ("PARTIAL (day ongoing)", partial_results)]:
            if not subset:
                continue

            print(f"\n  --- {label} results ({len(subset)} markets) ---")

            avg_f_brier = np.mean([r.forecast_brier for r in subset])
            avg_m_brier = np.mean([r.market_brier for r in subset])
            f_wins = sum(1 for r in subset if r.forecast_brier < r.market_brier)
            m_wins = sum(1 for r in subset if r.market_brier < r.forecast_brier)
            ties = len(subset) - f_wins - m_wins

            avg_f_winner = np.mean([r.forecast_winner_prob for r in subset])
            avg_m_winner = np.mean([r.market_winner_prob for r in subset])
            f_top1 = sum(1 for r in subset if r.forecast_rank == 1)
            m_top1 = sum(1 for r in subset if r.market_rank == 1)

            # Edge analysis: how often did we have a positive edge on the winner?
            positive_edges = [r for r in subset if r.forecast_winner_prob > r.market_winner_prob + 0.05]
            negative_edges = [r for r in subset if r.market_winner_prob > r.forecast_winner_prob + 0.05]

            print(f"  Avg Brier score:  Forecast={avg_f_brier:.4f}  Market={avg_m_brier:.4f}  "
                  f"{'📊 US better' if avg_f_brier < avg_m_brier else '🏪 Market better'}")
            print(f"  Head-to-head:     Forecast wins {f_wins} | Market wins {m_wins} | Ties {ties}")
            print(f"  Avg winner prob:  Forecast={avg_f_winner:.1%}  Market={avg_m_winner:.1%}")
            print(f"  Top-1 correct:    Forecast={f_top1}/{len(subset)}  Market={m_top1}/{len(subset)}")
            print(f"  Edge analysis:    Us > Market+5%: {len(positive_edges)}x | Market > Us+5%: {len(negative_edges)}x")

            # Simulated P&L — full smart filter matching scanner logic
            print(f"\n  📈 SIMULATED P&L (smart filter: top-{TOP_N_BUCKETS}, min prob {MIN_FORECAST_PROB:.0%},"
                  f" spread penalty, Kelly sized):")
            total_invested = 0.0
            total_return = 0.0
            bets_won = 0
            bets_lost = 0
            bet_details = []
            bankroll = 164.0

            for r in subset:
                # Rank our forecast probabilities
                ranked = sorted(r.forecast_probs.items(), key=lambda x: -x[1])
                top_n = {label for label, _ in ranked[:TOP_N_BUCKETS]}

                # Spread penalty
                eff_min_edge = MIN_EDGE
                if len(ranked) >= 2:
                    spread = ranked[0][1] - ranked[1][1]
                    if spread < 0.05:
                        eff_min_edge = MIN_EDGE + SPREAD_EDGE_BOOST

                for bucket_label, fp in r.forecast_probs.items():
                    if bucket_label not in top_n:
                        continue
                    if fp < MIN_FORECAST_PROB:
                        continue
                    mp = r.market_probs.get(bucket_label, 0.0)
                    edge = fp - mp
                    if edge > eff_min_edge and edge < MAX_EDGE and mp > 0.01:
                        # Kelly bet sizing
                        b = (1.0 / mp) - 1.0
                        kelly_f = ((b * fp) - (1.0 - fp)) / b if b > 0 else 0.0
                        bet = min(max(kelly_f * KELLY_FRACTION * bankroll, 0.0), MAX_BET_USDC)
                        if bet < 1.0:
                            continue
                        total_invested += bet
                        won = bucket_label == r.winning_bucket
                        if won:
                            payout = bet / mp
                            total_return += payout
                            bets_won += 1
                        else:
                            bets_lost += 1
                        bet_details.append((r.city_name, r.target_date, bucket_label, fp, mp, edge, bet, won))

            if total_invested > 0:
                profit = total_return - total_invested
                roi = profit / total_invested * 100
                print(f"  Bets: {bets_won} won, {bets_lost} lost ({bets_won + bets_lost} total)")
                print(f"  Invested: ${total_invested:.2f} | Returned: ${total_return:.2f} | "
                      f"Profit: ${profit:+.2f} ({roi:+.1f}% ROI)")
                for city, dt, bl, fp, mp, edge, bet, won in bet_details:
                    icon = "✅" if won else "❌"
                    payout = bet / mp if won else 0
                    print(f"    {icon} {city} {dt} \"{bl}\" — bet ${bet:.1f} @ {mp:.0%} "
                          f"(our: {fp:.0%}, edge: {edge:+.0%})"
                          f"{f' → won ${payout:.1f}' if won else ''}")
            else:
                print(f"  No bets would have been placed")

        # Summary line
        all_f_brier = np.mean([r.forecast_brier for r in results])
        all_m_brier = np.mean([r.market_brier for r in results])
        improvement = (all_m_brier - all_f_brier) / all_m_brier * 100 if all_m_brier > 0 else 0

        print(f"\n{'=' * 95}")
        if all_f_brier < all_m_brier:
            print(f"  ✅ OUR FORECAST IS {improvement:.1f}% MORE ACCURATE THAN THE MARKET (Brier score)")
        else:
            print(f"  ❌ MARKET IS {-improvement:.1f}% MORE ACCURATE THAN OUR FORECAST (Brier score)")
        print(f"  Forecast Brier: {all_f_brier:.4f}  |  Market Brier: {all_m_brier:.4f}")
        print(f"  (Lower Brier = better. Perfect = 0, random guess ~= 0.14 for 7 buckets)")
        print(f"{'=' * 95}")


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate weather forecast accuracy vs Polymarket")
    parser.add_argument("--days", type=int, default=1,
                        help="How many days back to validate (default: 1 = today only)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show skipped markets")
    args = parser.parse_args()

    try:
        asyncio.run(validate(days_back=args.days, verbose=args.verbose))
    except KeyboardInterrupt:
        print("\n\n👋 Stopped.")


if __name__ == "__main__":
    main()
