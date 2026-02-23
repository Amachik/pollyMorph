#!/usr/bin/env python3
"""
Weather Edge Scanner — Main Entry Point

Discovers active Polymarket weather markets, fetches ensemble forecasts,
calculates probability distributions, and identifies mispriced outcomes.

Usage:
    python -m tools.weather.scanner [--verbose] [--csv]
"""

import asyncio
import csv
import os
import sys
import argparse
from datetime import datetime, timezone, date
from typing import Dict, List, Tuple

import aiohttp

from .config import (
    CITIES, MIN_EDGE, MAX_EDGE, KELLY_FRACTION, MAX_BET_USDC, MIN_LIQUIDITY,
    TOP_N_BUCKETS, MIN_FORECAST_PROB, SAME_DAY_MIN_HOURS, SPREAD_EDGE_BOOST,
    MAX_BETS_PER_MARKET, MARKET_DISAGREE_CAP, MAX_LEAD_DAYS,
)
from .markets import WeatherMarket, WeatherOutcome, discover_weather_markets, market_summary
from .forecast import (
    get_forecast, calculate_bucket_probabilities, ForecastResult,
    get_current_weather, sanity_check_probabilities, CurrentWeather,
)
from .wunderground import fetch_wu_forecast_all_days, WUForecast
from .calibration import get_or_compute_calibration, CityCalibration
from .datalog import log_market, log_forecast, log_wu_forecast

import pytz

# ─── Edge Analysis ────────────────────────────────────────────────────────────

def calculate_kelly_bet(edge: float, market_prob: float, bankroll: float) -> float:
    """
    Calculate optimal bet size using fractional Kelly criterion.

    Kelly fraction = (bp - q) / b
    where b = decimal odds - 1, p = true prob, q = 1 - p

    We use KELLY_FRACTION (25%) of full Kelly for safety.
    """
    if edge <= 0 or market_prob <= 0 or market_prob >= 1.0:
        return 0.0

    true_prob = market_prob + edge
    if true_prob >= 1.0:
        true_prob = 0.98  # Cap

    # Decimal odds = 1 / market_prob (what you get paid per dollar if it wins)
    b = (1.0 / market_prob) - 1.0
    if b <= 0:
        return 0.0

    q = 1.0 - true_prob
    kelly = (b * true_prob - q) / b

    if kelly <= 0:
        return 0.0

    # Apply fractional Kelly and cap
    bet = kelly * KELLY_FRACTION * bankroll
    return min(bet, MAX_BET_USDC)


def expected_value(forecast_prob: float, market_price: float, bet_size: float) -> float:
    """Calculate expected value of a bet."""
    if market_price <= 0 or market_price >= 1.0:
        return 0.0
    # Win: get (1/market_price) * bet_size, lose: lose bet_size
    payout = bet_size / market_price
    ev = forecast_prob * payout - bet_size
    return ev


# ─── Display ──────────────────────────────────────────────────────────────────

def display_results(
    market: WeatherMarket,
    forecast: ForecastResult,
    bucket_probs: Dict[str, float],
    bankroll: float,
    verbose: bool = False,
    sanity_warnings: List[str] = None,
    observe_only: bool = False,
    city_edge_penalty: float = 0.0,
) -> List[Tuple]:  # (outcome, forecast_prob, edge, bet, ev, forecast_mean, forecast_std)
    """Display edge analysis for a single weather market."""
    city = CITIES.get(market.city_key)
    city_name = city.name if city else market.city_key
    unit = city.unit if city else "?"

    # Header
    print()
    print("=" * 90)
    print(f"  🌤️  {city_name} — {market.target_date.strftime('%B %d, %Y')}")
    print(f"  Forecast: {forecast.mean:.1f}°{unit} ± {forecast.std:.1f}°{unit}"
          f"  ({forecast.n_members} members from {', '.join(forecast.sources)})")
    print(f"  Market: ${market.volume:,.0f} volume, ${market.liquidity:,.0f} liquidity")
    if sanity_warnings:
        for w in sanity_warnings:
            print(f"  {w}")
    print("=" * 90)

    # Rank buckets by our forecast probability (for smart filtering)
    ranked = sorted(bucket_probs.items(), key=lambda x: -x[1])
    top_n_labels = {label for label, _ in ranked[:TOP_N_BUCKETS]}

    # Spread penalty: if our top-2 are very close, we're uncertain
    # City penalty: raise threshold for cities with poor historical accuracy
    effective_min_edge = MIN_EDGE + city_edge_penalty
    if len(ranked) >= 2:
        spread = ranked[0][1] - ranked[1][1]
        if spread < 0.05:  # Top-2 within 5% of each other
            effective_min_edge += SPREAD_EDGE_BOOST

    # Column headers
    print(f"  {'Bucket':<22} {'Forecast':>9} {'Market':>9} {'Edge':>9} {'EV/bet':>9} {'Signal':<18}")
    print("  " + "-" * 86)

    best_edge = 0.0
    best_outcome = None
    best_bet = 0.0
    opportunities = []

    for outcome in market.outcomes:
        label = outcome.bucket.label
        forecast_prob = bucket_probs.get(label, 0.0)
        market_prob = outcome.market_prob
        edge = forecast_prob - market_prob

        # Determine signal
        signal = ""
        bet = 0.0
        ev = 0.0

        # Smart filter: top-N + min forecast prob + min edge + not observe-only
        is_top_pick = label in top_n_labels
        meets_confidence = forecast_prob >= MIN_FORECAST_PROB

        # Market disagree cap: skip when market prices very low
        # (when market says <5%, it's usually right even if we disagree)
        market_too_cheap = market_prob < MARKET_DISAGREE_CAP

        if (edge > effective_min_edge and edge < MAX_EDGE
                and market_prob > 0.01 and is_top_pick and meets_confidence
                and not observe_only and not market_too_cheap):
            bet = calculate_kelly_bet(edge, market_prob, bankroll)
            ev = expected_value(forecast_prob, market_prob, bet)
            if bet >= 1.0:
                signal = f"🟢 BUY ${bet:.1f}"
                opportunities.append((outcome, forecast_prob, edge, bet, ev, forecast.mean, forecast.std))
                if edge > best_edge:
                    best_edge = edge
                    best_outcome = outcome
                    best_bet = bet
            elif verbose:
                signal = f"⚡ small"
        elif observe_only and edge > effective_min_edge and is_top_pick and meets_confidence:
            signal = "👁️ OBSERVE"
        elif edge > MIN_EDGE and not (is_top_pick and meets_confidence):
            if verbose:
                reason = "long-shot" if not is_top_pick else "low-conf"
                signal = f"⚠️ {reason}"
        elif edge < -MIN_EDGE and edge > -MAX_EDGE and market_prob > 0.05:
            signal = f"🔴 OVERPRICED"
        elif verbose:
            signal = "—"

        # Color coding for edge
        if abs(edge) > 0.15:
            edge_str = f"{edge:>+8.1%} ⚠"
        else:
            edge_str = f"{edge:>+8.1%}  "

        ev_str = f"${ev:>+.2f}" if bet > 0 else "     —"

        print(f"  {label:<22} {forecast_prob:>8.1%} {market_prob:>8.1%} {edge_str} {ev_str:>9} {signal:<18}")

    print("  " + "-" * 86)

    # Summary
    prob_sum = sum(bucket_probs.values())
    market_sum = sum(o.market_prob for o in market.outcomes)
    print(f"  Σ probs: forecast={prob_sum:.2f}, market={market_sum:.2f}")

    if best_outcome:
        print(f"  ⭐ BEST: BUY \"{best_outcome.bucket.label}\" @ ${best_outcome.market_prob:.2f}"
              f" — edge {best_edge:+.1%}, Kelly bet ${best_bet:.1f}")

    # Limit bets per market to reduce correlated losses
    if len(opportunities) > MAX_BETS_PER_MARKET:
        # Keep the ones with highest edge
        opportunities.sort(key=lambda x: x[2], reverse=True)  # x[2] = edge (unchanged)
        dropped = opportunities[MAX_BETS_PER_MARKET:]
        opportunities = opportunities[:MAX_BETS_PER_MARKET]
        if verbose:
            for d in dropped:
                print(f"  ⏭️  Skipped \"{d[0].bucket.label}\" (max {MAX_BETS_PER_MARKET} bets/market)")

    if not opportunities:
        print(f"  No actionable edges (threshold: {MIN_EDGE:.0%})")

    return opportunities


# ─── CSV Logging ──────────────────────────────────────────────────────────────

def log_edges_csv(
    market: WeatherMarket,
    forecast: ForecastResult,
    bucket_probs: Dict[str, float],
    csv_path: str,
):
    """Append edge data to a CSV file for historical tracking."""
    now = datetime.now(timezone.utc).isoformat()
    file_exists = os.path.exists(csv_path)

    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "city", "target_date", "bucket", "forecast_prob",
                "market_prob", "edge", "forecast_mean", "forecast_std",
                "n_members", "sources",
            ])

        for outcome in market.outcomes:
            label = outcome.bucket.label
            forecast_prob = bucket_probs.get(label, 0.0)
            edge = forecast_prob - outcome.market_prob
            writer.writerow([
                now, market.city_key, market.target_date.isoformat(),
                label, f"{forecast_prob:.4f}", f"{outcome.market_prob:.4f}",
                f"{edge:.4f}", f"{forecast.mean:.2f}", f"{forecast.std:.2f}",
                forecast.n_members, "|".join(forecast.sources),
            ])


# ─── Per-City Performance Tracking ───────────────────────────────────────────

def load_city_performance(min_bets: int = 3) -> Dict[str, Dict]:
    """
    Load per-city win/loss stats from position history.
    Returns {city_key: {'wins': N, 'losses': N, 'win_rate': float, 'edge_penalty': float}}

    edge_penalty: additional edge required for cities with poor track records.
    Cities with <33% win rate get a penalty; cities with >60% get a bonus.
    Only applied when we have >= min_bets resolved positions for that city.
    """
    import json as _json
    from pathlib import Path
    positions_file = Path(__file__).parent.parent.parent / "weather_positions.json"
    if not positions_file.exists():
        return {}

    try:
        with open(positions_file) as f:
            positions = _json.load(f)
    except Exception:
        return {}

    city_stats: Dict[str, Dict] = {}
    for pos in positions.values():
        status = pos.get("status", "")
        if status not in ("won", "lost"):
            continue
        ck = pos.get("city_key", "")
        if not ck:
            continue
        if ck not in city_stats:
            city_stats[ck] = {"wins": 0, "losses": 0}
        if status == "won":
            city_stats[ck]["wins"] += 1
        else:
            city_stats[ck]["losses"] += 1

    result = {}
    for ck, s in city_stats.items():
        total = s["wins"] + s["losses"]
        if total < min_bets:
            result[ck] = {"wins": s["wins"], "losses": s["losses"], "total": total,
                          "win_rate": None, "edge_penalty": 0.0}
            continue
        win_rate = s["wins"] / total
        # Penalty: +4% edge required per 10% below 40% win rate
        # Bonus: -2% edge required per 10% above 60% win rate (capped at -6%)
        if win_rate < 0.40:
            penalty = (0.40 - win_rate) * 0.40  # e.g. 20% win rate → +8% penalty
        elif win_rate > 0.60:
            penalty = -(win_rate - 0.60) * 0.20  # e.g. 80% win rate → -4% bonus
            penalty = max(penalty, -0.06)
        else:
            penalty = 0.0
        result[ck] = {"wins": s["wins"], "losses": s["losses"], "total": total,
                      "win_rate": win_rate, "edge_penalty": round(penalty, 3)}
    return result


# ─── Main Scanner ─────────────────────────────────────────────────────────────

async def run_scanner(bankroll: float = 164.0, verbose: bool = False, csv_log: bool = False):
    """Run the full weather edge scanner."""
    now = datetime.now(timezone.utc)
    print("=" * 90)
    print(f"  🌤️  WEATHER EDGE SCANNER — {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Bankroll: ${bankroll:.2f} | Min edge: {MIN_EDGE:.0%} | Kelly: {KELLY_FRACTION:.0%}")
    print("=" * 90)

    csv_path = os.path.join(os.path.dirname(__file__), "..", "weather_edges.csv") if csv_log else None

    # Load per-city performance stats to adjust edge thresholds
    city_performance = load_city_performance(min_bets=1)
    if city_performance:
        print("\n📊 City performance (edge adjustments):")
        for ck, perf in sorted(city_performance.items()):
            if perf["win_rate"] is not None:
                penalty_str = f"{perf['edge_penalty']:+.0%}" if perf["edge_penalty"] != 0 else "±0%"
                flag = "🔴" if perf["edge_penalty"] > 0.04 else ("🟡" if perf["edge_penalty"] > 0 else ("🟢" if perf["edge_penalty"] < 0 else "⚪"))
                print(f"   {flag} {ck:15s}: {perf['wins']}W/{perf['losses']}L "
                      f"({perf['win_rate']:.0%} wr) → edge adj {penalty_str}")

    async with aiohttp.ClientSession() as session:
        # Step 1: Discover markets
        print("\n📡 Discovering weather markets...")
        markets = await discover_weather_markets(session)
        print(f"   Found {len(markets)} active weather markets")

        if not markets:
            print("\n❌ No active weather markets found!")
            return []

        # Group markets by status and filter by lead time
        today = now.date()
        markets = [m for m in markets if (m.target_date - today).days <= MAX_LEAD_DAYS]
        active = [m for m in markets if m.target_date <= today]
        upcoming = [m for m in markets if m.target_date > today]
        print(f"   Active (today): {len(active)}, Upcoming (≤{MAX_LEAD_DAYS}d): {len(upcoming)}")

        for m in sorted(markets, key=lambda x: (x.target_date, x.city_key)):
            print(f"   • {market_summary(m)}")

        # Step 2: Compute calibrations for cities with markets
        city_keys = list(set(m.city_key for m in markets))
        calibrations: Dict[str, CityCalibration] = {}

        print(f"\n📐 Calibrating forecast models ({len(city_keys)} cities)...")
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
            else:
                print(f"   ⚠️  {ck}: no calibration data")

        # Step 3: Pre-fetch WU forecasts per city (1 API call per city covers all dates)
        # This avoids N×M WU calls (N cities × M markets per city)
        print(f"\n🌦️  Pre-fetching WU forecasts ({len(city_keys)} cities)...")
        wu_forecasts_by_city: Dict[str, Dict] = {}  # city_key → {date: WUForecast}
        async def _fetch_wu_city(ck: str):
            try:
                forecasts = await fetch_wu_forecast_all_days(session, ck)
                return ck, {fc.target_date: fc for fc in forecasts}
            except Exception:
                return ck, {}
        wu_results = await asyncio.gather(*[_fetch_wu_city(ck) for ck in city_keys])
        for ck, fc_map in wu_results:
            wu_forecasts_by_city[ck] = fc_map
            if fc_map:
                dates_str = ", ".join(str(d) for d in sorted(fc_map.keys()))
                print(f"   ✅ {CITIES[ck].name if ck in CITIES else ck}: {len(fc_map)} days ({dates_str})")
                # Log each WU forecast for later error calibration
                for fc_date, wu_fc in fc_map.items():
                    try:
                        log_wu_forecast(
                            ck, fc_date, wu_fc.high_temp,
                            lead_days=wu_fc.lead_days,
                        )
                    except Exception:
                        pass
            else:
                print(f"   ⚠️  {CITIES[ck].name if ck in CITIES else ck}: no WU forecast")

        # Pre-fetch current weather for cities with today's markets (1 call per city)
        today_city_keys = list(set(m.city_key for m in markets if m.target_date == today))
        current_weather_cache: Dict[str, CurrentWeather] = {}
        if today_city_keys:
            print(f"\n🌡️  Pre-fetching current weather ({len(today_city_keys)} cities)...")
            async def _fetch_current(ck: str):
                try:
                    return ck, await get_current_weather(session, ck)
                except Exception:
                    return ck, None
            cw_results = await asyncio.gather(*[_fetch_current(ck) for ck in today_city_keys])
            for ck, cw in cw_results:
                if cw:
                    current_weather_cache[ck] = cw

        # Step 4: Fetch forecasts and analyze edges
        all_opportunities = []

        # Sort: upcoming markets first (more time to trade), then by city
        markets.sort(key=lambda m: (m.target_date, m.city_key))

        for market in markets:
            city = CITIES.get(market.city_key)
            if not city:
                continue

            # Skip markets with very low liquidity
            if market.liquidity < MIN_LIQUIDITY and not verbose:
                if verbose:
                    print(f"\n   ⏭️  Skipping {city.name} {market.target_date} — low liquidity (${market.liquidity:.0f})")
                continue

            # For today's markets: use cached current weather (no extra API call)
            current_high_val = None
            hours_remaining_val = None
            sanity_warnings = []
            skip_same_day = False
            current = None

            if market.target_date == today:
                current = current_weather_cache.get(market.city_key)
                if current:
                    current_high_val = current.daily_high_so_far
                    # Estimate hours of potential warming remaining
                    try:
                        local_tz = pytz.timezone(city.tz)
                        local_now = datetime.now(timezone.utc).astimezone(local_tz)
                        peak_hour = 17  # 5 PM
                        hours_remaining_val = max(0.0, peak_hour - local_now.hour - local_now.minute / 60.0)
                    except Exception:
                        hours_remaining_val = 4.0

                    # Enhanced same-day detection using hourly trajectory
                    high_already_in = False
                    if current.forecast_remaining_max is not None:
                        remaining_upside = current.forecast_remaining_max - current_high_val
                        if remaining_upside <= 0.5:  # Less than 0.5° upside expected
                            high_already_in = True

                    # Skip same-day markets past the cutoff OR when high is already in
                    if hours_remaining_val < SAME_DAY_MIN_HOURS or high_already_in:
                        skip_same_day = True
                        if high_already_in:
                            reason = (f"⏰ OBSERVE ONLY: high likely locked at {current_high_val:.0f}° "
                                      f"(forecast remaining max: {current.forecast_remaining_max:.0f}°, "
                                      f"{hours_remaining_val:.1f}h left)")
                        else:
                            reason = (f"⏰ OBSERVE ONLY: {hours_remaining_val:.1f}h until peak "
                                      f"— market has real-time advantage")
                        if verbose:
                            print(f"\n   ⏭️  {city.name} {market.target_date} — {reason}")
                        sanity_warnings.append(reason)

            # Look up pre-fetched WU forecast for this city+date
            prefetched_wu = wu_forecasts_by_city.get(market.city_key, {}).get(market.target_date)

            # Fetch forecast with calibration and conditioning
            cal = calibrations.get(market.city_key)
            wu_str = f", WU={prefetched_wu.high_temp}°" if prefetched_wu else ""
            print(f"\n🌡️  Fetching forecast for {city.name} on {market.target_date}..."
                  f"{' [calibrated]' if cal else ''}"
                  f"{f' [conditioned: high={current_high_val:.0f}°, {hours_remaining_val:.1f}h left]' if current_high_val is not None else ''}"
                  f"{wu_str}")

            # Pass hourly trajectory data for smarter Bayesian conditioning
            forecast_remaining_max_val = None
            if market.target_date == today and current_high_val is not None:
                if current and current.forecast_remaining_max is not None:
                    forecast_remaining_max_val = current.forecast_remaining_max

            forecast = await get_forecast(
                session, market.city_key, market.target_date,
                calibration=cal,
                current_high=current_high_val,
                hours_remaining=hours_remaining_val,
                forecast_remaining_max=forecast_remaining_max_val,
                prefetched_wu=prefetched_wu,
            )

            if not forecast:
                print(f"   ⚠️  No forecast data available for {city.name}")
                continue

            # Calculate bucket probabilities
            buckets = [o.bucket for o in market.outcomes]
            bucket_probs = calculate_bucket_probabilities(forecast, buckets)

            # Sanity check warnings (post-Bayesian, just for display) — use cached current
            if market.target_date == today and current_high_val is not None and current:
                _, extra_warnings = sanity_check_probabilities(
                    bucket_probs, buckets, current,
                )
                # Merge: keep skip_same_day warning on top, add current temp info
                sanity_warnings = sanity_warnings + extra_warnings

            # Log market data and our forecast to persistent archive
            try:
                log_market(
                    market.city_key, market.target_date,
                    [{"label": o.bucket.label, "market_prob": o.market_prob,
                      "token_id": o.token_id} for o in market.outcomes],
                    liquidity=market.liquidity, slug=market.slug,
                )
                log_forecast(
                    market.city_key, market.target_date, bucket_probs,
                    forecast.mean, forecast.std, forecast.n_members,
                    mos_used=forecast.mos_used, sources=forecast.sources,
                )
            except Exception:
                pass  # Never let logging break scanning

            # Look up per-city edge penalty from historical performance
            city_perf = city_performance.get(market.city_key, {})
            city_edge_penalty = city_perf.get("edge_penalty", 0.0)

            # Display results (suppress BUY signals for observation-only markets)
            opportunities = display_results(
                market, forecast, bucket_probs, bankroll, verbose,
                sanity_warnings, observe_only=skip_same_day,
                city_edge_penalty=city_edge_penalty,
            )
            if not skip_same_day:
                all_opportunities.extend([(market, o, fp, e, b, ev, fm, fs) for o, fp, e, b, ev, fm, fs in opportunities])

            # Log to CSV
            if csv_path:
                log_edges_csv(market, forecast, bucket_probs, csv_path)

        # Final summary
        print("\n" + "=" * 90)
        print(f"  📊 SUMMARY — {len(all_opportunities)} actionable opportunities found")
        if all_opportunities:
            total_ev = sum(ev for _, _, _, _, _, ev, _, _ in all_opportunities)
            total_bet = sum(b for _, _, _, _, b, _, _, _ in all_opportunities)
            print(f"  Total bet: ${total_bet:.2f} | Total EV: ${total_ev:+.2f}")
            print()
            for market, outcome, fp, edge, bet, ev, fm, fs in sorted(all_opportunities, key=lambda x: -x[3]):
                city = CITIES.get(market.city_key)
                cn = city.name if city else market.city_key
                print(f"  🟢 {cn} {market.target_date} — BUY \"{outcome.bucket.label}\" "
                      f"@ ${outcome.market_prob:.2f} | edge {edge:+.1%} | bet ${bet:.1f} | EV ${ev:+.2f}")
        else:
            print("  No opportunities with sufficient edge. Markets may be efficient right now.")
            print("  Try again when new forecast data is released (every 6 hours).")
        print("=" * 90)

        if csv_path:
            print(f"\n📝 Edge data logged to: {csv_path}")

        return all_opportunities


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Weather Edge Scanner for Polymarket")
    parser.add_argument("--bankroll", type=float, default=164.0,
                        help="Available bankroll in USDC (default: 164)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show all markets including ones without edges")
    parser.add_argument("--csv", action="store_true",
                        help="Log edge data to CSV for historical tracking")
    args = parser.parse_args()

    try:
        asyncio.run(run_scanner(
            bankroll=args.bankroll,
            verbose=args.verbose,
            csv_log=args.csv,
        ))
    except KeyboardInterrupt:
        print("\n\n👋 Stopped.")


if __name__ == "__main__":
    main()
