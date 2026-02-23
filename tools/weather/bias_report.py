"""
Per-City Temperature Error Tracker

Reads resolved positions from weather_positions.json, backfills missing WU
actual temperatures, computes per-city directional bias (temp_error), and
prints a report showing where the bot is systematically wrong.

temp_error = actual_temp - forecast_mean
  positive → we predicted too cold (model runs cold, need to warm up)
  negative → we predicted too warm (model runs hot, need to cool down)

Usage:
    python -m tools.weather.bias_report            # report only
    python -m tools.weather.bias_report --backfill # fetch missing WU actuals first
"""

import asyncio
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

POSITIONS_FILE = Path(__file__).parent.parent.parent / "weather_positions.json"


def load_positions() -> Dict:
    if not POSITIONS_FILE.exists():
        return {}
    with open(POSITIONS_FILE) as f:
        return json.load(f)


def save_positions(positions: Dict) -> None:
    tmp = POSITIONS_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(positions, f, indent=2)
    tmp.replace(POSITIONS_FILE)


async def backfill_actuals(positions: Dict) -> int:
    """
    For resolved positions missing actual_temp, fetch from WU and compute temp_error.
    Returns count of positions backfilled.
    """
    from .wunderground import fetch_wu_daily

    to_fill = [
        (k, p) for k, p in positions.items()
        if p.get("status") in ("won", "lost")
        and p.get("actual_temp") is None
    ]

    if not to_fill:
        return 0

    print(f"   Backfilling {len(to_fill)} positions with WU actuals...")
    filled = 0

    async with aiohttp.ClientSession() as session:
        for key, pos in to_fill:
            try:
                td = date.fromisoformat(pos["target_date"])
                wu = await fetch_wu_daily(session, pos["city_key"], td)
                if wu and wu.is_complete:
                    pos["actual_temp"] = wu.high_temp
                    if pos.get("forecast_mean") is not None:
                        pos["temp_error"] = wu.high_temp - pos["forecast_mean"]
                    filled += 1
                await asyncio.sleep(0.3)  # gentle rate limit
            except Exception:
                continue

    if filled:
        save_positions(positions)
        print(f"   Backfilled {filled} positions.")

    return filled


def _infer_error_from_bucket(
    actual_temp: Optional[int],
    bucket_label: str,
    status: str,
) -> Optional[float]:
    """
    Infer approximate temp_error from bucket label + actual WU temp.

    When we won: actual was inside the bucket → error ≈ 0 (no directional info)
    When we lost: actual was outside the bucket → we can tell which direction.

    Examples:
      bucket="54°F or higher", actual=52, lost → we predicted too warm (+2° error, we ran HOT)
      bucket="38-39°F", actual=41, lost → we predicted too cold (-2° error, we ran COLD)
      bucket="30°C", actual=28, lost → we predicted too warm (+2° error, we ran HOT)

    Returns None if bucket can't be parsed or if we won (no directional info).
    """
    import re

    if actual_temp is None or not bucket_label:
        return None

    # Wins give no directional info (actual was in bucket)
    if status == "won":
        return None

    label = bucket_label.strip()

    # Pattern: "X or below" / "X°C or below"
    m = re.match(r"^(-?\d+(?:\.\d+)?)[°]?[CF]?\s+or\s+below$", label, re.IGNORECASE)
    if m:
        threshold = float(m.group(1))
        # We bet on ≤threshold, actual was above → actual > threshold → we ran cold
        # error = actual - threshold (positive = we ran cold)
        if actual_temp > threshold:
            return float(actual_temp - threshold)
        return None

    # Pattern: "X or higher" / "X°C or higher"
    m = re.match(r"^(-?\d+(?:\.\d+)?)[°]?[CF]?\s+or\s+higher$", label, re.IGNORECASE)
    if m:
        threshold = float(m.group(1))
        # We bet on ≥threshold, actual was below → actual < threshold → we ran hot
        # error = actual - threshold (negative = we ran hot)
        if actual_temp < threshold:
            return float(actual_temp - threshold)
        return None

    # Pattern: "X-Y" range bucket (e.g. "38-39°F", "30-31°C")
    m = re.match(r"^(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", label)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        mid = (lo + hi) / 2.0
        # error = actual - mid_of_bucket
        return float(actual_temp - mid)

    # Pattern: exact value "30°C" or "54°F"
    m = re.match(r"^(-?\d+(?:\.\d+)?)[°]?[CF]?$", label)
    if m:
        predicted = float(m.group(1))
        return float(actual_temp - predicted)

    return None


def compute_city_bias(positions: Dict) -> Dict[str, Dict]:
    """
    Compute per-city error statistics from resolved positions.

    Returns dict keyed by city_key with:
      n_total       — resolved positions
      n_with_error  — positions that have temp_error
      mean_error    — avg(actual - predicted): + = we run cold, - = we run hot
      mae           — mean absolute error
      errors        — list of individual errors (chronological)
      win_rate      — fraction won
      direction     — "COLD" / "HOT" / "OK"
    """
    city_data: Dict[str, Dict] = defaultdict(lambda: {
        "errors": [], "wins": 0, "losses": 0, "dates": []
    })

    for pos in positions.values():
        status = pos.get("status")
        if status not in ("won", "lost"):
            continue
        ck = pos.get("city_key", "")
        if not ck:
            continue

        if status == "won":
            city_data[ck]["wins"] += 1
        else:
            city_data[ck]["losses"] += 1

        err = pos.get("temp_error")

        # Fallback: infer directional error from bucket label + actual_temp
        # when forecast_mean is missing (old positions placed before tracking was added)
        if err is None:
            err = _infer_error_from_bucket(
                pos.get("actual_temp"),
                pos.get("bucket_label", ""),
                status,
            )

        if err is not None:
            city_data[ck]["errors"].append(err)
            city_data[ck]["dates"].append(pos.get("target_date", ""))

    result = {}
    for ck, d in city_data.items():
        total = d["wins"] + d["losses"]
        errors = d["errors"]
        mean_err = sum(errors) / len(errors) if errors else None
        mae = sum(abs(e) for e in errors) / len(errors) if errors else None

        if mean_err is None:
            direction = "unknown"
        elif mean_err > 0.5:
            direction = "COLD"   # we predicted too cold → actual was warmer
        elif mean_err < -0.5:
            direction = "HOT"    # we predicted too warm → actual was cooler
        else:
            direction = "OK"

        result[ck] = {
            "n_total": total,
            "n_with_error": len(errors),
            "wins": d["wins"],
            "losses": d["losses"],
            "win_rate": d["wins"] / total if total else 0,
            "mean_error": mean_err,
            "mae": mae,
            "errors": errors,
            "dates": d["dates"],
            "direction": direction,
        }

    return result


def print_bias_report(city_bias: Dict[str, Dict]) -> None:
    """Print a formatted per-city bias report."""
    print()
    print("=" * 80)
    print("  📊 PER-CITY TEMPERATURE ERROR REPORT")
    print("  temp_error = actual − predicted  |  + = we ran COLD  |  − = we ran HOT")
    print("=" * 80)

    if not city_bias:
        print("  No resolved positions yet.")
        print("=" * 80)
        return

    # Sort: cities with error data first (by abs mean error), then by name
    def sort_key(item):
        ck, d = item
        has_data = d["n_with_error"] > 0
        abs_err = abs(d["mean_error"]) if d["mean_error"] is not None else 0
        return (not has_data, -abs_err, ck)

    sorted_cities = sorted(city_bias.items(), key=sort_key)

    for ck, d in sorted_cities:
        total = d["n_total"]
        wr = d["win_rate"]
        wr_flag = "✅" if wr >= 0.50 else ("🟡" if wr >= 0.35 else "🔴")

        if d["n_with_error"] == 0:
            print(f"\n  {wr_flag} {ck:<15}  {d['wins']}W/{d['losses']}L ({wr:.0%} wr)"
                  f"  │  no temp data yet")
            continue

        me = d["mean_error"]
        mae = d["mae"]
        n = d["n_with_error"]
        direction = d["direction"]

        dir_icon = {"COLD": "🥶", "HOT": "🔥", "OK": "✅", "unknown": "❓"}.get(direction, "❓")
        bar_len = min(20, int(abs(me) * 4)) if me else 0
        bar = ("←" * bar_len) if me < 0 else ("→" * bar_len)
        bar_str = f"HOT {'←' * bar_len}" if me < 0 else f"{'→' * bar_len} COLD"

        print(f"\n  {wr_flag} {ck:<15}  {d['wins']}W/{d['losses']}L ({wr:.0%} wr)"
              f"  │  {dir_icon} mean_err={me:+.2f}°  MAE={mae:.2f}°  n={n}")

        # Sparkline of recent errors
        recent = d["errors"][-10:]
        recent_dates = d["dates"][-10:]
        spark = "  ".join(
            f"{e:+.1f}°" for e in recent
        )
        print(f"     Recent errors (oldest→newest): {spark}")

        # Trend: is bias getting better or worse?
        if len(d["errors"]) >= 6:
            first_half = d["errors"][:len(d["errors"]) // 2]
            second_half = d["errors"][len(d["errors"]) // 2:]
            trend = sum(second_half) / len(second_half) - sum(first_half) / len(first_half)
            trend_str = f"trend={trend:+.2f}° ({'improving ✅' if abs(trend) < abs(me) else 'worsening ⚠️'})"
            print(f"     {trend_str}")

        # Actionable recommendation
        if direction == "COLD" and abs(me) > 1.0:
            print(f"     ⚡ ACTION: MOS is running {abs(me):.1f}° cold for {ck}. "
                  f"Consider raising MOS_HISTORY_DAYS or checking residual bias.")
        elif direction == "HOT" and abs(me) > 1.0:
            print(f"     ⚡ ACTION: MOS is running {abs(me):.1f}° hot for {ck}. "
                  f"Residual correction may be overcorrecting.")

    print()
    # Overall summary
    all_errors = [e for d in city_bias.values() for e in d["errors"]]
    all_wins = sum(d["wins"] for d in city_bias.values())
    all_losses = sum(d["losses"] for d in city_bias.values())
    total_n = all_wins + all_losses

    print(f"  Overall: {all_wins}W/{all_losses}L ({all_wins/total_n:.0%} wr)" if total_n else "")
    if all_errors:
        overall_mean = sum(all_errors) / len(all_errors)
        overall_mae = sum(abs(e) for e in all_errors) / len(all_errors)
        print(f"  Temp error (all cities): mean={overall_mean:+.2f}°  MAE={overall_mae:.2f}°  n={len(all_errors)}")
        if abs(overall_mean) > 0.5:
            direction = "cold" if overall_mean > 0 else "hot"
            print(f"  ⚠️  Systematic {direction} bias of {abs(overall_mean):.2f}° across all cities.")
    print("=" * 80)


async def run_report(backfill: bool = False) -> None:
    positions = load_positions()

    if not positions:
        print("No positions found.")
        return

    if backfill:
        await backfill_actuals(positions)
        positions = load_positions()  # reload after backfill

    city_bias = compute_city_bias(positions)
    print_bias_report(city_bias)


def main():
    backfill = "--backfill" in sys.argv
    asyncio.run(run_report(backfill=backfill))


if __name__ == "__main__":
    main()
