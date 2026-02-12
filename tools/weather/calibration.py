"""
Forecast Calibration — Bias Correction, Model Weighting, and Bayesian Conditioning

Computes per-city, per-model bias corrections using recent historical data.
Applies RMSE-based model weighting so better models get more influence.
For today's markets, performs Bayesian conditioning on the current observed high.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from .config import (
    City, CITIES, OPEN_METEO_ENSEMBLE_URL, OPEN_METEO_FORECAST_URL,
    ENSEMBLE_MODELS,
)
from .wunderground import fetch_wu_daily

# ─── Configuration ────────────────────────────────────────────────────────────

CALIBRATION_DAYS = 7          # Days of history (balanced: enough data, fast enough to compute)
CALIBRATION_FILE = Path(__file__).parent.parent.parent / "weather_calibration.json"
MAX_RETRIES = 2
RETRY_DELAY = 1.0

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class ModelCalibration:
    """Calibration data for one model in one city."""
    model: str
    city_key: str
    mean_bias: float      # Average (forecast - actual) in native degrees
    rmse: float           # Root mean square error
    n_days: int           # Number of days used for calibration
    weight: float = 1.0   # Accuracy-based weight (higher = better model)


@dataclass
class CityCalibration:
    """All calibration data for one city."""
    city_key: str
    models: Dict[str, ModelCalibration]  # model_name → calibration
    overall_bias: float    # Weighted average bias across all models
    overall_rmse: float    # Weighted average RMSE
    last_updated: str      # ISO timestamp


# ─── Fetch Actual Daily Highs ─────────────────────────────────────────────────

async def _fetch_actual_highs(
    session: aiohttp.ClientSession,
    city: City,
    city_key: str,
    start_date: date,
    end_date: date,
) -> Dict[date, float]:
    """
    Fetch actual observed daily high temperatures.

    Primary source: Weather Underground (Polymarket's resolution source).
    Fallback: Open-Meteo historical data.
    """
    result = {}

    # ── Try WU first (resolution source, whole-degree integers) ──
    current = start_date
    wu_count = 0
    while current <= end_date:
        wu = await fetch_wu_daily(session, city_key, current)
        if wu and wu.n_observations > 0 and wu.is_complete:
            result[current] = float(wu.high_temp)
            wu_count += 1
        current = date.fromordinal(current.toordinal() + 1)
        await asyncio.sleep(0.15)

    # If WU got most days, return (skip Open-Meteo)
    expected_days = (end_date - start_date).days + 1
    if wu_count >= expected_days * 0.7:
        return result

    # ── Fallback: Open-Meteo for missing days ──
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "daily": "temperature_2m_max",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": city.tz,
    }
    if city.unit == "F":
        params["temperature_unit"] = "fahrenheit"

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                OPEN_METEO_FORECAST_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return result  # Return whatever WU got
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return result

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        temps = daily.get("temperature_2m_max", [])

        for d_str, t in zip(dates, temps):
            d = date.fromisoformat(d_str)
            if t is not None and d not in result:  # Don't overwrite WU data
                result[d] = float(t)
        return result

    return result


async def _fetch_ensemble_hindcast(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
    model: str,
) -> List[float]:
    """Fetch ensemble forecast daily maxes for a specific date and model."""
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "hourly": "temperature_2m",
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "models": model,
    }
    if city.unit == "F":
        params["temperature_unit"] = "fahrenheit"

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                OPEN_METEO_ENSEMBLE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return []
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return []

        hourly = data.get("hourly", {})
        member_keys = sorted([k for k in hourly if "member" in k])

        if not member_keys:
            single = hourly.get("temperature_2m", [])
            valid = [v for v in single if v is not None]
            return [max(valid)] if valid else []

        daily_maxes = []
        for key in member_keys:
            valid = [v for v in hourly[key] if v is not None]
            if valid:
                daily_maxes.append(max(valid))
        return daily_maxes

    return []


# ─── Calibration Computation ─────────────────────────────────────────────────

async def compute_calibration(
    session: aiohttp.ClientSession,
    city_key: str,
    n_days: int = CALIBRATION_DAYS,
) -> Optional[CityCalibration]:
    """
    Compute calibration data for a city by comparing ensemble forecasts
    to actual temperatures over the past n_days.
    """
    city = CITIES.get(city_key)
    if not city:
        return None

    today = datetime.now(timezone.utc).date()
    # Use yesterday as end date (today may not be final)
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=n_days - 1)

    # Fetch actual daily highs
    actuals = await _fetch_actual_highs(session, city, city_key, start_date, end_date)
    if not actuals:
        return None
    await asyncio.sleep(0.15)

    # For each model, compute bias and RMSE over the calibration period
    model_cals: Dict[str, ModelCalibration] = {}

    for model in ENSEMBLE_MODELS:
        biases = []
        sq_errors = []

        for target_date, actual_high in sorted(actuals.items()):
            maxes = await _fetch_ensemble_hindcast(session, city, target_date, model)
            await asyncio.sleep(0.15)

            if maxes:
                ensemble_mean = sum(maxes) / len(maxes)
                bias = ensemble_mean - actual_high
                biases.append(bias)
                sq_errors.append(bias ** 2)

        if biases:
            mean_bias = sum(biases) / len(biases)
            rmse = (sum(sq_errors) / len(sq_errors)) ** 0.5
            model_cals[model] = ModelCalibration(
                model=model,
                city_key=city_key,
                mean_bias=round(mean_bias, 2),
                rmse=round(max(rmse, 0.1), 2),  # Floor at 0.1 to avoid div by zero
                n_days=len(biases),
            )

    if not model_cals:
        return None

    # Compute accuracy-based weights: inversely proportional to RMSE²
    # This gives much more weight to accurate models
    total_inv_rmse_sq = sum(1.0 / (mc.rmse ** 2) for mc in model_cals.values())
    for mc in model_cals.values():
        mc.weight = round((1.0 / (mc.rmse ** 2)) / total_inv_rmse_sq, 4)

    # Compute weighted overall bias and RMSE
    overall_bias = sum(mc.mean_bias * mc.weight for mc in model_cals.values())
    overall_rmse = sum(mc.rmse * mc.weight for mc in model_cals.values())

    return CityCalibration(
        city_key=city_key,
        models=model_cals,
        overall_bias=round(overall_bias, 2),
        overall_rmse=round(overall_rmse, 2),
        last_updated=datetime.now(timezone.utc).isoformat(),
    )


# ─── Apply Calibration ───────────────────────────────────────────────────────

def apply_bias_correction(
    samples_by_model: Dict[str, List[float]],
    cal: CityCalibration,
) -> Tuple[np.ndarray, List[str]]:
    """
    Apply per-model bias correction and accuracy-based weighting.

    For each model:
    1. Subtract the model's mean bias from all members (de-bias)
    2. Resample members weighted by model accuracy

    Returns:
        corrected_samples: np.ndarray of bias-corrected daily max samples
        sources: list of source descriptions
    """
    corrected = []
    sources = []

    for model, raw_samples in samples_by_model.items():
        if not raw_samples:
            continue

        mc = cal.models.get(model)
        if mc:
            # De-bias: shift samples by negative of mean bias
            bias = mc.mean_bias
            weight = mc.weight
            debiased = [s - bias for s in raw_samples]

            # Weighted resampling: include proportional number of samples
            # If model has weight 0.6 and 50 members → effectively 50 members
            # but contributing 60% of the final pool
            n_effective = max(1, round(len(debiased) * weight * len(ENSEMBLE_MODELS)))
            if n_effective >= len(debiased):
                corrected.extend(debiased)
            else:
                # Subsample
                rng = np.random.default_rng(42)
                indices = rng.choice(len(debiased), size=n_effective, replace=True)
                corrected.extend([debiased[i] for i in indices])

            sources.append(f"{model}({len(debiased)}→{n_effective}w,b={bias:+.1f})")
        else:
            # No calibration data — use raw with equal weight
            corrected.extend(raw_samples)
            sources.append(f"{model}({len(raw_samples)})")

    return np.array(corrected, dtype=np.float64), sources


def bayesian_condition_on_current(
    samples: np.ndarray,
    current_high: float,
    hours_remaining: float,
) -> np.ndarray:
    """
    Bayesian conditioning: for today's markets, condition the forecast
    distribution on the known current high temperature.

    The daily high MUST be >= current_high. Additionally, with fewer hours
    remaining, the daily high is increasingly unlikely to rise much further.

    Algorithm:
    1. Remove all samples below current_high (impossible)
    2. For remaining samples, apply a decay that makes large exceedances
       less likely when few hours remain (temperature peaks usually mid-afternoon)

    Args:
        samples: augmented ensemble samples
        current_high: today's observed high so far
        hours_remaining: estimated hours of potential warming remaining

    Returns:
        conditioned samples (resampled to original length)
    """
    if len(samples) == 0:
        return samples

    # Step 1: Keep only samples >= current high
    valid = samples[samples >= current_high - 0.5]

    if len(valid) < 5:
        # If nearly all samples are below current high, our forecast was badly wrong.
        # Create a tight distribution around the current high with small upside
        rng = np.random.default_rng(42)
        upside = max(1.0, hours_remaining * 0.3)  # Potential additional warming
        return current_high + rng.exponential(scale=upside, size=len(samples))

    # Step 2: Apply time-based decay to exceedance above current high
    # With 0 hours left, almost no further warming expected
    # With 6+ hours, significant warming still possible
    exceedance = valid - current_high
    decay_scale = max(0.5, hours_remaining * 0.8)  # degrees per hour of potential warming
    weights = np.exp(-exceedance / max(decay_scale, 0.1))
    weights /= weights.sum()

    # Step 3: Resample with weights to get correct distribution
    rng = np.random.default_rng(42)
    indices = rng.choice(len(valid), size=len(samples), replace=True, p=weights)
    return valid[indices]


# ─── Persistence ──────────────────────────────────────────────────────────────

def save_calibration(calibrations: Dict[str, CityCalibration]):
    """Save calibration data to JSON file."""
    data = {}
    for city_key, cal in calibrations.items():
        data[city_key] = {
            "city_key": cal.city_key,
            "overall_bias": cal.overall_bias,
            "overall_rmse": cal.overall_rmse,
            "last_updated": cal.last_updated,
            "models": {
                model: {
                    "mean_bias": mc.mean_bias,
                    "rmse": mc.rmse,
                    "weight": mc.weight,
                    "n_days": mc.n_days,
                }
                for model, mc in cal.models.items()
            },
        }
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_calibration() -> Dict[str, CityCalibration]:
    """Load cached calibration data from JSON file."""
    if not CALIBRATION_FILE.exists():
        return {}

    try:
        with open(CALIBRATION_FILE) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

    result = {}
    for city_key, cdata in data.items():
        models = {}
        for model, mdata in cdata.get("models", {}).items():
            models[model] = ModelCalibration(
                model=model,
                city_key=city_key,
                mean_bias=mdata["mean_bias"],
                rmse=mdata["rmse"],
                weight=mdata.get("weight", 1.0),
                n_days=mdata.get("n_days", 0),
            )
        result[city_key] = CityCalibration(
            city_key=city_key,
            models=models,
            overall_bias=cdata.get("overall_bias", 0.0),
            overall_rmse=cdata.get("overall_rmse", 1.0),
            last_updated=cdata.get("last_updated", ""),
        )
    return result


def is_calibration_fresh(cal: CityCalibration, max_age_hours: float = 12.0) -> bool:
    """Check if calibration data is fresh enough to use."""
    try:
        updated = datetime.fromisoformat(cal.last_updated)
        age = datetime.now(timezone.utc) - updated
        return age.total_seconds() < max_age_hours * 3600
    except (ValueError, TypeError):
        return False


# ─── High-Level Interface ────────────────────────────────────────────────────

async def get_or_compute_calibration(
    session: aiohttp.ClientSession,
    city_key: str,
) -> Optional[CityCalibration]:
    """
    Get calibration for a city — load from cache if fresh, else compute.
    """
    cached = load_calibration()
    if city_key in cached and is_calibration_fresh(cached[city_key]):
        return cached[city_key]

    cal = await compute_calibration(session, city_key)
    if cal:
        cached[city_key] = cal
        save_calibration(cached)
    return cal
