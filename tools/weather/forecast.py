"""
Multi-Source Weather Forecasting with Ensemble Probability Estimation

Uses Open-Meteo ensemble API (ECMWF, GFS, ICON) and NOAA NWS to produce
probability distributions for daily high temperature.

Key insight: ensemble weather models give us 50+ independent simulations.
By computing the daily max for each member and adding observation uncertainty,
we get a robust probability distribution for any temperature bucket.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from math import erf, sqrt

logger = logging.getLogger(__name__)
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from scipy.stats import t as student_t, gaussian_kde

from .config import (
    City, CITIES, OPEN_METEO_ENSEMBLE_URL, OPEN_METEO_FORECAST_URL,
    NWS_API_BASE, ENSEMBLE_MODELS, NOISE_REPLICATIONS,
)
from .markets import TempBucket
from .calibration import (
    CityCalibration, apply_bias_correction, bayesian_condition_on_current,
)
from .wunderground import fetch_wu_forecast, WUForecast
from .mos import get_or_build_mos, generate_mos_samples, StationMOS
from .datalog import (
    log_ensemble_batch, log_deterministic, log_current_weather, log_forecast,
)

# Models for deterministic multi-model fallback (when ensemble fails)
DETERMINISTIC_MODELS = [
    "ecmwf_ifs025", "gfs_seamless", "icon_seamless",
    "gem_seamless", "jma_seamless", "meteofrance_seamless",
]

# Extra deterministic models that have MOS profiles but no ensemble API
# We fetch their single deterministic forecast and create synthetic spread
MOS_ONLY_MODELS = [
    "jma_seamless", "meteofrance_seamless", "ukmo_seamless", "knmi_seamless",
]
MOS_ONLY_SYNTHETIC_MEMBERS = 10  # Synthetic members per deterministic model
MOS_ONLY_SYNTHETIC_STD_C = 1.2   # Synthetic spread in °C (scaled for °F)

# Max retries per API call
MAX_RETRIES = 2
RETRY_DELAY = 1.0  # seconds

# ─── WU Forecast Weighting ──────────────────────────────────────────────────
# WU IS the resolution source. Their forecast gets massive weight.
# We generate synthetic ensemble members centered on WU's forecast
# with tight uncertainty (WU forecasts their own station best).
# WU synthetic member count by lead time — shorter lead = more WU weight
# With ~120 ensemble members, these give approximate WU fractions:
#   0d: 200/(200+120) = 62.5% WU — today, WU has partial observations
#   1d: 160/(160+120) = 57% WU — tomorrow, strongest WU advantage
#   2d: 100/(100+120) = 45% WU — balanced
#   3d+: 60/(60+120)  = 33% WU — more ensemble, WU less reliable
WU_MEMBERS_BY_LEAD = {
    0: 200,
    1: 160,
    2: 100,
    3: 60,
}
WU_MEMBERS_DEFAULT = 50        # 4+ days
WU_FORECAST_STD_1D = 1.2       # WU forecast std for 1-day lead (°C base)
WU_FORECAST_STD_2D = 1.8       # 2-day lead
WU_FORECAST_STD_3D = 2.5       # 3-day lead
WU_FORECAST_STD_4D = 3.2       # 4+ day lead

# Ensemble disagreement threshold — if model means differ by more than this
# (in native degrees), widen the distribution to account for uncertainty
ENSEMBLE_DISAGREE_THRESHOLD_C = 2.0  # °C
ENSEMBLE_DISAGREE_THRESHOLD_F = 3.5  # °F
ENSEMBLE_DISAGREE_SPREAD_BOOST = 1.3 # multiply spread by this when models disagree

# Lead-time dependent uncertainty multiplier for ensemble models
# Ensemble spread is calibrated for ~2-day lead; scale for other horizons
LEAD_TIME_SPREAD_MULT = {
    0: 0.6,   # Today: much tighter (current obs constrain it)
    1: 0.85,  # Tomorrow: tighter than default
    2: 1.0,   # 2 days: baseline
    3: 1.2,   # 3 days: wider
    4: 1.4,   # 4+ days: much wider
}

# Student-t degrees of freedom for heavy-tailed probability estimation
# Lower = heavier tails = more probability on unlikely outcomes
# Weather errors are fatter-tailed than Gaussian (forecast busts)
STUDENT_T_DF = 6.0

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class ForecastResult:
    """Result of a multi-model ensemble forecast for a city on a specific date."""
    city_key: str
    target_date: date
    daily_high_samples: np.ndarray   # Raw ensemble samples of daily max temp
    augmented_samples: np.ndarray    # Samples with observation noise added
    mean: float                      # Mean of augmented samples
    std: float                       # Std dev of augmented samples
    n_members: int                   # Number of raw ensemble members
    lead_days: int = 1               # Forecast lead time in days
    wu_forecast: Optional[int] = None  # WU's own forecast (if available)
    sources: List[str] = field(default_factory=list)  # Which models contributed
    mos_used: bool = False             # True if MOS-corrected (use empirical probs)


@dataclass
class CurrentWeather:
    """Current observed weather for sanity checking today's markets."""
    city_key: str
    temperature: float               # Current temperature in city's native unit
    apparent_temp: float             # Feels-like temperature
    daily_high_so_far: float         # Highest temp recorded today so far
    forecast_remaining_max: Optional[float] = None  # Max temp forecast for remaining hours
    timestamp: datetime = None
    source: str = "Open-Meteo"


# ─── Main Forecast Function ──────────────────────────────────────────────────

async def get_forecast(
    session: aiohttp.ClientSession,
    city_key: str,
    target_date: date,
    calibration: Optional[CityCalibration] = None,
    current_high: Optional[float] = None,
    hours_remaining: Optional[float] = None,
    reference_date: Optional[date] = None,
    forecast_remaining_max: Optional[float] = None,
) -> Optional[ForecastResult]:
    """
    Fetch ensemble weather forecasts from multiple sources and combine them
    into a probability-ready ForecastResult.

    Args:
        reference_date: Override "today" for lead-time calculation (used in backtesting).
                        If None, uses actual current date.

    Steps:
    1. Fetch ensemble forecasts per-model (sequentially to avoid rate limits)
    2. Apply per-model bias correction and accuracy weighting (if calibrated)
    3. If ensemble yields too few members, fallback to deterministic multi-model
    4. Optionally fetch NOAA NWS deterministic forecast (US cities)
    5. Add observation uncertainty noise
    6. For today: Bayesian-condition on current observed high
    7. Return augmented sample distribution
    """
    city = CITIES.get(city_key)
    if not city:
        return None

    today = reference_date or datetime.now(timezone.utc).date()
    lead_days = max(0, (target_date - today).days)

    # Steps 1-3: Fetch WU forecast, ensemble models, MOS-only models ALL in parallel
    # This replaces 5+ sequential API calls with 3 concurrent ones

    async def _fetch_wu():
        try:
            return await fetch_wu_forecast(session, city_key, target_date)
        except Exception:
            return None

    async def _fetch_ensembles():
        # Try batch first (1 API call for all models); fall back to parallel individual
        batch = await _fetch_open_meteo_ensemble_batch(
            session, city, target_date, ENSEMBLE_MODELS,
        )
        if batch:
            return batch
        # Fallback: parallel individual model fetches
        async def _one(m):
            return await _fetch_open_meteo_ensemble(session, city, target_date, m)
        results = await asyncio.gather(*[_one(m) for m in ENSEMBLE_MODELS])
        out = {}
        for name, members in results:
            if name and members:
                out[name] = members
        return out

    async def _fetch_mos_det():
        try:
            return await _fetch_mos_only_deterministic(session, city, target_date)
        except Exception:
            return {}

    async def _fetch_mos_data():
        try:
            return await get_or_build_mos(session, city_key)
        except Exception:
            return None

    wu_fc, ensemble_result, mos_only, mos = await asyncio.gather(
        _fetch_wu(), _fetch_ensembles(), _fetch_mos_det(), _fetch_mos_data(),
    )

    # Log fetched data to disk for long-term accumulation
    # Only log during live runs (not backtests) to avoid polluting archive
    # with retroactive data that wasn't actually fetched on that date
    if reference_date is None:
        try:
            if ensemble_result:
                log_ensemble_batch(city_key, target_date, ensemble_result)
            if mos_only:
                for mn, (det_val, members) in mos_only.items():
                    log_deterministic(city_key, target_date, mn,
                                      det_val, members or None)
        except Exception:
            pass  # Never let logging break forecasting

    # Merge ensemble + MOS-only deterministic results
    samples_by_model: Dict[str, List[float]] = dict(ensemble_result)
    if mos_only:
        for model_name, (det_val, members) in mos_only.items():
            if model_name not in samples_by_model and members:
                samples_by_model[model_name] = members

    use_mos = False

    if mos and samples_by_model:
        raw_samples = generate_mos_samples(
            mos,
            samples_by_model,
            wu_forecast_temp=wu_fc.high_temp if wu_fc else None,
            lead_days=lead_days,
        )
        sources = [f"MOS({mos.n_days}d,{mos.n_models}m)"]
        for model, p in mos.profiles.items():
            short = model.split("_")[0]
            sources.append(f"{short}(b={p.mean_bias:+.1f},w={p.skill_weight:.0%})")
        if wu_fc is not None:
            sources.append(f"WU(μ={wu_fc.high_temp})")
        use_mos = True
    else:
        # ── Fallback: old calibration path ──
        if calibration and samples_by_model:
            raw_samples, sources = apply_bias_correction(samples_by_model, calibration)
        elif samples_by_model:
            all_samples = []
            sources = []
            for model, samples in samples_by_model.items():
                all_samples.extend(samples)
                sources.append(f"{model}({len(samples)})")
            raw_samples = np.array(all_samples, dtype=np.float64)
        else:
            raw_samples = np.array([], dtype=np.float64)
            sources = []

        # Deterministic fallback if < 10 members
        if len(raw_samples) < 10:
            det_result = await _fetch_deterministic_multimodel(session, city, target_date)
            if det_result:
                det_name, det_samples = det_result
                raw_samples = np.concatenate([raw_samples, np.array(det_samples)]) if len(raw_samples) > 0 else np.array(det_samples)
                sources.append(f"{det_name}({len(det_samples)})")

        # NWS for US cities
        if city.unit == "F":
            nws_result = await _fetch_nws_forecast(session, city, target_date)
            if isinstance(nws_result, tuple) and nws_result[0] is not None:
                nws_name, nws_samples = nws_result
                if calibration:
                    nws_samples = [s - calibration.overall_bias for s in nws_samples]
                raw_samples = np.concatenate([raw_samples, np.array(nws_samples)]) if len(raw_samples) > 0 else np.array(nws_samples)
                sources.append(f"{nws_name}({len(nws_samples)})")

        # Model disagreement detection
        disagree_boost = 1.0
        if len(samples_by_model) >= 2:
            model_means = [np.mean(s) for s in samples_by_model.values() if len(s) > 0]
            if len(model_means) >= 2:
                model_spread = max(model_means) - min(model_means)
                threshold = ENSEMBLE_DISAGREE_THRESHOLD_F if city.unit == "F" else ENSEMBLE_DISAGREE_THRESHOLD_C
                if model_spread > threshold:
                    disagree_boost = ENSEMBLE_DISAGREE_SPREAD_BOOST
                    sources.append(f"disagree({model_spread:.1f}°)")

        # WU forecast injection (non-MOS path)
        wu_n = WU_MEMBERS_BY_LEAD.get(lead_days, WU_MEMBERS_DEFAULT)
        if wu_fc is not None:
            wu_std = _wu_forecast_std(lead_days, city.unit)
            rng = np.random.default_rng(42)
            wu_samples = rng.normal(wu_fc.high_temp, wu_std, size=wu_n)
            raw_samples = np.concatenate([raw_samples, wu_samples]) if len(raw_samples) > 0 else wu_samples
            sources.append(f"WU({wu_n},μ={wu_fc.high_temp},σ={wu_std:.1f})")

        # Lead-time spread scaling + disagreement boost
        if len(raw_samples) > 0:
            spread_mult = LEAD_TIME_SPREAD_MULT.get(min(lead_days, 4), 1.4) * disagree_boost
            ensemble_mean = float(np.mean(raw_samples))
            if spread_mult != 1.0:
                raw_samples = ensemble_mean + (raw_samples - ensemble_mean) * spread_mult

    if len(raw_samples) == 0:
        return None

    # Step 9: Add observation uncertainty noise
    # MOS already captures grid-to-station error, so use minimal noise
    noise_scale = city.obs_uncertainty * 0.25 if use_mos else city.obs_uncertainty
    augmented = _augment_with_noise(raw_samples, noise_scale)

    # Step 10: Bayesian conditioning on current high (for today's markets)
    if current_high is not None and hours_remaining is not None:
        augmented = bayesian_condition_on_current(
            augmented, current_high, hours_remaining,
            forecast_remaining_max=forecast_remaining_max,
        )

    mean = float(np.mean(augmented))
    std = float(np.std(augmented))

    return ForecastResult(
        city_key=city_key,
        target_date=target_date,
        daily_high_samples=raw_samples,
        augmented_samples=augmented,
        mean=mean,
        std=max(std, 0.5),  # Floor std at 0.5° to avoid overconfidence
        n_members=len(raw_samples),
        lead_days=lead_days,
        wu_forecast=wu_fc.high_temp if wu_fc else None,
        sources=sources,
        mos_used=use_mos,
    )


# ─── Open-Meteo Ensemble API ─────────────────────────────────────────────────

async def _fetch_open_meteo_ensemble(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
    model: str,
) -> Tuple[Optional[str], List[float]]:
    """
    Fetch ensemble forecast from Open-Meteo and return daily max temps
    for each ensemble member. Includes retry logic for transient failures.
    """
    result = await _fetch_open_meteo_ensemble_batch(session, city, target_date, [model])
    if model in result:
        return (model, result[model])
    return (None, [])


async def _fetch_open_meteo_ensemble_batch(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
    models: List[str],
) -> Dict[str, List[float]]:
    """
    Fetch ensemble forecasts for MULTIPLE models in a single API call.
    Open-Meteo supports comma-separated models parameter.
    Returns {model_name: [daily_max_per_member, ...]}.
    """
    date_str = target_date.isoformat()
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "hourly": "temperature_2m",
        "start_date": date_str,
        "end_date": date_str,
        "models": ",".join(models),
    }
    if city.unit == "F":
        params["temperature_unit"] = "fahrenheit"

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                OPEN_METEO_ENSEMBLE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 429:  # Rate limited
                    logger.warning(f"Open-Meteo 429 rate limit (attempt {attempt+1})")
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                    continue
                if resp.status != 200:
                    logger.warning(f"Open-Meteo HTTP {resp.status}")
                    return {}
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Open-Meteo request failed: {e} (attempt {attempt+1})")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return {}

        # Detect daily API limit (returns 200 OK with error in body)
        if data.get("error"):
            reason = data.get("reason", "unknown")
            logger.error(f"Open-Meteo API error: {reason}")
            return {}  # Fail fast, don't retry

        hourly = data.get("hourly", {})
        if not hourly:
            return {}

        # Parse per-model ensemble members from the combined response
        # Keys look like: temperature_2m_member00_ecmwf_ifs025, etc.
        # Or for single-model: temperature_2m_member00
        result: Dict[str, List[float]] = {}

        for model in models:
            # Multi-model response: keys have model suffix
            member_keys = sorted([
                k for k in hourly.keys()
                if k.startswith("temperature_2m_member") and k.endswith(f"_{model}")
            ])

            if not member_keys:
                # Single-model fallback: keys without model suffix
                if len(models) == 1:
                    member_keys = sorted([
                        k for k in hourly.keys()
                        if k.startswith("temperature_2m_member")
                    ])

            if member_keys:
                daily_maxes = []
                for key in member_keys:
                    values = hourly[key]
                    valid = [v for v in values if v is not None]
                    if valid:
                        daily_maxes.append(max(valid))
                if daily_maxes:
                    result[model] = daily_maxes
            else:
                # Deterministic fallback: temperature_2m_MODEL
                single_key = f"temperature_2m_{model}"
                single = hourly.get(single_key, [])
                if not single and len(models) == 1:
                    single = hourly.get("temperature_2m", [])
                valid = [v for v in single if v is not None]
                if valid:
                    result[model] = [max(valid)]

        return result

    return {}


async def _fetch_mos_only_deterministic(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
) -> Dict[str, tuple]:
    """
    Fetch deterministic daily max for ALL models in a single API call.

    Returns {model_name: (det_val, synthetic_members)} where:
      - det_val: raw deterministic daily max (exact value for MOS logging)
      - synthetic_members: noisy samples for MOS-only models, [] for ensemble models

    For MOS-only models (JMA, MeteoFrance, UKMO, KNMI): creates synthetic ensemble.
    For ensemble models (ECMWF, GFS, etc.): returns empty members (real ensemble
    data comes from the ensemble API). The det_val is logged for long-term MOS use.
    """
    date_str = target_date.isoformat()
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "daily": "temperature_2m_max",
        "start_date": date_str,
        "end_date": date_str,
        "models": ",".join(set(MOS_ONLY_MODELS + ENSEMBLE_MODELS)),
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
                    logger.warning(f"Open-Meteo forecast 429 (attempt {attempt+1})")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return {}
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return {}

        # Detect daily API limit
        if data.get("error"):
            logger.error(f"Open-Meteo forecast API error: {data.get('reason', 'unknown')}")
            return {}

        daily = data.get("daily", {})
        result: Dict[str, tuple] = {}
        rng = np.random.default_rng()
        std = MOS_ONLY_SYNTHETIC_STD_C * (1.8 if city.unit == "F" else 1.0)
        mos_only_set = set(MOS_ONLY_MODELS)

        for key, values in daily.items():
            if key == "time" or not key.startswith("temperature_2m_max_"):
                continue
            model_name = key.replace("temperature_2m_max_", "")
            if values and values[0] is not None:
                det_val = float(values[0])
                if model_name in mos_only_set:
                    # MOS-only models: create synthetic ensemble for forecast merge
                    synthetic = rng.normal(det_val, std, size=MOS_ONLY_SYNTHETIC_MEMBERS)
                    result[model_name] = (det_val, synthetic.tolist())
                else:
                    # Ensemble models: det_val logged for MOS, no synthetic needed
                    result[model_name] = (det_val, [])

        return result

    return {}


async def _fetch_deterministic_multimodel(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
) -> Optional[Tuple[str, List[float]]]:
    """
    Fallback: fetch deterministic forecasts from multiple models.
    Each model gives one daily-max value, so we get ~6 samples.
    Less accurate than ensemble but covers longer forecast horizons.
    """
    date_str = target_date.isoformat()
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "hourly": "temperature_2m",
        "start_date": date_str,
        "end_date": date_str,
        "models": ",".join(DETERMINISTIC_MODELS),
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
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return None

        hourly = data.get("hourly", {})
        if not hourly:
            return None

        # Each model returns: temperature_2m_{model_name}
        daily_maxes = []
        for key in hourly:
            if key.startswith("temperature_2m_") and key != "temperature_2m":
                values = hourly[key]
                valid = [v for v in values if v is not None]
                if valid:
                    daily_maxes.append(max(valid))

        # Also check bare "temperature_2m" if present
        if "temperature_2m" in hourly:
            valid = [v for v in hourly["temperature_2m"] if v is not None]
            if valid:
                daily_maxes.append(max(valid))

        if daily_maxes:
            return ("det-multi", daily_maxes)
        return None

    return None


# ─── NOAA NWS Forecast (US Cities) ───────────────────────────────────────────

async def _fetch_nws_forecast(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
) -> Tuple[Optional[str], List[float]]:
    """
    Fetch NOAA NWS hourly forecast and extract the daily high for the target date.
    Returns a single deterministic forecast value (treated as 1 ensemble member).
    """
    headers = {
        "User-Agent": "PollyMorph Weather Bot (github.com/pollymorph)",
        "Accept": "application/geo+json",
    }

    try:
        # Step 1: Get grid point info
        points_url = f"{NWS_API_BASE}/points/{city.lat:.4f},{city.lon:.4f}"
        async with session.get(
            points_url, headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return (None, [])
            points_data = await resp.json()

        forecast_url = points_data.get("properties", {}).get("forecastHourly", "")
        if not forecast_url:
            return (None, [])

        # Step 2: Fetch hourly forecast
        async with session.get(
            forecast_url, headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return (None, [])
            forecast_data = await resp.json()

        periods = forecast_data.get("properties", {}).get("periods", [])
        if not periods:
            return (None, [])

        # Step 3: Find max temperature for the target date
        target_temps = []
        for period in periods:
            start_str = period.get("startTime", "")
            if not start_str:
                continue
            try:
                dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                if dt.date() == target_date:
                    temp = period.get("temperature")
                    if temp is not None:
                        # NWS returns in Fahrenheit for US cities
                        target_temps.append(float(temp))
            except (ValueError, TypeError):
                continue

        if not target_temps:
            return (None, [])

        daily_high = max(target_temps)
        return ("NWS", [daily_high])

    except Exception:
        return (None, [])


# ─── Current Weather (Sanity Check) ──────────────────────────────────────────

async def get_current_weather(
    session: aiohttp.ClientSession,
    city_key: str,
) -> Optional[CurrentWeather]:
    """
    Fetch current observed weather for a city. Used to sanity-check
    today's market forecasts — if the current temp already exceeds
    a bucket, that bucket's probability should be near zero.
    """
    city = CITIES.get(city_key)
    if not city:
        return None

    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "current": "temperature_2m,apparent_temperature",
        "hourly": "temperature_2m",
        "timezone": "auto",
        "forecast_days": 1,
        "past_days": 0,
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

        current = data.get("current", {})
        hourly = data.get("hourly", {})

        temp = current.get("temperature_2m")
        apparent = current.get("apparent_temperature", temp)

        if temp is None:
            return None

        # Find today's high so far from hourly data AND forecast remaining max
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        now_str = current.get("time", "")

        high_so_far = temp
        forecast_remaining_max = None
        if times and temps:
            future_temps = []
            for t, v in zip(times, temps):
                if v is not None:
                    if t <= now_str:
                        high_so_far = max(high_so_far, v)
                    else:
                        future_temps.append(v)
            if future_temps:
                forecast_remaining_max = max(future_temps)

        result = CurrentWeather(
            city_key=city_key,
            temperature=float(temp),
            apparent_temp=float(apparent) if apparent else float(temp),
            daily_high_so_far=float(high_so_far),
            forecast_remaining_max=float(forecast_remaining_max) if forecast_remaining_max is not None else None,
            timestamp=datetime.now(timezone.utc),
            source="Open-Meteo",
        )

        # Log current weather snapshot
        try:
            log_current_weather(
                city_key, result.temperature, result.daily_high_so_far,
                result.forecast_remaining_max, result.apparent_temp,
            )
        except Exception:
            pass

        return result

    return None


def sanity_check_probabilities(
    bucket_probs: Dict[str, float],
    buckets: List[TempBucket],
    current: CurrentWeather,
) -> Tuple[Dict[str, float], List[str]]:
    """
    Adjust forecast probabilities using current observed temperature.

    Rules:
    1. If today's high so far already exceeds a bucket's upper bound,
       that bucket's probability → ~0 (the high can only go higher).
    2. Buckets below the current high get zeroed; remaining buckets
       are renormalized.
    3. Returns adjusted probabilities and a list of warning messages.
    """
    warnings = []
    adjusted = dict(bucket_probs)
    high_so_far = current.daily_high_so_far

    zeroed_any = False
    for bucket in buckets:
        label = bucket.label
        if label not in adjusted:
            continue

        # If the high today is already above this bucket's ceiling,
        # this bucket is impossible (high can only increase)
        if bucket.high != float('inf') and high_so_far > bucket.high + 0.5:
            old_prob = adjusted[label]
            if old_prob > 0.01:
                warnings.append(
                    f"⚠ \"{label}\" zeroed: current high {high_so_far:.0f}° already exceeds bucket ceiling"
                )
            adjusted[label] = 0.001  # Near-zero, not absolute zero
            zeroed_any = True

    # Renormalize if we zeroed anything
    if zeroed_any:
        total = sum(adjusted.values())
        if total > 0:
            adjusted = {k: v / total for k, v in adjusted.items()}

    # Add informational warning about current conditions
    unit = buckets[0].unit if buckets else "?"
    warnings.insert(0, f"📍 Current: {current.temperature:.0f}°{unit}, high so far: {high_so_far:.0f}°{unit}")

    return adjusted, warnings


# ─── Probability Calculation ─────────────────────────────────────────────────

def _augment_with_noise(
    samples: np.ndarray,
    obs_uncertainty: float,
) -> np.ndarray:
    """
    Add observation uncertainty noise to ensemble samples.

    For each raw sample, generate NOISE_REPLICATIONS copies with added
    Gaussian noise. This smooths the probability distribution and accounts
    for the difference between model grid cells and point observations.
    """
    n = len(samples)
    total = n * NOISE_REPLICATIONS
    augmented = np.empty(total, dtype=np.float64)

    rng = np.random.default_rng(42)  # Fixed seed for reproducibility within a run

    for i in range(NOISE_REPLICATIONS):
        noise = rng.normal(0, obs_uncertainty, size=n)
        augmented[i * n:(i + 1) * n] = samples + noise

    return augmented


def calculate_bucket_probabilities(
    forecast: ForecastResult,
    buckets: List[TempBucket],
) -> Dict[str, float]:
    """
    Calculate the probability of the daily high falling in each temperature bucket.

    Two modes depending on whether MOS was used:

    MOS mode (forecast.mos_used=True):
        Pure empirical with Laplace smoothing. The MOS-corrected samples already
        capture station-specific error distributions, so parametric blending
        would dilute the signal. 600+ samples give smooth enough probabilities.

    Legacy mode:
        Hybrid 70% empirical + 30% Student-t (heavy-tailed) to cover forecast
        busts and prevent zero-probability gaps.

    Returns dict mapping bucket label → probability (0.0 - 1.0).
    """
    samples = forecast.augmented_samples
    n = len(samples)

    if n == 0:
        # Uniform fallback
        return {b.label: 1.0 / len(buckets) for b in buckets}

    n_buckets = len(buckets)
    mu = forecast.mean
    sigma = forecast.std

    # For MOS mode: fit Gaussian KDE on raw samples for smooth integration
    kde = None
    if forecast.mos_used and n >= 50:
        try:
            kde = gaussian_kde(samples, bw_method='silverman')
        except Exception:
            kde = None  # Fall back to empirical if KDE fails

    result = {}
    for bucket in buckets:
        # Bucket boundaries: ±0.5 to account for WU whole-degree rounding
        low_bound = bucket.low - 0.5 if bucket.low != float('-inf') else float('-inf')
        high_bound = bucket.high + 0.5 if bucket.high != float('inf') else float('inf')

        if forecast.mos_used and kde is not None:
            # KDE mode: smooth density integration over bucket range
            # Handle infinite bounds by clamping to ±50 from mean
            kde_low = low_bound if low_bound != float('-inf') else mu - 50
            kde_high = high_bound if high_bound != float('inf') else mu + 50
            prob = float(kde.integrate_box_1d(kde_low, kde_high))
            prob = max(prob, 0.002)  # Floor to prevent zero-probability
        elif forecast.mos_used:
            # Fallback: pure empirical with Laplace smoothing
            count = float(np.sum((samples >= low_bound) & (samples < high_bound)))
            alpha = 1.0
            prob = (count + alpha) / (n + alpha * n_buckets)
        else:
            # Legacy mode: empirical + Student-t blend
            count = float(np.sum((samples >= low_bound) & (samples < high_bound)))
            empirical_prob = count / n
            parametric_prob = _student_t_cdf(high_bound, mu, sigma) - _student_t_cdf(low_bound, mu, sigma)
            prob = 0.70 * empirical_prob + 0.30 * parametric_prob
            prob = max(prob, 0.005)

        result[bucket.label] = prob

    # Normalize so probabilities sum to 1.0
    total = sum(result.values())
    if total > 0:
        result = {k: v / total for k, v in result.items()}

    return result


def _norm_cdf(x: float, mu: float, sigma: float) -> float:
    """Cumulative distribution function of the normal distribution."""
    if x == float('inf'):
        return 1.0
    if x == float('-inf'):
        return 0.0
    if sigma <= 0:
        return 1.0 if x >= mu else 0.0
    return 0.5 * (1.0 + erf((x - mu) / (sigma * sqrt(2))))


def _student_t_cdf(x: float, mu: float, sigma: float) -> float:
    """
    CDF of a Student-t distribution with STUDENT_T_DF degrees of freedom.
    Heavier tails than Gaussian — captures forecast bust scenarios where
    the actual temp is 3-4° away from the forecast.
    """
    if x == float('inf'):
        return 1.0
    if x == float('-inf'):
        return 0.0
    if sigma <= 0:
        return 1.0 if x >= mu else 0.0
    z = (x - mu) / sigma
    return float(student_t.cdf(z, STUDENT_T_DF))


def _wu_forecast_std(lead_days: int, unit: str) -> float:
    """
    Get the appropriate standard deviation for WU forecast synthetic members.
    Tighter for shorter lead times (WU forecasts are more accurate near-term).
    Adjusted for unit (Fahrenheit has ~1.8x larger values than Celsius).
    """
    base_stds = {
        0: WU_FORECAST_STD_1D * 0.7,  # Today: very tight
        1: WU_FORECAST_STD_1D,
        2: WU_FORECAST_STD_2D,
        3: WU_FORECAST_STD_3D,
    }
    std = base_stds.get(lead_days, WU_FORECAST_STD_4D)
    # Fahrenheit scale is ~1.8x Celsius
    if unit == "F":
        std *= 1.8
    return std
