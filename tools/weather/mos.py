"""
Station-Specific Model Output Statistics (MOS)

Learns the exact relationship between NWP model ensemble output and Weather
Underground station readings (Polymarket's resolution source). Uses 14 days of
historical errors to build empirical error distributions that capture:

  - Station-specific biases (grid-cell vs. airport sensor)
  - Non-Gaussian error patterns (fat tails, asymmetry)
  - Model-specific strengths at each location

This is the same technique used by the US National Weather Service (NWS MOS)
but specifically calibrated to Polymarket's resolution stations.

Key insight: instead of assuming Gaussian forecast errors, we resample from
the ACTUAL historical error distribution.  For a new forecast with model
mean M, each historical error e_i generates a corrected sample M − e_i.
With 14 days × 5 models = 70 error-based samples PLUS 161 bias-corrected
ensemble members, the resulting distribution is far more accurate than
the raw ensemble alone.
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from .config import (
    City, CITIES, OPEN_METEO_ENSEMBLE_URL, ENSEMBLE_MODELS,
)
from .wunderground import fetch_wu_daily
from .datalog import load_logged_ensembles, load_logged_deterministic, load_logged_actual

# ─── Configuration ────────────────────────────────────────────────────────────

MOS_HISTORY_DAYS = 30          # Days of history (more data = more stable error profiles)
MOS_CACHE_FILE = Path(__file__).parent.parent.parent / "weather_mos.json"
MOS_FRESHNESS_HOURS = 18       # Rebuild MOS after this many hours
MOS_MIN_DAYS = 3               # Minimum usable days for valid MOS

# Historical forecast API — archives past model runs, returns per-model daily max
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
# Models available on historical-forecast API
# 8 independent model families — doubled from original 4
HISTORICAL_MODELS = [
    "ecmwf_ifs025",           # ECMWF IFS 0.25° — world's best global model
    "gfs_seamless",           # NOAA GFS — strong for North America
    "icon_seamless",          # DWD ICON — good for Europe
    "gem_global",             # Environment Canada GEM
    "jma_seamless",           # Japan Meteorological Agency GSM
    "meteofrance_seamless",   # Météo-France ARPEGE/AROME
    "ukmo_seamless",          # UK Met Office — excellent global model
    "knmi_seamless",          # KNMI Harmonie — independent European model
]

MAX_RETRIES = 2
RETRY_DELAY = 1.0

# Concurrency limiter — avoid hammering APIs
# Open-Meteo doesn't rate-limit aggressively; WU is the bottleneck
_MOS_SEM = asyncio.Semaphore(12)

# ─── Sample Generation Tuning ────────────────────────────────────────────────

# Fraction of final samples from MOS error resampling vs bias-corrected ensemble
MOS_ERROR_FRAC = 0.50          # 50% from historical error resampling (up from 45%)
RAW_ENSEMBLE_FRAC = 0.25       # 25% from bias-corrected raw ensemble members
WU_FORECAST_FRAC = 0.25        # 25% from WU's own forecast (when available)

# Recency weighting: exponential decay halflife for MOS error resampling
# Recent errors are more relevant (current weather regime persistence)
RECENCY_HALFLIFE_DAYS = 5.0    # Half-weight after 5 days

# WU forecast std by lead days (in °C; scaled ×1.8 for °F cities)
# Tightened — WU forecasts their own station; their error is small short-range.
WU_LEAD_STD = {0: 0.4, 1: 0.7, 2: 1.2, 3: 1.8}
WU_LEAD_STD_DEFAULT = 2.2

# Total target samples for probability estimation
N_TARGET_SAMPLES = 1000


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class ModelErrorProfile:
    """Historical error distribution for one model at one station."""
    model: str
    errors: List[float]      # (deterministic_max − WU_actual) per day, native units
    mean_bias: float
    std_error: float
    rmse: float
    mae: float
    n_days: int
    skill_weight: float = 0.0  # Normalised 1/RMSE² weight across models
    det_ens_offset: float = 0.0  # avg(deterministic − ensemble_mean), for correction


@dataclass
class StationMOS:
    """Complete MOS data for one weather station."""
    city_key: str
    profiles: Dict[str, ModelErrorProfile]   # model_name → error profile
    n_days: int                               # Days with WU data
    n_models: int
    last_updated: str                         # ISO-8601 UTC


# ─── Historical Forecast Fetcher (Bulk) ───────────────────────────────────────

async def _fetch_historical_model_maxes(
    session: aiohttp.ClientSession,
    city: City,
    start_date: date,
    end_date: date,
) -> Dict[str, Dict[date, float]]:
    """
    Fetch per-model daily max temps from the historical forecast API.
    Returns {model_name: {date: daily_max_temp}} for all available models.

    This is a single API call that returns ~15 days of archived model forecasts,
    far more efficient than per-day ensemble hindcasts.
    """
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "daily": "temperature_2m_max",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "models": ",".join(HISTORICAL_MODELS),
    }
    if city.unit == "F":
        params["temperature_unit"] = "fahrenheit"

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                HISTORICAL_FORECAST_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 429:
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

        daily = data.get("daily", {})
        dates_str = daily.get("time", [])
        dates = [date.fromisoformat(d) for d in dates_str]

        result: Dict[str, Dict[date, float]] = {}
        for key, values in daily.items():
            if key == "time" or not key.startswith("temperature_2m_max_"):
                continue
            # key = "temperature_2m_max_ecmwf_ifs025" → model = "ecmwf_ifs025"
            model_name = key.replace("temperature_2m_max_", "")
            model_data: Dict[date, float] = {}
            for d, v in zip(dates, values):
                if v is not None:
                    model_data[d] = float(v)
            if model_data:
                result[model_name] = model_data

        return result

    return {}


async def _fetch_ensemble_maxes(
    session: aiohttp.ClientSession,
    city: City,
    target_date: date,
    model: str,
) -> List[float]:
    """
    Fetch ensemble daily-max temps for a single date/model from Open-Meteo.
    Returns list of per-member daily maxes (empty list on failure).
    Used as fallback when historical forecast API is unavailable.
    """
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
        member_keys = sorted(k for k in hourly if "member" in k)

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


# ─── MOS Building ────────────────────────────────────────────────────────────

async def build_station_mos(
    session: aiohttp.ClientSession,
    city_key: str,
    n_days: int = MOS_HISTORY_DAYS,
) -> Optional[StationMOS]:
    """
    Build MOS for a single station by comparing ensemble hindcasts to WU actuals.

    For each of the last n_days:
      1. Fetch the WU observed daily high (resolution-source truth)
      2. For each ensemble model, fetch the ensemble daily maxes
      3. Compute error = ensemble_mean − WU_actual

    Returns None if insufficient data.
    """
    city = CITIES.get(city_key)
    if not city:
        return None

    today = datetime.now(timezone.utc).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=n_days - 1)

    # ── Step 1: Fetch WU actuals (parallel with bounded concurrency) ──
    import time as _time
    _t0 = _time.time()
    print(f"      [{city_key}] Step 1: Fetching {n_days} days of WU actuals...")
    wu_actuals: Dict[date, int] = {}
    days_list = []
    d = start_date
    while d <= end_date:
        days_list.append(d)
        d += timedelta(days=1)

    _wu_ok = 0
    _wu_fail = 0

    async def _fetch_wu_one(target_d: date):
        nonlocal _wu_ok, _wu_fail
        async with _MOS_SEM:
            try:
                wu = await fetch_wu_daily(session, city_key, target_d)
                if wu and wu.is_complete:
                    _wu_ok += 1
                    return target_d, wu.high_temp
                else:
                    _wu_fail += 1
            except Exception:
                _wu_fail += 1
            return target_d, None

    wu_results = await asyncio.gather(*[_fetch_wu_one(d) for d in days_list])
    for target_d, high in wu_results:
        if high is not None:
            wu_actuals[target_d] = high

    print(f"      [{city_key}] Step 1 done: {len(wu_actuals)}/{n_days} WU days "
          f"({_wu_fail} failed) in {_time.time()-_t0:.1f}s")

    if len(wu_actuals) < MOS_MIN_DAYS:
        print(f"      [{city_key}] ✗ Insufficient WU data ({len(wu_actuals)} < {MOS_MIN_DAYS})")
        return None

    # ── Step 2: Bulk-fetch model hindcasts via historical forecast API ──
    _t1 = _time.time()
    print(f"      [{city_key}] Step 2: Fetching historical model hindcasts...")
    try:
        model_hindcasts = await _fetch_historical_model_maxes(
            session, city, start_date, end_date,
        )
    except Exception:
        model_hindcasts = {}
    print(f"      [{city_key}] Step 2 done: {len(model_hindcasts)} models in {_time.time()-_t1:.1f}s")

    # ── Step 2b: Supplement with logged data for dates the API missed ──
    # The historical forecast API only covers ~15 days. Our data archive has:
    #   - WU actuals (ground truth) — always safe to use
    #   - Deterministic model values (JMA, MeteoFrance, etc.) — safe for MOS errors
    #   - Ensemble member data — NOT safe for MOS error profiles!
    #
    # WHY ensemble means can't be used for MOS errors:
    # MOS errors are computed as (deterministic_max - wu_actual). At forecast time,
    # generate_mos_samples subtracts det_ens_offset to convert det→ensemble space.
    # If we inject ensemble means here, the error is already in ensemble space,
    # causing a DOUBLE offset correction (+3-4°F for GFS at US stations).
    # Only deterministic values are compatible with the MOS error/offset framework.
    _logged_det = 0
    _logged_actuals = 0

    for d in days_list:
        # First: try to fill in missing WU actuals from our logged archive
        if d not in wu_actuals:
            logged_actual = load_logged_actual(city_key, d)
            if logged_actual is not None:
                wu_actuals[d] = logged_actual
                _logged_actuals += 1

        if d not in wu_actuals:
            continue  # Still no ground truth for this day, skip

        # Load deterministic model values (safe for MOS — these ARE deterministic)
        logged_det = load_logged_deterministic(city_key, d)
        for model_name, det_val in logged_det.items():
            if model_name not in model_hindcasts:
                model_hindcasts[model_name] = {}
            if d not in model_hindcasts[model_name]:
                model_hindcasts[model_name][d] = det_val
                _logged_det += 1

    if _logged_actuals:
        print(f"      [{city_key}] Step 2b: +{_logged_actuals} WU actuals from archive")
    if _logged_det:
        print(f"      [{city_key}] Step 2b: +{_logged_det} deterministic data points from archive")

    # ── Step 3: Compute per-model errors against WU actuals ──
    profiles: Dict[str, ModelErrorProfile] = {}

    for model, hindcast_data in model_hindcasts.items():
        errors: List[float] = []
        for d in sorted(wu_actuals.keys()):
            if d in hindcast_data:
                model_max = hindcast_data[d]
                wu_actual = wu_actuals[d]
                errors.append(model_max - wu_actual)

        if len(errors) >= MOS_MIN_DAYS:
            err_arr = np.array(errors, dtype=np.float64)
            profiles[model] = ModelErrorProfile(
                model=model,
                errors=errors,
                mean_bias=float(np.mean(err_arr)),
                std_error=float(np.std(err_arr)),
                rmse=float(np.sqrt(np.mean(err_arr ** 2))),
                mae=float(np.mean(np.abs(err_arr))),
                n_days=len(errors),
            )

    if not profiles:
        return None

    _t2 = _time.time()
    # ── Step 4: Compute deterministic-to-ensemble offset (parallel) ──
    # The historical forecast API returns deterministic model output, but at
    # forecast time we use ensemble means. These can differ significantly
    # (e.g. GFS deterministic is ~3°F warmer than GFS ensemble mean).
    # We measure the offset on recent days where ensemble data is available.
    # Only for models that HAVE ensemble APIs — skip deterministic-only models.
    from .config import ENSEMBLE_MODELS
    ensemble_set = set(ENSEMBLE_MODELS)
    recent_days = sorted(wu_actuals.keys())[-5:]  # Last 5 days only

    # Build all (model, date) pairs that need ensemble fetches
    offset_tasks = []
    for model in list(profiles.keys()):
        if model not in ensemble_set:
            continue
        hindcast_data = model_hindcasts.get(model, {})
        for d in recent_days:
            if d in hindcast_data:
                offset_tasks.append((model, d, hindcast_data[d]))

    async def _fetch_offset(model: str, target_d: date, det_val: float):
        async with _MOS_SEM:
            try:
                ens_maxes = await _fetch_ensemble_maxes(session, city, target_d, model)
            except Exception:
                ens_maxes = []
            if ens_maxes:
                return model, det_val - float(np.mean(ens_maxes))
            return model, None

    offset_results = await asyncio.gather(
        *[_fetch_offset(m, d, dv) for m, d, dv in offset_tasks]
    )

    # Group offsets by model
    model_offsets: Dict[str, List[float]] = {}
    for model, offset_val in offset_results:
        if offset_val is not None:
            model_offsets.setdefault(model, []).append(offset_val)
    for model, offs in model_offsets.items():
        if model in profiles:
            profiles[model].det_ens_offset = float(np.mean(offs))
    print(f"      [{city_key}] Step 4 done: {len(offset_tasks)} offset fetches in {_time.time()-_t2:.1f}s")

    # ── Step 5: Compute skill weights (inverse std_error²) ──
    # After MOS corrects the bias, only ERROR VARIABILITY matters for weighting.
    # Use std_error (not RMSE which includes bias). A model with huge bias but
    # low variability (e.g. ECMWF at NYC: bias=-5°F, std=0.95°F) is excellent
    # after correction — it gives tight, consistent corrected samples.
    total_inv = sum(1.0 / max(p.std_error, 0.1) ** 2 for p in profiles.values())
    for p in profiles.values():
        p.skill_weight = (1.0 / max(p.std_error, 0.1) ** 2) / total_inv

    return StationMOS(
        city_key=city_key,
        profiles=profiles,
        n_days=len(wu_actuals),
        n_models=len(profiles),
        last_updated=datetime.now(timezone.utc).isoformat(),
    )


# ─── MOS-Corrected Sample Generation ─────────────────────────────────────────

def generate_mos_samples(
    mos: StationMOS,
    ensemble_by_model: Dict[str, List[float]],
    wu_forecast_temp: Optional[int] = None,
    lead_days: int = 1,
    n_target: int = N_TARGET_SAMPLES,
) -> np.ndarray:
    """
    Generate MOS-corrected forecast samples for probability estimation.

    Combines three sources:
      1. MOS error resampling  — model_mean − historical_error → station-corrected
      2. Bias-corrected ensemble — raw members − bias → day-specific uncertainty
      3. WU forecast (optional) — tight distribution around resolution source's own prediction

    Returns np.ndarray of ~n_target samples in the city's native temperature unit.
    """
    rng = np.random.default_rng()
    all_samples: List[float] = []

    # Determine WU allocation (more weight for shorter lead times)
    # WU IS the resolution source — their forecast deserves heavy weight.
    # Dynamic boost: when WU disagrees with model consensus, trust WU more
    # (correlated model bias is the #1 failure mode; WU is independent).
    has_wu = wu_forecast_temp is not None
    wu_frac = 0.0
    if has_wu:
        wu_frac_by_lead = {0: 0.35, 1: 0.30, 2: 0.22, 3: 0.12}
        wu_frac = wu_frac_by_lead.get(lead_days, 0.07)

        # Dynamic WU boost: measure disagreement between WU and model consensus
        model_means = []
        for model, raw_members in ensemble_by_model.items():
            if raw_members:
                model_means.append(float(np.mean(raw_members)))
        if model_means:
            consensus = float(np.mean(model_means))
            city = CITIES.get(mos.city_key)
            scale = 1.8 if (city and city.unit == "F") else 1.0
            disagreement = abs(wu_forecast_temp - consensus) / scale  # in °C
            # If WU disagrees by >1.5°C, boost WU weight by up to 50%
            if disagreement > 1.5:
                boost = min(0.50, (disagreement - 1.5) * 0.25)
                wu_frac = min(0.55, wu_frac * (1.0 + boost))

    remaining = 1.0 - wu_frac
    mos_frac = remaining * (MOS_ERROR_FRAC / (MOS_ERROR_FRAC + RAW_ENSEMBLE_FRAC))
    raw_frac = remaining * (RAW_ENSEMBLE_FRAC / (MOS_ERROR_FRAC + RAW_ENSEMBLE_FRAC))

    n_mos_total = int(n_target * mos_frac)
    n_raw_total = int(n_target * raw_frac)
    n_wu = int(n_target * wu_frac)

    # ── 1. MOS error resampling (per-model, skill-weighted, recency-weighted) ──
    for model, raw_members in ensemble_by_model.items():
        profile = mos.profiles.get(model)
        if not profile or not profile.errors:
            continue

        model_mean = float(np.mean(raw_members))
        n_mos = max(1, int(n_mos_total * profile.skill_weight))

        # Adjust errors for deterministic-to-ensemble offset:
        # error_i was computed as (det_max - actual), but we're correcting ensemble_mean.
        # adjusted_error = error_i - offset, so corrected = ens_mean - adjusted_error
        offset = profile.det_ens_offset
        adjusted_errors = [e - offset for e in profile.errors]

        # Recency weighting: errors are chronological (oldest first).
        # Give exponentially more weight to recent errors (current weather regime).
        n_errors = len(adjusted_errors)
        decay = np.log(2) / RECENCY_HALFLIFE_DAYS
        weights = np.array([np.exp(-decay * (n_errors - 1 - i)) for i in range(n_errors)])
        weights /= weights.sum()

        error_indices = rng.choice(n_errors, size=n_mos, replace=True, p=weights)
        mos_samples = [model_mean - adjusted_errors[i] for i in error_indices]
        all_samples.extend(mos_samples)

    # ── 2. Bias-corrected raw ensemble members (day-specific uncertainty) ──
    for model, raw_members in ensemble_by_model.items():
        profile = mos.profiles.get(model)
        # Adjusted bias: mean_bias from deterministic, minus offset = ensemble bias
        bias = (profile.mean_bias - profile.det_ens_offset) if profile else 0.0
        weight = profile.skill_weight if profile else (1.0 / max(len(ensemble_by_model), 1))

        corrected = [m - bias for m in raw_members]
        n_raw = max(1, int(n_raw_total * weight))

        if n_raw >= len(corrected):
            all_samples.extend(corrected)
        else:
            indices = rng.choice(len(corrected), size=n_raw, replace=True)
            all_samples.extend([corrected[i] for i in indices])

    # ── 3. WU forecast injection (resolution source's own prediction) ──
    if has_wu and n_wu > 0:
        std_base = WU_LEAD_STD.get(lead_days, WU_LEAD_STD_DEFAULT)
        city = CITIES.get(mos.city_key)
        if city and city.unit == "F":
            std_base *= 1.8
        wu_samples = rng.normal(wu_forecast_temp, std_base, size=n_wu)
        all_samples.extend(wu_samples.tolist())

    if not all_samples:
        return np.array([], dtype=np.float64)

    return np.array(all_samples, dtype=np.float64)


# ─── MOS Bucket Probabilities ────────────────────────────────────────────────

def mos_bucket_probabilities(
    samples: np.ndarray,
    buckets,
) -> Dict[str, float]:
    """
    Compute bucket probabilities from MOS samples using pure empirical counting
    with Laplace smoothing.  No Gaussian assumption.

    WU reports whole-degree integers, so bucket boundaries are adjusted by ±0.5
    to account for rounding.
    """
    n = len(samples)
    n_buckets = len(buckets)

    if n == 0:
        return {b.label: 1.0 / n_buckets for b in buckets}

    result: Dict[str, float] = {}
    alpha = 1.0  # Laplace smoothing pseudocount per bucket

    for bucket in buckets:
        low = bucket.low - 0.5 if bucket.low != float('-inf') else float('-inf')
        high = bucket.high + 0.5 if bucket.high != float('inf') else float('inf')

        count = float(np.sum((samples >= low) & (samples < high)))
        result[bucket.label] = (count + alpha) / (n + alpha * n_buckets)

    # Normalise
    total = sum(result.values())
    if total > 0:
        result = {k: v / total for k, v in result.items()}

    return result


# ─── Cache Management ─────────────────────────────────────────────────────────

def save_mos_cache(all_mos: Dict[str, StationMOS]):
    """Persist MOS data to disk."""
    data = {}
    for city_key, mos in all_mos.items():
        data[city_key] = {
            "city_key": mos.city_key,
            "n_days": mos.n_days,
            "n_models": mos.n_models,
            "last_updated": mos.last_updated,
            "profiles": {
                model: {
                    "model": p.model,
                    "errors": p.errors,
                    "mean_bias": p.mean_bias,
                    "std_error": p.std_error,
                    "rmse": p.rmse,
                    "mae": p.mae,
                    "n_days": p.n_days,
                    "skill_weight": p.skill_weight,
                    "det_ens_offset": p.det_ens_offset,
                }
                for model, p in mos.profiles.items()
            },
        }
    with open(MOS_CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_mos_cache() -> Dict[str, StationMOS]:
    """Load MOS data from disk."""
    if not MOS_CACHE_FILE.exists():
        return {}
    try:
        with open(MOS_CACHE_FILE) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

    result: Dict[str, StationMOS] = {}
    for city_key, cdata in data.items():
        profiles: Dict[str, ModelErrorProfile] = {}
        for model, pdata in cdata.get("profiles", {}).items():
            profiles[model] = ModelErrorProfile(
                model=pdata["model"],
                errors=pdata["errors"],
                mean_bias=pdata["mean_bias"],
                std_error=pdata["std_error"],
                rmse=pdata["rmse"],
                mae=pdata["mae"],
                n_days=pdata["n_days"],
                skill_weight=pdata.get("skill_weight", 0.0),
                det_ens_offset=pdata.get("det_ens_offset", 0.0),
            )
        result[city_key] = StationMOS(
            city_key=city_key,
            profiles=profiles,
            n_days=cdata.get("n_days", 0),
            n_models=cdata.get("n_models", 0),
            last_updated=cdata.get("last_updated", ""),
        )
    return result


def is_mos_fresh(mos: StationMOS) -> bool:
    """Check if MOS data is recent enough to use."""
    try:
        updated = datetime.fromisoformat(mos.last_updated)
        age = datetime.now(timezone.utc) - updated
        return age.total_seconds() < MOS_FRESHNESS_HOURS * 3600
    except (ValueError, TypeError):
        return False


_mos_memory_cache: Dict[str, StationMOS] = {}


async def get_or_build_mos(
    session: aiohttp.ClientSession,
    city_key: str,
) -> Optional[StationMOS]:
    """Get MOS — from memory cache, disk cache, or rebuild."""
    global _mos_memory_cache

    # 1. In-memory cache (avoids re-reading JSON 84 times in backtest)
    if city_key in _mos_memory_cache and is_mos_fresh(_mos_memory_cache[city_key]):
        return _mos_memory_cache[city_key]

    # 2. Disk cache
    cached = load_mos_cache()
    _mos_memory_cache.update(cached)  # Populate memory cache
    if city_key in cached and is_mos_fresh(cached[city_key]):
        return cached[city_key]

    # 3. Rebuild
    mos = await build_station_mos(session, city_key)
    if mos:
        cached[city_key] = mos
        _mos_memory_cache[city_key] = mos
        save_mos_cache(cached)
    return mos


async def build_all_mos(
    session: aiohttp.ClientSession,
) -> Dict[str, StationMOS]:
    """Build MOS for all stations. Parallel build with bounded concurrency."""
    cached = load_mos_cache()
    result: Dict[str, StationMOS] = {}

    # Separate cached vs stale
    to_build = []
    for city_key in CITIES:
        if city_key in cached and is_mos_fresh(cached[city_key]):
            result[city_key] = cached[city_key]
            print(f"   ✓ {city_key}: cached ({cached[city_key].n_days}d, {cached[city_key].n_models}m)")
        else:
            to_build.append(city_key)

    if to_build:
        print(f"   ⏳ Building MOS for {len(to_build)} stations in parallel...")

        # Build up to 4 stations concurrently (each station already uses _MOS_SEM internally)
        station_sem = asyncio.Semaphore(4)

        async def _build_one(ck: str):
            async with station_sem:
                return ck, await build_station_mos(session, ck)

        mos_results = await asyncio.gather(
            *[_build_one(ck) for ck in to_build], return_exceptions=True
        )

        for item in mos_results:
            if isinstance(item, Exception):
                continue
            city_key, mos = item
            if mos:
                result[city_key] = mos
                bias_strs = [
                    "%s:%+.1f" % (p.model.split("_")[0], p.mean_bias)
                    for p in mos.profiles.values()
                ]
                print(f"   ✓ {city_key}: {mos.n_days} days, {mos.n_models} models, "
                      f"biases=[{', '.join(bias_strs)}]")
            else:
                print(f"   ✗ {city_key}: insufficient data")

    save_mos_cache(result)
    return result


# ─── Diagnostic Helpers ───────────────────────────────────────────────────────

def mos_summary(mos: StationMOS) -> str:
    """One-line summary of MOS data for display."""
    parts = []
    for model, p in sorted(mos.profiles.items(), key=lambda x: x[1].skill_weight, reverse=True):
        short = model.split("_")[0]
        parts.append(f"{short}(b={p.mean_bias:+.1f},rmse={p.rmse:.1f},w={p.skill_weight:.0%})")
    return f"MOS[{mos.n_days}d]: {', '.join(parts)}"
