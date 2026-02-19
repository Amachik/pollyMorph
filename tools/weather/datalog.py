"""
Weather Data Logger — Persistent archive of all fetched data.

Logs every API response to disk so we accumulate a growing dataset over time.
This is critical because:
  1. Open-Meteo historical forecast API only goes back ~15 days
  2. Polymarket removes resolved market data after a while
  3. More data → better MOS calibration → better forecasts → more profit

Data is stored as JSON files organized by type and date:
  data/weather/
    ensembles/       # Per-model ensemble member forecasts
    deterministic/   # MOS-only deterministic model forecasts
    markets/         # Market prices, outcomes, liquidity
    actuals/         # WU observed highs (ground truth)
    current/         # Current weather snapshots (for same-day analysis)
    forecasts/       # Our final combined forecast probabilities

Each file is named: {city_key}_{target_date}.json
Files are append-safe: if data already exists for a city+date, it merges
(keeping the latest fetch timestamp for each model/source).
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ─── Storage Root ────────────────────────────────────────────────────────────

DATA_ROOT = Path(__file__).parent.parent.parent / "data" / "weather"

ENSEMBLE_DIR = DATA_ROOT / "ensembles"
DETERMINISTIC_DIR = DATA_ROOT / "deterministic"
MARKETS_DIR = DATA_ROOT / "markets"
ACTUALS_DIR = DATA_ROOT / "actuals"
CURRENT_DIR = DATA_ROOT / "current"
FORECASTS_DIR = DATA_ROOT / "forecasts"

ALL_DIRS = [ENSEMBLE_DIR, DETERMINISTIC_DIR, MARKETS_DIR, ACTUALS_DIR,
            CURRENT_DIR, FORECASTS_DIR]


def _ensure_dirs():
    """Create all data directories if they don't exist."""
    for d in ALL_DIRS:
        d.mkdir(parents=True, exist_ok=True)


def _file_path(directory: Path, city_key: str, target_date: date) -> Path:
    """Standard file path: directory/city_key_YYYY-MM-DD.json"""
    return directory / f"{city_key}_{target_date.isoformat()}.json"


def _load_existing(path: Path) -> Dict[str, Any]:
    """Load existing JSON data or return empty dict."""
    if path.exists():
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    # Clean up stale .tmp files from interrupted writes
    tmp = path.with_suffix(".tmp")
    if tmp.exists():
        try:
            tmp.unlink()
        except OSError:
            pass
    return {}


def _save(path: Path, data: Dict[str, Any]):
    """Atomically write JSON data."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2, default=str)
        tmp.rename(path)
    except OSError as e:
        logger.warning(f"Failed to save {path}: {e}")
        if tmp.exists():
            tmp.unlink()


# ─── Ensemble Forecasts ─────────────────────────────────────────────────────

def log_ensemble(
    city_key: str,
    target_date: date,
    model_name: str,
    member_daily_maxes: List[float],
    fetch_date: Optional[date] = None,
):
    """
    Log ensemble forecast data: per-member daily max temperatures.

    Stored as:
    {
      "city_key": "nyc",
      "target_date": "2026-02-15",
      "models": {
        "ecmwf_ifs025": {
          "members": [45.2, 46.1, ...],
          "n_members": 50,
          "fetched_at": "2026-02-14T18:00:00+00:00",
          "fetch_date": "2026-02-14"
        },
        ...
      }
    }
    """
    _ensure_dirs()
    path = _file_path(ENSEMBLE_DIR, city_key, target_date)
    data = _load_existing(path)

    if "models" not in data:
        data["city_key"] = city_key
        data["target_date"] = target_date.isoformat()
        data["models"] = {}

    fd = fetch_date or datetime.now(timezone.utc).date()
    data["models"][model_name] = {
        "members": member_daily_maxes,
        "n_members": len(member_daily_maxes),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "fetch_date": fd.isoformat(),
    }

    _save(path, data)


def log_ensemble_batch(
    city_key: str,
    target_date: date,
    models_data: Dict[str, List[float]],
    fetch_date: Optional[date] = None,
):
    """Log multiple ensemble models in a single file write (avoids N read/write cycles)."""
    if not models_data:
        return
    _ensure_dirs()
    path = _file_path(ENSEMBLE_DIR, city_key, target_date)
    data = _load_existing(path)

    if "models" not in data:
        data["city_key"] = city_key
        data["target_date"] = target_date.isoformat()
        data["models"] = {}

    fd = fetch_date or datetime.now(timezone.utc).date()
    now_iso = datetime.now(timezone.utc).isoformat()
    for model_name, members in models_data.items():
        if members:
            data["models"][model_name] = {
                "members": members,
                "n_members": len(members),
                "fetched_at": now_iso,
                "fetch_date": fd.isoformat(),
            }

    _save(path, data)


# ─── Deterministic Forecasts (MOS-only models) ──────────────────────────────

def log_deterministic(
    city_key: str,
    target_date: date,
    model_name: str,
    forecast_value: float,
    synthetic_members: Optional[List[float]] = None,
    fetch_date: Optional[date] = None,
):
    """
    Log deterministic model forecast (JMA, MeteoFrance, UKMO, KNMI).

    Stored as:
    {
      "city_key": "london",
      "target_date": "2026-02-15",
      "models": {
        "jma_gsm": {
          "value": 8.5,
          "synthetic_members": [8.2, 8.7, ...],
          "fetched_at": "...",
          "fetch_date": "2026-02-14"
        }
      }
    }
    """
    _ensure_dirs()
    path = _file_path(DETERMINISTIC_DIR, city_key, target_date)
    data = _load_existing(path)

    if "models" not in data:
        data["city_key"] = city_key
        data["target_date"] = target_date.isoformat()
        data["models"] = {}

    fd = fetch_date or datetime.now(timezone.utc).date()
    entry = {
        "value": forecast_value,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "fetch_date": fd.isoformat(),
    }
    if synthetic_members:
        entry["synthetic_members"] = synthetic_members

    data["models"][model_name] = entry
    _save(path, data)


# ─── Market Data ─────────────────────────────────────────────────────────────

def log_market(
    city_key: str,
    target_date: date,
    outcomes: List[Dict[str, Any]],
    liquidity: float = 0.0,
    slug: str = "",
    resolved: bool = False,
    winning_label: Optional[str] = None,
):
    """
    Log market data: per-bucket prices, liquidity, resolution status.

    outcomes: list of {label, market_prob, token_id, ...}

    Stored as:
    {
      "city_key": "nyc",
      "target_date": "2026-02-15",
      "slug": "highest-temperature-in-new-york-city-on-february-15-2026",
      "liquidity": 5000.0,
      "resolved": false,
      "winning_label": null,
      "snapshots": [
        {
          "timestamp": "2026-02-14T18:00:00+00:00",
          "outcomes": [
            {"label": "40-41°F", "market_prob": 0.34, "token_id": "..."},
            ...
          ]
        }
      ]
    }
    """
    _ensure_dirs()
    path = _file_path(MARKETS_DIR, city_key, target_date)
    data = _load_existing(path)

    data["city_key"] = city_key
    data["target_date"] = target_date.isoformat()
    if slug:
        data["slug"] = slug
    data["liquidity"] = liquidity
    data["resolved"] = resolved
    if winning_label:
        data["winning_label"] = winning_label

    if "snapshots" not in data:
        data["snapshots"] = []

    # Add new snapshot
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "outcomes": outcomes,
    }
    data["snapshots"].append(snapshot)

    # Cap at 50 snapshots per market to prevent unbounded file growth
    if len(data["snapshots"]) > 50:
        data["snapshots"] = data["snapshots"][-50:]

    _save(path, data)


# ─── WU Actuals (Ground Truth) ──────────────────────────────────────────────

def log_actual(
    city_key: str,
    target_date: date,
    high_temp: float,
    low_temp: Optional[float] = None,
    source: str = "wunderground",
    station_id: Optional[str] = None,
):
    """
    Log observed actual temperature (from Weather Underground).

    Stored as:
    {
      "city_key": "nyc",
      "target_date": "2026-02-15",
      "high_temp": 45.0,
      "low_temp": 32.0,
      "source": "wunderground",
      "station_id": "KLGA",
      "logged_at": "2026-02-16T02:00:00+00:00"
    }
    """
    _ensure_dirs()
    path = _file_path(ACTUALS_DIR, city_key, target_date)
    data = {
        "city_key": city_key,
        "target_date": target_date.isoformat(),
        "high_temp": high_temp,
        "source": source,
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }
    if low_temp is not None:
        data["low_temp"] = low_temp
    if station_id:
        data["station_id"] = station_id

    _save(path, data)


# ─── Current Weather Snapshots ───────────────────────────────────────────────

def log_current_weather(
    city_key: str,
    temperature: float,
    daily_high_so_far: float,
    forecast_remaining_max: Optional[float] = None,
    apparent_temp: Optional[float] = None,
):
    """
    Log current weather snapshot. Multiple snapshots per day are appended.

    Stored as:
    {
      "city_key": "nyc",
      "date": "2026-02-15",
      "snapshots": [
        {
          "timestamp": "2026-02-15T14:00:00+00:00",
          "temperature": 42.0,
          "daily_high_so_far": 43.5,
          "forecast_remaining_max": 45.0,
          "apparent_temp": 38.0
        },
        ...
      ]
    }
    """
    _ensure_dirs()
    today = datetime.now(timezone.utc).date()
    path = _file_path(CURRENT_DIR, city_key, today)
    data = _load_existing(path)

    if "snapshots" not in data:
        data["city_key"] = city_key
        data["date"] = today.isoformat()
        data["snapshots"] = []

    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": temperature,
        "daily_high_so_far": daily_high_so_far,
    }
    if forecast_remaining_max is not None:
        snapshot["forecast_remaining_max"] = forecast_remaining_max
    if apparent_temp is not None:
        snapshot["apparent_temp"] = apparent_temp

    data["snapshots"].append(snapshot)

    # Cap at 100 snapshots per day to prevent unbounded growth
    if len(data["snapshots"]) > 100:
        data["snapshots"] = data["snapshots"][-100:]

    _save(path, data)


# ─── Our Forecast Probabilities ──────────────────────────────────────────────

def log_forecast(
    city_key: str,
    target_date: date,
    bucket_probs: Dict[str, float],
    forecast_mean: float,
    forecast_std: float,
    n_members: int,
    mos_used: bool = False,
    sources: Optional[List[str]] = None,
):
    """
    Log our final forecast probabilities for a market.

    Stored as:
    {
      "city_key": "nyc",
      "target_date": "2026-02-15",
      "snapshots": [
        {
          "timestamp": "2026-02-14T18:00:00+00:00",
          "bucket_probs": {"40-41°F": 0.34, "42-43°F": 0.28, ...},
          "mean": 42.5,
          "std": 2.1,
          "n_members": 250,
          "mos_used": true,
          "sources": ["ecmwf_ifs025", "gfs025", ...]
        }
      ]
    }
    """
    _ensure_dirs()
    path = _file_path(FORECASTS_DIR, city_key, target_date)
    data = _load_existing(path)

    if "snapshots" not in data:
        data["city_key"] = city_key
        data["target_date"] = target_date.isoformat()
        data["snapshots"] = []

    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bucket_probs": bucket_probs,
        "mean": forecast_mean,
        "std": forecast_std,
        "n_members": n_members,
        "mos_used": mos_used,
    }
    if sources:
        snapshot["sources"] = sources

    data["snapshots"].append(snapshot)

    # Cap at 20 snapshots per market to prevent unbounded growth
    if len(data["snapshots"]) > 20:
        data["snapshots"] = data["snapshots"][-20:]

    _save(path, data)


# ─── Data Loading (for future use in MOS/calibration) ───────────────────────

def load_logged_ensembles(
    city_key: str,
    target_date: date,
) -> Dict[str, List[float]]:
    """Load logged ensemble data for a city+date. Returns {model: [members]}."""
    path = _file_path(ENSEMBLE_DIR, city_key, target_date)
    data = _load_existing(path)
    models = data.get("models", {})
    return {name: info["members"] for name, info in models.items() if "members" in info}


def load_logged_deterministic(
    city_key: str,
    target_date: date,
) -> Dict[str, float]:
    """Load logged deterministic model values for a city+date.

    Returns {model_name: deterministic_daily_max}.
    These are the actual deterministic forecast values — safe for MOS
    error computation (unlike ensemble means which differ by det_ens_offset).
    """
    path = _file_path(DETERMINISTIC_DIR, city_key, target_date)
    data = _load_existing(path)
    models = data.get("models", {})
    return {name: info["value"] for name, info in models.items()
            if "value" in info and info["value"] is not None}


def load_logged_actual(city_key: str, target_date: date) -> Optional[float]:
    """Load logged actual high temp for a city+date."""
    path = _file_path(ACTUALS_DIR, city_key, target_date)
    data = _load_existing(path)
    return data.get("high_temp")


def load_logged_market(city_key: str, target_date: date) -> Optional[Dict]:
    """Load logged market data for a city+date."""
    path = _file_path(MARKETS_DIR, city_key, target_date)
    data = _load_existing(path)
    return data if data else None


def list_logged_dates(city_key: str, data_type: str = "ensembles") -> List[date]:
    """List all dates with logged data for a city."""
    dir_map = {
        "ensembles": ENSEMBLE_DIR,
        "deterministic": DETERMINISTIC_DIR,
        "markets": MARKETS_DIR,
        "actuals": ACTUALS_DIR,
        "forecasts": FORECASTS_DIR,
    }
    directory = dir_map.get(data_type, ENSEMBLE_DIR)
    if not directory.exists():
        return []

    dates = []
    prefix = f"{city_key}_"
    for f in directory.glob(f"{prefix}*.json"):
        date_str = f.stem.replace(prefix, "")
        try:
            dates.append(date.fromisoformat(date_str))
        except ValueError:
            continue
    return sorted(dates)


def get_data_stats() -> Dict[str, int]:
    """Get counts of logged data files by type."""
    stats = {}
    for name, directory in [
        ("ensembles", ENSEMBLE_DIR),
        ("deterministic", DETERMINISTIC_DIR),
        ("markets", MARKETS_DIR),
        ("actuals", ACTUALS_DIR),
        ("current", CURRENT_DIR),
        ("forecasts", FORECASTS_DIR),
    ]:
        if directory.exists():
            stats[name] = len(list(directory.glob("*.json")))
        else:
            stats[name] = 0
    return stats
