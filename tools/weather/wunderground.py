"""
Weather Underground API — Polymarket's resolution source for weather markets.

Fetches actual observed temperatures from the same stations that Polymarket uses
to resolve markets. This is critical because Open-Meteo grid-point data can differ
by 1-2°C from WU station observations (different sensors, different locations).

WU API key: public key used by wunderground.com frontend (rate-limited but free).
"""

import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp

from .config import CITIES, City

# ─── WU API ──────────────────────────────────────────────────────────────────

# Public API key used by wunderground.com (same one their frontend uses)
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
WU_API_BASE = "https://api.weather.com/v1/location"

MAX_RETRIES = 2
RETRY_DELAY = 1.0


@dataclass
class WUObservation:
    """A single hourly observation from Weather Underground."""
    timestamp: int          # Unix timestamp (UTC)
    temp: float             # Temperature in native units (°F or °C)
    temp_raw: int           # Raw integer temp as reported by WU


@dataclass
class WUDailyResult:
    """Daily temperature summary from Weather Underground observations."""
    station_id: str
    city_key: str
    target_date: date
    high_temp: int          # Highest observed temp (whole degrees, matches WU resolution)
    low_temp: int           # Lowest observed temp
    n_observations: int     # Number of hourly observations
    observations: List[WUObservation]
    unit: str               # "F" or "C"
    is_complete: bool       # True if full day of data available


async def fetch_wu_daily(
    session: aiohttp.ClientSession,
    city_key: str,
    target_date: date,
) -> Optional[WUDailyResult]:
    """
    Fetch actual daily temperature observations from Weather Underground.

    Uses the same API that wunderground.com's frontend uses.
    Returns whole-degree max/min matching Polymarket's resolution precision.
    """
    city = CITIES.get(city_key)
    if not city or not city.wunderground_id or not city.wu_country:
        return None

    station = city.wunderground_id
    country = city.wu_country
    units = "e" if city.unit == "F" else "m"  # e=imperial, m=metric
    date_str = target_date.strftime("%Y%m%d")

    url = (
        f"{WU_API_BASE}/{station}:9:{country}/observations/historical.json"
        f"?apiKey={WU_API_KEY}&units={units}"
        f"&startDate={date_str}&endDate={date_str}"
    )

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return None

                data = await resp.json()
                obs_list = data.get("observations", [])

                if not obs_list:
                    return None

                observations = []
                temps = []
                for obs in obs_list:
                    temp = obs.get("temp")
                    if temp is not None:
                        temps.append(int(temp))
                        observations.append(WUObservation(
                            timestamp=obs.get("valid_time_gmt", 0),
                            temp=float(temp),
                            temp_raw=int(temp),
                        ))

                if not temps:
                    return None

                # Check if we have a full day of data
                # Full day typically has 24-48 observations
                # Partial day (still in progress) has fewer
                is_complete = len(observations) >= 20

                return WUDailyResult(
                    station_id=station,
                    city_key=city_key,
                    target_date=target_date,
                    high_temp=max(temps),
                    low_temp=min(temps),
                    n_observations=len(observations),
                    observations=observations,
                    unit=city.unit,
                    is_complete=is_complete,
                )

        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
            continue
        except Exception:
            return None

    return None


async def fetch_wu_multi_day(
    session: aiohttp.ClientSession,
    city_key: str,
    start_date: date,
    end_date: date,
) -> Dict[date, int]:
    """
    Fetch actual daily high temps from WU for a range of dates.
    Returns {date: high_temp} dict. Only includes complete days.
    """
    results = {}
    current = start_date
    while current <= end_date:
        result = await fetch_wu_daily(session, city_key, current)
        if result and result.is_complete:
            results[current] = result.high_temp
        current = date.fromordinal(current.toordinal() + 1)
        await asyncio.sleep(0.2)  # Rate limit
    return results


WU_FORECAST_URL = "https://api.weather.com/v3/wx/forecast/daily/5day"


@dataclass
class WUForecast:
    """WU's own high temperature forecast for a specific date."""
    station_id: str
    city_key: str
    target_date: date
    high_temp: int          # Predicted daily high (whole degrees, native unit)
    low_temp: Optional[int]
    lead_days: int          # How many days ahead (0=today, 1=tomorrow, etc.)
    unit: str


async def fetch_wu_forecast(
    session: aiohttp.ClientSession,
    city_key: str,
    target_date: date,
) -> Optional[WUForecast]:
    """
    Fetch Weather Underground's own high temperature forecast.

    This is the SINGLE MOST VALUABLE data source because WU IS the resolution
    source for Polymarket weather markets. WU's forecast for their own station
    is the best predictor of the resolution value.

    Uses calendarDayTemperatureMax which matches Polymarket's midnight-to-midnight
    whole-degree resolution.
    """
    city = CITIES.get(city_key)
    if not city or not city.wunderground_id:
        return None

    units = "e" if city.unit == "F" else "m"
    params = {
        "icaoCode": city.wunderground_id,
        "format": "json",
        "units": units,
        "language": "en-US",
        "apiKey": WU_API_KEY,
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                WU_FORECAST_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                if resp.status != 200:
                    return None

                data = await resp.json()

                # calendarDayTemperatureMax = midnight-to-midnight max (matches Polymarket)
                highs = data.get("calendarDayTemperatureMax", [])
                lows = data.get("calendarDayTemperatureMin", [])
                dates = data.get("validTimeLocal", [])

                if not highs or not dates:
                    return None

                # Find the target date in the forecast
                for i, date_str in enumerate(dates):
                    if not date_str:
                        continue
                    forecast_date = date.fromisoformat(date_str[:10])
                    if forecast_date == target_date and i < len(highs):
                        high = highs[i]
                        if high is None:
                            continue
                        low = lows[i] if i < len(lows) else None
                        today = datetime.now(timezone.utc).date()
                        lead = (target_date - today).days
                        return WUForecast(
                            station_id=city.wunderground_id,
                            city_key=city_key,
                            target_date=target_date,
                            high_temp=int(high),
                            low_temp=int(low) if low is not None else None,
                            lead_days=max(0, lead),
                            unit=city.unit,
                        )
                return None

        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
            continue
        except Exception:
            return None

    return None


async def fetch_wu_forecast_all_days(
    session: aiohttp.ClientSession,
    city_key: str,
) -> List[WUForecast]:
    """
    Fetch WU forecasts for all available days (up to 5).
    Returns list of WUForecast objects, one per day.
    """
    city = CITIES.get(city_key)
    if not city or not city.wunderground_id:
        return []

    units = "e" if city.unit == "F" else "m"
    params = {
        "icaoCode": city.wunderground_id,
        "format": "json",
        "units": units,
        "language": "en-US",
        "apiKey": WU_API_KEY,
    }

    try:
        async with session.get(
            WU_FORECAST_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception:
        return []

    highs = data.get("calendarDayTemperatureMax", [])
    lows = data.get("calendarDayTemperatureMin", [])
    dates = data.get("validTimeLocal", [])
    today = datetime.now(timezone.utc).date()

    results = []
    for i, date_str in enumerate(dates):
        if not date_str or i >= len(highs):
            continue
        high = highs[i]
        if high is None:
            continue
        forecast_date = date.fromisoformat(date_str[:10])
        low = lows[i] if i < len(lows) else None
        lead = (forecast_date - today).days
        results.append(WUForecast(
            station_id=city.wunderground_id,
            city_key=city_key,
            target_date=forecast_date,
            high_temp=int(high),
            low_temp=int(low) if low is not None else None,
            lead_days=max(0, lead),
            unit=city.unit,
        ))

    return results


async def fetch_wu_current_high(
    session: aiohttp.ClientSession,
    city_key: str,
) -> Optional[int]:
    """
    Fetch the current day's running high from WU.
    Returns the max temp observed so far today (whole degrees).
    """
    today = datetime.now(timezone.utc).date()
    result = await fetch_wu_daily(session, city_key, today)
    if result:
        return result.high_temp
    return None


# ─── Resolution Source Parsing ───────────────────────────────────────────────

def parse_wu_station_from_description(description: str) -> Optional[Tuple[str, str]]:
    """
    Extract WU station ID and station name from Polymarket event description.

    Returns (station_id, station_name) or None.

    Example description:
        "...recorded at the Incheon Intl Airport Station...
         https://www.wunderground.com/history/daily/kr/incheon/RKSI"
    """
    # Extract station name
    name_match = re.search(r"recorded at the (.+?) in degrees", description)
    station_name = name_match.group(1) if name_match else None

    # Extract station ID from WU URL
    url_match = re.search(
        r"wunderground\.com/history/daily/(\w+)/[\w%-]+/(\w+)",
        description,
    )
    if url_match:
        station_id = url_match.group(2)
        return (station_id, station_name or station_id)

    return None


def parse_resolution_url(description: str) -> Optional[str]:
    """Extract the full WU resolution URL from a market description."""
    match = re.search(r"(https://www\.wunderground\.com/history/daily/\S+)", description)
    return match.group(1) if match else None


def parse_precision(description: str) -> Optional[str]:
    """
    Parse the temperature precision from market rules.
    Returns "whole_C", "whole_F", or None.
    """
    match = re.search(r"whole degrees (Celsius|Fahrenheit)", description)
    if match:
        return f"whole_{match.group(1)[0]}"
    return None
