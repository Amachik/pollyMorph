"""
Polymarket Weather Market Discovery & Parsing
Discovers active weather markets, parses temperature buckets, extracts prices.
"""

import re
import json
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple

import aiohttp

from .config import CITIES, CITY_NAME_MAP, GAMMA_API_BASE, MONTH_NAMES

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class TempBucket:
    """A temperature range bucket from a Polymarket weather market."""
    label: str            # Original label, e.g. "36-37°F"
    low: float            # Lower bound (inclusive), -inf for "X or below"
    high: float           # Upper bound (inclusive), inf for "X or higher"
    unit: str             # "F" or "C"

    def contains(self, temp: float) -> bool:
        """Check if a temperature falls in this bucket (with 0.5 rounding buffer)."""
        return (self.low - 0.5) <= temp < (self.high + 0.5)


@dataclass
class WeatherOutcome:
    """A single outcome (temperature bucket) in a weather market."""
    bucket: TempBucket
    token_id: str         # CLOB token ID for "Yes"
    market_id: str        # Polymarket market/condition ID
    market_prob: float    # Current Yes price (0.0 - 1.0)


@dataclass
class WeatherMarket:
    """A complete weather market with all its outcomes."""
    event_id: str
    slug: str
    title: str
    city_key: str         # Key into CITIES dict
    target_date: date
    outcomes: List[WeatherOutcome] = field(default_factory=list)
    volume: float = 0.0
    liquidity: float = 0.0
    description: str = ""             # Full event description with rules
    resolution_source: str = ""       # WU resolution URL
    wu_station_id: str = ""           # Extracted WU station code (e.g. "RKSI")


# ─── Market Discovery ────────────────────────────────────────────────────────

async def discover_weather_markets(session: aiohttp.ClientSession) -> List[WeatherMarket]:
    """
    Discover all active weather markets on Polymarket.

    Strategy: construct expected slugs for each city × date combination,
    then fetch event details from the Gamma API. This is deterministic
    and avoids unreliable search/tag endpoints.
    """
    today = datetime.now(timezone.utc).date()
    # Check today, tomorrow, and day after
    target_dates = [
        today,
        date.fromordinal(today.toordinal() + 1),
        date.fromordinal(today.toordinal() + 2),
    ]

    # Build list of (slug, city_key, target_date) to fetch
    fetch_list = []
    for city_key, city in CITIES.items():
        for d in target_dates:
            month_name = MONTH_NAMES[d.month]
            slug = f"highest-temperature-in-{city.slug_name}-on-{month_name}-{d.day}-{d.year}"
            fetch_list.append((slug, city_key, d))

    # Fetch all events concurrently (batched to avoid rate limits)
    markets = []
    batch_size = 10
    for i in range(0, len(fetch_list), batch_size):
        batch = fetch_list[i:i + batch_size]
        tasks = [
            _fetch_weather_event(session, slug, city_key, target_date)
            for slug, city_key, target_date in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, WeatherMarket):
                markets.append(result)
            elif isinstance(result, Exception):
                pass  # Silently skip failed fetches (market may not exist yet)

        # Small delay between batches to be nice to the API
        if i + batch_size < len(fetch_list):
            await asyncio.sleep(0.2)

    return markets


async def _fetch_weather_event(
    session: aiohttp.ClientSession,
    slug: str,
    city_key: str,
    target_date: date,
) -> Optional[WeatherMarket]:
    """Fetch a single weather event by slug and parse its markets."""
    url = f"{GAMMA_API_BASE}/events?slug={slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    # Gamma API returns a list; take first match
    events = data if isinstance(data, list) else [data]
    if not events:
        return None

    event = events[0]
    event_id = str(event.get("id", ""))
    title = event.get("title", slug)
    event_slug = event.get("slug", slug)

    # Extract resolution source and description
    description = event.get("description", "")
    resolution_source = event.get("resolutionSource", "")

    # Extract WU station ID from resolution URL or description
    wu_station_id = ""
    wu_match = re.search(
        r"wunderground\.com/history/daily/\w+/[\w%-]+/(\w+)",
        resolution_source or description,
    )
    if wu_match:
        wu_station_id = wu_match.group(1)

    # Parse the markets array — each market is one temperature bucket
    raw_markets = event.get("markets", [])
    if not raw_markets:
        return None

    city = CITIES.get(city_key)
    if not city:
        return None

    outcomes = []
    total_volume = 0.0
    total_liquidity = 0.0

    for m in raw_markets:
        outcome = _parse_market_outcome(m, city.unit)
        if outcome:
            outcomes.append(outcome)

        # Accumulate volume and liquidity
        vol_str = m.get("volume", "0")
        liq_str = m.get("liquidity", "0")
        try:
            total_volume += float(vol_str)
        except (ValueError, TypeError):
            pass
        try:
            total_liquidity += float(liq_str)
        except (ValueError, TypeError):
            pass

    if not outcomes:
        return None

    # Sort outcomes by bucket lower bound
    outcomes.sort(key=lambda o: o.bucket.low)

    return WeatherMarket(
        event_id=event_id,
        slug=event_slug,
        title=title,
        city_key=city_key,
        target_date=target_date,
        outcomes=outcomes,
        volume=total_volume,
        liquidity=total_liquidity,
        description=description,
        resolution_source=resolution_source,
        wu_station_id=wu_station_id,
    )


def _parse_market_outcome(market: dict, unit: str) -> Optional[WeatherOutcome]:
    """Parse a single market (one temperature bucket) from Gamma API response."""
    # Get the bucket label from groupItemTitle (preferred) or question
    label = market.get("groupItemTitle", "")
    if not label:
        # Try to extract from question: "Will the highest temp... be 36-37°F?"
        question = market.get("question", "")
        label = _extract_bucket_from_question(question, unit)
    if not label:
        return None

    # Parse temperature bucket from label
    bucket = parse_bucket(label, unit)
    if not bucket:
        return None

    # Parse clobTokenIds — JSON string '["token1", "token2"]'
    raw_tokens = market.get("clobTokenIds", "")
    if isinstance(raw_tokens, str):
        try:
            tokens = json.loads(raw_tokens)
        except (json.JSONDecodeError, ValueError):
            return None
    elif isinstance(raw_tokens, list):
        tokens = raw_tokens
    else:
        return None

    if len(tokens) < 2:
        return None

    # token[0] = Yes, token[1] = No
    yes_token = tokens[0]

    # Parse current price from outcomePrices
    raw_prices = market.get("outcomePrices", "")
    if isinstance(raw_prices, str):
        try:
            prices = json.loads(raw_prices)
        except (json.JSONDecodeError, ValueError):
            prices = []
    elif isinstance(raw_prices, list):
        prices = raw_prices
    else:
        prices = []

    yes_price = 0.0
    if prices:
        try:
            yes_price = float(prices[0])
        except (ValueError, TypeError, IndexError):
            pass

    # Get market/condition ID
    market_id = market.get("conditionId", market.get("id", ""))

    return WeatherOutcome(
        bucket=bucket,
        token_id=yes_token,
        market_id=str(market_id),
        market_prob=yes_price,
    )


# ─── Bucket Parsing ──────────────────────────────────────────────────────────

def parse_bucket(label: str, unit: str) -> Optional[TempBucket]:
    """
    Parse a temperature bucket label into a TempBucket.

    Handles formats:
      "33°F or below"   → (-inf, 33]
      "34-35°F"         → [34, 35]
      "36-37°F"         → [36, 37]
      "44°F or higher"  → [44, inf)
      "4°C"             → [4, 4]     (single degree, Celsius markets)
      "13°C"            → [13, 13]
      "-2°C"            → [-2, -2]   (negative temperatures)
      "32°C or below"   → (-inf, 32]
      "32°C or higher"  → [32, inf)
    """
    label = label.strip()

    # Pattern: "X°U or below" / "X°U or lower"
    m = re.match(r'^(-?\d+)\s*°\s*[FCfc]\s+or\s+(below|lower)', label)
    if m:
        high = int(m.group(1))
        return TempBucket(label=label, low=float('-inf'), high=float(high), unit=unit)

    # Pattern: "X°U or higher" / "X°U or above"
    m = re.match(r'^(-?\d+)\s*°\s*[FCfc]\s+or\s+(higher|above)', label)
    if m:
        low = int(m.group(1))
        return TempBucket(label=label, low=float(low), high=float('inf'), unit=unit)

    # Pattern: "X-Y°U" (range, e.g. "34-35°F")
    m = re.match(r'^(-?\d+)\s*-\s*(-?\d+)\s*°\s*[FCfc]', label)
    if m:
        low = int(m.group(1))
        high = int(m.group(2))
        return TempBucket(label=label, low=float(low), high=float(high), unit=unit)

    # Pattern: "X°U" (single degree, e.g. "4°C")
    m = re.match(r'^(-?\d+)\s*°\s*[FCfc]$', label)
    if m:
        val = int(m.group(1))
        return TempBucket(label=label, low=float(val), high=float(val), unit=unit)

    # Pattern: just a number with unit context (fallback)
    m = re.match(r'^(-?\d+)$', label)
    if m:
        val = int(m.group(1))
        return TempBucket(label=f"{val}°{unit}", low=float(val), high=float(val), unit=unit)

    return None


def _extract_bucket_from_question(question: str, unit: str) -> str:
    """Extract temperature bucket from a market question string."""
    # "Will the highest temperature in NYC on February 12 be 36-37°F?"
    m = re.search(r'be\s+(.+?)\s*\?', question)
    if m:
        return m.group(1).strip()

    # "highest temperature ... 36-37°F"
    pattern = r'(-?\d+(?:\s*-\s*-?\d+)?\s*°\s*[FCfc](?:\s+or\s+\w+)?)'
    m = re.search(pattern, question)
    if m:
        return m.group(1).strip()

    return ""


# ─── Utility ─────────────────────────────────────────────────────────────────

def market_summary(market: WeatherMarket) -> str:
    """One-line summary of a weather market."""
    city = CITIES.get(market.city_key)
    city_name = city.name if city else market.city_key
    n = len(market.outcomes)
    return f"{city_name} {market.target_date} — {n} buckets, ${market.volume:,.0f} vol, ${market.liquidity:,.0f} liq"
