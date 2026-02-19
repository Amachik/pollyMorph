"""
Weather Bot Configuration
City definitions, coordinates, weather station mappings, and trading parameters.
"""

from dataclasses import dataclass
from typing import Optional

# ─── Trading Parameters ──────────────────────────────────────────────────────

# Minimum edge (forecast_prob - market_prob) to consider a trade
MIN_EDGE = 0.08  # 8%

# Maximum edge — if edge exceeds this, something may be wrong (stale market, bad data)
MAX_EDGE = 0.45  # 45% (was 60%; huge edges are usually model errors)

# Kelly criterion fraction (fractional Kelly for safety)
KELLY_FRACTION = 0.25  # Use 25% of full Kelly

# Maximum bet size per outcome (USDC)
MAX_BET_USDC = 20.0

# Minimum market liquidity to consider trading (USDC)
MIN_LIQUIDITY = 200.0

# Number of noise replications per ensemble member for probability smoothing
NOISE_REPLICATIONS = 20

# ─── Smart Bet Selection ─────────────────────────────────────────────────────

# Only bet on buckets ranked in our top-N most likely outcomes
TOP_N_BUCKETS = 3

# Minimum forecast probability on a bucket before we'll bet on it
# Backtest: bets where our prob < 25% were 0W/17L = pure loss
MIN_FORECAST_PROB = 0.25  # 25% (was 15%)

# Maximum bets per city+date to limit correlated losses
# (Toronto, Buenos Aires often had 2-3 losing bets on same market)
MAX_BETS_PER_MARKET = 2

# Market disagree cap: skip when market price is extremely low
# Optimized via backtest: 3% keeps big wins (39x Buenos Aires) while filtering garbage
MARKET_DISAGREE_CAP = 0.03  # Skip if market < 3% regardless of our forecast

# For same-day markets: minimum hours until typical peak (5PM local) to trade
# Below this, the market already has too much real-time info; skip same-day
SAME_DAY_MIN_HOURS = 4.0

# Spread penalty: if our top-2 buckets are very close in probability,
# we're uncertain — require a larger edge
SPREAD_EDGE_BOOST = 0.04  # Add 4% to min_edge when top-2 are within 5% of each other

# ─── API Endpoints ───────────────────────────────────────────────────────────

OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
NWS_API_BASE = "https://api.weather.gov"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Ensemble models to use (ordered by accuracy)
# ecmwf_ifs025:                 51 members — European Centre, world's best global model
# gfs_seamless:                 31 members — NOAA GFS, strong for North America
# icon_seamless:                40 members — DWD ICON, good for Europe
# gem_global:                   21 members — Environment Canada GEM, independent physics
# bom_access_global_ensemble:   18 members — Australian BOM ACCESS-GE2, southern hemisphere
# Total: 161 members across 5 independent model families
ENSEMBLE_MODELS = [
    "ecmwf_ifs025",
    "gfs_seamless",
    "icon_seamless",
    "gem_global",
    "bom_access_global_ensemble",
]

# ─── City Definitions ────────────────────────────────────────────────────────

@dataclass
class City:
    """Configuration for a weather market city."""
    name: str                          # Display name
    polymarket_name: str               # As it appears in Polymarket market titles
    slug_name: str                     # As it appears in Polymarket event slugs
    lat: float                         # Latitude (of resolution weather station)
    lon: float                         # Longitude
    unit: str                          # "F" (Fahrenheit) or "C" (Celsius)
    tz: str                            # IANA timezone string
    wunderground_id: str = ""          # Weather Underground station ID (resolution source)
    wu_country: str = ""               # ISO country code for WU API (e.g. "US", "KR")
    obs_uncertainty: float = 1.0       # Observation uncertainty in native unit (degrees)
    # NWS forecast office info (US cities only, looked up dynamically)
    nws_office: Optional[str] = None
    nws_grid_x: Optional[int] = None
    nws_grid_y: Optional[int] = None


CITIES = {
    "nyc": City(
        name="New York City",
        polymarket_name="NYC",
        slug_name="nyc",
        lat=40.7789,          # LaGuardia Airport (KLGA)
        lon=-73.8740,
        unit="F",
        tz="America/New_York",
        wunderground_id="KLGA",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "chicago": City(
        name="Chicago",
        polymarket_name="Chicago",
        slug_name="chicago",
        lat=41.9742,          # O'Hare International (KORD)
        lon=-87.9073,
        unit="F",
        tz="America/Chicago",
        wunderground_id="KORD",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "atlanta": City(
        name="Atlanta",
        polymarket_name="Atlanta",
        slug_name="atlanta",
        lat=33.6407,          # Hartsfield-Jackson (KATL)
        lon=-84.4277,
        unit="F",
        tz="America/New_York",
        wunderground_id="KATL",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "dallas": City(
        name="Dallas",
        polymarket_name="Dallas",
        slug_name="dallas",
        lat=32.8471,          # Dallas Love Field (KDAL) — Polymarket resolution source
        lon=-96.8517,
        unit="F",
        tz="America/Chicago",
        wunderground_id="KDAL",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "miami": City(
        name="Miami",
        polymarket_name="Miami",
        slug_name="miami",
        lat=25.7959,          # Miami International (KMIA)
        lon=-80.2870,
        unit="F",
        tz="America/New_York",
        wunderground_id="KMIA",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "seattle": City(
        name="Seattle",
        polymarket_name="Seattle",
        slug_name="seattle",
        lat=47.4502,          # SeaTac Airport (KSEA)
        lon=-122.3088,
        unit="F",
        tz="America/Los_Angeles",
        wunderground_id="KSEA",
        wu_country="US",
        obs_uncertainty=1.0,
    ),
    "london": City(
        name="London",
        polymarket_name="London",
        slug_name="london",
        lat=51.5053,          # London City Airport (EGLC) — Polymarket resolution source
        lon=0.0553,
        unit="C",
        tz="Europe/London",
        wunderground_id="EGLC",
        wu_country="GB",
        obs_uncertainty=0.8,
    ),
    "seoul": City(
        name="Seoul",
        polymarket_name="Seoul",
        slug_name="seoul",
        lat=37.4602,          # Incheon Intl Airport (RKSI) — Polymarket resolution source
        lon=126.4407,
        unit="C",
        tz="Asia/Seoul",
        wunderground_id="RKSI",
        wu_country="KR",
        obs_uncertainty=0.8,
    ),
    "toronto": City(
        name="Toronto",
        polymarket_name="Toronto",
        slug_name="toronto",
        lat=43.6777,          # Pearson International (CYYZ)
        lon=-79.6248,
        unit="C",
        tz="America/Toronto",
        wunderground_id="CYYZ",
        wu_country="CA",
        obs_uncertainty=0.8,
    ),
    "buenos_aires": City(
        name="Buenos Aires",
        polymarket_name="Buenos Aires",
        slug_name="buenos-aires",
        lat=-34.5588,         # Ezeiza Airport (SAEZ)
        lon=-58.4156,
        unit="C",
        tz="America/Argentina/Buenos_Aires",
        wunderground_id="SAEZ",
        wu_country="AR",
        obs_uncertainty=0.8,
    ),
    "ankara": City(
        name="Ankara",
        polymarket_name="Ankara",
        slug_name="ankara",
        lat=40.1281,          # Esenboga Airport (LTAC)
        lon=32.9951,
        unit="C",
        tz="Europe/Istanbul",
        wunderground_id="LTAC",
        wu_country="TR",
        obs_uncertainty=0.8,
    ),
    "wellington": City(
        name="Wellington",
        polymarket_name="Wellington",
        slug_name="wellington",
        lat=-41.3272,         # Wellington Airport (NZWN)
        lon=174.8050,
        unit="C",
        tz="Pacific/Auckland",
        wunderground_id="NZWN",
        wu_country="NZ",
        obs_uncertainty=0.8,
    ),
    "auckland": City(
        name="Auckland",
        polymarket_name="Auckland",
        slug_name="auckland",
        lat=-37.0082,         # Auckland Airport (NZAA)
        lon=174.7850,
        unit="C",
        tz="Pacific/Auckland",
        wunderground_id="NZAA",
        wu_country="NZ",
        obs_uncertainty=0.8,
    ),
}

# Reverse lookup: Polymarket title fragment → city key
# Handles both "NYC" and "New York City" style references
CITY_NAME_MAP = {}
for _key, _city in CITIES.items():
    CITY_NAME_MAP[_city.polymarket_name.lower()] = _key
    CITY_NAME_MAP[_city.name.lower()] = _key
    CITY_NAME_MAP[_city.slug_name.lower()] = _key

# Month name lookup for slug construction
MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december",
}
