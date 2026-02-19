#!/usr/bin/env python3
"""
BTC 5-minute Up/Down market scanner.

Discovers active markets from the btc-up-or-down-5m series on Polymarket
and returns structured market data for the strategy engine.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

GAMMA_API = "https://gamma-api.polymarket.com"
SERIES_SLUG = "btc-up-or-down-5m"
BTC_SLUG_PREFIX = "btc-updown-5m-"


@dataclass
class BtcMarket:
    event_slug: str
    question: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    up_price: float       # Gamma last-trade price for Up (lagging, for reference)
    down_price: float     # Gamma last-trade price for Down (lagging, for reference)
    best_bid_up: float    # Gamma bestBid — AMM real-time bid for Up
    best_ask_up: float    # Gamma bestAsk — AMM real-time ask for Up
    best_bid_down: float  # AMM real-time bid for Down = 1 - bestAsk_up
    best_ask_down: float  # AMM real-time ask for Down = 1 - bestBid_up
    mid_up: float         # AMM mid-price for Up = (bestBid + bestAsk) / 2
    mid_down: float       # AMM mid-price for Down
    amm_spread: float     # AMM spread for Up token
    end_time: datetime    # when the market resolves
    start_time: datetime  # when the 5-min window starts (event start)
    liquidity: float
    volume: float
    spread: float
    accepting_orders: bool


def _current_window_slugs(n: int = 4) -> List[str]:
    """
    Compute the slugs for the current and next N-1 BTC 5m windows.
    Slug format: btc-updown-5m-{unix_start_timestamp}
    Windows align to 5-minute UTC boundaries (300s).
    The slug timestamp is the WINDOW START (not end).
    """
    import math
    now_ts = int(datetime.now(timezone.utc).timestamp())
    # Start of current window = floor to 5-min boundary
    current_start = (now_ts // 300) * 300
    return [f"{BTC_SLUG_PREFIX}{current_start + i * 300}" for i in range(n)]


async def fetch_active_markets(session: aiohttp.ClientSession, limit: int = 4) -> List[BtcMarket]:
    """
    Fetch active BTC 5m up/down markets from Gamma API.
    Uses Gamma bestBid/bestAsk as real-time AMM prices (update continuously).
    Computes slugs from current time (5-min boundaries) and fetches each directly.
    Returns markets sorted by end_time ascending (soonest first).
    """
    slugs = _current_window_slugs(limit)

    async def fetch_event(slug: str):
        url = f"{GAMMA_API}/events"
        try:
            async with session.get(url, params={"slug": slug}) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data[0] if data else None
        except Exception as e:
            print(f"  ⚠️  Error fetching {slug}: {e}")
            return None

    results = await asyncio.gather(*[fetch_event(s) for s in slugs])
    events = [e for e in results if e is not None]

    markets = []
    for event in events:
        try:
            mkt_list = event.get("markets", [])
            if not mkt_list:
                continue
            mkt = mkt_list[0]

            if not mkt.get("acceptingOrders", False):
                continue

            token_ids = json.loads(mkt.get("clobTokenIds", "[]"))
            if len(token_ids) < 2:
                continue

            outcomes = json.loads(mkt.get("outcomes", '["Up","Down"]'))
            prices_raw = json.loads(mkt.get("outcomePrices", "[0.5,0.5]"))
            up_idx = outcomes.index("Up") if "Up" in outcomes else 0
            down_idx = 1 - up_idx

            end_time = datetime.fromisoformat(
                mkt["endDate"].replace("Z", "+00:00")
            )
            # eventStartTime is the actual 5-min window start (from market field)
            start_str = (
                mkt.get("eventStartTime")
                or event.get("startTime")
                or event.get("startDate")
                or mkt["startDate"]
            )
            start_time = datetime.fromisoformat(
                start_str.replace("Z", "+00:00")
            )

            now = datetime.now(timezone.utc)
            if end_time <= now:
                continue

            up_price = float(prices_raw[up_idx])
            down_price = float(prices_raw[down_idx])

            # Gamma bestBid/bestAsk reflect the AMM's current real-time price
            # These update continuously as the AMM reprices based on trading flow
            amm_bid = float(mkt.get("bestBid") or up_price - 0.005)
            amm_ask = float(mkt.get("bestAsk") or up_price + 0.005)
            amm_mid = (amm_bid + amm_ask) / 2.0

            markets.append(BtcMarket(
                event_slug=event["slug"],
                question=mkt["question"],
                condition_id=mkt["conditionId"],
                up_token_id=token_ids[up_idx],
                down_token_id=token_ids[down_idx],
                up_price=up_price,
                down_price=down_price,
                best_bid_up=amm_bid,
                best_ask_up=amm_ask,
                best_bid_down=1.0 - amm_ask,
                best_ask_down=1.0 - amm_bid,
                mid_up=round(amm_mid, 4),
                mid_down=round(1.0 - amm_mid, 4),
                amm_spread=round(amm_ask - amm_bid, 4),
                end_time=end_time,
                start_time=start_time,
                liquidity=float(mkt.get("liquidityNum", 0)),
                volume=float(mkt.get("volumeNum", 0)),
                spread=float(mkt.get("spread", 0.01)),
                accepting_orders=mkt.get("acceptingOrders", False),
            ))
        except Exception as e:
            print(f"  ⚠️  Error parsing market {event.get('slug', '?')}: {e}")
            continue

    return markets


def seconds_to_resolution(market: BtcMarket) -> float:
    """Seconds remaining until market resolves."""
    now = datetime.now(timezone.utc)
    return max(0.0, (market.end_time - now).total_seconds())


def seconds_into_window(market: BtcMarket) -> float:
    """Seconds elapsed since the 5-min window started (0 if not started yet)."""
    now = datetime.now(timezone.utc)
    elapsed = (now - market.start_time).total_seconds()
    return max(0.0, elapsed)


def is_window_active(market: BtcMarket) -> bool:
    """True if we are currently inside this market's 5-min trading window."""
    now = datetime.now(timezone.utc)
    return market.start_time <= now < market.end_time


async def main():
    """Quick test — print active markets."""
    async with aiohttp.ClientSession() as session:
        markets = await fetch_active_markets(session)
        now = datetime.now(timezone.utc)
        active = [m for m in markets if is_window_active(m)]
        upcoming = [m for m in markets if not is_window_active(m)]
        print(f"Found {len(markets)} BTC 5m markets ({len(active)} active, {len(upcoming)} upcoming)\n")
        print("=== CURRENTLY ACTIVE ===")
        for m in active:
            secs = seconds_to_resolution(m)
            elapsed = seconds_into_window(m)
            dev = abs(m.mid_up - 0.50)
            print(f"  {m.question}")
            print(f"    AMM: mid={m.mid_up:.3f} bid={m.best_bid_up:.3f} ask={m.best_ask_up:.3f} spread={m.amm_spread:.3f}  dev={dev:.3f}")
            print(f"    last_trade: Up={m.up_price:.3f} Down={m.down_price:.3f}  liq=${m.liquidity:.0f}")
            print(f"    Elapsed: {elapsed:.0f}s / 300s  |  Resolves in: {secs:.0f}s")
        print("\n=== UPCOMING (next 3) ===")
        for m in sorted(upcoming, key=lambda x: x.end_time)[:3]:
            secs = seconds_to_resolution(m)
            print(f"  {m.question}  (in {secs:.0f}s)")


if __name__ == "__main__":
    asyncio.run(main())
