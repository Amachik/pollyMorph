#!/usr/bin/env python3
"""
Mean-reversion strategy for BTC 5-minute Up/Down markets.

Core insight: these markets are near-coin-flips. When price drifts significantly
from 0.50 with little time left, the market is likely overreacting to short-term
momentum. We fade the move by buying the cheaper side.

Signal logic:
  - deviation = abs(up_price - 0.50)
  - time_weight = elapsed / 300  (how far into the 5-min window we are)
  - signal_strength = deviation * time_weight
  - Trade if signal_strength > MIN_SIGNAL and deviation > MIN_DEVIATION
  - Buy the CHEAPER side (fade the move)

Fee awareness:
  - Maker fee: 1% (100bps), Taker fee: 1% (100bps)
  - To profit, we need edge > 2% (buy at X, resolve at 1.0, net = 1.0 - X - 0.01 > 0)
  - So we need to buy at < 0.49 to break even as taker
  - Maker rebate: 100% of maker fee returned (makerRebatesFeeShareBps=10000)
    → if we post a limit order that gets filled, fee is effectively 0%
  - Strategy: ALWAYS post limit orders (maker), never market orders (taker)
"""

from dataclasses import dataclass
from typing import Optional

from .scanner import BtcMarket, seconds_to_resolution, seconds_into_window, is_window_active

# ─── Strategy Parameters ─────────────────────────────────────────────────────

MIN_DEVIATION = 0.04        # Price must be at least 4¢ from 0.50 (so ≤0.46 or ≥0.54)
MIN_SIGNAL = 0.015          # deviation * time_weight threshold
MIN_SECS_REMAINING = 30     # Don't trade in last 30 seconds (too risky)
MAX_SECS_REMAINING = 270    # Don't trade in first 30 seconds (window just opened, volatile)
MIN_LIQUIDITY = 1000        # Minimum pool liquidity in USDC
TAKER_FEE = 0.01            # 1% taker fee
MAKER_FEE = 0.00            # 0% effective maker fee (100% rebate)
MIN_EDGE_AFTER_FEES = 0.02  # Minimum edge after fees to place order

# Kelly fraction — conservative since this is a new strategy
KELLY_FRACTION = 0.10
MAX_BET_USDC = 20.0
MIN_BET_USDC = 5.0          # Polymarket minimum order size


@dataclass
class TradeSignal:
    side: str               # "Up" or "Down" (which outcome to BUY)
    token_id: str           # CLOB token ID to buy
    entry_price: float      # limit order price to post
    fair_value: float       # our estimated fair value
    edge: float             # fair_value - entry_price (after fees)
    deviation: float        # abs(up_price - 0.50)
    time_weight: float      # elapsed / 300
    signal_strength: float  # deviation * time_weight
    secs_remaining: float
    secs_elapsed: float
    bet_size: float         # USDC to risk
    reason: str


def evaluate_market(market: BtcMarket, bankroll: float) -> Optional[TradeSignal]:
    """
    Evaluate a BTC 5m market for a mean-reversion trade.
    Returns a TradeSignal if conditions are met, else None.
    """
    # Only trade markets whose 5-min window is currently open
    if not is_window_active(market):
        return None

    secs_remaining = seconds_to_resolution(market)
    secs_elapsed = seconds_into_window(market)

    # Time filters
    if secs_remaining < MIN_SECS_REMAINING:
        return None
    if secs_remaining > MAX_SECS_REMAINING:
        return None
    if not market.accepting_orders:
        return None
    if market.liquidity < MIN_LIQUIDITY:
        return None

    # Deviation from fair value (0.50)
    # Use AMM mid-price (Gamma bestBid/bestAsk average) — updates in real-time
    ref_price = market.mid_up
    deviation = ref_price - 0.50  # positive = Up is overpriced
    abs_dev = abs(deviation)

    if abs_dev < MIN_DEVIATION:
        return None

    # Time weight: how far into the 5-min window (0→1)
    # Use elapsed out of 300s total window
    time_weight = min(secs_elapsed / 300.0, 1.0)

    signal_strength = abs_dev * time_weight

    if signal_strength < MIN_SIGNAL:
        return None

    # Determine which side to buy (fade the move)
    if deviation > 0:
        # Up is overpriced → buy Down (cheaper side)
        side = "Down"
        token_id = market.down_token_id
        fair_value = 0.50
        market_ask = market.best_ask_down  # CLOB ask for Down (real-time if clob_live)
    else:
        # Down is overpriced → buy Up (cheaper side)
        side = "Up"
        token_id = market.up_token_id
        fair_value = 0.50
        market_ask = market.best_ask_up  # CLOB ask for Up (real-time if clob_live)

    # Post a limit order at the current ask (maker order, 0% fee)
    # We post AT the ask to get filled quickly without crossing the spread
    entry_price = round(market_ask, 2)
    entry_price = min(entry_price, 0.49)  # never pay more than 0.49 (need edge)
    entry_price = max(entry_price, 0.01)

    # Edge after maker fees (0% effective)
    edge = fair_value - entry_price  # if resolves at 1.0, payout = 1.0/entry_price tokens

    if edge < MIN_EDGE_AFTER_FEES:
        return None

    # Kelly bet size
    # p = fair_value (prob of winning), b = (1/entry_price) - 1 (net odds)
    p = fair_value
    b = (1.0 / entry_price) - 1.0
    q = 1.0 - p
    kelly = (p * b - q) / b if b > 0 else 0.0
    kelly = max(kelly, 0.0)

    bet_size = kelly * KELLY_FRACTION * bankroll
    bet_size = min(bet_size, MAX_BET_USDC)
    bet_size = max(bet_size, MIN_BET_USDC)

    if bet_size > bankroll * 0.25:
        bet_size = bankroll * 0.25

    reason = (
        f"{'Up' if deviation > 0 else 'Down'} overpriced by {abs_dev:.3f} [AMM mid={ref_price:.3f}] "
        f"({secs_elapsed:.0f}s into window, {secs_remaining:.0f}s left) "
        f"→ buy {side} @ {entry_price:.2f}, edge={edge:.3f}"
    )

    return TradeSignal(
        side=side,
        token_id=token_id,
        entry_price=entry_price,
        fair_value=fair_value,
        edge=edge,
        deviation=abs_dev,
        time_weight=time_weight,
        signal_strength=signal_strength,
        secs_remaining=secs_remaining,
        secs_elapsed=secs_elapsed,
        bet_size=round(bet_size, 2),
        reason=reason,
    )
