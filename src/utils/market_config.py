"""
Centralized Vietnamese Market Configuration

Single source of truth for market hours, VN30 symbols, and exchange rules.
All modules should import from here instead of defining their own constants.
"""

from datetime import time, datetime
from enum import Enum
from typing import Set

import pytz

# Vietnamese timezone
VN_TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')


class MarketSession(Enum):
    """Vietnamese market trading sessions."""
    PRE_MARKET = "pre_market"
    MORNING = "morning"
    LUNCH_BREAK = "lunch_break"
    AFTERNOON = "afternoon"
    POST_MARKET = "post_market"
    CLOSED = "closed"


# ============================================================================
# Market Hours
# ============================================================================

MORNING_START = time(9, 0)    # 09:00
MORNING_END = time(11, 30)    # 11:30
AFTERNOON_START = time(13, 0) # 13:00
AFTERNOON_END = time(15, 0)   # 15:00

# Decimal-hour equivalents (for code using hour + minute/60 pattern)
MORNING_START_HOUR = 9.0
MORNING_END_HOUR = 11.5
AFTERNOON_START_HOUR = 13.0
AFTERNOON_END_HOUR = 15.0

TRADING_HOURS = {
    'morning_start': MORNING_START_HOUR,
    'morning_end': MORNING_END_HOUR,
    'afternoon_start': AFTERNOON_START_HOUR,
    'afternoon_end': AFTERNOON_END_HOUR,
}


def get_current_market_session(dt: datetime = None) -> MarketSession:
    """
    Determine current Vietnamese market trading session.

    Args:
        dt: datetime to check. If None, uses current Vietnam time.
            If naive, assumes Vietnam timezone.
    """
    if dt is None:
        dt = datetime.now(VN_TIMEZONE)
    elif dt.tzinfo is None:
        dt = VN_TIMEZONE.localize(dt)

    # Weekend check
    if dt.weekday() >= 5:
        return MarketSession.CLOSED

    hour = dt.hour + dt.minute / 60.0

    if hour < MORNING_START_HOUR:
        return MarketSession.PRE_MARKET
    elif MORNING_START_HOUR <= hour <= MORNING_END_HOUR:
        return MarketSession.MORNING
    elif MORNING_END_HOUR < hour < AFTERNOON_START_HOUR:
        return MarketSession.LUNCH_BREAK
    elif AFTERNOON_START_HOUR <= hour <= AFTERNOON_END_HOUR:
        return MarketSession.AFTERNOON
    else:
        return MarketSession.POST_MARKET


def is_market_open(dt: datetime = None) -> bool:
    """Check if Vietnamese market is currently open for trading."""
    session = get_current_market_session(dt)
    return session in (MarketSession.MORNING, MarketSession.AFTERNOON)


# ============================================================================
# VN30 Symbols (official 30 constituents)
# ============================================================================

VN30_SYMBOLS: Set[str] = {
    'ACB', 'BCM', 'BID', 'BVH', 'CTG',
    'FPT', 'GAS', 'GVR', 'HDB', 'HPG',
    'MBB', 'MSN', 'MWG', 'PLX', 'POW',
    'SAB', 'SSB', 'SSI', 'STB', 'TCB',
    'TPB', 'VCB', 'VHM', 'VIC', 'VJC',
    'VNM', 'VPB', 'VRE', 'KDH', 'SHB',
}


def is_vn30(symbol: str) -> bool:
    """Check if a symbol is in the VN30 index."""
    return symbol.upper() in VN30_SYMBOLS


# ============================================================================
# Exchange & Commission Rules
# ============================================================================

COMMISSION_RATE_VN30 = 0.0010    # 0.10% for VN30 stocks
COMMISSION_RATE_STANDARD = 0.0015  # 0.15% for non-VN30
SECURITIES_TAX_RATE = 0.0010     # 0.10% on sells only
MIN_COMMISSION_VND = 1_000       # Minimum commission
MAX_ORDER_VALUE_VND = 500_000_000  # 500M VND per order
STANDARD_LOT_SIZE = 100          # Shares must be multiples of 100
PRICE_TICK_VND = 100             # Price multiples of 100 VND
