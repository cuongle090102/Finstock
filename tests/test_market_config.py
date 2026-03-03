"""
Unit tests for centralized market configuration.
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.market_config import (
    MarketSession, VN30_SYMBOLS, VN_TIMEZONE,
    get_current_market_session, is_market_open, is_vn30,
    MORNING_START_HOUR, MORNING_END_HOUR,
    AFTERNOON_START_HOUR, AFTERNOON_END_HOUR,
    COMMISSION_RATE_VN30, COMMISSION_RATE_STANDARD,
    STANDARD_LOT_SIZE, PRICE_TICK_VND,
)


class TestVN30Symbols:
    """Test VN30 symbol set."""

    def test_vn30_has_exactly_30_symbols(self):
        assert len(VN30_SYMBOLS) == 30

    def test_known_vn30_members(self):
        for symbol in ['VIC', 'VCB', 'HPG', 'FPT', 'GAS', 'MSN', 'VNM']:
            assert symbol in VN30_SYMBOLS, f"{symbol} should be in VN30"

    def test_is_vn30_function(self):
        assert is_vn30('VIC') is True
        assert is_vn30('vic') is True  # case insensitive
        assert is_vn30('INVALID') is False
        assert is_vn30('') is False

    def test_all_symbols_are_uppercase(self):
        for symbol in VN30_SYMBOLS:
            assert symbol == symbol.upper()
            assert len(symbol) <= 4  # Vietnamese stock codes are 3-4 chars


class TestMarketHours:
    """Test market session detection."""

    def test_morning_session(self):
        # Monday 10:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 10, 0))
        assert get_current_market_session(dt) == MarketSession.MORNING
        assert is_market_open(dt) is True

    def test_afternoon_session(self):
        # Monday 14:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 14, 0))
        assert get_current_market_session(dt) == MarketSession.AFTERNOON
        assert is_market_open(dt) is True

    def test_lunch_break(self):
        # Monday 12:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 12, 0))
        assert get_current_market_session(dt) == MarketSession.LUNCH_BREAK
        assert is_market_open(dt) is False

    def test_pre_market(self):
        # Monday 08:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 8, 0))
        assert get_current_market_session(dt) == MarketSession.PRE_MARKET
        assert is_market_open(dt) is False

    def test_post_market(self):
        # Monday 16:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 16, 0))
        assert get_current_market_session(dt) == MarketSession.POST_MARKET
        assert is_market_open(dt) is False

    def test_weekend_closed(self):
        # Saturday 10:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 7, 10, 0))
        assert get_current_market_session(dt) == MarketSession.CLOSED
        assert is_market_open(dt) is False

    def test_sunday_closed(self):
        # Sunday 10:00 VN time
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 8, 10, 0))
        assert get_current_market_session(dt) == MarketSession.CLOSED

    def test_session_boundaries(self):
        # Morning start boundary: 09:00
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 9, 0))
        assert get_current_market_session(dt) == MarketSession.MORNING

        # Morning end boundary: 11:30
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 11, 30))
        assert get_current_market_session(dt) == MarketSession.MORNING

        # Afternoon start: 13:00
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 13, 0))
        assert get_current_market_session(dt) == MarketSession.AFTERNOON

        # Afternoon end: 15:00
        dt = VN_TIMEZONE.localize(datetime(2026, 2, 9, 15, 0))
        assert get_current_market_session(dt) == MarketSession.AFTERNOON

    def test_naive_datetime_assumed_vietnam(self):
        # Naive datetime should be treated as Vietnam timezone
        dt = datetime(2026, 2, 9, 10, 0)
        session = get_current_market_session(dt)
        assert session == MarketSession.MORNING


class TestConstants:
    """Test market constants are sensible."""

    def test_commission_rates(self):
        assert COMMISSION_RATE_VN30 < COMMISSION_RATE_STANDARD
        assert COMMISSION_RATE_VN30 == 0.0010
        assert COMMISSION_RATE_STANDARD == 0.0015

    def test_lot_size(self):
        assert STANDARD_LOT_SIZE == 100

    def test_price_tick(self):
        assert PRICE_TICK_VND == 100

    def test_trading_hours_order(self):
        assert MORNING_START_HOUR < MORNING_END_HOUR
        assert MORNING_END_HOUR < AFTERNOON_START_HOUR
        assert AFTERNOON_START_HOUR < AFTERNOON_END_HOUR
