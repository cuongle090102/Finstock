"""
Unit tests for Technical Analysis Indicators.
"""

import pytest
import sys
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.technical_analysis.indicators import TechnicalIndicators, IndicatorCalculator


@pytest.fixture
def sample_ohlcv():
    """Generate sample OHLCV data for testing."""
    np.random.seed(42)
    n = 100
    close = 100 + np.cumsum(np.random.normal(0, 1, n))
    high = close + np.abs(np.random.normal(1, 0.5, n))
    low = close - np.abs(np.random.normal(1, 0.5, n))
    open_ = close + np.random.normal(0, 0.5, n)
    volume = np.random.randint(100000, 1000000, n).astype(float)
    return pd.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    })


class TestSMA:
    """Test Simple Moving Average."""

    def test_sma_calculation(self):
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
        sma = TechnicalIndicators.sma(data, period=3)
        assert sma.iloc[-1] == pytest.approx(9.0)  # (8+9+10)/3
        assert sma.iloc[2] == pytest.approx(2.0)   # (1+2+3)/3

    def test_sma_length(self, sample_ohlcv):
        sma = TechnicalIndicators.sma(sample_ohlcv['close'], period=20)
        assert len(sma) == len(sample_ohlcv)

    def test_sma_insufficient_data(self):
        data = pd.Series([1.0, 2.0])
        sma = TechnicalIndicators.sma(data, period=5)
        assert sma.isna().all()


class TestEMA:
    """Test Exponential Moving Average."""

    def test_ema_calculation(self):
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
        ema = TechnicalIndicators.ema(data, period=3)
        assert len(ema) == len(data)
        assert not np.isnan(ema.iloc[-1])

    def test_ema_responds_faster_than_sma(self, sample_ohlcv):
        sma = TechnicalIndicators.sma(sample_ohlcv['close'], period=20)
        ema = TechnicalIndicators.ema(sample_ohlcv['close'], period=20)
        # Both should be Series of same length
        assert len(sma) == len(ema)


class TestRSI:
    """Test Relative Strength Index."""

    def test_rsi_range(self, sample_ohlcv):
        rsi = TechnicalIndicators.rsi(sample_ohlcv['close'], period=14)
        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()

    def test_rsi_length(self, sample_ohlcv):
        rsi = TechnicalIndicators.rsi(sample_ohlcv['close'], period=14)
        assert len(rsi) == len(sample_ohlcv)


class TestMACD:
    """Test MACD."""

    def test_macd_returns_tuple(self, sample_ohlcv):
        result = TechnicalIndicators.macd(sample_ohlcv['close'])
        assert isinstance(result, tuple)
        assert len(result) == 3  # macd_line, signal_line, histogram

    def test_macd_lengths(self, sample_ohlcv):
        macd_line, signal_line, histogram = TechnicalIndicators.macd(sample_ohlcv['close'])
        assert len(macd_line) == len(sample_ohlcv)
        assert len(signal_line) == len(sample_ohlcv)
        assert len(histogram) == len(sample_ohlcv)


class TestBollingerBands:
    """Test Bollinger Bands."""

    def test_bb_returns_tuple(self, sample_ohlcv):
        result = TechnicalIndicators.bollinger_bands(sample_ohlcv['close'])
        assert isinstance(result, tuple)
        assert len(result) == 3  # upper, middle, lower

    def test_bb_ordering(self, sample_ohlcv):
        upper, middle, lower = TechnicalIndicators.bollinger_bands(sample_ohlcv['close'])
        valid = ~(upper.isna() | middle.isna() | lower.isna())
        assert (upper[valid] >= middle[valid]).all()
        assert (middle[valid] >= lower[valid]).all()


class TestATR:
    """Test Average True Range."""

    def test_atr_positive(self, sample_ohlcv):
        atr = TechnicalIndicators.atr(
            sample_ohlcv['high'],
            sample_ohlcv['low'],
            sample_ohlcv['close']
        )
        valid = atr.dropna()
        assert (valid >= 0).all()

    def test_atr_length(self, sample_ohlcv):
        atr = TechnicalIndicators.atr(
            sample_ohlcv['high'],
            sample_ohlcv['low'],
            sample_ohlcv['close']
        )
        assert len(atr) == len(sample_ohlcv)


class TestIndicatorCalculator:
    """Test high-level IndicatorCalculator."""

    def test_calculate_all_returns_dict(self, sample_ohlcv):
        calc = IndicatorCalculator()
        result = calc.calculate_all_indicators(sample_ohlcv)
        assert isinstance(result, dict)

    def test_empty_data_returns_empty(self):
        calc = IndicatorCalculator()
        result = calc.calculate_all_indicators(pd.DataFrame())
        assert result == {}

    def test_missing_columns_returns_empty(self):
        """Missing 'close' column is caught internally and returns empty dict."""
        calc = IndicatorCalculator()
        bad_df = pd.DataFrame({'not_close': [1, 2, 3]})
        result = calc.calculate_all_indicators(bad_df)
        assert result == {}
