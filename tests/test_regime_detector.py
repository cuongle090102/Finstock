"""
Unit tests for RegimeDetector.
"""

import pytest
import sys
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.regime.regime_detector import RegimeDetector, MarketRegime, RegimeResult


@pytest.fixture
def detector():
    return RegimeDetector()


@pytest.fixture
def trending_data():
    """Generate trending (upward) market data."""
    np.random.seed(42)
    n = 100
    trend = np.linspace(100, 150, n)
    noise = np.random.normal(0, 1, n)
    close = trend + noise
    high = close + np.abs(np.random.normal(1, 0.5, n))
    low = close - np.abs(np.random.normal(1, 0.5, n))
    return pd.DataFrame({
        'high': high,
        'low': low,
        'close': close
    })


@pytest.fixture
def ranging_data():
    """Generate ranging (sideways) market data."""
    np.random.seed(42)
    n = 100
    # Oscillate around 100 with small noise
    close = 100 + 2 * np.sin(np.linspace(0, 8 * np.pi, n)) + np.random.normal(0, 0.3, n)
    high = close + np.abs(np.random.normal(0.5, 0.2, n))
    low = close - np.abs(np.random.normal(0.5, 0.2, n))
    return pd.DataFrame({
        'high': high,
        'low': low,
        'close': close
    })


class TestRegimeDetector:
    """Test RegimeDetector core functionality."""

    def test_initialization_defaults(self, detector):
        assert detector.adx_period == 14
        assert detector.adx_trending_threshold == 25.0
        assert detector.adx_ranging_threshold == 20.0
        assert detector.weights['adx'] == 0.50
        assert detector.weights['volatility'] == 0.30
        assert detector.weights['price_action'] == 0.20

    def test_custom_initialization(self):
        d = RegimeDetector(adx_period=20, adx_trending_threshold=30.0)
        assert d.adx_period == 20
        assert d.adx_trending_threshold == 30.0

    def test_weights_sum_to_one(self, detector):
        assert sum(detector.weights.values()) == pytest.approx(1.0)


class TestADXCalculation:
    """Test ADX calculation."""

    def test_adx_returns_series(self, detector, trending_data):
        adx = detector.calculate_adx(
            trending_data['high'],
            trending_data['low'],
            trending_data['close']
        )
        assert isinstance(adx, pd.Series)
        assert len(adx) == len(trending_data)

    def test_adx_values_reasonable(self, detector, trending_data):
        adx = detector.calculate_adx(
            trending_data['high'],
            trending_data['low'],
            trending_data['close']
        )
        valid_adx = adx.dropna()
        assert (valid_adx >= 0).all()
        assert (valid_adx <= 100).all()

    def test_adx_insufficient_data(self, detector):
        """ADX with too few data points returns NaN."""
        short_data = pd.Series([100, 101, 102])
        result = detector.calculate_adx(short_data, short_data - 1, short_data)
        assert result.isna().all()


class TestVolatilityRatio:
    """Test volatility ratio calculation."""

    def test_returns_series(self, detector, trending_data):
        ratio = detector.calculate_volatility_ratio(trending_data['close'])
        assert isinstance(ratio, pd.Series)
        assert len(ratio) == len(trending_data)

    def test_insufficient_data(self, detector):
        short = pd.Series([100, 101, 102])
        ratio = detector.calculate_volatility_ratio(short)
        assert ratio.isna().all()


class TestPriceActionRegime:
    """Test price action regime detection."""

    def test_returns_series(self, detector, trending_data):
        result = detector.detect_price_action_regime(
            trending_data['close'],
            trending_data['high'],
            trending_data['low']
        )
        assert isinstance(result, pd.Series)

    def test_values_in_range(self, detector, trending_data):
        result = detector.detect_price_action_regime(
            trending_data['close'],
            trending_data['high'],
            trending_data['low']
        )
        assert set(result.unique()).issubset({-1, 0, 1})


class TestDetectRegime:
    """Test full regime detection."""

    def test_trending_data_detection(self, detector, trending_data):
        result = detector.detect_regime(trending_data)
        assert isinstance(result, RegimeResult)
        assert isinstance(result.regime, MarketRegime)
        assert 0.0 <= result.confidence <= 1.0
        # Strong trend should be detected as trending
        assert result.regime == MarketRegime.TRENDING

    def test_ranging_data_detection(self, detector, ranging_data):
        result = detector.detect_regime(ranging_data)
        assert isinstance(result, RegimeResult)
        # Ranging data should not be detected as trending
        assert result.regime in (MarketRegime.RANGING, MarketRegime.NEUTRAL)

    def test_insufficient_data_returns_neutral(self, detector):
        short = pd.DataFrame({
            'high': [101, 102],
            'low': [99, 100],
            'close': [100, 101]
        })
        result = detector.detect_regime(short)
        assert result.regime == MarketRegime.NEUTRAL
        assert result.confidence == 0.0

    def test_result_has_indicators(self, detector, trending_data):
        result = detector.detect_regime(trending_data)
        assert 'adx_vote' in result.indicators
        assert 'vol_vote' in result.indicators
        assert 'price_vote' in result.indicators

    def test_regime_for_strategy_selection(self, detector, trending_data):
        regime_str = detector.get_regime_for_strategy_selection(trending_data)
        assert regime_str in ("trending", "ranging", "neutral")

    def test_low_confidence_returns_neutral(self, detector):
        """With high confidence threshold, ambiguous data returns neutral."""
        np.random.seed(123)
        n = 100
        close = 100 + np.random.normal(0, 0.5, n)
        df = pd.DataFrame({
            'high': close + 0.5,
            'low': close - 0.5,
            'close': close
        })
        regime = detector.get_regime_for_strategy_selection(df, confidence_threshold=0.99)
        assert regime == "neutral"
