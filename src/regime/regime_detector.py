"""
Regime Detection Module
Detects market regime (trending, ranging, neutral) using multiple statistical methods
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, Literal
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MarketRegime(Enum):
    """Market regime types."""
    TRENDING = "trending"
    RANGING = "ranging"
    NEUTRAL = "neutral"


@dataclass
class RegimeResult:
    """Result of regime detection."""
    regime: MarketRegime
    confidence: float
    adx_value: float
    volatility_ratio: float
    indicators: Dict[str, float]


class RegimeDetector:
    """
    Detects market regime using multiple indicators:
    - ADX (Average Directional Index) for trend strength
    - Volatility clustering for range detection
    - Multiple timeframe analysis

    Weighted voting system:
    - ADX: 50% weight (primary indicator)
    - Volatility: 30% weight
    - Price action: 20% weight
    """

    def __init__(
        self,
        adx_period: int = 14,
        adx_trending_threshold: float = 25.0,
        adx_ranging_threshold: float = 20.0,
        volatility_short_period: int = 20,
        volatility_long_period: int = 60
    ):
        """
        Initialize regime detector.

        Args:
            adx_period: Period for ADX calculation
            adx_trending_threshold: ADX above this = trending
            adx_ranging_threshold: ADX below this = ranging
            volatility_short_period: Short-term volatility period
            volatility_long_period: Long-term volatility period
        """
        self.adx_period = adx_period
        self.adx_trending_threshold = adx_trending_threshold
        self.adx_ranging_threshold = adx_ranging_threshold
        self.volatility_short_period = volatility_short_period
        self.volatility_long_period = volatility_long_period

        # Weights for regime voting
        self.weights = {
            'adx': 0.50,
            'volatility': 0.30,
            'price_action': 0.20
        }

    def calculate_adx(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: Optional[int] = None
    ) -> pd.Series:
        """
        Calculate Average Directional Index (ADX).

        ADX measures trend strength (not direction):
        - ADX > 25: Strong trend
        - ADX 20-25: Moderate trend
        - ADX < 20: Weak trend or ranging

        Args:
            high: High prices
            low: Low prices
            close: Close prices
            period: ADX period (default: self.adx_period)

        Returns:
            ADX series
        """
        if period is None:
            period = self.adx_period

        if len(high) < period + 1:
            return pd.Series([np.nan] * len(high), index=high.index)

        # Calculate True Range
        high_low = high - low
        high_close = np.abs(high - close.shift(1))
        low_close = np.abs(low - close.shift(1))
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))

        # Calculate Directional Movement
        plus_dm = high.diff()
        minus_dm = -low.diff()

        # Only positive directional movements count
        plus_dm[plus_dm < 0] = 0
        plus_dm[(plus_dm < minus_dm)] = 0
        minus_dm[minus_dm < 0] = 0
        minus_dm[(minus_dm < plus_dm)] = 0

        # Smooth using Wilder's smoothing (EMA-like)
        alpha = 1.0 / period

        tr_smooth = true_range.ewm(alpha=alpha, adjust=False).mean()
        plus_dm_smooth = plus_dm.ewm(alpha=alpha, adjust=False).mean()
        minus_dm_smooth = minus_dm.ewm(alpha=alpha, adjust=False).mean()

        # Calculate Directional Indicators
        plus_di = 100 * (plus_dm_smooth / tr_smooth)
        minus_di = 100 * (minus_dm_smooth / tr_smooth)

        # Calculate DX (Directional Index)
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)

        # Calculate ADX (smoothed DX)
        adx = dx.ewm(alpha=alpha, adjust=False).mean()

        return adx

    def calculate_volatility_ratio(
        self,
        close: pd.Series,
        short_period: Optional[int] = None,
        long_period: Optional[int] = None
    ) -> pd.Series:
        """
        Calculate volatility ratio (short-term volatility / long-term volatility).

        Ratio > 1: Recent volatility higher than average (potential breakout)
        Ratio < 1: Recent volatility lower than average (consolidation)

        Args:
            close: Close prices
            short_period: Short-term period
            long_period: Long-term period

        Returns:
            Volatility ratio series
        """
        if short_period is None:
            short_period = self.volatility_short_period
        if long_period is None:
            long_period = self.volatility_long_period

        if len(close) < long_period:
            return pd.Series([np.nan] * len(close), index=close.index)

        # Calculate returns
        returns = close.pct_change()

        # Calculate rolling standard deviations
        short_vol = returns.rolling(window=short_period).std()
        long_vol = returns.rolling(window=long_period).std()

        # Avoid division by zero
        volatility_ratio = short_vol / long_vol.replace(0, np.nan)

        return volatility_ratio

    def detect_price_action_regime(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        lookback: int = 20
    ) -> pd.Series:
        """
        Detect regime based on price action patterns.

        Returns:
            Series with values: 1 (trending), 0 (neutral), -1 (ranging)
        """
        if len(close) < lookback:
            return pd.Series([0] * len(close), index=close.index)

        # Calculate higher highs and lower lows
        rolling_max = high.rolling(window=lookback).max()
        rolling_min = low.rolling(window=lookback).min()

        # Calculate price position within range
        price_position = (close - rolling_min) / (rolling_max - rolling_min)

        # Trending: Price consistently in upper/lower portion of range
        trending_up = price_position > 0.7
        trending_down = price_position < 0.3

        # Ranging: Price oscillating in middle portion
        ranging = (price_position >= 0.3) & (price_position <= 0.7)

        regime_signal = pd.Series(0, index=close.index)
        regime_signal[trending_up | trending_down] = 1  # Trending
        regime_signal[ranging] = -1  # Ranging

        return regime_signal

    def detect_regime(
        self,
        df: pd.DataFrame,
        high_col: str = 'high',
        low_col: str = 'low',
        close_col: str = 'close'
    ) -> RegimeResult:
        """
        Detect current market regime using weighted voting.

        Args:
            df: DataFrame with OHLC data
            high_col: Column name for high prices
            low_col: Column name for low prices
            close_col: Column name for close prices

        Returns:
            RegimeResult with detected regime and confidence
        """
        try:
            if len(df) < self.volatility_long_period:
                logger.warning(f"Insufficient data for regime detection: {len(df)} rows")
                return RegimeResult(
                    regime=MarketRegime.NEUTRAL,
                    confidence=0.0,
                    adx_value=0.0,
                    volatility_ratio=1.0,
                    indicators={}
                )

            high = df[high_col]
            low = df[low_col]
            close = df[close_col]

            # 1. Calculate ADX (50% weight)
            adx = self.calculate_adx(high, low, close)
            current_adx = adx.iloc[-1]

            if pd.isna(current_adx):
                current_adx = 20.0  # Default neutral value

            # ADX voting
            if current_adx > self.adx_trending_threshold:
                adx_vote = MarketRegime.TRENDING
                adx_confidence = min(1.0, (current_adx - self.adx_trending_threshold) / 20.0)
            elif current_adx < self.adx_ranging_threshold:
                adx_vote = MarketRegime.RANGING
                adx_confidence = min(1.0, (self.adx_ranging_threshold - current_adx) / 10.0)
            else:
                adx_vote = MarketRegime.NEUTRAL
                adx_confidence = 0.5

            # 2. Calculate volatility ratio (30% weight)
            vol_ratio = self.calculate_volatility_ratio(close)
            current_vol_ratio = vol_ratio.iloc[-1]

            if pd.isna(current_vol_ratio):
                current_vol_ratio = 1.0

            # Volatility voting
            if current_vol_ratio > 1.3:
                vol_vote = MarketRegime.TRENDING
                vol_confidence = min(1.0, (current_vol_ratio - 1.0) / 0.5)
            elif current_vol_ratio < 0.7:
                vol_vote = MarketRegime.RANGING
                vol_confidence = min(1.0, (1.0 - current_vol_ratio) / 0.3)
            else:
                vol_vote = MarketRegime.NEUTRAL
                vol_confidence = 0.5

            # 3. Price action regime (20% weight)
            price_action = self.detect_price_action_regime(close, high, low)
            current_price_action = price_action.iloc[-1]

            if current_price_action == 1:
                price_vote = MarketRegime.TRENDING
                price_confidence = 0.8
            elif current_price_action == -1:
                price_vote = MarketRegime.RANGING
                price_confidence = 0.8
            else:
                price_vote = MarketRegime.NEUTRAL
                price_confidence = 0.5

            # Weighted voting
            votes = {
                MarketRegime.TRENDING: 0.0,
                MarketRegime.RANGING: 0.0,
                MarketRegime.NEUTRAL: 0.0
            }

            votes[adx_vote] += self.weights['adx'] * adx_confidence
            votes[vol_vote] += self.weights['volatility'] * vol_confidence
            votes[price_vote] += self.weights['price_action'] * price_confidence

            # Determine final regime
            final_regime = max(votes, key=votes.get)
            final_confidence = votes[final_regime] / sum(self.weights.values())

            logger.info(
                f"Regime detected: {final_regime.value} (confidence: {final_confidence:.2f})"
                f" | ADX: {current_adx:.2f}, Vol Ratio: {current_vol_ratio:.2f}"
            )

            return RegimeResult(
                regime=final_regime,
                confidence=final_confidence,
                adx_value=current_adx,
                volatility_ratio=current_vol_ratio,
                indicators={
                    'adx_vote': adx_vote.value,
                    'adx_confidence': adx_confidence,
                    'vol_vote': vol_vote.value,
                    'vol_confidence': vol_confidence,
                    'price_vote': price_vote.value,
                    'price_confidence': price_confidence
                }
            )

        except Exception as e:
            logger.error(f"Error detecting regime: {e}")
            return RegimeResult(
                regime=MarketRegime.NEUTRAL,
                confidence=0.0,
                adx_value=0.0,
                volatility_ratio=1.0,
                indicators={}
            )

    def get_regime_for_strategy_selection(
        self,
        df: pd.DataFrame,
        confidence_threshold: float = 0.6
    ) -> Literal["trending", "ranging", "neutral"]:
        """
        Get regime suitable for strategy selection.

        Args:
            df: DataFrame with OHLC data
            confidence_threshold: Minimum confidence to trust regime

        Returns:
            Regime string: "trending", "ranging", or "neutral"
        """
        result = self.detect_regime(df)

        # If confidence is low, default to neutral
        if result.confidence < confidence_threshold:
            return "neutral"

        return result.regime.value
