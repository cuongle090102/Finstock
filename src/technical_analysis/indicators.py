"""Comprehensive Technical Analysis Indicators Library for Vietnamese Market Trading.

This module provides vectorized implementations of common technical indicators
optimized for real-time streaming data and Vietnamese market characteristics.
"""

import pandas as pd
import numpy as np
from typing import Union, Tuple, Optional, Dict, Any
from enum import Enum
import logging
import warnings

logger = logging.getLogger(__name__)

class IndicatorType(Enum):
    """Types of technical indicators."""
    TREND = "trend"
    MOMENTUM = "momentum"
    VOLATILITY = "volatility"
    VOLUME = "volume"
    SUPPORT_RESISTANCE = "support_resistance"

class TechnicalIndicators:
    """Vectorized technical indicator calculations for high-performance trading."""
    
    @staticmethod
    def sma(prices: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average."""
        if len(prices) < period:
            return pd.Series([np.nan] * len(prices), index=prices.index)
        return prices.rolling(window=period, min_periods=period).mean()
    
    @staticmethod
    def ema(prices: pd.Series, period: int, alpha: Optional[float] = None) -> pd.Series:
        """Exponential Moving Average."""
        if alpha is None:
            alpha = 2.0 / (period + 1)
        return prices.ewm(alpha=alpha, adjust=False).mean()
    
    @staticmethod
    def wma(prices: pd.Series, period: int) -> pd.Series:
        """Weighted Moving Average."""
        if len(prices) < period:
            return pd.Series([np.nan] * len(prices), index=prices.index)
            
        weights = np.arange(1, period + 1)
        def weighted_mean(x):
            return np.average(x, weights=weights)
        
        return prices.rolling(window=period, min_periods=period).apply(weighted_mean, raw=True)
    
    @staticmethod
    def rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index."""
        if len(prices) < period + 1:
            return pd.Series([np.nan] * len(prices), index=prices.index)
            
        delta = prices.diff()
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        avg_gains = gains.ewm(span=period, adjust=False).mean()
        avg_losses = losses.ewm(span=period, adjust=False).mean()
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def bollinger_bands(prices: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Bollinger Bands (Upper, Middle, Lower)."""
        if len(prices) < period:
            nan_series = pd.Series([np.nan] * len(prices), index=prices.index)
            return nan_series, nan_series, nan_series
            
        middle = TechnicalIndicators.sma(prices, period)
        std = prices.rolling(window=period, min_periods=period).std()
        
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        
        return upper, middle, lower
    
    @staticmethod
    def macd(prices: pd.Series, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD (MACD Line, Signal Line, Histogram)."""
        if len(prices) < max(fast_period, slow_period):
            nan_series = pd.Series([np.nan] * len(prices), index=prices.index)
            return nan_series, nan_series, nan_series
            
        ema_fast = TechnicalIndicators.ema(prices, fast_period)
        ema_slow = TechnicalIndicators.ema(prices, slow_period)
        
        macd_line = ema_fast - ema_slow
        signal_line = TechnicalIndicators.ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
    def stochastic_oscillator(high: pd.Series, low: pd.Series, close: pd.Series, 
                            k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """Stochastic Oscillator (%K, %D)."""
        if len(high) < k_period:
            nan_series = pd.Series([np.nan] * len(high), index=high.index)
            return nan_series, nan_series
            
        lowest_low = low.rolling(window=k_period, min_periods=k_period).min()
        highest_high = high.rolling(window=k_period, min_periods=k_period).max()
        
        k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(window=d_period, min_periods=d_period).mean()
        
        return k_percent, d_percent
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average True Range."""
        if len(high) < 2:
            return pd.Series([np.nan] * len(high), index=high.index)
            
        high_low = high - low
        high_close = np.abs(high - close.shift(1))
        low_close = np.abs(low - close.shift(1))
        
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        true_range = pd.Series(true_range, index=high.index)
        
        return true_range.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """On-Balance Volume."""
        if len(close) < 2 or len(volume) != len(close):
            return pd.Series([np.nan] * len(close), index=close.index)
            
        price_change = close.diff()
        volume_direction = np.where(price_change > 0, volume, 
                                  np.where(price_change < 0, -volume, 0))
        
        obv = pd.Series(volume_direction, index=close.index).cumsum()
        return obv
    
    @staticmethod
    def volume_sma(volume: pd.Series, period: int = 20) -> pd.Series:
        """Volume Simple Moving Average."""
        return TechnicalIndicators.sma(volume, period)
    
    @staticmethod
    def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
        """Volume Weighted Average Price."""
        if len(high) != len(volume) or len(high) < 1:
            return pd.Series([np.nan] * len(high), index=high.index)
            
        typical_price = (high + low + close) / 3
        volume_price = typical_price * volume
        
        cumulative_volume_price = volume_price.cumsum()
        cumulative_volume = volume.cumsum()
        
        vwap = cumulative_volume_price / cumulative_volume
        return vwap
    
    @staticmethod
    def vietnamese_market_indicators(prices: pd.Series, volume: pd.Series, 
                                   session_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Vietnamese market specific indicators."""
        indicators = {}
        
        # Standard indicators
        indicators['sma_5'] = TechnicalIndicators.sma(prices, 5)
        indicators['sma_20'] = TechnicalIndicators.sma(prices, 20)
        indicators['ema_12'] = TechnicalIndicators.ema(prices, 12)
        indicators['rsi_14'] = TechnicalIndicators.rsi(prices, 14)
        
        # Vietnamese market specific calculations
        if len(prices) >= 20:
            # Morning/Afternoon session volatility
            daily_range = (prices.rolling(20).max() - prices.rolling(20).min()) / prices.rolling(20).mean()
            indicators['volatility_20d'] = daily_range * 100
            
            # VN30 momentum (if applicable)
            if len(prices) >= 30:
                price_momentum = (prices / prices.shift(30) - 1) * 100
                indicators['momentum_30d'] = price_momentum
        
        # Volume patterns for Vietnamese market
        if len(volume) >= 10:
            volume_ma = TechnicalIndicators.sma(volume, 10)
            volume_ratio = volume / volume_ma
            indicators['volume_ratio'] = volume_ratio
            
            # Identify unusual volume spikes (common in VN market)
            volume_spike = volume_ratio > 2.0
            indicators['volume_spike'] = volume_spike
        
        # Session-specific indicators
        if session_info:
            current_session = session_info.get('current_session', 'unknown')
            indicators['session'] = current_session
            
            # Different behavior patterns for morning vs afternoon
            if current_session == 'morning':
                indicators['session_strength'] = 1.2  # Morning typically stronger
            elif current_session == 'afternoon':
                indicators['session_strength'] = 0.9  # Afternoon typically weaker
            else:
                indicators['session_strength'] = 1.0
                
        return indicators
    
    @staticmethod
    def support_resistance_levels(prices: pd.Series, window: int = 20, 
                                strength: int = 2) -> Dict[str, pd.Series]:
        """Calculate dynamic support and resistance levels."""
        if len(prices) < window:
            nan_series = pd.Series([np.nan] * len(prices), index=prices.index)
            return {'support': nan_series, 'resistance': nan_series}
        
        # Local minima for support
        support_levels = prices.rolling(window=window, center=True).min()
        
        # Local maxima for resistance  
        resistance_levels = prices.rolling(window=window, center=True).max()
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }
    
    @staticmethod
    def momentum_oscillators(prices: pd.Series) -> Dict[str, pd.Series]:
        """Collection of momentum oscillators."""
        oscillators = {}
        
        if len(prices) >= 14:
            oscillators['rsi'] = TechnicalIndicators.rsi(prices, 14)
            
        if len(prices) >= 26:
            macd, signal, histogram = TechnicalIndicators.macd(prices)
            oscillators['macd'] = macd
            oscillators['macd_signal'] = signal
            oscillators['macd_histogram'] = histogram
            
        # Rate of Change
        if len(prices) >= 10:
            roc = ((prices / prices.shift(10)) - 1) * 100
            oscillators['roc_10'] = roc
            
        # Williams %R
        if len(prices) >= 14:
            highest_high = prices.rolling(14).max()
            lowest_low = prices.rolling(14).min()
            williams_r = -100 * (highest_high - prices) / (highest_high - lowest_low)
            oscillators['williams_r'] = williams_r
            
        return oscillators

class IndicatorCalculator:
    """High-level indicator calculator for strategy integration."""
    
    def __init__(self, vietnamese_market: bool = True):
        self.vietnamese_market = vietnamese_market
        self.indicators = TechnicalIndicators()
        
    def calculate_all_indicators(self, market_data: pd.DataFrame, 
                               symbol: str = None) -> Dict[str, Any]:
        """Calculate comprehensive set of indicators for market data."""
        try:
            if market_data.empty:
                return {}
                
            results = {}
            
            # Required columns
            required_cols = ['close']
            if not all(col in market_data.columns for col in required_cols):
                raise ValueError(f"Missing required columns. Need: {required_cols}")
                
            close = market_data['close']
            high = market_data.get('high', close)
            low = market_data.get('low', close)
            volume = market_data.get('volume', pd.Series([0] * len(close), index=close.index))
            
            # Trend indicators
            results['trend'] = {
                'sma_5': self.indicators.sma(close, 5),
                'sma_10': self.indicators.sma(close, 10),
                'sma_20': self.indicators.sma(close, 20),
                'sma_50': self.indicators.sma(close, 50),
                'ema_12': self.indicators.ema(close, 12),
                'ema_26': self.indicators.ema(close, 26)
            }
            
            # Momentum indicators
            results['momentum'] = self.indicators.momentum_oscillators(close)
            
            # Volatility indicators
            bb_upper, bb_middle, bb_lower = self.indicators.bollinger_bands(close)
            results['volatility'] = {
                'bb_upper': bb_upper,
                'bb_middle': bb_middle,
                'bb_lower': bb_lower,
                'atr': self.indicators.atr(high, low, close) if len(high) > 1 else pd.Series([np.nan] * len(close), index=close.index)
            }
            
            # Volume indicators
            if not volume.isna().all() and volume.sum() > 0:
                results['volume'] = {
                    'obv': self.indicators.obv(close, volume),
                    'volume_sma': self.indicators.volume_sma(volume),
                    'vwap': self.indicators.vwap(high, low, close, volume)
                }
            else:
                results['volume'] = {
                    'obv': pd.Series([np.nan] * len(close), index=close.index),
                    'volume_sma': pd.Series([np.nan] * len(close), index=close.index),
                    'vwap': pd.Series([np.nan] * len(close), index=close.index)
                }
            
            # Support/Resistance
            results['support_resistance'] = self.indicators.support_resistance_levels(close)
            
            # Vietnamese market specific
            if self.vietnamese_market:
                session_info = {
                    'current_session': 'morning',  # This would come from market session detector
                    'symbol': symbol
                }
                results['vietnamese_specific'] = self.indicators.vietnamese_market_indicators(
                    close, volume, session_info
                )
            
            return results
            
        except (KeyError, ValueError, TypeError, ZeroDivisionError) as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}
    
    def get_trading_signals(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Generate trading signals from calculated indicators."""
        signals = {
            'buy_signals': [],
            'sell_signals': [],
            'neutral_signals': []
        }
        
        try:
            trend = indicators.get('trend', {})
            momentum = indicators.get('momentum', {})
            volatility = indicators.get('volatility', {})
            
            # Golden Cross signal
            sma_10 = trend.get('sma_10')
            sma_20 = trend.get('sma_20')
            
            if sma_10 is not None and sma_20 is not None and len(sma_10) > 1:
                current_10 = sma_10.iloc[-1]
                current_20 = sma_20.iloc[-1]
                prev_10 = sma_10.iloc[-2]
                prev_20 = sma_20.iloc[-2]
                
                if prev_10 <= prev_20 and current_10 > current_20:
                    signals['buy_signals'].append('golden_cross')
                elif prev_10 >= prev_20 and current_10 < current_20:
                    signals['sell_signals'].append('death_cross')
            
            # RSI signals
            rsi = momentum.get('rsi')
            if rsi is not None and len(rsi) > 0:
                current_rsi = rsi.iloc[-1]
                if not pd.isna(current_rsi):
                    if current_rsi < 30:
                        signals['buy_signals'].append('rsi_oversold')
                    elif current_rsi > 70:
                        signals['sell_signals'].append('rsi_overbought')
            
            # Bollinger Bands signals
            bb_upper = volatility.get('bb_upper')
            bb_lower = volatility.get('bb_lower')
            
            if bb_upper is not None and bb_lower is not None and len(bb_upper) > 0:
                # This would need current price to compare
                pass  # Implementation would depend on current price context
            
        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Error generating trading signals: {e}")
            
        return signals

# Utility functions for strategy integration
def calculate_indicator_confidence(indicators: Dict[str, Any], 
                                 signal_type: str = 'buy') -> float:
    """Calculate confidence score based on multiple indicators alignment."""
    try:
        confidence = 0.5  # Base confidence
        signal_count = 0
        
        # Trend alignment
        trend = indicators.get('trend', {})
        sma_5 = trend.get('sma_5')
        sma_20 = trend.get('sma_20')
        
        if sma_5 is not None and sma_20 is not None and len(sma_5) > 0:
            if signal_type == 'buy' and sma_5.iloc[-1] > sma_20.iloc[-1]:
                confidence += 0.1
                signal_count += 1
            elif signal_type == 'sell' and sma_5.iloc[-1] < sma_20.iloc[-1]:
                confidence += 0.1
                signal_count += 1
        
        # Momentum confirmation
        momentum = indicators.get('momentum', {})
        rsi = momentum.get('rsi')
        
        if rsi is not None and len(rsi) > 0:
            current_rsi = rsi.iloc[-1]
            if not pd.isna(current_rsi):
                if signal_type == 'buy' and 30 <= current_rsi <= 50:
                    confidence += 0.15
                    signal_count += 1
                elif signal_type == 'sell' and 50 <= current_rsi <= 70:
                    confidence += 0.15
                    signal_count += 1
        
        # Vietnamese market specific
        vn_specific = indicators.get('vietnamese_specific', {})
        session_strength = vn_specific.get('session_strength', 1.0)
        
        if isinstance(session_strength, (int, float)):
            confidence *= session_strength
        
        # Volume confirmation
        volume = indicators.get('volume', {})
        volume_ratio = vn_specific.get('volume_ratio')
        
        if volume_ratio is not None and len(volume_ratio) > 0:
            current_volume_ratio = volume_ratio.iloc[-1]
            if not pd.isna(current_volume_ratio) and current_volume_ratio > 1.2:
                confidence += 0.1
                signal_count += 1
        
        # Normalize confidence based on number of confirming signals
        if signal_count > 0:
            confidence = min(confidence, 1.0)
        else:
            confidence = 0.3  # Low confidence if no signals align
            
        return confidence
        
    except (KeyError, TypeError, IndexError, ValueError) as e:
        logger.error(f"Error calculating indicator confidence: {e}")
        return 0.5

# Export main classes
__all__ = [
    'TechnicalIndicators',
    'IndicatorCalculator', 
    'IndicatorType',
    'calculate_indicator_confidence'
]