"""Momentum Strategy implementation with Vietnamese market support.

This strategy identifies and trades momentum breakouts using Rate of Change (ROC),
MACD, and trend following indicators, optimized for Vietnamese market patterns.
"""

from typing import Dict, Any, List, Optional, Tuple
from src.strategies.base_strategy import BaseStrategy, SignalType
from src.technical_analysis.indicators import TechnicalIndicators
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class MomentumStrategy(BaseStrategy):
    """Momentum trading strategy for Vietnamese market.
    
    Uses ROC, MACD, and trend strength to identify strong directional movements
    Generates BUY signals on strong upward momentum with confirmation
    Generates SELL signals on strong downward momentum with confirmation
    """
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__("Momentum", params)
        
        # Rate of Change parameters
        self.roc_short_period = params.get('roc_short_period', 10)
        self.roc_long_period = params.get('roc_long_period', 20)
        self.roc_threshold = params.get('roc_threshold', 5.0)  # 5% ROC threshold
        self.roc_acceleration_period = params.get('roc_acceleration_period', 5)
        
        # MACD parameters
        self.macd_fast = params.get('macd_fast', 12)
        self.macd_slow = params.get('macd_slow', 26)
        self.macd_signal = params.get('macd_signal', 9)
        self.macd_threshold = params.get('macd_threshold', 0.0)
        
        # Momentum confirmation parameters
        self.momentum_confirmation_period = params.get('momentum_confirmation_period', 3)
        self.volume_surge_multiplier = params.get('volume_surge_multiplier', 2.0)
        self.min_momentum_strength = params.get('min_momentum_strength', 0.6)
        
        # Trend following parameters
        self.trend_sma_fast = params.get('trend_sma_fast', 20)
        self.trend_sma_slow = params.get('trend_sma_slow', 50)
        self.trend_alignment_boost = params.get('trend_alignment_boost', 0.15)
        self.counter_trend_penalty = params.get('counter_trend_penalty', 0.3)
        
        # Vietnamese market parameters
        self.vn30_momentum_boost = params.get('vn30_momentum_boost', 0.1)
        self.morning_session_multiplier = params.get('morning_session_multiplier', 1.2)
        self.afternoon_session_multiplier = params.get('afternoon_session_multiplier', 0.95)
        self.market_open_filter_minutes = params.get('market_open_filter_minutes', 15)  # Avoid first 15 min
        
        # Signal management
        self.momentum_duration_threshold = params.get('momentum_duration_threshold', 30)  # Minutes
        self.max_signals_per_hour = params.get('max_signals_per_hour', 3)
        self.signal_decay_factor = params.get('signal_decay_factor', 0.95)
        
        # Data structures
        self.price_buffers = {}
        self.volume_buffers = {}
        self.roc_buffers = {}
        self.macd_buffers = {}
        self.momentum_signals = {}
        self.signal_counts = {}
        
        # VN30 symbols (centralized)
        from src.utils.market_config import VN30_SYMBOLS
        self.vn30_symbols = VN30_SYMBOLS
        
    def _initialize_strategy(self):
        """Initialize strategy-specific parameters and indicators."""
        self.logger.info(f"Initializing Momentum Strategy:")
        self.logger.info(f"  ROC: {self.roc_short_period}/{self.roc_long_period}-period, threshold: {self.roc_threshold}%")
        self.logger.info(f"  MACD: {self.macd_fast}/{self.macd_slow}/{self.macd_signal}")
        self.logger.info(f"  Trend SMA: {self.trend_sma_fast}/{self.trend_sma_slow}")
        self.logger.info(f"  Volume surge: {self.volume_surge_multiplier}x")
    
    def calculate_rate_of_change(self, prices: pd.Series) -> Dict[str, pd.Series]:
        """Calculate Rate of Change indicators for momentum detection."""
        if len(prices) < max(self.roc_short_period, self.roc_long_period) + 1:
            return {
                'roc_short': pd.Series([np.nan] * len(prices), index=prices.index),
                'roc_long': pd.Series([np.nan] * len(prices), index=prices.index),
                'roc_acceleration': pd.Series([np.nan] * len(prices), index=prices.index)
            }
        
        # Short and long period ROC
        roc_short = ((prices / prices.shift(self.roc_short_period)) - 1) * 100
        roc_long = ((prices / prices.shift(self.roc_long_period)) - 1) * 100
        
        # ROC acceleration (rate of change of ROC)
        roc_acceleration = roc_short.diff(self.roc_acceleration_period)
        
        return {
            'roc_short': roc_short,
            'roc_long': roc_long,
            'roc_acceleration': roc_acceleration
        }
    
    def calculate_macd_momentum(self, prices: pd.Series) -> Dict[str, Any]:
        """Calculate MACD with momentum analysis."""
        if len(prices) < self.macd_slow + self.macd_signal:
            return {
                'macd_line': pd.Series([np.nan] * len(prices), index=prices.index),
                'signal_line': pd.Series([np.nan] * len(prices), index=prices.index),
                'histogram': pd.Series([np.nan] * len(prices), index=prices.index),
                'momentum_strength': 0.0,
                'trend_direction': 'neutral'
            }
        
        macd_line, signal_line, histogram = TechnicalIndicators.macd(
            prices, self.macd_fast, self.macd_slow, self.macd_signal
        )
        
        # Calculate momentum strength from MACD
        if len(histogram) > 10:
            recent_histogram = histogram.tail(10)
            momentum_strength = abs(recent_histogram.mean()) / prices.tail(10).std() if prices.tail(10).std() > 0 else 0
            momentum_strength = min(momentum_strength, 1.0)
        else:
            momentum_strength = 0.0
        
        # Determine trend direction
        current_macd = macd_line.iloc[-1] if not pd.isna(macd_line.iloc[-1]) else 0
        current_signal = signal_line.iloc[-1] if not pd.isna(signal_line.iloc[-1]) else 0
        
        if current_macd > current_signal and current_macd > self.macd_threshold:
            trend_direction = 'bullish'
        elif current_macd < current_signal and current_macd < -self.macd_threshold:
            trend_direction = 'bearish'
        else:
            trend_direction = 'neutral'
        
        return {
            'macd_line': macd_line,
            'signal_line': signal_line,
            'histogram': histogram,
            'momentum_strength': momentum_strength,
            'trend_direction': trend_direction
        }
    
    def calculate_trend_alignment(self, prices: pd.Series) -> Dict[str, Any]:
        """Calculate trend alignment for momentum confirmation."""
        if len(prices) < self.trend_sma_slow:
            return {
                'sma_fast': pd.Series([np.nan] * len(prices), index=prices.index),
                'sma_slow': pd.Series([np.nan] * len(prices), index=prices.index),
                'trend_aligned': False,
                'trend_strength': 0.0,
                'trend_direction': 'neutral'
            }
        
        sma_fast = TechnicalIndicators.sma(prices, self.trend_sma_fast)
        sma_slow = TechnicalIndicators.sma(prices, self.trend_sma_slow)
        
        # Calculate trend alignment
        current_price = prices.iloc[-1]
        current_fast = sma_fast.iloc[-1] if not pd.isna(sma_fast.iloc[-1]) else current_price
        current_slow = sma_slow.iloc[-1] if not pd.isna(sma_slow.iloc[-1]) else current_price
        
        # Trend direction and strength
        if current_price > current_fast > current_slow:
            trend_direction = 'up'
            trend_aligned = True
            trend_strength = (current_fast - current_slow) / current_slow
        elif current_price < current_fast < current_slow:
            trend_direction = 'down'
            trend_aligned = True
            trend_strength = (current_slow - current_fast) / current_slow
        else:
            trend_direction = 'sideways'
            trend_aligned = False
            trend_strength = 0.0
        
        return {
            'sma_fast': sma_fast,
            'sma_slow': sma_slow,
            'trend_aligned': trend_aligned,
            'trend_strength': min(abs(trend_strength), 0.1) if trend_strength else 0.0,
            'trend_direction': trend_direction
        }
    
    def detect_momentum_signals(self, symbol: str, prices: pd.Series, volumes: pd.Series = None) -> List[Dict]:
        """Detect momentum trading opportunities."""
        signals = []
        
        required_length = max(
            self.roc_long_period + 1,
            self.macd_slow + self.macd_signal,
            self.trend_sma_slow
        ) + 10
        
        if len(prices) < required_length:
            return signals
        
        # Calculate momentum indicators
        roc_data = self.calculate_rate_of_change(prices)
        macd_data = self.calculate_macd_momentum(prices)
        trend_data = self.calculate_trend_alignment(prices)
        
        # Get current values
        current_price = prices.iloc[-1]
        current_roc_short = roc_data['roc_short'].iloc[-1] if not pd.isna(roc_data['roc_short'].iloc[-1]) else 0
        current_roc_long = roc_data['roc_long'].iloc[-1] if not pd.isna(roc_data['roc_long'].iloc[-1]) else 0
        roc_acceleration = roc_data['roc_acceleration'].iloc[-1] if not pd.isna(roc_data['roc_acceleration'].iloc[-1]) else 0
        
        # Volume surge detection
        volume_surge = False
        if volumes is not None and len(volumes) >= 20:
            avg_volume = volumes.tail(20).mean()
            current_volume = volumes.iloc[-1]
            volume_surge = current_volume >= (avg_volume * self.volume_surge_multiplier)
        
        # Market timing filter (avoid first 15 minutes)
        current_session = self.get_current_market_session()
        if current_session.value in ['morning', 'afternoon']:
            # Simple time-based filter - in real implementation would check actual market open time
            session_filter_passed = True  # Simplified for this implementation
        else:
            session_filter_passed = False
        
        if not session_filter_passed:
            return signals
        
        # Session multipliers
        session_multiplier = 1.0
        if current_session.value == 'morning':
            session_multiplier = self.morning_session_multiplier
        elif current_session.value == 'afternoon':
            session_multiplier = self.afternoon_session_multiplier
        
        # BULLISH MOMENTUM SIGNAL
        bullish_conditions = [
            current_roc_short > self.roc_threshold * session_multiplier,  # Strong short-term momentum
            current_roc_long > 0,  # Positive long-term momentum
            roc_acceleration > 0,  # Accelerating momentum
            macd_data['trend_direction'] == 'bullish',  # MACD confirmation
            macd_data['momentum_strength'] >= self.min_momentum_strength  # Strong momentum
        ]
        
        # Trend alignment bonus (not required but boosts confidence)
        trend_alignment_bonus = (
            trend_data['trend_direction'] == 'up' and trend_data['trend_aligned']
        )
        
        if sum(bullish_conditions) >= 4:  # At least 4 of 5 conditions
            confidence = self._calculate_momentum_confidence(
                'BUY', roc_data, macd_data, trend_data, volume_surge, 
                symbol, session_multiplier, trend_alignment_bonus
            )
            
            signals.append({
                'timestamp': prices.index[-1],
                'signal_type': SignalType.BUY,
                'confidence': confidence,
                'price': current_price,
                'signal_reason': 'bullish_momentum',
                'roc_short': current_roc_short,
                'roc_long': current_roc_long,
                'roc_acceleration': roc_acceleration,
                'macd_trend': macd_data['trend_direction'],
                'momentum_strength': macd_data['momentum_strength'],
                'trend_aligned': trend_alignment_bonus,
                'volume_surge': volume_surge,
                'session_multiplier': session_multiplier
            })
        
        # BEARISH MOMENTUM SIGNAL
        bearish_conditions = [
            current_roc_short < -self.roc_threshold * session_multiplier,  # Strong negative momentum
            current_roc_long < 0,  # Negative long-term momentum
            roc_acceleration < 0,  # Decelerating (more negative)
            macd_data['trend_direction'] == 'bearish',  # MACD confirmation
            macd_data['momentum_strength'] >= self.min_momentum_strength  # Strong momentum
        ]
        
        # Trend alignment bonus for bearish signals
        trend_alignment_bonus = (
            trend_data['trend_direction'] == 'down' and trend_data['trend_aligned']
        )
        
        if sum(bearish_conditions) >= 4:  # At least 4 of 5 conditions
            confidence = self._calculate_momentum_confidence(
                'SELL', roc_data, macd_data, trend_data, volume_surge,
                symbol, session_multiplier, trend_alignment_bonus
            )
            
            signals.append({
                'timestamp': prices.index[-1],
                'signal_type': SignalType.SELL,
                'confidence': confidence,
                'price': current_price,
                'signal_reason': 'bearish_momentum',
                'roc_short': current_roc_short,
                'roc_long': current_roc_long,
                'roc_acceleration': roc_acceleration,
                'macd_trend': macd_data['trend_direction'],
                'momentum_strength': macd_data['momentum_strength'],
                'trend_aligned': trend_alignment_bonus,
                'volume_surge': volume_surge,
                'session_multiplier': session_multiplier
            })
        
        return signals
    
    def _calculate_momentum_confidence(self, signal_type: str, roc_data: Dict, macd_data: Dict,
                                     trend_data: Dict, volume_surge: bool, symbol: str,
                                     session_multiplier: float, trend_aligned: bool) -> float:
        """Calculate confidence score for momentum signals."""
        confidence = 0.6  # Base confidence
        
        # ROC strength boost
        current_roc = roc_data['roc_short'].iloc[-1] if not pd.isna(roc_data['roc_short'].iloc[-1]) else 0
        roc_strength = abs(current_roc) / self.roc_threshold
        confidence += min(roc_strength - 1, 0.15)  # Up to 15% boost for strong ROC
        
        # MACD momentum strength
        confidence += macd_data['momentum_strength'] * 0.2
        
        # Trend alignment boost
        if trend_aligned:
            confidence += self.trend_alignment_boost
        else:
            confidence -= self.counter_trend_penalty  # Penalty for counter-trend
        
        # Volume surge confirmation
        if volume_surge:
            confidence += 0.1
        
        # VN30 stock boost (more liquid, cleaner momentum)
        if symbol in self.vn30_symbols:
            confidence += self.vn30_momentum_boost
        
        # Session multiplier
        confidence *= session_multiplier
        
        # ROC acceleration boost
        roc_accel = roc_data['roc_acceleration'].iloc[-1] if not pd.isna(roc_data['roc_acceleration'].iloc[-1]) else 0
        if signal_type == 'BUY' and roc_accel > 0:
            confidence += 0.05
        elif signal_type == 'SELL' and roc_accel < 0:
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def _is_new_momentum_signal(self, symbol: str, signal_data: Dict) -> bool:
        """Check if this is a new momentum signal with rate limiting."""
        import pandas as pd
        def _tz_naive(ts):
            t = pd.to_datetime(ts)
            return t.tz_localize(None) if t.tzinfo else t
        current_time = _tz_naive(signal_data['timestamp'])

        # Initialize signal tracking for new symbols
        if symbol not in self.signal_counts:
            self.signal_counts[symbol] = []

        # Clean old signals (older than 1 hour)
        cutoff_time = current_time - timedelta(hours=1)
        self.signal_counts[symbol] = [
            sig_time for sig_time in self.signal_counts[symbol]
            if _tz_naive(sig_time) > cutoff_time
        ]
        
        # Check rate limit
        if len(self.signal_counts[symbol]) >= self.max_signals_per_hour:
            return False
        
        # Check against last momentum signal
        if symbol in self.momentum_signals:
            last_signal = self.momentum_signals[symbol]
            try:
                import pandas as pd
                def _tz_naive(ts):
                    t = pd.to_datetime(ts)
                    return t.tz_localize(None) if t.tzinfo else t
                time_diff_secs = (_tz_naive(current_time) - _tz_naive(last_signal['timestamp'])).total_seconds()
            except (TypeError, ValueError):
                time_diff_secs = 9999

            # Minimum duration between momentum signals
            if time_diff_secs < self.momentum_duration_threshold * 60:
                return False
            
            # Allow opposite direction signals immediately
            if last_signal.get('signal_type') != signal_data['signal_type']:
                return True
        
        return True
    
    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data and generate momentum signals."""
        try:
            if market_data.empty:
                return
            
            # Process each symbol
            for symbol in market_data.get('symbol', pd.Series()).unique():
                if pd.isna(symbol):
                    continue
                
                symbol_data = market_data[market_data['symbol'] == symbol].copy()
                if symbol_data.empty or 'close' not in symbol_data.columns:
                    continue
                
                # Initialize buffers for new symbols
                if symbol not in self.price_buffers:
                    self.price_buffers[symbol] = pd.Series(dtype=float)
                    self.volume_buffers[symbol] = pd.Series(dtype=float)
                
                # Update data buffers
                if 'timestamp' in symbol_data.columns:
                    new_prices = symbol_data.set_index('timestamp')['close']
                    new_volumes = symbol_data.set_index('timestamp').get('volume', pd.Series())
                else:
                    new_prices = symbol_data['close']
                    new_volumes = symbol_data.get('volume', pd.Series())
                
                # Maintain buffer size (keep last 120 points)
                self.price_buffers[symbol] = pd.concat([self.price_buffers[symbol], new_prices]).tail(120)
                if not new_volumes.empty:
                    self.volume_buffers[symbol] = pd.concat([self.volume_buffers[symbol], new_volumes]).tail(120)
                
                # Generate signals if market is open and sufficient data
                required_length = max(self.roc_long_period + 1, self.macd_slow + self.macd_signal, self.trend_sma_slow) + 10
                if (self.is_market_open() and len(self.price_buffers[symbol]) >= required_length):
                    
                    signals = self.detect_momentum_signals(
                        symbol,
                        self.price_buffers[symbol],
                        self.volume_buffers.get(symbol, pd.Series())
                    )
                    
                    # Process and publish new signals
                    for signal_data in signals:
                        if self._is_new_momentum_signal(symbol, signal_data):
                            signal = self.create_signal(
                                symbol=symbol,
                                signal_type=signal_data['signal_type'],
                                confidence=signal_data['confidence'],
                                price=signal_data['price'],
                                metadata={
                                    'strategy_type': 'Momentum',
                                    'signal_reason': signal_data['signal_reason'],
                                    'roc_short': signal_data['roc_short'],
                                    'roc_long': signal_data['roc_long'],
                                    'roc_acceleration': signal_data['roc_acceleration'],
                                    'macd_trend': signal_data['macd_trend'],
                                    'momentum_strength': signal_data['momentum_strength'],
                                    'trend_aligned': signal_data['trend_aligned'],
                                    'volume_surge': signal_data['volume_surge'],
                                    'session_multiplier': signal_data['session_multiplier'],
                                    'vn30_member': symbol in self.vn30_symbols,
                                    'roc_threshold': self.roc_threshold
                                }
                            )
                            
                            if signal:
                                self.publish_signal(signal)
                                self.momentum_signals[symbol] = signal_data
                                self.signal_counts[symbol].append(signal_data['timestamp'])
        
        except Exception as e:
            self.logger.error(f"Error processing market data in Momentum strategy: {e}")
    
    def generate_signal(self, market_data: pd.DataFrame = None) -> Dict[str, Any]:
        """Generate a single trading signal based on market data.

        This method is called by AdaptiveStrategy. It processes the data
        and returns the most recent signal, or None if no signal.
        """
        if market_data is not None and not market_data.empty:
            self.on_data(market_data)

        signals = self.generate_signals()
        return signals[-1] if signals else None

    def generate_signals(self) -> List[Dict[str, Any]]:
        """Generate momentum signals based on current market data."""
        signals = []

        try:
            if not self.is_market_open():
                return signals
            
            current_session = self.get_current_market_session()
            required_length = max(self.roc_long_period + 1, self.macd_slow + self.macd_signal, self.trend_sma_slow) + 10
            
            for symbol, prices in self.price_buffers.items():
                if len(prices) < required_length:
                    continue
                
                volumes = self.volume_buffers.get(symbol, pd.Series())
                momentum_signals = self.detect_momentum_signals(symbol, prices, volumes)
                
                for signal_data in momentum_signals:
                    signal = self.create_signal(
                        symbol=symbol,
                        signal_type=signal_data['signal_type'],
                        confidence=signal_data['confidence'],
                        price=signal_data['price'],
                        metadata={
                            'strategy_type': 'Momentum_Scan',
                            'signal_reason': signal_data['signal_reason'],
                            'roc_short': signal_data['roc_short'],
                            'momentum_strength': signal_data['momentum_strength'],
                            'market_session': current_session.value,
                            'vn30_member': symbol in self.vn30_symbols
                        }
                    )
                    
                    if signal:
                        signals.append(signal)
        
        except Exception as e:
            self.logger.error(f"Error generating Momentum signals: {e}")
        
        return signals
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get detailed momentum strategy status."""
        base_status = self.get_status()
        
        momentum_status = {
            'roc_periods': f"{self.roc_short_period}/{self.roc_long_period}",
            'roc_threshold': f"{self.roc_threshold}%",
            'macd_settings': f"{self.macd_fast}/{self.macd_slow}/{self.macd_signal}",
            'trend_sma': f"{self.trend_sma_fast}/{self.trend_sma_slow}",
            'symbols_tracking': len(self.price_buffers),
            'vn30_coverage': sum(1 for symbol in self.price_buffers.keys() if symbol in self.vn30_symbols),
            'active_signals': len(self.momentum_signals),
            'volume_surge_multiplier': f"{self.volume_surge_multiplier}x",
            'min_momentum_strength': self.min_momentum_strength,
            'max_signals_per_hour': self.max_signals_per_hour,
            'avg_data_points': np.mean([len(prices) for prices in self.price_buffers.values()]) if self.price_buffers else 0,
            'session_multipliers': {
                'morning': self.morning_session_multiplier,
                'afternoon': self.afternoon_session_multiplier
            }
        }
        
        base_status.update({'momentum_details': momentum_status})
        return base_status