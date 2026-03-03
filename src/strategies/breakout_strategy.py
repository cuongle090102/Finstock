"""Breakout Strategy implementation with Vietnamese market support.

This strategy identifies and trades breakouts from support/resistance levels,
optimized for Vietnamese market characteristics including VN30 stocks and
market session patterns.
"""

from typing import Dict, Any, List, Optional, Tuple
from src.strategies.base_strategy import BaseStrategy, SignalType
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class BreakoutStrategy(BaseStrategy):
    """Support/Resistance Breakout trading strategy for Vietnamese market.
    
    Generates BUY signals when price breaks above resistance with volume
    Generates SELL signals when price breaks below support with volume
    Includes Vietnamese market session filtering and VN30 optimization.
    """
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__("Breakout", params)
        
        # Breakout detection parameters
        self.lookback_period = params.get('lookback_period', 20)  # Period for S/R calculation
        self.breakout_threshold = params.get('breakout_threshold', 0.015)  # 1.5% breakout threshold
        self.volume_multiplier = params.get('volume_multiplier', 1.5)  # Volume confirmation
        self.consolidation_threshold = params.get('consolidation_threshold', 0.02)  # 2% consolidation range
        
        # Signal validation parameters
        self.min_consolidation_periods = params.get('min_consolidation_periods', 10)  # Minimum consolidation
        self.breakout_confirmation_periods = params.get('breakout_confirmation_periods', 3)  # Confirmation candles
        self.max_false_breakout_ratio = params.get('max_false_breakout_ratio', 0.3)  # False breakout filter
        
        # Vietnamese market specific
        self.morning_session_multiplier = params.get('morning_session_multiplier', 1.2)  # Morning volatility
        self.afternoon_session_multiplier = params.get('afternoon_session_multiplier', 0.9)  # Afternoon adjustment
        
        # Internal data structures
        self.price_history = {}
        self.volume_history = {}
        self.support_resistance_levels = {}
        self.consolidation_zones = {}
        self.breakout_signals_cache = {}
        self.last_breakout_time = {}
        
        # VN30 symbols (centralized)
        from src.utils.market_config import VN30_SYMBOLS
        self.vn30_symbols = VN30_SYMBOLS
        
    def _initialize_strategy(self):
        """Initialize strategy-specific parameters and indicators."""
        self.logger.info(f"Initializing Breakout Strategy with {self.lookback_period}-period lookback")
        self.logger.info(f"Breakout threshold: {self.breakout_threshold*100:.1f}%, Volume multiplier: {self.volume_multiplier}x")
    
    def calculate_support_resistance(self, prices: pd.Series, volumes: pd.Series = None) -> Dict[str, float]:
        """Calculate dynamic support and resistance levels."""
        if len(prices) < self.lookback_period:
            return {'support': np.nan, 'resistance': np.nan, 'strength': 0.0}
        
        # Get recent price data
        recent_prices = prices.tail(self.lookback_period)
        recent_volumes = volumes.tail(self.lookback_period) if volumes is not None else None
        
        # Calculate support (lowest low) and resistance (highest high)
        support_level = recent_prices.min()
        resistance_level = recent_prices.max()
        
        # Calculate level strength based on touches and volume
        support_touches = sum(abs(price - support_level) / support_level < 0.01 for price in recent_prices)
        resistance_touches = sum(abs(price - resistance_level) / resistance_level < 0.01 for price in recent_prices)
        
        # Volume-weighted strength calculation
        strength = (support_touches + resistance_touches) / self.lookback_period
        if recent_volumes is not None:
            avg_volume = recent_volumes.mean()
            volume_strength = min(recent_volumes.iloc[-1] / avg_volume, 2.0) if avg_volume > 0 else 1.0
            strength *= volume_strength
        
        return {
            'support': support_level,
            'resistance': resistance_level,
            'strength': min(strength, 1.0),
            'support_touches': support_touches,
            'resistance_touches': resistance_touches
        }
    
    def detect_consolidation_zone(self, prices: pd.Series, min_periods: int = None) -> Dict[str, Any]:
        """Detect if price is in consolidation (sideways movement)."""
        min_periods = min_periods or self.min_consolidation_periods
        
        if len(prices) < min_periods:
            return {'in_consolidation': False, 'consolidation_range': 0.0, 'periods': 0}
        
        recent_prices = prices.tail(min_periods)
        price_range = (recent_prices.max() - recent_prices.min()) / recent_prices.mean()
        
        # Check if price range is within consolidation threshold
        in_consolidation = price_range <= self.consolidation_threshold
        
        return {
            'in_consolidation': in_consolidation,
            'consolidation_range': price_range,
            'periods': len(recent_prices),
            'high': recent_prices.max(),
            'low': recent_prices.min(),
            'mid': recent_prices.mean()
        }
    
    def detect_breakout_signals(self, symbol: str, prices: pd.Series, volumes: pd.Series = None) -> List[Dict]:
        """Detect breakout signals with Vietnamese market considerations."""
        signals = []
        
        if len(prices) < self.lookback_period + self.breakout_confirmation_periods:
            return signals
        
        # Calculate support/resistance levels
        sr_levels = self.calculate_support_resistance(prices, volumes)
        if np.isnan(sr_levels['support']) or np.isnan(sr_levels['resistance']):
            return signals
        
        # Check consolidation
        consolidation = self.detect_consolidation_zone(prices)
        
        # Get current price and recent prices for confirmation
        current_price = prices.iloc[-1]
        confirmation_prices = prices.tail(self.breakout_confirmation_periods)
        
        # Volume confirmation
        volume_confirmation = True
        if volumes is not None and len(volumes) >= self.lookback_period:
            avg_volume = volumes.tail(self.lookback_period).mean()
            recent_volume = volumes.iloc[-1]
            volume_confirmation = recent_volume >= (avg_volume * self.volume_multiplier)
        
        # Session multiplier based on Vietnamese market sessions
        current_session = self.get_current_market_session()
        session_multiplier = 1.0
        if current_session.value == 'morning':
            session_multiplier = self.morning_session_multiplier
        elif current_session.value == 'afternoon':
            session_multiplier = self.afternoon_session_multiplier
        
        # Resistance Breakout (BUY signal)
        resistance_breakout = (
            current_price > sr_levels['resistance'] * (1 + self.breakout_threshold * session_multiplier)
            and all(p > sr_levels['resistance'] for p in confirmation_prices.tail(self.breakout_confirmation_periods))
        )
        
        # Support Breakdown (SELL signal) 
        support_breakdown = (
            current_price < sr_levels['support'] * (1 - self.breakout_threshold * session_multiplier)
            and all(p < sr_levels['support'] for p in confirmation_prices.tail(self.breakout_confirmation_periods))
        )
        
        # Generate signals with confidence scoring
        if resistance_breakout and volume_confirmation:
            confidence = self._calculate_breakout_confidence(
                'BUY', sr_levels, consolidation, volume_confirmation, symbol, current_price
            )
            
            signals.append({
                'timestamp': prices.index[-1],
                'signal_type': SignalType.BUY,
                'confidence': confidence,
                'price': current_price,
                'breakout_type': 'resistance_breakout',
                'resistance_level': sr_levels['resistance'],
                'support_level': sr_levels['support'],
                'level_strength': sr_levels['strength'],
                'volume_confirmation': volume_confirmation,
                'consolidation_confirmed': consolidation['in_consolidation'],
                'session_multiplier': session_multiplier
            })
            
        elif support_breakdown and volume_confirmation:
            confidence = self._calculate_breakout_confidence(
                'SELL', sr_levels, consolidation, volume_confirmation, symbol, current_price
            )
            
            signals.append({
                'timestamp': prices.index[-1],
                'signal_type': SignalType.SELL,
                'confidence': confidence,
                'price': current_price,
                'breakout_type': 'support_breakdown',
                'resistance_level': sr_levels['resistance'],
                'support_level': sr_levels['support'],
                'level_strength': sr_levels['strength'],
                'volume_confirmation': volume_confirmation,
                'consolidation_confirmed': consolidation['in_consolidation'],
                'session_multiplier': session_multiplier
            })
        
        return signals
    
    def _calculate_breakout_confidence(self, signal_type: str, sr_levels: Dict, 
                                     consolidation: Dict, volume_conf: bool, 
                                     symbol: str, current_price: float) -> float:
        """Calculate confidence score for breakout signals."""
        confidence = 0.6  # Base confidence
        
        # Level strength boost
        confidence += sr_levels['strength'] * 0.15
        
        # Consolidation confirmation boost
        if consolidation['in_consolidation']:
            confidence += 0.15
        
        # Volume confirmation boost
        if volume_conf:
            confidence += 0.1
        
        # VN30 stock boost
        if symbol in self.vn30_symbols:
            confidence += 0.05
        
        # Market session considerations
        current_session = self.get_current_market_session()
        if current_session.value == 'morning':
            confidence *= 1.1  # Morning breakouts often stronger
        elif current_session.value == 'afternoon':
            confidence *= 0.95  # Afternoon breakouts less reliable
        
        # Distance from level (stronger breakouts get higher confidence)
        if signal_type == 'BUY':
            distance_ratio = (current_price - sr_levels['resistance']) / sr_levels['resistance']
        else:
            distance_ratio = (sr_levels['support'] - current_price) / sr_levels['support']
        
        confidence += min(distance_ratio * 5, 0.1)  # Cap at 0.1 boost
        
        return min(confidence, 1.0)
    
    def _is_new_breakout_signal(self, symbol: str, signal_data: Dict) -> bool:
        """Check if this is a new breakout signal to avoid duplicates."""
        if symbol not in self.last_breakout_time:
            return True
        
        last_signal = self.last_breakout_time[symbol]
        current_time = signal_data['timestamp']

        # Minimum 30 minutes between breakout signals
        try:
            import pandas as pd
            def _tz_naive(ts):
                t = pd.to_datetime(ts)
                return t.tz_localize(None) if t.tzinfo else t
            time_diff_secs = (_tz_naive(current_time) - _tz_naive(last_signal.get('timestamp', current_time))).total_seconds()
            return time_diff_secs > 1800
        except (TypeError, ValueError):
            return True
    
    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data and update breakout indicators."""
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
                
                # Update price history
                if symbol not in self.price_history:
                    self.price_history[symbol] = pd.Series(dtype=float)
                    self.volume_history[symbol] = pd.Series(dtype=float)
                
                # Add new data points
                if 'timestamp' in symbol_data.columns:
                    new_prices = symbol_data.set_index('timestamp')['close']
                    new_volumes = symbol_data.set_index('timestamp').get('volume', pd.Series())
                else:
                    new_prices = symbol_data['close']
                    new_volumes = symbol_data.get('volume', pd.Series())
                
                # Update buffers (keep last 200 points for efficiency)
                self.price_history[symbol] = pd.concat([self.price_history[symbol], new_prices]).tail(200)
                if not new_volumes.empty:
                    self.volume_history[symbol] = pd.concat([self.volume_history[symbol], new_volumes]).tail(200)
                
                # Generate breakout signals if market is open and we have enough data
                if (self.is_market_open() and 
                    len(self.price_history[symbol]) >= self.lookback_period + self.breakout_confirmation_periods):
                    
                    signals = self.detect_breakout_signals(
                        symbol,
                        self.price_history[symbol],
                        self.volume_history.get(symbol, pd.Series())
                    )
                    
                    # Process and publish new signals
                    for signal_data in signals:
                        if self._is_new_breakout_signal(symbol, signal_data):
                            signal = self.create_signal(
                                symbol=symbol,
                                signal_type=signal_data['signal_type'],
                                confidence=signal_data['confidence'],
                                price=signal_data['price'],
                                metadata={
                                    'strategy_type': 'Breakout',
                                    'breakout_type': signal_data['breakout_type'],
                                    'resistance_level': signal_data['resistance_level'],
                                    'support_level': signal_data['support_level'],
                                    'level_strength': signal_data['level_strength'],
                                    'volume_confirmation': signal_data['volume_confirmation'],
                                    'consolidation_confirmed': signal_data['consolidation_confirmed'],
                                    'session_multiplier': signal_data['session_multiplier'],
                                    'lookback_period': self.lookback_period,
                                    'vn30_member': symbol in self.vn30_symbols
                                }
                            )
                            
                            if signal:
                                self.publish_signal(signal)
                                self.last_breakout_time[symbol] = signal_data
                
        except Exception as e:
            self.logger.error(f"Error processing market data in Breakout strategy: {e}")
    
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
        """Generate breakout signals based on current market data."""
        signals = []

        try:
            # Only generate signals during Vietnamese market hours
            if not self.is_market_open():
                return signals
            
            current_session = self.get_current_market_session()
            
            for symbol, prices in self.price_history.items():
                if len(prices) < self.lookback_period + self.breakout_confirmation_periods:
                    continue
                
                # Calculate current support/resistance
                volumes = self.volume_history.get(symbol, pd.Series())
                sr_levels = self.calculate_support_resistance(prices, volumes)
                
                if np.isnan(sr_levels['support']) or np.isnan(sr_levels['resistance']):
                    continue
                
                current_price = prices.iloc[-1]
                
                # Check for immediate breakout opportunities
                breakout_signals = self.detect_breakout_signals(symbol, prices, volumes)
                
                for breakout_signal in breakout_signals:
                    signal = self.create_signal(
                        symbol=symbol,
                        signal_type=breakout_signal['signal_type'],
                        confidence=breakout_signal['confidence'],
                        price=current_price,
                        metadata={
                            'strategy_type': 'Breakout_Scan',
                            'breakout_type': breakout_signal['breakout_type'],
                            'resistance_level': sr_levels['resistance'],
                            'support_level': sr_levels['support'],
                            'level_strength': sr_levels['strength'],
                            'market_session': current_session.value,
                            'vn30_member': symbol in self.vn30_symbols,
                            'consolidation_threshold': self.consolidation_threshold
                        }
                    )
                    
                    if signal:
                        signals.append(signal)
        
        except Exception as e:
            self.logger.error(f"Error generating Breakout signals: {e}")
        
        return signals
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get detailed breakout strategy status information."""
        base_status = self.get_status()
        
        # Add breakout-specific information
        breakout_status = {
            'lookback_period': self.lookback_period,
            'breakout_threshold': f"{self.breakout_threshold*100:.1f}%",
            'volume_multiplier': f"{self.volume_multiplier}x",
            'symbols_tracking': len(self.price_history),
            'signals_cache_size': len(self.breakout_signals_cache),
            'vn30_coverage': sum(1 for symbol in self.price_history.keys() if symbol in self.vn30_symbols),
            'avg_data_points': np.mean([len(prices) for prices in self.price_history.values()]) if self.price_history else 0,
            'recent_breakouts': len(self.last_breakout_time),
            'session_adjustments': {
                'morning_multiplier': self.morning_session_multiplier,
                'afternoon_multiplier': self.afternoon_session_multiplier
            }
        }
        
        base_status.update({'breakout_details': breakout_status})
        return base_status